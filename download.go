package got

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"os/exec"
	  "encoding/binary"
)

// Reuse large copy buffers across goroutines to reduce allocations/GC.
// 1MiB is usually large enough to be syscall-efficient without exploding RAM.
var copyBufPool = sync.Pool{New: func() any { return make([]byte, 1024*1024) }}

type (
	part struct {
	    r    Range
	    path string
	}
	// Info holds downloadable file info.
	Info struct {
		Size      uint64
		Rangeable bool
		SidxAll   []*SidxRanges
	}

	// ProgressFunc to show progress state, called by RunProgress based on interval.
	ProgressFunc func(d *Download)

	// Download holds downloadable file config and infos.
	Download struct {
		Client *http.Client

		Concurrency uint

		URL, Dir, Dest string

		Interval, ChunkSize, MinChunkSize, MaxChunkSize uint64

		Header []GotHeader

		StopProgress bool

		path string

		unsafeName string

		ctx context.Context

		size, lastSize uint64

		info *Info

		chunks []*Chunk

		startedAt time.Time

		Keys []*RawKeys
	}

	GotHeader struct {
		Key   string
		Value string
	}

	RawKeys struct {
		Kid string
		Key string
	}
)

// makeAlignedChunks builds chunk ranges that never split any init/media segment range.
//
// We use the first parsed sidx's ranges. In typical DASH/CMAF flows this is sufficient
// because audio/video segments are often aligned; if they aren't, prefer a manifest-based
// segment list.
func makeAlignedChunks(totalSize, targetChunkSize uint64, sidxAll []*SidxRanges) []*Chunk {
	if totalSize == 0 {
		return nil
	}
	if targetChunkSize == 0 || targetChunkSize >= totalSize {
		c := new(Chunk)
		c.Start = 0
		c.End = totalSize - 1
		return []*Chunk{c}
	}
	if len(sidxAll) == 0 || sidxAll[0] == nil {
		return nil
	}

	sidx := sidxAll[0]
	units := make([]Range, 0, 1+len(sidx.Segments))
	units = append(units, sidx.Init)
	units = append(units, sidx.Segments...)
	if len(units) == 0 {
		return nil
	}

	// Sort + sanitize.
	sort.Slice(units, func(i, j int) bool { return units[i].Start < units[j].Start })
	sanitized := make([]Range, 0, len(units)+1)
	lastEOF := int64(totalSize - 1)
	for _, u := range units {
		if u.End < 0 || u.Start > lastEOF {
			continue
		}
		if u.Start < 0 {
			u.Start = 0
		}
		if u.End > lastEOF {
			u.End = lastEOF
		}
		if u.End < u.Start {
			continue
		}
		sanitized = append(sanitized, u)
	}
	if len(sanitized) == 0 {
		return nil
	}

	// Merge overlapping/adjacent ranges.
	merged := make([]Range, 0, len(sanitized)+1)
	cur := sanitized[0]
	for i := 1; i < len(sanitized); i++ {
		u := sanitized[i]
		if u.Start <= cur.End+1 {
			if u.End > cur.End {
				cur.End = u.End
			}
			continue
		}
		merged = append(merged, cur)
		cur = u
	}
	merged = append(merged, cur)

	// Fill any gaps so we still cover the whole file.
	filled := make([]Range, 0, len(merged)+2)
	pos := int64(0)
	for _, u := range merged {
		if pos < u.Start {
			filled = append(filled, Range{Start: pos, End: u.Start - 1})
		}
		filled = append(filled, u)
		pos = u.End + 1
	}
	if pos <= lastEOF {
		filled = append(filled, Range{Start: pos, End: lastEOF})
	}

	// Group units into chunks without splitting any unit.
	chunks := make([]*Chunk, 0, (totalSize+targetChunkSize-1)/targetChunkSize)
	var curStart, curEnd int64
	curStart = filled[0].Start
	curEnd = filled[0].End
	for i := 1; i < len(filled); i++ {
		u := filled[i]
		// If adding this unit would exceed the target, flush current chunk.
		if curEnd-curStart+1+(u.End-u.Start+1) > int64(targetChunkSize) {
			c := new(Chunk)
			c.Start = uint64(curStart)
			c.End = uint64(curEnd)
			chunks = append(chunks, c)
			curStart = u.Start
			curEnd = u.End
			continue
		}
		curEnd = u.End
	}
	// Flush last chunk.
	last := new(Chunk)
	last.Start = uint64(curStart)
	last.End = uint64(curEnd)
	chunks = append(chunks, last)

	return chunks
}

// GetInfo probes the URL (cheaply) to determine:
//   - total size (when available)
//   - whether the server supports byte-range requests (RFC 7233)
//
// IMPORTANT: This method must NOT download the file to disk.
// The actual download happens in Start() so progress reporting can run.
func (d *Download) GetInfo() (*Info, error) {

	var (
		err error
		req *http.Request
		res *http.Response
	)

	// Probe with a 1-byte range request. If the server supports ranges it should
	// respond with 206 and Content-Range: bytes 0-0/<total>.
	if req, err = NewRequest(d.ctx, "GET", d.URL, append(d.Header, GotHeader{"Range", "bytes=0-0"})); err != nil {
		return &Info{}, err
	}

	if res, err = d.Client.Do(req); err != nil {
		return &Info{}, err
	}
	defer res.Body.Close()

	if res.StatusCode >= 300 {
		return &Info{}, fmt.Errorf("response status code is not ok: %d", res.StatusCode)
	}

	// Capture Content-Disposition filename (non-trusted) if present.
	d.unsafeName = res.Header.Get("content-disposition")

	// Range supported.
	if res.StatusCode == http.StatusPartialContent {
		// Body should be tiny (1 byte). Only drain it when it's actually tiny,
		// so a misbehaving server can't trick us into downloading a full file here.
		if res.ContentLength > 0 && res.ContentLength <= 1024*1024 {
			_, _ = io.Copy(io.Discard, res.Body)
		}

		cr := res.Header.Get("content-range")
		if cr == "" {
			return &Info{}, fmt.Errorf("expected content-range header on 206 response")
		}
		l := strings.Split(cr, "/")
		if len(l) != 2 {
			return &Info{}, fmt.Errorf("invalid content-range header: %s", cr)
		}
		length, perr := strconv.ParseUint(l[1], 10, 64)
		if perr != nil {
			return &Info{}, fmt.Errorf("invalid content-range total size: %s", cr)
		}

		info := &Info{Size: length, Rangeable: true}
		// Best-effort sidx range extraction (does not affect errors).
		d.tryFetchSidxRanges(d.ctx, info)
		return info, nil
	}

	// Range not supported (server may ignore Range and respond 200).
	// Do NOT read the body here (it could be the whole file). We'll download in Start().
	var size uint64
	if res.ContentLength > 0 {
		size = uint64(res.ContentLength)
	}

	info := &Info{Size: size, Rangeable: false}
	return info, nil
}

// tryFetchSidxRanges is a best-effort probe that downloads a small prefix of the file and
// attempts to extract all top-level sidx ranges. It never errors GetInfo().
func (d *Download) tryFetchSidxRanges(ctx context.Context, info *Info) {
	if info == nil || !info.Rangeable || info.Size == 0 {
		return
	}

	// Many fragmented MP4/DASH files place sidx near the start. We only fetch a small prefix
	// to keep GetInfo() cheap.
	const probeMax = 4 * 1024 * 1024 // 4 MiB
	end := int64(probeMax - 1)
	if info.Size > 0 {
		last := int64(info.Size - 1)
		if end > last {
			end = last
		}
	}
	if end < 0 {
		return
	}

	req, err := NewRequest(ctx, "GET", d.URL, append(d.Header, GotHeader{"Range", fmt.Sprintf("bytes=0-%d", end)}))
	if err != nil {
		return
	}

	res, err := d.Client.Do(req)
	if err != nil {
		return
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusPartialContent {
		return
	}

	buf, err := io.ReadAll(io.LimitReader(res.Body, end+1))
	if err != nil || len(buf) == 0 {
		return
	}

	// Parse sidx from the probe buffer (must be seekable).
	sidxAll, err := FromReadSeekerAll(bytes.NewReader(buf))
	if err != nil || len(sidxAll) == 0 {
		return
	}

	info.SidxAll = sidxAll
}

// Init set defaults and split file into chunks and gets Info,
// you should call Init before Start
func (d *Download) Init() (err error) {

	// Set start time.
	d.startedAt = time.Now()

	// Set default client.
	if d.Client == nil {
		d.Client = DefaultClient
	}

	// Set default context.
	if d.ctx == nil {
		d.ctx = context.Background()
	}

	// Reset progress counters in case Download is reused.
	atomic.StoreUint64(&d.size, 0)
	atomic.StoreUint64(&d.lastSize, 0)

	// Get URL info and partial content support state (no disk IO here).
	if d.info, err = d.GetInfo(); err != nil {
		return err
	}

	// If the server doesn't support ranges, we'll download in Start() with a single GET.
	if d.info.Rangeable == false {
		return nil
	}

	// Set concurrency default.
	if d.Concurrency == 0 {
		d.Concurrency = getDefaultConcurrency()
	}

	// Set default chunk size
	if d.ChunkSize == 0 {
		d.ChunkSize = getDefaultChunkSize(d.info.Size, d.MinChunkSize, d.MaxChunkSize, uint64(d.Concurrency))
	}

	// If user provided a chunk size bigger than (or equal to) the file,
	// force a single chunk download.
	if d.ChunkSize >= d.info.Size {
		d.ChunkSize = d.info.Size
	}

	// If we have sidx-derived segment ranges, prefer chunk boundaries that never
	// split any init/media segment.
	if aligned := makeAlignedChunks(d.info.Size, d.ChunkSize, d.info.SidxAll); len(aligned) > 0 {
		d.chunks = aligned
		return nil
	}

	chunksLen := (d.info.Size + d.ChunkSize - 1) / d.ChunkSize

	if chunksLen == 0 {
		chunksLen = 1
	}

	d.chunks = make([]*Chunk, 0, chunksLen)

	for i := uint64(0); i < chunksLen; i++ {
		chunk := new(Chunk)
		d.chunks = append(d.chunks, chunk)

		chunk.Start = d.ChunkSize * i
		chunk.End = chunk.Start + d.ChunkSize - 1

		if chunk.End >= d.info.Size || i == chunksLen-1 {
			chunk.End = d.info.Size - 1
			break
		}
	}

	return nil
}

// Start downloads the file chunks, and merges them.
// Must be called only after init
func (d *Download) Start() (err error) {
	// Single-stream download fallback.
	if d.info.Rangeable == false {
		return d.downloadSingle(d.ctx)
	}

	filePath := d.Path()
	if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return err
	}

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Allocate the file completely so that we can write concurrently.
	if err := file.Truncate(int64(d.TotalSize())); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(d.ctx)
	defer cancel()

	// Download chunks.
	errs := make(chan error, 1)
	go d.dl(ctx, cancel, file, errs)

	select {
	case err = <-errs:
	case <-d.ctx.Done():
		err = d.ctx.Err()
	}

	return err
}

// StartToTempParts downloads to a temp directory as:
//
//	_init.mp4 + 0000.mp4s, 0001.mp4s, ... (when sidx exists)
//
// or just numbered parts (fallback).
func (d *Download) StartToTempParts(ctx context.Context) (string, []string, error) {
	// Ensure init was called and chunk plan computed.
	// (Init() is required before Start() too, but callers might not have done it.)
	if d.info == nil || (d.IsRangeable() && len(d.chunks) == 0 && (d.info.SidxAll == nil || len(d.info.SidxAll) == 0)) {
		if ctx != nil {
			d.ctx = ctx
		}
		if err := d.Init(); err != nil {
			return "", nil, err
		}
	}

	// For non-range servers, you can't chunk-dump; fall back or error.
	if d.info != nil && d.info.Rangeable == false {
		return "", nil, fmt.Errorf("server does not support ranges; cannot dump parts")
	}

	return d.DownloadPartsIntoFinalDir(ctx)
}

// RunProgress runs ProgressFunc based on Interval and updates lastSize.
func (d *Download) RunProgress(fn ProgressFunc) {

	// Always emit final progress update
	defer fn(d)

	// Set default interval.
	if d.Interval == 0 {
		d.Interval = uint64(150 / runtime.NumCPU())
	}

	ticker := time.NewTicker(time.Duration(d.Interval) * time.Millisecond)
	defer ticker.Stop()

	// Show initial progress file for tiny files
	fn(d)
	atomic.StoreUint64(&d.lastSize, atomic.LoadUint64(&d.size))

	for {
		select {
		case <-d.ctx.Done():
			return

		case <-ticker.C:
			if d.StopProgress {
				return
			}

			fn(d)
			atomic.StoreUint64(&d.lastSize, atomic.LoadUint64(&d.size))
		}
	}
}

// Context returns download context.
func (d *Download) Context() context.Context {
	return d.ctx
}

// TotalSize returns file total size (0 if unknown).
func (d *Download) TotalSize() uint64 {
	return d.info.Size
}

// Size returns downloaded size.
func (d *Download) Size() uint64 {
	return atomic.LoadUint64(&d.size)
}

// Speed returns download speed.
func (d *Download) Speed() uint64 {
	if d.Interval == 0 {
		return 0
	}
	return (atomic.LoadUint64(&d.size) - atomic.LoadUint64(&d.lastSize)) / d.Interval * 1000
}

// AvgSpeed returns average download speed.
func (d *Download) AvgSpeed() uint64 {

	if totalMills := d.TotalCost().Milliseconds(); totalMills > 0 {
		return uint64(atomic.LoadUint64(&d.size) / uint64(totalMills) * 1000)
	}

	return 0
}

// TotalCost returns download duration.
func (d *Download) TotalCost() time.Duration {
	return time.Since(d.startedAt)
}

// Write updates progress size.
func (d *Download) Write(b []byte) (int, error) {
	n := len(b)
	atomic.AddUint64(&d.size, uint64(n))
	return n, nil
}

// IsRangeable returns file server partial content support state.
func (d *Download) IsRangeable() bool {
	return d.info.Rangeable
}

// Download chunks
func (d *Download) dl(ctx context.Context, cancel context.CancelFunc, dest io.WriterAt, errC chan error) {
	var (
		wg   sync.WaitGroup
		max  = make(chan struct{}, d.Concurrency)
		once sync.Once
	)

	for i := 0; i < len(d.chunks); i++ {
		max <- struct{}{}
		wg.Add(1)

		go func(i int) {
			defer wg.Done()
			defer func() { <-max }()

			if err := d.DownloadChunk(ctx,
				d.chunks[i],
				&OffsetWriter{dest, int64(d.chunks[i].Start)},
			); err != nil {
				once.Do(func() {
					// Stop the rest of the chunk requests ASAP.
					cancel()
					errC <- err
				})
				return
			}
		}(i)
	}

	wg.Wait()
	once.Do(func() { errC <- nil })
}

// Return constant path which will not change once the download starts
func (d *Download) Path() string {

	// Set the default path
	if d.path == "" {

		d.path = GetFilename(d.URL) // default case
		if d.Dest != "" {
			d.path = d.Dest
		} else if d.unsafeName != "" {
			if path := getNameFromHeader(d.unsafeName); path != "" {
				d.path = path
			}
		}
		d.path = filepath.Join(d.Dir, d.path)
	}

	return d.path
}

func (d *Download) addProgress(n int) {
	atomic.AddUint64(&d.size, uint64(n))
}

// DownloadChunk downloads a file chunk.
func (d *Download) DownloadChunk(ctx context.Context, c *Chunk, dest io.Writer) error {

	var (
		err error
		req *http.Request
		res *http.Response
	)

	if req, err = NewRequest(ctx, "GET", d.URL, d.Header); err != nil {
		return err
	}

	contentRange := fmt.Sprintf("bytes=%d-%d", c.Start, c.End)
	req.Header.Set("Range", contentRange)

	if res, err = d.Client.Do(req); err != nil {
		//logWarn("GET %s (Range %s) failed: %v", d.URL, contentRange, err)
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("expected 206 Partial Content, got %d", res.StatusCode)
	}

	// Verify the length
	expectedLen := int64(c.End - c.Start + 1)
	if res.ContentLength != -1 && res.ContentLength != expectedLen {
		return fmt.Errorf(
			"Range request returned invalid Content-Length: %d however the range was: %s",
			res.ContentLength, contentRange,
		)
	}

	buf := copyBufPool.Get().([]byte)
	defer copyBufPool.Put(buf)

	// Copy exactly the requested number of bytes. This protects us against a server
	// that (incorrectly) sends more data than the requested range.
	limited := io.LimitReader(res.Body, expectedLen)

		// Count progress by also writing the bytes into d (which only increments the counter).
		n, err := io.CopyBuffer(io.MultiWriter(dest, d), limited, buf)
		if err != nil {
			return err
		}
		if n != expectedLen {
			return fmt.Errorf("range request returned %d bytes, expected %d", n, expectedLen)
		}

	

	return nil

}

func (d *Download) downloadSingle(ctx context.Context) error {
	filePath := d.Path()
	if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return err
	}

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	req, err := NewRequest(ctx, "GET", d.URL, d.Header)
	if err != nil {
		return err
	}

	res, err := d.Client.Do(req)
	if err != nil {
		logWarn("GET %s failed: %v", d.URL, err)
		return err
	}
	defer res.Body.Close()

	if res.StatusCode >= 300 {
		return fmt.Errorf("response status code is not ok: %d", res.StatusCode)
	}

	buf := copyBufPool.Get().([]byte)
	defer copyBufPool.Put(buf)

	if d.Keys != nil {
		return fmt.Errorf("Implement decryption of parts")
	} else {
		_, err = io.CopyBuffer(io.MultiWriter(file, d), res.Body, buf)
		return err
	}

}

// DownloadPartsIntoFinalDir downloads init+segments (preferred via sidx) or falls back to d.chunks
// into a temp folder next to the final output path, then renames it to "<final>.parts".
//
// Returns: partsDir (final), ordered file list (init first if present).
func (d *Download) DownloadPartsIntoFinalDir(ctx context.Context) (string, []string, error) {
	// Ensure Init() has run (Init fills d.info and d.chunks).
	if d.info == nil {
		if ctx != nil {
			d.ctx = ctx
		}
		if err := d.Init(); err != nil {
			return "", nil, err
		}
	}

	// We can only do chunk/segment style when rangeable.
	if d.info == nil || !d.info.Rangeable {
		return "", nil, fmt.Errorf("server does not support ranges; cannot download parts")
	}

	if ctx == nil {
		ctx = d.ctx
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	finalPath := d.Path()
	tmpDir := finalPath + ".parts.tmp"
	finalDir := finalPath + ".parts"

	_ = os.RemoveAll(tmpDir)
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return "", nil, err
	}

	// Build the plan: ordered list of parts to write.

	var parts []part

	// Prefer sidx init+segments if present.
	if d.info.SidxAll != nil && len(d.info.SidxAll) > 0 && d.info.SidxAll[0] != nil {
		sidx := d.info.SidxAll[0]

		parts = append(parts, part{
			r:    sidx.Init,
			path: filepath.Join(tmpDir, "_init.mp4"),
		})

		width := 4
		if n := len(sidx.Segments); n >= 10000 {
			width = 5
		} else if n >= 100000 {
			width = 6
		}

		for i, seg := range sidx.Segments {
			name := fmt.Sprintf("%0*d.mp4s", width, i)
			parts = append(parts, part{
				r:    seg,
				path: filepath.Join(tmpDir, name),
			})
		}
	} else {
		// Fallback: use computed d.chunks.
		// WARNING: this can split MP4 boxes; streaming decrypt+merge is unreliable without real segment boundaries.
		width := 4
		if n := len(d.chunks); n >= 10000 {
			width = 5
		} else if n >= 100000 {
			width = 6
		}

		for i, ch := range d.chunks {
			name := fmt.Sprintf("%0*d.mp4s", width, i)
			parts = append(parts, part{
				r:    Range{Start: int64(ch.Start), End: int64(ch.End)},
				path: filepath.Join(tmpDir, name),
			})
		}
	}

	ready := make([]chan struct{}, len(parts))
	for i := range ready {
		ready[i] = make(chan struct{})
	}

	var (
		once sync.Once
		errC = make(chan error, 1)
	)
	fail := func(err error) {
		once.Do(func() {
			// unblock anyone waiting on ready[i]
			for i := range ready {
				select {
				case <-ready[i]:
				default:
					close(ready[i])
				}
			}
			cancel()
			errC <- err
		})
	}

	if len(parts) < 2 {
		_ = os.RemoveAll(tmpDir)
		return "", nil, fmt.Errorf("need init + at least one segment")
	}

	keysArg, err := d.shakaKeysArg()
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return "", nil, err
	}

	// Choose output name:
	outPath := finalPath // + ".clear.mp4"
	streamType := "video" // change to "audio" for audio-only

	decDone := startDecryptAndMergeWhileDownloading(
		runCtx,
		d.packagerPath(),
		keysArg,
		streamType,
		tmpDir,
		parts,
		ready,
		outPath,
		8,
		fail,
	)

	// ------------------------------------------------------------
	// Existing: Download concurrently; CHANGE to close ready[i]
	// ------------------------------------------------------------
	var (
		wg  sync.WaitGroup
		sem = make(chan struct{}, d.Concurrency)
	)

	for i := range parts {
		i := i
		sem <- struct{}{}
		wg.Add(1)

		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			p := parts[i]
			f, err := os.Create(p.path)
			if err != nil {
				fail(err)
				return
			}

			ch := &Chunk{Start: uint64(p.r.Start), End: uint64(p.r.End)}
			if err := d.DownloadChunk(runCtx, ch, f); err != nil {
				_ = f.Close()
				fail(err)
				return
			}
			if err := f.Close(); err != nil {
				fail(err)
				return
			}

			close(ready[i])
		}()
	}

	wg.Wait()

	select {
	case derr := <-decDone:
		if derr != nil {
			_ = os.RemoveAll(tmpDir)
			return "", nil, derr
		}
	case err := <-errC:
		_ = os.RemoveAll(tmpDir)
		return "", nil, err
	default:
		// If downloads finished but decrypt still running, wait:
		select {
		case derr := <-decDone:
			if derr != nil {
				_ = os.RemoveAll(tmpDir)
				return "", nil, derr
			}
		case err := <-errC:
			_ = os.RemoveAll(tmpDir)
			return "", nil, err
		}
	}

	// If any downloader error happened:
	select {
	case err := <-errC:
		_ = os.RemoveAll(tmpDir)
		return "", nil, err
	default:
	}

	// ------------------------------------------------------------
	// CHANGE HERE: when streaming, don't rename tmpDir -> finalDir.
	// Instead, cleanup tmpDir and return final output file.
	// ------------------------------------------------------------
	_ = os.RemoveAll(tmpDir)
	_ = os.RemoveAll(finalDir) // keep behavior if you want; optional

	return outPath, []string{outPath}, nil
}

type decResult struct {
	idx     int
	fragOut string
	err     error
}

// startDecryptAndMergeWhileDownloading runs concurrently with the downloader.
//
// It uses Shaka Packager as a "decrypt engine" on small, parseable MP4 inputs.
// IMPORTANT: Shaka Packager will not emit output for init-only MP4 (no samples), so we:
//
//  1) wait for init + first segment
//  2) build combined0 = init + seg0
//  3) decrypt combined0 -> dec0.mp4
//  4) extract init_clear (everything before first moof) + frag0 (from first moof onward)
//  5) open final output, write init_clear once, append frag0
//  6) for seg1..N: decrypt (init+segN) -> decN.mp4 -> extract fragN (moof+mdat only)
//  7) merge goroutine appends fragN strictly in order
//
// parts: ordered list where parts[0] is init and parts[1:] are segments in order.
// ready[i]: must be closed when parts[i].path is fully downloaded.
// fail(err): cancels outer context and unblocks waiters (your helper).
func startDecryptAndMergeWhileDownloading(
	ctx context.Context,
	packagerPath, keysArg, streamType, tmpDir string,
	parts []part,
	ready []chan struct{},
	outPath string,
	workers int,
	fail func(error),
) <-chan error {
	done := make(chan error, 1)

	if workers <= 0 {
		workers = 4
	}
	if streamType == "" {
		streamType = "video"
	}

	type decResult struct {
		idx     int    // segment index (0 == parts[1], 1 == parts[2], ...)
		fragOut string // decrypted fragment-only file (starts at moof)
		err     error
	}

	// Helper: decrypt init+seg into decrypted mp4 (ftyp/moov/moof/mdat...)
	shakaDecryptCombined := func(segIdx int, initPath, segPath string) (decPath string, err error) {
		combined := filepath.Join(tmpDir, fmt.Sprintf("comb_%06d.mp4", segIdx))
		if err := concatTwoFiles(combined, initPath, segPath); err != nil {
			return "", err
		}

		decCombined := filepath.Join(tmpDir, fmt.Sprintf("dec_%06d.mp4", segIdx))
		if err := shakaDecryptFile(ctx, packagerPath, keysArg, streamType, tmpDir, combined, decCombined); err != nil {
			_ = os.Remove(combined)
			return "", err
		}
		_ = os.Remove(combined)
		return decCombined, nil
	}

	// Helper: extract init (everything before first top-level moof) to outPath.
	// NOTE: This version assumes 32-bit box sizes for top-level boxes (typical CMAF).
	extractInitUpToMoof := func(inPath, outPath string) error {
		in, err := os.Open(inPath)
		if err != nil {
			return err
		}
		defer in.Close()

		out, err := os.Create(outPath)
		if err != nil {
			return err
		}
		defer out.Close()

		for {
			var hdr [8]byte
			_, err := io.ReadFull(in, hdr[:])
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return fmt.Errorf("no moof found in %s", inPath)
			}
			if err != nil {
				return err
			}

			size32 := binary.BigEndian.Uint32(hdr[0:4])
			typ := string(hdr[4:8])

			if size32 < 8 || size32 == 1 {
				return fmt.Errorf("unsupported/invalid top-level box size=%d type=%q in %s (need largesize support?)", size32, typ, inPath)
			}

			// Stop before moof (init is everything before the first moof).
			if typ == "moof" {
				return nil
			}

			// Write header + payload.
			if _, err := out.Write(hdr[:]); err != nil {
				return err
			}
			payload := int64(size32) - 8
			if payload > 0 {
				if _, err := io.CopyN(out, in, payload); err != nil {
					return err
				}
			}
		}
	}

	go func() {
		// Basic validation.
		if len(parts) < 2 {
			err := fmt.Errorf("need init + at least one segment")
			fail(err)
			done <- err
			return
		}
		if len(ready) != len(parts) {
			err := fmt.Errorf("ready length (%d) != parts length (%d)", len(ready), len(parts))
			fail(err)
			done <- err
			return
		}

		initPath := parts[0].path

		// 1) Wait for init and seg0 to be downloaded.
		select {
		case <-ctx.Done():
			done <- ctx.Err()
			return
		case <-ready[0]:
		}
		select {
		case <-ctx.Done():
			done <- ctx.Err()
			return
		case <-ready[1]:
		}

		// 2) Decrypt combined0.
		seg0Path := parts[1].path
		dec0, err := shakaDecryptCombined(0, initPath, seg0Path)
		if err != nil {
			err = fmt.Errorf("decrypt combined seg0: %w", err)
			fail(err)
			done <- err
			return
		}
		defer os.Remove(dec0)

		// 3) Extract init_clear and frag0 from dec0.
		initClear := filepath.Join(tmpDir, "init_clear.mp4")
		if err := extractInitUpToMoof(dec0, initClear); err != nil {
			err = fmt.Errorf("extract init_clear: %w", err)
			fail(err)
			done <- err
			return
		}

		frag0 := filepath.Join(tmpDir, "frag_000000.m4s")
		if err := stripToFirstMoof(dec0, frag0); err != nil {
			err = fmt.Errorf("extract frag0: %w", err)
			fail(err)
			done <- err
			return
		}

		// 4) Open output and write init once + frag0.
		if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
			fail(err)
			done <- err
			return
		}

		outF, err := os.Create(outPath)
		if err != nil {
			fail(err)
			done <- err
			return
		}
		defer outF.Close()

		if err := copyFileToWriter(outF, initClear); err != nil {
			err = fmt.Errorf("write init_clear: %w", err)
			fail(err)
			done <- err
			return
		}
		if err := appendFile(outF, frag0); err != nil {
			err = fmt.Errorf("append frag0: %w", err)
			fail(err)
			done <- err
			return
		}
		_ = os.Remove(frag0)

		// 5) Decrypt remaining segments concurrently (segIdx 1..N-1) and merge in order.
		// Segment index here: 0 means parts[1], 1 means parts[2], etc.
		// We already processed segIdx=0, so next expected is 1.
		next := 1
		pending := make(map[int]string, 64)

		jobs := make(chan int, workers*2)           // segIdx (>=1)
		results := make(chan decResult, workers*2)  // out-of-order results

		// Feed jobs when each segment download completes.
		go func() {
			defer close(jobs)
			for segIdx := 1; segIdx < len(parts)-1; segIdx++ {
				partIndex := segIdx + 1 // segIdx=1 -> parts[2]
				select {
				case <-ctx.Done():
					return
				case <-ready[partIndex]:
				}
				select {
				case <-ctx.Done():
					return
				case jobs <- segIdx:
				}
			}
		}()

		// Workers.
		var wg sync.WaitGroup
		wg.Add(workers)
		for w := 0; w < workers; w++ {
			go func() {
				defer wg.Done()
				for segIdx := range jobs {
					segPath := parts[segIdx+1].path // segIdx=1 -> parts[2]

					decPath, err := shakaDecryptCombined(segIdx, initPath, segPath)
					if err != nil {
						results <- decResult{idx: segIdx, err: fmt.Errorf("decrypt seg %d: %w", segIdx, err)}
						continue
					}

					fragOnly := filepath.Join(tmpDir, fmt.Sprintf("frag_%06d.m4s", segIdx))
					if err := stripToFirstMoof(decPath, fragOnly); err != nil {
						_ = os.Remove(decPath)
						results <- decResult{idx: segIdx, err: fmt.Errorf("strip moof seg %d: %w", segIdx, err)}
						continue
					}
					_ = os.Remove(decPath)

					results <- decResult{idx: segIdx, fragOut: fragOnly}
				}
			}()
		}

		go func() {
			wg.Wait()
			close(results)
		}()

		// Merge results in order.
		for {
			select {
			case <-ctx.Done():
				done <- ctx.Err()
				return

			case r, ok := <-results:
				if !ok {
					// flush remaining pending in order
					for {
						p, ok := pending[next]
						if !ok {
							break
						}
						if err := appendFile(outF, p); err != nil {
							err = fmt.Errorf("append frag %d: %w", next, err)
							fail(err)
							done <- err
							return
						}
						_ = os.Remove(p)
						delete(pending, next)
						next++
					}

					// Validate complete
					if next != len(parts)-1 {
						err := fmt.Errorf("merge incomplete: wrote %d/%d segments", next, len(parts)-1)
						fail(err)
						done <- err
						return
					}

					done <- nil
					return
				}

				if r.err != nil {
					fail(r.err)
					done <- r.err
					return
				}

				pending[r.idx] = r.fragOut

				// Flush contiguous
				for {
					p, ok := pending[next]
					if !ok {
						break
					}
					if err := appendFile(outF, p); err != nil {
						err = fmt.Errorf("append frag %d: %w", next, err)
						fail(err)
						done <- err
						return
					}
					_ = os.Remove(p)
					delete(pending, next)
					next++
				}
			}
		}
	}()

	return done
}


func shakaDecryptFile(ctx context.Context, packagerPath, keysArg, streamType, tempDir, inPath, outPath string) error {
	args := []string{
		fmt.Sprintf("in=%s,stream=%s,output=%s", inPath, streamType, outPath),
		"--enable_raw_key_decryption",
		"--keys", keysArg,
		"--temp_dir", tempDir,
		"--v=0",
		//"--quiet",
	}
	cmd := exec.CommandContext(ctx, packagerPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func copyFileToWriter(dst io.Writer, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(dst, f)
	return err
}

func appendFile(dst *os.File, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	_, err = io.Copy(dst, f)
	_ = f.Close()
	return err
}

func concatTwoFiles(outPath, initPath, segPath string) error {
	out, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer out.Close()

	if err := copyFileToWriter(out, initPath); err != nil {
		return err
	}
	if err := copyFileToWriter(out, segPath); err != nil {
		return err
	}
	return nil
}

func stripToFirstMoof(inPath, outPath string) error {
	in, err := os.Open(inPath)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer out.Close()

	var hdr [8]byte
	var offset int64 = 0

	for {
		_, err := io.ReadFull(in, hdr[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return fmt.Errorf("no moof found in %s", inPath)
		}
		if err != nil {
			return err
		}

		size32 := binary.BigEndian.Uint32(hdr[0:4])
		typ := string(hdr[4:8])

		headerSize := int64(8)
		boxSize := int64(size32)

		if size32 == 1 {
			var ext [8]byte
			if _, err := io.ReadFull(in, ext[:]); err != nil {
				return err
			}
			boxSize = int64(binary.BigEndian.Uint64(ext[:]))
			headerSize = 16
		}
		if boxSize < headerSize {
			return fmt.Errorf("invalid box size %d at offset %d in %s", boxSize, offset, inPath)
		}

		// if moof, seek back to start of box and copy remainder
		if typ == "moof" {
			if _, err := in.Seek(offset, io.SeekStart); err != nil {
				return err
			}
			_, err := io.Copy(out, in)
			return err
		}

		offset += boxSize
		if _, err := in.Seek(offset, io.SeekStart); err != nil {
			return err
		}
	}
}

func extractInitUpToMoof(inPath, outPath string) error {
	in, err := os.Open(inPath)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer out.Close()

	var hdr [8]byte
	for {
		_, err := io.ReadFull(in, hdr[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return fmt.Errorf("no moof found in %s", inPath)
		}
		if err != nil {
			return err
		}

		size32 := binary.BigEndian.Uint32(hdr[0:4])
		typ := string(hdr[4:8])

		headerSize := int64(8)
		boxSize := int64(size32)

		if size32 == 1 {
			var ext [8]byte
			if _, err := io.ReadFull(in, ext[:]); err != nil {
				return err
			}
			boxSize = int64(binary.BigEndian.Uint64(ext[:]))
			headerSize = 16
		}
		if boxSize < headerSize {
			return fmt.Errorf("invalid box size %d in %s", boxSize, inPath)
		}

		// If this is moof, stop; init is everything before moof.
		if typ == "moof" {
			return nil
		}

		// Write the whole box (header already read) to out.
		if _, err := out.Write(hdr[:]); err != nil {
			return err
		}
		if headerSize == 16 {
			// We already consumed the largesize bytes into ext in memory above,
			// but did not write them. So we must re-read them or track them.
			// Easiest: seek back 8 and read+write full 16 header.
			if _, err := in.Seek(-8, io.SeekCurrent); err != nil {
				return err
			}
			var fullHdr [16]byte
			if _, err := io.ReadFull(in, fullHdr[:]); err != nil {
				return err
			}
			if _, err := out.Write(fullHdr[:]); err != nil {
				return err
			}
			// We wrote hdr twice in this branch, so adjust:
			// Simpler alternative: DON'T use this largesize branch unless needed.
			// (Most CMAF top-level boxes are 32-bit sized.)
			return fmt.Errorf("largesize init extraction not implemented cleanly; add proper handling if needed")
		}

		// Copy the payload.
		payload := boxSize - headerSize
		if payload > 0 {
			if _, err := io.CopyN(out, in, payload); err != nil {
				return err
			}
		}
	}
}



// NewDownload returns new *Download with context.
func NewDownload(ctx context.Context, URL, dest string) *Download {
	return &Download{
		ctx:    ctx,
		URL:    URL,
		Dest:   dest,
		Client: DefaultClient,
	}
}

func getDefaultConcurrency() uint {

	c := uint(runtime.NumCPU() * 3)

	// Set default max concurrency to 20.
	if c > 64 {
		c = 64
	}

	// Set default min concurrency to 4.
	if c <= 2 {
		c = 4
	}

	return c
}

func getDefaultChunkSize(totalSize, min, max, concurrency uint64) uint64 {

	cs := totalSize / concurrency

	// if chunk size >= 102400000 bytes set default to (ChunkSize / 2)
	if cs >= 102400000 {
		cs = cs / 2
	}

	// Set default min chunk size to 2m, or file size / 2
	if min == 0 {

		min = 2097152

		if min >= totalSize {
			min = totalSize / 2
		}
	}

	// if Chunk size < Min size set chunk size to min.
	if cs < min {
		cs = min
	}

	// Change ChunkSize if MaxChunkSize are set and ChunkSize > Max size
	if max > 0 && cs > max {
		cs = max
	}

	// When chunk size > total file size, divide chunk / 2
	if cs >= totalSize {
		cs = totalSize / 2
	}

	return cs
}
