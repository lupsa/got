package got

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Reuse large copy buffers across goroutines to reduce allocations/GC.
// 1MiB is usually large enough to be syscall-efficient without exploding RAM.
var copyBufPool = sync.Pool{New: func() any { return make([]byte, 1024*1024) }}

type (

	// Info holds downloadable file info.
	Info struct {
		Size      uint64
		Rangeable bool
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
	}

	GotHeader struct {
		Key   string
		Value string
	}
)

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

		return &Info{Size: length, Rangeable: true}, nil
	}

	// Range not supported (server may ignore Range and respond 200).
	// Do NOT read the body here (it could be the whole file). We'll download in Start().
	var size uint64
	if res.ContentLength > 0 {
		size = uint64(res.ContentLength)
	}

	return &Info{Size: size, Rangeable: false}, nil
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
		logWarn("GET %s (Range %s) failed: %v", d.URL, contentRange, err)
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

	_, err = io.CopyBuffer(io.MultiWriter(file, d), res.Body, buf)
	return err
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
