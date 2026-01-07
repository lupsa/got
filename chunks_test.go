package got

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// A tiny deterministic HTTP server that supports byte ranges and serves exactly N bytes.
func newRangeServer(t *testing.T, size int) *httptest.Server {
	t.Helper()

	data := bytes.Repeat([]byte{0xAB}, size)

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only support GET
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			// Non-range: return whole file (200)
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
			return
		}

		// Expect: "bytes=start-end"
		if !strings.HasPrefix(rangeHdr, "bytes=") {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		parts := strings.Split(strings.TrimPrefix(rangeHdr, "bytes="), "-")
		if len(parts) != 2 {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}

		start, err1 := strconv.ParseInt(parts[0], 10, 64)
		end, err2 := strconv.ParseInt(parts[1], 10, 64)
		if err1 != nil || err2 != nil || start < 0 || end < start {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}

		if start >= int64(len(data)) {
			http.Error(w, "range out of bounds", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}

		body := data[start : end+1] // inclusive end

		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	}))
}

func TestChunks_Invariants(t *testing.T) {
	const (
		fileSize  = 1000
		chunkSize = 128
	)

	srv := newRangeServer(t, fileSize)
	defer srv.Close()

	d := &Download{
		URL:       srv.URL,
		ChunkSize: chunkSize,
		// Concurrency doesn't matter for Init() chunk splitting
	}

	if err := d.Init(); err != nil {
		t.Fatalf("Init() error: %v", err)
	}
	if !d.IsRangeable() {
		t.Fatalf("expected server to be rangeable")
	}

	if d.TotalSize() != fileSize {
		t.Fatalf("TotalSize: got %d want %d", d.TotalSize(), fileSize)
	}

	// Expected number of chunks = ceil(fileSize/chunkSize)
	wantChunks := (uint64(fileSize) + chunkSize - 1) / chunkSize
	if uint64(len(d.chunks)) != wantChunks {
		t.Fatalf("chunks count: got %d want %d", len(d.chunks), wantChunks)
	}

	// Invariants:
	// - first start == 0
	// - last end == fileSize-1
	// - contiguous: prev.End+1 == next.Start
	// - each non-last has length chunkSize
	// - last has length <= chunkSize
	if len(d.chunks) == 0 {
		t.Fatalf("expected at least 1 chunk")
	}

	if d.chunks[0].Start != 0 {
		t.Fatalf("first chunk start: got %d want 0", d.chunks[0].Start)
	}

	last := d.chunks[len(d.chunks)-1]
	if last.End != uint64(fileSize-1) {
		t.Fatalf("last chunk end: got %d want %d", last.End, fileSize-1)
	}

	for i := 0; i < len(d.chunks); i++ {
		c := d.chunks[i]
		if c.End < c.Start {
			t.Fatalf("chunk %d invalid: start=%d end=%d", i, c.Start, c.End)
		}

		length := c.End - c.Start + 1 // inclusive end

		if i < len(d.chunks)-1 {
			if length != chunkSize {
				t.Fatalf("chunk %d length: got %d want %d (start=%d end=%d)", i, length, chunkSize, c.Start, c.End)
			}
			next := d.chunks[i+1]
			if c.End+1 != next.Start {
				t.Fatalf("chunk %d -> %d not contiguous: prev.End+1=%d next.Start=%d", i, i+1, c.End+1, next.Start)
			}
		} else {
			// last chunk
			if length == 0 || length > chunkSize {
				t.Fatalf("last chunk length: got %d want 1..%d", length, chunkSize)
			}
		}
	}
}

func TestChunks_ChunkSizeBiggerThanFile(t *testing.T) {
	const fileSize = 1000

	srv := newRangeServer(t, fileSize)
	defer srv.Close()

	d := &Download{
		URL:       srv.URL,
		ChunkSize: 4096, // bigger than file
	}

	if err := d.Init(); err != nil {
		t.Fatalf("Init() error: %v", err)
	}
	if !d.IsRangeable() {
		t.Fatalf("expected server to be rangeable")
	}

	if len(d.chunks) != 1 {
		t.Fatalf("expected 1 chunk when ChunkSize > fileSize; got %d", len(d.chunks))
	}

	if d.chunks[0].Start != 0 {
		t.Fatalf("single chunk start: got %d want 0", d.chunks[0].Start)
	}
	if d.chunks[0].End != uint64(fileSize-1) {
		t.Fatalf("single chunk end: got %d want %d", d.chunks[0].End, fileSize-1)
	}
}

// Optional sanity: ensure the server really behaves like your downloader expects.
func TestRangeServer_Returns206AndCorrectLength(t *testing.T) {
	srv := newRangeServer(t, 1000)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.Header.Set("Range", "bytes=10-19")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http request: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusPartialContent {
		t.Fatalf("status: got %d want 206", res.StatusCode)
	}
	b, _ := io.ReadAll(res.Body)
	if len(b) != 10 {
		t.Fatalf("body length: got %d want 10", len(b))
	}
}
