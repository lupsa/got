package got_test

import (
	"testing"

	"github.com/melbahja/got"
)

func TestSidxRangesFromFile(t *testing.T) {
	r, err := got.FromFile("testdata/test_init.mp4")
	if err != nil {
		t.Fatalf("FromFile failed: %v", err)
	}

	// --- basic sanity checks ---
	if r.Init.Start != 0 {
		t.Fatalf("init.Start = %d, want 0", r.Init.Start)
	}
	if r.Init.End <= 0 {
		t.Fatalf("init.End = %d, want > 0", r.Init.End)
	}

	if len(r.Segments) == 0 {
		t.Fatalf("no segments found")
	}

	// --- check first segment ---
	seg0 := r.Segments[0]
	if seg0.Start != r.Init.End+1 {
		t.Fatalf(
			"seg0.Start = %d, want init.End+1 = %d",
			seg0.Start, r.Init.End+1,
		)
	}
	if seg0.End < seg0.Start {
		t.Fatalf(
			"seg0.End < seg0.Start (%d < %d)",
			seg0.End, seg0.Start,
		)
	}

	// --- verify contiguity of all segments ---
	for i := 1; i < len(r.Segments); i++ {
		prev := r.Segments[i-1]
		cur := r.Segments[i]

		if cur.Start != prev.End+1 {
			t.Fatalf(
				"segment %d not contiguous: start=%d, want %d",
				i, cur.Start, prev.End+1,
			)
		}
	}

	// --- optional: log useful info ---
	t.Logf("init range: bytes=%d-%d", r.Init.Start, r.Init.End)
	t.Logf("segments: %d", len(r.Segments))
	t.Logf("first segment: bytes=%d-%d", seg0.Start, seg0.End)
}
