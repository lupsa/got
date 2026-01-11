package got

import (
	"fmt"
	"io"
	"os"

	"github.com/abema/go-mp4"
)

// Range is an inclusive HTTP byte-range: bytes=Start-End
type Range struct {
	Start int64
	End   int64
}

// SidxRanges holds init + all segment ranges.
type SidxRanges struct {
	Init     Range
	Segments []Range
}

// FromReadSeekerAll extracts init + segment ranges from ALL top-level sidx boxes found.
// This is useful when files contain multiple sidx boxes (e.g., one per track).
func FromReadSeekerAll(r io.ReadSeeker) ([]*SidxRanges, error) {
	boxes, err := mp4.ExtractBoxWithPayload(r, nil, mp4.BoxPath{mp4.BoxTypeSidx()})
	if err != nil {
		return nil, err
	}
	if len(boxes) == 0 {
		return nil, fmt.Errorf("no sidx box found")
	}

	out := make([]*SidxRanges, 0, len(boxes))
	for _, b := range boxes {
		sidx, ok := b.Payload.(*mp4.Sidx)
		if !ok {
			return nil, fmt.Errorf("sidx payload has unexpected type %T", b.Payload)
		}

		// b.Info.Offset/Size are uint64.
		sidxOffset := int64(b.Info.Offset)
		sidxSize := int64(b.Info.Size)

		// ISO BMFF: first_offset is relative to the end of the sidx box.
		firstSegAbs := sidxOffset + sidxSize + int64(sidx.GetFirstOffset())

		rng := &SidxRanges{
			Init:     Range{Start: 0, End: firstSegAbs - 1},
			Segments: make([]Range, 0, len(sidx.References)),
		}

		var acc uint64
		for _, ref := range sidx.References {
			start := firstSegAbs + int64(acc)
			end := start + int64(ref.ReferencedSize) - 1
			rng.Segments = append(rng.Segments, Range{Start: start, End: end})
			acc += uint64(ref.ReferencedSize)
		}

		out = append(out, rng)
	}

	return out, nil
}

// FromFile opens a file and extracts init + segment ranges from the first top-level sidx.
func FromFile(path string) (*SidxRanges, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return FromReadSeeker(f)
}

// FromReadSeeker extracts init + segment ranges from the first top-level sidx.
// r must be seekable because ExtractBoxWithPayload requires io.ReadSeeker. :contentReference[oaicite:2]{index=2}
func FromReadSeeker(r io.ReadSeeker) (*SidxRanges, error) {
	// Extract top-level sidx box (with decoded payload). :contentReference[oaicite:3]{index=3}
	boxes, err := mp4.ExtractBoxWithPayload(r, nil, mp4.BoxPath{mp4.BoxTypeSidx()})
	if err != nil {
		return nil, err
	}
	if len(boxes) == 0 {
		return nil, fmt.Errorf("no sidx box found")
	}

	b := boxes[0]
	sidx, ok := b.Payload.(*mp4.Sidx)
	if !ok {
		return nil, fmt.Errorf("sidx payload has unexpected type %T", b.Payload)
	}

	// b.Info.Offset/Size are uint64. :contentReference[oaicite:4]{index=4}
	sidxOffset := int64(b.Info.Offset)
	sidxSize := int64(b.Info.Size)

	// ISO BMFF: first_offset is relative to the end of the sidx box.
	firstSegAbs := sidxOffset + sidxSize + int64(sidx.GetFirstOffset())

	out := &SidxRanges{
		Init: Range{
			Start: 0,
			End:   firstSegAbs - 1,
		},
		Segments: make([]Range, 0, len(sidx.References)),
	}

	var acc uint64
	for _, ref := range sidx.References {
		start := firstSegAbs + int64(acc)
		end := start + int64(ref.ReferencedSize) - 1
		out.Segments = append(out.Segments, Range{Start: start, End: end})
		acc += uint64(ref.ReferencedSize)
	}

	return out, nil
}
