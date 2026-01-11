package got

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
)

// Optional: allow overriding packager path via env var.
// If empty, we use "packager" and rely on PATH.
func (d *Download) packagerPath() string {
	if v := os.Getenv("SHAKA_PACKAGER"); v != "" {
		return v
	}
	return "packager"
}

// Convert d.Keys (kid:key) into Shaka Packager --keys argument.
// Shaka expects hex (no 0x), typically 32 hex chars each.
func (d *Download) shakaKeysArg() (string, error) {
	if len(d.Keys) == 0 {
		return "", fmt.Errorf("no keys provided")
	}
	parts := make([]string, 0, len(d.Keys))
	for i, k := range d.Keys {
		kid := strings.TrimSpace(k.Kid)
		key := strings.TrimSpace(k.Key)
		if kid == "" || key == "" {
			return "", fmt.Errorf("empty kid/key at index %d", i)
		}
		// label is optional; it's convenient when multiple keys exist.
		// Format supported by packager: label=1:key_id=<kid>:key=<key>
		parts = append(parts, fmt.Sprintf("label=%d:key_id=%s:key=%s", i+1, kid, key))
	}
	return strings.Join(parts, ","), nil
}

// Read MP4 top-level box type from the first box header (size + type) of a file.
// Returns "" if we can't read it.
func mp4FirstBoxTypeFromFile(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()

	var hdr [8]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return ""
	}
	return string(hdr[4:8])
}

// Peek MP4 top-level box type from an io.Reader WITHOUT consuming bytes for the caller.
func mp4FirstBoxTypeFromReader(r *bufio.Reader) string {
	hdr, err := r.Peek(8)
	if err != nil || len(hdr) < 8 {
		return ""
	}
	return string(hdr[4:8])
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Close()
}

// STREAMING: decrypt on-the-fly: enc (stdin) -> packager -> dec (stdout).
// Critical fix: if the chunk is not a standalone init/media fragment (e.g. starts with "mfra"),
// we skip packager and just copy bytes through directly.
func (d *Download) decryptStreamWithShaka(ctx context.Context, enc io.Reader, dec io.Writer, stream string) error {
	keysArg, err := d.shakaKeysArg()
	if err != nil {
		return err
	}
	if stream == "" {
		stream = "video"
	}

	br := bufio.NewReader(enc)

	// If this "chunk" isn't a standalone init/media fragment, packager will fail.
	// In that case, keep it as-is and write through.
	bt := mp4FirstBoxTypeFromReader(br)
	switch bt {
	case "ftyp", "moov", "styp", "moof":
		// ok: attempt decryption
	default:
		_, err := io.Copy(dec, br)
		return err
	}

	// output_format=mp4 is important when output is stdout.
	inSpec := fmt.Sprintf("input=/dev/stdin,stream=%s,output=/dev/stdout,output_format=mp4", stream)

	cmd := exec.CommandContext(
		ctx,
		d.packagerPath(),
		inSpec,
		"--enable_raw_key_decryption",
		"--keys", keysArg,
	)

	cmd.Stdin = br

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	_, copyErr := io.Copy(dec, stdout)
	errBytes, _ := io.ReadAll(stderr)
	waitErr := cmd.Wait()

	if copyErr != nil {
		return copyErr
	}
	if waitErr != nil {
		return fmt.Errorf("packager failed: %w\n%s", waitErr, string(errBytes))
	}
	return nil
}

// FILE-BASED: decrypt one file using Shaka Packager raw key decryption.
// Also skips packager for non-fragment chunks (e.g. mfra).
func (d *Download) decryptFileWithShaka(ctx context.Context, inPath, outPath string) error {
	keysArg, err := d.shakaKeysArg()
	if err != nil {
		return err
	}

	bt := mp4FirstBoxTypeFromFile(inPath)
	switch bt {
	case "ftyp", "moov", "styp", "moof":
		// ok
	default:
		return copyFile(inPath, outPath)
	}

	// output_format=mp4 avoids reliance on output filename extension.
	inSpecVideo := fmt.Sprintf("input=%s,stream=video,output=%s,output_format=mp4", inPath, outPath)
	inSpecAudio := fmt.Sprintf("input=%s,stream=audio,output=%s,output_format=mp4", inPath, outPath)

	run := func(inSpec string) ([]byte, error) {
		cmd := exec.CommandContext(
			ctx,
			d.packagerPath(),
			inSpec,
			"--enable_raw_key_decryption",
			"--keys", keysArg,
		)
		return cmd.CombinedOutput()
	}

	out, err := run(inSpecVideo)
	if err == nil {
		return nil
	}
	out2, err2 := run(inSpecAudio)
	if err2 == nil {
		return nil
	}
	return fmt.Errorf("packager failed (video then audio): %w\n%s\n---\n%v\n%s", err, string(out), err2, string(out2))
}

// Small helper so we can avoid double-counting progress when decrypting.
func (d *Download) addDownloadedBytes(n int64) {
	if n > 0 {
		atomic.AddUint64(&d.size, uint64(n))
	}
}

// Temp file helpers (if you still use file-based decrypt somewhere).
func createTempFile(dir, pattern string) (*os.File, error) {
	if dir == "" {
		return os.CreateTemp("", pattern)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return os.CreateTemp(dir, pattern)
}

func tempDirForDownload(destPath string) string {
	base := filepath.Dir(destPath)
	return filepath.Join(base, ".got_tmp")
}
