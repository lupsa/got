//go:build windows

package main

import "fmt"

// Windows doesn't handle the block-style very well
var (
	progressStyle = "single"
	r, l          = "[", "]"
)

func color(content ...any) string {
	return fmt.Sprint(content...)
}
