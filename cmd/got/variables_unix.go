//go:build !windows

package main

import (
	"fmt"

	"gitlab.com/poldi1405/go-ansi"
)

var (
	progressStyle = "single"
	r, l          = "[", "]"
)

func color(content ...any) string {
	return ansi.Blue(fmt.Sprint(content...))
}
