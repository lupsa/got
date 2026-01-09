package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/dustin/go-humanize"
	"github.com/melbahja/got"
	"github.com/urfave/cli/v2"
	"gitlab.com/poldi1405/go-ansi"
	"gitlab.com/poldi1405/go-indicators/progress"
	"golang.org/x/term"
)

var version string

var HeaderSlice []got.GotHeader

func main() {

	// New context.
	ctx, cancel := context.WithCancel(context.Background())

	interruptChan := make(chan os.Signal, 1)

	signal.Notify(interruptChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-interruptChan
		cancel()
		signal.Stop(interruptChan)
	}()

	// CLI app.
	app := &cli.App{
		Name:  "Got",
		Usage: "The fastest http downloader.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "output",
				Usage:   "Download `path`, if dir passed the path witll be `dir + output`.",
				Aliases: []string{"o"},
			},
			&cli.StringFlag{
				Name:    "dir",
				Usage:   "Save downloaded file to a `directory`.",
				Aliases: []string{"d"},
			},
			&cli.StringFlag{
				Name:    "file",
				Usage:   "Batch download from list of urls in a `file`.",
				Aliases: []string{"bf", "f"},
			},
			&cli.Uint64Flag{
				Name:    "size",
				Usage:   "Chunk size in `bytes` to split the file.",
				Aliases: []string{"chunk"},
			},
			&cli.UintFlag{
				Name:    "concurrency",
				Usage:   "Chunks that will be downloaded concurrently.",
				Aliases: []string{"c"},
			},
			&cli.StringSliceFlag{
				Name:    "header",
				Usage:   `Set these HTTP-Headers on the requests. The format has to be: -H "Key: Value"`,
				Aliases: []string{"H"},
			},
			&cli.StringFlag{
				Name:    "agent",
				Usage:   `Set user agent for got HTTP requests.`,
				Aliases: []string{"u"},
			},
		},
		Version: version,
		Authors: []*cli.Author{
			{
				Name:  "Mohamed Elbahja and Contributors",
				Email: "bm9qdW5r@gmail.com",
			},
		},
		Action: func(c *cli.Context) error {
			return run(ctx, c)
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, c *cli.Context) error {

	var (
		g *got.Got           = got.NewWithContext(ctx)
		p *progress.Progress = new(progress.Progress)
	)

	// Set progress style.
	p.SetStyle(progressStyle)

	// Progress func.
	g.ProgressFunc = func(d *got.Download) {
		width := getWidth()

		// 55 is just an estimation of the text shown with the progress.
		p.Width = width - 55
		if p.Width < 0 {
			p.Width = 0
		}

		size := d.Size()
		total := d.TotalSize()
		speed := d.Speed()

		// Unknown total size (no Content-Length / no range support). Don't pretend it's 100%.
		if total == 0 {
			fmt.Printf(
				" %s @ %s/s%s\r",
				humanize.Bytes(size),
				humanize.Bytes(speed),
				ansi.ClearRight(),
			)
			return
		}

		perc, err := progress.GetPercentage(float64(size), float64(total))
		if err != nil {
			perc = 0
		}
		if perc > 100 {
			perc = 100
		}

		var bar string
		if width <= 46 || p.Width == 0 {
			bar = ""
		} else {
			bar = r + color(p.GetBar(perc, 100)) + l
		}

		fmt.Printf(
			" %6.2f%% %s %s/%s @ %s/s%s\r",
			perc,
			bar,
			humanize.Bytes(size),
			humanize.Bytes(total),
			humanize.Bytes(speed),
			ansi.ClearRight(),
		)
	}

	info, err := os.Stdin.Stat()

	if err != nil {
		return err
	}

	// Ensure output directory exists (MkdirAll is idempotent).
	if dir := c.String("dir"); dir != "" {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return err
		}
	}

	// Set default user agent.
	if c.String("agent") != "" {
		got.UserAgent = c.String("agent")
	}

	// Parse headers BEFORE any downloads so they apply to stdin/file batches too.
	if c.StringSlice("header") != nil {
		header := c.StringSlice("header")

		for _, h := range header {
			split := strings.SplitN(h, ":", 2)
			if len(split) == 1 {
				return errors.New("malformatted header " + h)
			}
			HeaderSlice = append(HeaderSlice, got.GotHeader{Key: split[0], Value: strings.TrimSpace(split[1])})
		}
	}

	// Piped stdin
	if info.Mode()&os.ModeNamedPipe > 0 || info.Size() > 0 {

		if err := multiDownload(ctx, c, g, bufio.NewScanner(os.Stdin)); err != nil {
			return err
		}
	}

	// Batch file.
	if c.String("file") != "" {

		file, err := os.Open(c.String("file"))

		if err != nil {
			return err
		}

		defer file.Close()

		if err := multiDownload(ctx, c, g, bufio.NewScanner(file)); err != nil {
			return err
		}
	}

	// Download from args.
	for _, url := range c.Args().Slice() {

		if err = download(ctx, c, g, url); err != nil {
			return err
		}

		fmt.Print(ansi.ClearLine())
		//		fmt.Println(fmt.Sprintf("✔ %s", url))
	}

	return nil
}

func getWidth() int {

	if width, _, err := term.GetSize(0); err == nil && width > 0 {
		return width
	}

	return 80
}

func multiDownload(ctx context.Context, c *cli.Context, g *got.Got, scanner *bufio.Scanner) error {

	for scanner.Scan() {

		url := strings.TrimSpace(scanner.Text())

		if url == "" {
			continue
		}

		if err := download(ctx, c, g, url); err != nil {
			return err
		}

		fmt.Print(ansi.ClearLine())
		//		fmt.Println(fmt.Sprintf("✔ %s", url))
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func download(ctx context.Context, c *cli.Context, g *got.Got, url string) (err error) {
	_ = ctx // ctx is carried by g (got.NewWithContext) and injected by Got.Do.

	if url, err = getURL(url); err != nil {
		return err
	}

	return g.Do(&got.Download{
		URL:         url,
		Dir:         c.String("dir"),
		Dest:        c.String("output"),
		Header:      HeaderSlice,
		Interval:    150,
		ChunkSize:   c.Uint64("size"),
		Concurrency: c.Uint("concurrency"),
	})
}

func getURL(URL string) (string, error) {

	// net/url parses inputs without a scheme as a *path* (e.g. "example.com/a"),
	// which would turn into "https:example.com/a" if we only set u.Scheme.
	// Prefix a scheme explicitly so we reliably get "https://...".
	if !strings.Contains(URL, "://") {
		URL = "https://" + URL
	}

	u, err := url.Parse(URL)

	if err != nil {
		return "", err
	}

	return u.String(), nil
}
