package got

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

// Got holds got download config.
type Got struct {
	ProgressFunc

	Client *http.Client

	ctx context.Context
}

// UserAgent is the default Got user agent to send http requests.
var UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:146.0) Gecko/20100101 Firefox/146.0"

// ErrDownloadAborted - When download is aborted by the OS before it is completed, ErrDownloadAborted will be triggered
var ErrDownloadAborted = errors.New("Operation aborted")

// DefaultClient is the default http client for got requests.
var DefaultClient = &http.Client{
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,

		// For file downloads we almost always want the raw bytes (no gzip/deflate),
		// especially because Range requests + compression can produce unexpected
		// Content-Length/content-range behavior.
		DisableCompression: true,

		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,

		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 200,
		MaxConnsPerHost:     0, // 0 = no explicit cap (Go will manage). Or set e.g. 200.

		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,

		// Optional but often helpful for download tools:
		ForceAttemptHTTP2: true,
	},
}

// Download creates *Download item and runs it.
func (g Got) Download(URL, dest string) error {

	return g.Do(&Download{
		ctx:    g.ctx,
		URL:    URL,
		Dest:   dest,
		Client: g.Client,
	})
}

// Do inits and runs ProgressFunc if set and starts the Download.
func (g Got) Do(dl *Download) error {

	// If the caller constructed Download manually (common), make sure we still
	// honor Got's configured context and client.
	if dl.ctx == nil {
		dl.ctx = g.ctx
	}
	if dl.Client == nil {
		dl.Client = g.Client
	}

	if err := dl.Init(); err != nil {
		return err
	}

	if g.ProgressFunc != nil {

		defer func() {
			dl.StopProgress = true
		}()

		go dl.RunProgress(g.ProgressFunc)
	}

	return dl.Start()
}

// New returns new *Got with default context and client.
func New() *Got {
	return NewWithContext(context.Background())
}

// NewWithContext wants Context and returns *Got with default http client.
func NewWithContext(ctx context.Context) *Got {
	return &Got{
		ctx:    ctx,
		Client: DefaultClient,
	}
}

// NewRequest returns a new http.Request and error if any.
func NewRequest(ctx context.Context, method, URL string, header []GotHeader) (req *http.Request, err error) {

	if req, err = http.NewRequestWithContext(ctx, method, URL, nil); err != nil {
		return
	}

	req.Header.Set("User-Agent", UserAgent)

	for _, h := range header {
		req.Header.Set(h.Key, h.Value)
	}

	return
}
