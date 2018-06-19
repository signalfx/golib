// +build !go1.5

package sfxclient

import (
	"net/http"

	"context"
)

type canceler interface {
	CancelRequest(*http.Request)
}

func (h *HTTPSink) withCancel(ctx context.Context, req *http.Request, respValidator responseValidator) (err error) {
	canCancel, ok := h.Client.Transport.(canceler)
	if !ok {
		resp, err := h.Client.Do(req)
		return h.handleResponse(resp, err, respValidator)
	}

	c := make(chan error, 1)
	go func() {
		resp, err := h.Client.Do(req)
		c <- h.handleResponse(resp, err, respValidator)
	}()
	select {
	case <-ctx.Done():
		canCancel.CancelRequest(req)
		<-c // Wait for f to return.
		return ctx.Err()
	case err := <-c:
		return err
	}
}
