// +build go1.5

package sfxclient

import (
	"net/http"

	"context"
)

func (h *HTTPSink) withCancel(ctx context.Context, req *http.Request, respValidator responseValidator) (err error) {
	req.Cancel = ctx.Done()
	resp, err := h.Client.Do(req)
	return h.handleResponse(resp, err, respValidator)
}
