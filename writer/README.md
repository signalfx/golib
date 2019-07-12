# Writers

See the [godoc](https://godoc.org/github.com/signalfx/golib/writer) for
information on what they are and how to use them.

## Compiling

The ring buffer writer is generated from a common template in the
`template/ring.go` module.  This module has a go:generate comment that will be
recognized when you run `go generate ./...` on the golib repo.  For the
generate script to work, you must install the code generation tool with `go
install github.com/mauricelam/genny`.

Also, `span_ring_test.go` is automatically generated from
`datapoint_ring_test.go` by the same `go generate` command, so be sure to
make changes in the datapoint test module.

