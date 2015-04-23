package zkplus

import (
	"testing"
	"time"

	"github.com/signalfx/golib/zkplus/zktest"
	"github.com/stretchr/testify/assert"
)

func TestInnerBuilder(t *testing.T) {
	z, ch, _ := zktest.New().Connect()
	builder := NewBuilder().Connector(&StaticConnector{C: z, Ch: ch})

	builder.AppendPathPrefix("a")
	builder.AppendPathPrefix("b")

	zkp, err := builder.Build()
	assert.NoError(t, err)
	assert.Equal(t, "/a/b", zkp.pathPrefix)

	q := NewBuilder().ZkPlus(zkp)
	assert.Equal(t, "/a/b", q.pathPrefix)

	zkp2, _, err := q.BuildDirect()
	assert.NoError(t, err)
	assert.Equal(t, "/a/b", zkp2.pathPrefix)
}

func TestBuildBadPath(t *testing.T) {
	z, ch, _ := zktest.New().Connect()
	builder := NewBuilder().Connector(&StaticConnector{C: z, Ch: ch})

	_, err := builder.PathPrefix("badstart").Build()
	assert.Equal(t, errInvalidPathPrefix, err)

	_, err = builder.PathPrefix("/badstart/").Build()
	assert.Equal(t, errInvalidPathSuffix, err)

	_, _, err = builder.BuildDirect()
	assert.Equal(t, errInvalidPathSuffix, err)

	zkp, err := builder.PathPrefix("").Build()
	assert.NoError(t, err)
	assert.Equal(t, "", zkp.pathPrefix)
}

func TestDialZkConnector(t *testing.T) {
	builder := NewBuilder().DialZkConnector([]string{"BADHOST.example.com"}, time.Second, nil)
	zkp, err := builder.Build()
	assert.NoError(t, err)
	_, _, err = zkp.zkConnector.Conn()
	assert.NoError(t, err)
}
