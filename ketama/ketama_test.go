package ketama

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

type SimpleBucket struct {
	Labels  []byte
	Weights uint32
}

func (s *SimpleBucket) Label() []byte {
	return s.Labels
}

func (s *SimpleBucket) Weight() uint32 {
	return s.Weights
}

var _ Bucket = &SimpleBucket{}

func randStr(r *rand.Rand) string {
	s := ""
	for i := 0; i < 10; i++ {
		c := 'a' + r.Intn(26)
		s += string(c)
	}
	return s
}

func TestKetama(t *testing.T) {
	s := []Bucket{
		&SimpleBucket{
			Labels:  []byte("w1"),
			Weights: 1,
		},
		&SimpleBucket{
			Labels:  []byte("w2"),
			Weights: 2,
		},
	}
	c := New(s)
	r := rand.New(rand.NewSource(0))
	i0 := 0
	i1 := 0
	for i := 0; i < 100000; i++ {
		str := randStr(r)
		buck := c.Hash([]byte(str))
		if buck == s[0] {
			i0++
		} else if buck == s[1] {
			i1++
		} else {
			panic("NOPE")
		}
	}
	r0 := float64(i0) / float64(i0+i1)
	r1 := float64(i1) / float64(i0+i1)
	fmt.Printf("%f %f\n", r0, r1)
	assert.True(t, r0 > .1 && r0 < .4)
	assert.True(t, r1 > .5 && r1 < .75)
}

func TestKetamaNil(t *testing.T) {
	c := New([]Bucket{})
	assert.Nil(t, c.Hash(nil))
}
