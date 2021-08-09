package bloomfilter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStandardBloomFilter(t *testing.T) {
	bf := NewStandardBloomFilter(0)
	bf.Insert("foo")
	assert.Equal(t, true, bf.Member("foo"))
	bf.Insert("bar")
	assert.Equal(t, true, bf.Member("bar"))
	assert.Equal(t, false, bf.Member("foobar"))
	assert.Equal(t, false, bf.Member("barbar"))
}
