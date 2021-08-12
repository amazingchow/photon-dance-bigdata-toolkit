package cuckoofilter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCuckooFilter(t *testing.T) {
	bf := NewCuckooFilter(0)
	bf.Insert("BTC")
	assert.Equal(t, true, bf.Lookup("BTC"))
	bf.Insert("ETH")
	assert.Equal(t, true, bf.Lookup("ETH"))
	assert.Equal(t, false, bf.Lookup("PHA"))
	bf.Insert("PHA")
	assert.Equal(t, true, bf.Lookup("PHA"))
	bf.Delete("PHA")
	assert.Equal(t, false, bf.Lookup("PHA"))
}
