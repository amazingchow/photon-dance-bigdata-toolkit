package bloomfilter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloomFilter(t *testing.T) {
	bf := NewBloomFilter(0, true)
	bf.Insert("BTC")
	assert.Equal(t, true, bf.Member("BTC"))
	bf.Insert("ETH")
	assert.Equal(t, true, bf.Member("ETH"))
	assert.Equal(t, false, bf.Member("PHA"))
	bf.Insert("PHA")
	assert.Equal(t, true, bf.Member("PHA"))
	bf.MarkDelete("PHA")
	assert.Equal(t, false, bf.Member("PHA"))
}
