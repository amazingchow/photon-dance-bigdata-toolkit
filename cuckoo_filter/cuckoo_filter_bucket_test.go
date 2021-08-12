package cuckoofilter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCuckooFilterBucket(t *testing.T) {
	bk := new(Bucket)
	assert.Equal(t, true, bk.Insert('H'))
	assert.Equal(t, true, bk.Insert('e'))
	assert.Equal(t, true, bk.Insert('l'))
	assert.Equal(t, true, bk.Insert('l'))
	assert.Equal(t, false, bk.Insert('o'))
	assert.Equal(t, false, bk.Delete('o'))
	assert.Equal(t, true, bk.Delete('H'))
	assert.Equal(t, 1, bk.GetFingerprintIndex('e'))
	assert.Equal(t, 2, bk.GetFingerprintIndex('l'))
	bk.Reset()
	assert.Equal(t, -1, bk.GetFingerprintIndex('e'))
	assert.Equal(t, -1, bk.GetFingerprintIndex('l'))
}
