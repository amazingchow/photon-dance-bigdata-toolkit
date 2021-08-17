package simhash

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnglishTypeSimHash(t *testing.T) {
	sh := NewSimHash(ENGLISH, "")
	text, err := ioutil.ReadFile("./fixtures/english1.txt")
	assert.Empty(t, err)
	f1 := sh.Fingerprint(text, 5)
	t.Logf("fingerprint: %s", sh.FingerprintToString(f1))
	text, err = ioutil.ReadFile("./fixtures/english1.txt")
	assert.Empty(t, err)
	f2 := sh.Fingerprint(text, 5)
	t.Logf("fingerprint: %s", sh.FingerprintToString(f2))
	t.Log(sh.IsEqual(f1, f2))
}

func TestChinsesTypeSimHash(t *testing.T) {
	sh := NewSimHash(CHINESE, "./fixtures/dictionary.txt")
	text, err := ioutil.ReadFile("./fixtures/chinese1.txt")
	assert.Empty(t, err)
	f1 := sh.Fingerprint(text, 5)
	t.Logf("fingerprint: %s", sh.FingerprintToString(f1))
	text, err = ioutil.ReadFile("./fixtures/chinese1.txt")
	assert.Empty(t, err)
	f2 := sh.Fingerprint(text, 5)
	t.Logf("fingerprint: %s", sh.FingerprintToString(f2))
	t.Log(sh.IsEqual(f1, f2))
}
