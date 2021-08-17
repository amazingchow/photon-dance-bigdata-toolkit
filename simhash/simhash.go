package simhash

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"github.com/huichen/sego"

	"github.com/amazingchow/photon-dance-bigdata-toolkit/hash"
)

type LanguageType int8

const (
	ENGLISH LanguageType = 0
	CHINESE LanguageType = 1
)

type HashWeightPair struct {
	h uint64
	w float32
}

var (
	// https://stackoverflow.com/questions/14682641/count-number-of-1s-in-binary-format-of-decimal-number/14682688#14682688
	_PopcountLookUpTable = [256]uint8{
		0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
		1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
		1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
		2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
		1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
		2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
		2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
		3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
		1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
		2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
		2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
		3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
		2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
		3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
		3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
		4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
	}
)

// SimHash implements the Standard-Cuckoo-Filter mentioned by
// "Detecting Near-Duplicates for Web Crawling".
type SimHash struct {
	language    LanguageType
	chSegmenter *sego.Segmenter
	chRegExp    *regexp.Regexp
}

func NewSimHash(language LanguageType, dict string) *SimHash {
	sh := &SimHash{language: language}
	if language == CHINESE {
		sh.chSegmenter = new(sego.Segmenter)
		sh.chSegmenter.LoadDictionary(dict)
		sh.chRegExp = regexp.MustCompile("[\u4E00-\u9FA5]+")
	}
	return sh
}

// text --> concordance
func (sh *SimHash) processTokenize(text []byte) map[string]float32 {
	concordance := make(map[string]float32)
	switch sh.language {
	case ENGLISH:
		{
			fc := func(r rune) bool { return !unicode.IsLetter(r) }
			words := strings.FieldsFunc(Bytes2String(text), fc)
			for _, w := range words {
				concordance[strings.ToLower(w)] += 1.0
			}
			total := float32(len(words))
			for k := range concordance {
				concordance[k] /= total
			}
		}
	case CHINESE:
		{
			segments := sh.chSegmenter.Segment(text)
			words := sh.chRegExp.FindAllString(sego.SegmentsToString(segments, false), -1)
			for _, w := range words {
				concordance[w] += 1.0
			}
			total := float32(len(words))
			for k := range concordance {
				concordance[k] /= total
			}
		}
	default:
		{
			panic("unsupported language")
		}
	}
	return concordance
}

// concordance --> concordance (remove stopwords)
func (sh *SimHash) processStopwords(concordance map[string]float32) {
	switch sh.language {
	case ENGLISH:
		{
			for k := range concordance {
				if _, ok := EnStopWords[k]; ok {
					delete(concordance, k)
				} else if _, ok := SpStopWords[k]; ok {
					delete(concordance, k)
				}
			}
		}
	case CHINESE:
		{
			for k := range concordance {
				if _, ok := ChStopWords[k]; ok {
					delete(concordance, k)
				} else if _, ok := SpStopWords[k]; ok {
					delete(concordance, k)
				}
			}
		}
	default:
		{
			panic("unsupported language")
		}
	}
}

func (sh *SimHash) Fingerprint(text []byte, topNOpts ...uint32) uint64 {
	concordance := sh.processTokenize(text)
	sh.processStopwords(concordance)

	hashWeightPairs := make([]HashWeightPair, len(concordance))
	idx := 0
	for k, v := range concordance {
		hashWeightPairs[idx] = HashWeightPair{
			h: hash.FNV1A64(k),
			w: v,
		}
	}
	// sort the slice by weight, higher first.
	sort.Slice(hashWeightPairs, func(i, j int) bool {
		return hashWeightPairs[i].w > hashWeightPairs[j].w
	})
	if len(topNOpts) > 0 && topNOpts[0] < uint32(len(hashWeightPairs)) {
		hashWeightPairs = hashWeightPairs[:topNOpts[0]]
	}

	weights := make([]float32, 64)
	for i := 63; i >= 0; i-- {
		for j := range hashWeightPairs {
			if hashWeightPairs[j].h>>(64-i-1)&1 == 1 {
				weights[i] += hashWeightPairs[j].w
			} else {
				weights[i] -= hashWeightPairs[j].w
			}
		}
	}

	var fingerprint uint64
	var base uint64 = 1
	for i := 63; i >= 0; i-- {
		if weights[i] >= 0 {
			fingerprint += base
		}
		base <<= 1
	}

	return fingerprint
}

func (sh *SimHash) FingerprintToString(fingerprint uint64) string {
	return fmt.Sprintf("%064b", fingerprint)
}

func (sh *SimHash) IsEqual(lhs uint64, rhs uint64, nOpts ...uint8) bool {
	var n uint8 = 3
	if len(nOpts) > 0 {
		n = nOpts[0]
	}

	var cnt uint8 = 0

	lhs ^= rhs
	for (lhs > 0) && (cnt <= n) {
		cnt += _PopcountLookUpTable[lhs&0xff]
		lhs >>= 8
	}

	return cnt <= n
}
