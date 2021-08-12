package cuckoofilter

const (
	_NullFp     = 0
	_BucketSize = 4
)

type (
	Fingerprint byte
	Bucket      [_BucketSize]Fingerprint
)

func (bk *Bucket) Insert(fp Fingerprint) bool {
	for i, x := range bk {
		if x == _NullFp {
			bk[i] = fp
			return true
		}
	}
	return false
}

func (bk *Bucket) Delete(fp Fingerprint) bool {
	for i, x := range bk {
		if x == fp {
			bk[i] = _NullFp
			return true
		}
	}
	return false
}

func (bk *Bucket) GetFingerprintIndex(fp Fingerprint) int {
	for i, x := range bk {
		if x == fp {
			return i
		}
	}
	return -1
}

func (bk *Bucket) Reset() {
	for i := range bk {
		bk[i] = _NullFp
	}
}
