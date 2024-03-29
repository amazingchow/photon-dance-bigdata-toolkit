package util

import (
	"fmt"
	"runtime"
)

func BitReverseUint32(x uint32) uint32 {
	x = (x&0x55555555)<<1 | (x&0xaaaaaaaa)>>1
	x = (x&0x33333333)<<2 | (x&0xcccccccc)>>2
	x = (x&0x0f0f0f0f)<<4 | (x&0xf0f0f0f0)>>4
	x = (x&0x00ff00ff)<<8 | (x&0xff00ff00)>>8
	return (x&0x0000ffff)<<16 | (x&0xffff0000)>>16
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

// for more info, see: https://golang.org/pkg/runtime/#MemStats
func MemUsage() string {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return fmt.Sprintf("Alloc = %v MiB\totalAlloc = %v MiB\tSys = %v MiB\n",
		bToMb(ms.Alloc), bToMb(ms.TotalAlloc), bToMb(ms.Sys))
}
