package simhash

import (
	"encoding/binary"
	"unsafe"
)

// Bytes2String fast type conversion from byte array to string, both share the same mem pointer.
func Bytes2String(buf []byte) string {
	return *(*string)(unsafe.Pointer(&buf))
}

func GetByteOrder() binary.ByteOrder {
	var nativeEndian binary.ByteOrder

	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	switch buf {
	case [2]byte{0xCD, 0xAB}:
		nativeEndian = binary.LittleEndian
	case [2]byte{0xAB, 0xCD}:
		nativeEndian = binary.BigEndian
	default:
		panic("Could not determine native endianness.")
	}

	return nativeEndian
}
