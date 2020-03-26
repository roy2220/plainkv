package bptree

import "errors"

var (
	errCorrupted  = errors.New("bptree: corrupted")
	errOutOfRange = errors.New("bptree: out of range")
)

func copyBytes(data []byte) []byte {
	buffer := make([]byte, len(data))
	copy(buffer, data)
	return buffer
}
