package bptree

import "errors"

var (
	errCorrupted      = errors.New("bptree: corrupted")
	errOutOfRange     = errors.New("bptree: out of range")
	errEndOfIteration = errors.New("bptree: end of iteration")
)

func copyBytes(data []byte) []byte {
	buffer := make([]byte, len(data))
	copy(buffer, data)
	return buffer
}
