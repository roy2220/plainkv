package bptree

import (
	"encoding/binary"

	"github.com/roy2220/fsm"
)

const maxValueSize = 129

type value []byte

type valueFactory struct {
	FileStorage *fsm.FileStorage
}

func (vf valueFactory) CreateValue(rawValue []byte) value {
	if len(rawValue) < maxValueSize {
		return rawValue
	}

	value := value(make([]byte, maxValueSize))
	i := copy(value, rawValue[:maxValueSize-8])
	valueOverflowAddr := vf.allocateValueOverflow(rawValue[i:])
	binary.BigEndian.PutUint64(value[i:], uint64(valueOverflowAddr))
	return value
}

func (vf valueFactory) DestroyValue(value value) int {
	if n := len(value); n < maxValueSize {
		return n
	}

	i, valueOverflowAddr, valueOverflow := vf.getValueOverflow(value)
	vf.freeValueOverflow(valueOverflowAddr)
	valueSize := i + len(valueOverflow)
	return valueSize
}

func (vf valueFactory) ReadValue(value value) []byte {
	if len(value) < maxValueSize {
		return copyBytes(value)
	}

	i, _, valueOverflow := vf.getValueOverflow(value)
	rawValue := make([]byte, i+len(valueOverflow))
	copy(rawValue, value[:i])
	copy(rawValue[i:], valueOverflow)
	return rawValue
}

func (vf valueFactory) GetValueSize(value value) int {
	if n := len(value); n < maxValueSize {
		return n
	}

	i, _, valueOverflow := vf.getValueOverflow(value)
	valueSize := i + len(valueOverflow)
	return valueSize
}

func (vf valueFactory) allocateValueOverflow(valueOverflow []byte) int64 {
	valueOverflowRawSize := make([]byte, binary.MaxVarintLen64)
	valueOverflowRawSize = valueOverflowRawSize[:binary.PutUvarint(valueOverflowRawSize, uint64(len(valueOverflow)))]
	valueOverflowAddr, buffer := vf.FileStorage.AllocateSpace(len(valueOverflowRawSize) + len(valueOverflow))
	j := copy(buffer, valueOverflowRawSize)
	copy(buffer[j:], valueOverflow)
	return valueOverflowAddr
}

func (vf valueFactory) freeValueOverflow(valueOverflowAddr int64) {
	vf.FileStorage.FreeSpace(valueOverflowAddr)
}

func (vf valueFactory) getValueOverflow(value value) (int, int64, []byte) {
	i := maxValueSize - 8
	valueOverflowAddr := int64(binary.BigEndian.Uint64(value[i:]))
	data := vf.FileStorage.AccessSpace(valueOverflowAddr)
	n, j := binary.Uvarint(data)

	if j <= 0 {
		panic(errCorrupted)
	}

	valueOverflowSize := int(n)
	return i, valueOverflowAddr, data[j : j+valueOverflowSize]
}
