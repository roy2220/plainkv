package bptree

import (
	"encoding/binary"

	"github.com/roy2220/fsm"
)

const (
	maxValueSize    = 129
	valuePrefixSize = maxValueSize - 8
)

type value []byte

type valueFactory struct {
	FileStorage *fsm.FileStorage
}

func (vf valueFactory) CreateValue(rawValue []byte) value {
	if len(rawValue) < maxValueSize {
		return rawValue
	}

	value := value(make([]byte, maxValueSize))
	copy(value, rawValue[:valuePrefixSize])
	valueOverflowAddr := vf.allocateValueOverflow(rawValue[valuePrefixSize:])
	binary.BigEndian.PutUint64(value[valuePrefixSize:], uint64(valueOverflowAddr))
	return value
}

func (vf valueFactory) DestroyValue(value value) int {
	if n := len(value); n < maxValueSize {
		return n
	}

	valueOverflowAddr, valueOverflow := vf.getValueOverflow(value)
	vf.freeValueOverflow(valueOverflowAddr)
	valueSize := valuePrefixSize + len(valueOverflow)
	return valueSize
}

func (vf valueFactory) ReadValue(value value, dataOffset int, buffer []byte) int {
	if n := len(value); n < maxValueSize {
		if dataOffset >= n {
			return 0
		}

		return copy(buffer, value[dataOffset:])
	}

	if dataOffset+len(buffer) <= valuePrefixSize {
		return copy(buffer, value[dataOffset:])
	}

	_, valueOverflow := vf.getValueOverflow(value)

	if dataOffset >= valuePrefixSize+len(valueOverflow) {
		return 0
	}

	var i int

	if dataOffset < valuePrefixSize {
		i = copy(buffer, value[dataOffset:valuePrefixSize])
		i += copy(buffer[i:], valueOverflow)
	} else {
		i = copy(buffer, valueOverflow[dataOffset-valuePrefixSize:])
	}

	return i
}

func (vf valueFactory) ReadValueAll(value value) []byte {
	if len(value) < maxValueSize {
		return copyBytes(value)
	}

	_, valueOverflow := vf.getValueOverflow(value)
	rawValue := make([]byte, valuePrefixSize+len(valueOverflow))
	copy(rawValue, value[:valuePrefixSize])
	copy(rawValue[valuePrefixSize:], valueOverflow)
	return rawValue
}

func (vf valueFactory) GetRawValueSize(value value) int {
	if n := len(value); n < maxValueSize {
		return n
	}

	_, valueOverflow := vf.getValueOverflow(value)
	valueSize := valuePrefixSize + len(valueOverflow)
	return valueSize
}

func (vf valueFactory) allocateValueOverflow(valueOverflow []byte) int64 {
	valueOverflowRawSize := make([]byte, binary.MaxVarintLen64)
	valueOverflowRawSize = valueOverflowRawSize[:binary.PutUvarint(valueOverflowRawSize, uint64(len(valueOverflow)))]
	valueOverflowAddr, buffer := vf.FileStorage.AllocateSpace(len(valueOverflowRawSize) + len(valueOverflow))
	i := copy(buffer, valueOverflowRawSize)
	copy(buffer[i:], valueOverflow)
	return valueOverflowAddr
}

func (vf valueFactory) freeValueOverflow(valueOverflowAddr int64) {
	vf.FileStorage.FreeSpace(valueOverflowAddr)
}

func (vf valueFactory) getValueOverflow(value value) (int64, []byte) {
	valueOverflowAddr := int64(binary.BigEndian.Uint64(value[valuePrefixSize:]))
	data := vf.FileStorage.AccessSpace(valueOverflowAddr)
	n, i := binary.Uvarint(data)

	if i <= 0 {
		panic(errCorrupted)
	}

	valueOverflowSize := int(n)
	return valueOverflowAddr, data[i : i+valueOverflowSize]
}
