package bptree

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"unsafe"

	"github.com/roy2220/fsm"
)

var (
	// MinKey presents the minimum key in a B+ tree.
	MinKey = []byte("MIN_KEY")

	// MaxKey presents the maximum key in a B+ tree.
	MaxKey = []byte("MAX_KEY")
)

const (
	maxKeySize    = 257
	keyPrefixSize = maxKeySize - 8
)

type key []byte

type keyComparer struct {
	FileStorage *fsm.FileStorage
}

func (kc keyComparer) CompareKey(key key, rawKey []byte) int {
	if len(key) < maxKeySize || len(rawKey) <= keyPrefixSize {
		return bytes.Compare(key, rawKey)
	}

	if d := bytes.Compare(key[:keyPrefixSize], rawKey[:keyPrefixSize]); d != 0 {
		return d
	}

	keyOverflowAddr := int64(binary.BigEndian.Uint64(key[keyPrefixSize:]))
	data := kc.FileStorage.AccessSpace(keyOverflowAddr)
	n, i := binary.Uvarint(data)

	if i <= 0 {
		panic(errCorrupted)
	}

	keyOverflowSize := int(n)
	keyOverflow := data[i : i+keyOverflowSize]
	return bytes.Compare(keyOverflow, rawKey[keyPrefixSize:])
}

type keyFactory struct {
	FileStorage *fsm.FileStorage
}

func (kf keyFactory) CreateKey(rawKey []byte) key {
	if len(rawKey) < maxKeySize {
		return rawKey
	}

	key := key(make([]byte, maxKeySize))
	copy(key, rawKey[:keyPrefixSize])
	keyOverflowAddr := kf.allocateKeyOverflow(rawKey[keyPrefixSize:])
	binary.BigEndian.PutUint64(key[keyPrefixSize:], uint64(keyOverflowAddr))
	return key
}

func (kf keyFactory) DestroyKey(key []byte) int {
	if n := len(key); n < maxKeySize {
		return n
	}

	keyOverflowAddr, keyOverflow := kf.getKeyOverflow(key)
	kf.destroyKeyOverflow(keyOverflowAddr)
	keySize := keyPrefixSize + len(keyOverflow)
	return keySize
}

func (kf keyFactory) ReadKey(key key, dataOffset int, buffer []byte) int {
	if n := len(key); n < maxKeySize {
		if dataOffset >= n {
			return 0
		}

		return copy(buffer, key[dataOffset:])
	}

	if dataOffset+len(buffer) <= keyPrefixSize {
		return copy(buffer, key[dataOffset:])
	}

	_, keyOverflow := kf.getKeyOverflow(key)

	if dataOffset >= keyPrefixSize+len(keyOverflow) {
		return 0
	}

	var i int

	if dataOffset < keyPrefixSize {
		i = copy(buffer, key[dataOffset:keyPrefixSize])
		i += copy(buffer[i:], keyOverflow)
	} else {
		i = copy(buffer, keyOverflow[dataOffset-keyPrefixSize:])
	}

	return i
}

func (kf keyFactory) ReadKeyAll(key key) []byte {
	if len(key) < maxKeySize {
		return copyBytes(key)
	}

	_, keyOverflow := kf.getKeyOverflow(key)
	rawKey := make([]byte, keyPrefixSize+len(keyOverflow))
	copy(rawKey, key[:keyPrefixSize])
	copy(rawKey[keyPrefixSize:], keyOverflow)
	return rawKey
}

func (kf keyFactory) GetRawKeySize(key []byte) int {
	if n := len(key); n < maxKeySize {
		return n
	}

	_, keyOverflow := kf.getKeyOverflow(key)
	keySize := keyPrefixSize + len(keyOverflow)
	return keySize
}

func (kf keyFactory) allocateKeyOverflow(keyOverflow []byte) int64 {
	keyOverflowRawSize := make([]byte, binary.MaxVarintLen64)
	keyOverflowRawSize = keyOverflowRawSize[:binary.PutUvarint(keyOverflowRawSize, uint64(len(keyOverflow)))]
	keyOverflowAddr, buffer := kf.FileStorage.AllocateSpace(len(keyOverflowRawSize) + len(keyOverflow))
	i := copy(buffer, keyOverflowRawSize)
	copy(buffer[i:], keyOverflow)
	return keyOverflowAddr
}

func (kf keyFactory) destroyKeyOverflow(keyOverflowAddr int64) {
	kf.FileStorage.FreeSpace(keyOverflowAddr)
}

func (kf keyFactory) getKeyOverflow(key key) (int64, []byte) {
	keyOverflowAddr := int64(binary.BigEndian.Uint64(key[keyPrefixSize:]))
	data := kf.FileStorage.AccessSpace(keyOverflowAddr)
	n, i := binary.Uvarint(data)

	if i <= 0 {
		panic(errCorrupted)
	}

	keyOverflowSize := int(n)
	return keyOverflowAddr, data[i : i+keyOverflowSize]
}

func isMaxKey(rawKey []byte) bool {
	return unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&rawKey)).Data) ==
		unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&MaxKey)).Data)
}

func isMinKey(rawKey []byte) bool {
	return unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&rawKey)).Data) ==
		unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&MinKey)).Data)
}
