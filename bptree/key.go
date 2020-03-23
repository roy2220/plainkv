package bptree

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"unsafe"

	"github.com/roy2220/fsm"
)

var (
	// MaxKey presents the minimum key of a B+ tree.
	MaxKey = []byte("MAX_KEY")

	// MinKey presents the maximum key of a B+ tree.
	MinKey = []byte("MIN_KEY")
)

const maxKeySize = 257

type key []byte

type keyComparer struct {
	FileStorage *fsm.FileStorage
}

func (kc keyComparer) CompareKey(key key, rawKey []byte) int {
	i := maxKeySize - 8

	if len(key) < maxKeySize || len(rawKey) <= i {
		return bytes.Compare(key, rawKey)
	}

	if d := bytes.Compare(key[:i], rawKey[:i]); d != 0 {
		return d
	}

	keyOverflowAddr := int64(binary.BigEndian.Uint64(key[i:]))
	data := kc.FileStorage.AccessSpace(keyOverflowAddr)
	n, j := binary.Uvarint(data)

	if j <= 0 {
		panic(errCorrupted)
	}

	keyOverflowSize := int(n)
	keyOverflow := data[j : j+keyOverflowSize]
	return bytes.Compare(keyOverflow, rawKey[i:])
}

type keyFactory struct {
	FileStorage *fsm.FileStorage
}

func (kf keyFactory) CreateKey(rawKey []byte) key {
	if len(rawKey) < maxKeySize {
		return rawKey
	}

	key := key(make([]byte, maxKeySize))
	i := copy(key, rawKey[:maxKeySize-8])
	keyOverflowAddr := kf.allocateKeyOverflow(rawKey[i:])
	binary.BigEndian.PutUint64(key[i:], uint64(keyOverflowAddr))
	return key
}

func (kf keyFactory) DestroyKey(key []byte) int {
	if n := len(key); n < maxKeySize {
		return n
	}

	i, keyOverflowAddr, keyOverflow := kf.getKeyOverflow(key)
	kf.destroyKeyOverflow(keyOverflowAddr)
	keySize := i + len(keyOverflow)
	return keySize
}

func (kf keyFactory) ReadKey(key key) []byte {
	if len(key) < maxKeySize {
		return copyBytes(key)
	}

	i, _, keyOverflow := kf.getKeyOverflow(key)
	rawKey := make([]byte, i+len(keyOverflow))
	copy(rawKey, key[:i])
	copy(rawKey[i:], keyOverflow)
	return rawKey
}

func (kf keyFactory) GetKeySize(key []byte) int {
	if n := len(key); n < maxKeySize {
		return n
	}

	i, _, keyOverflow := kf.getKeyOverflow(key)
	keySize := i + len(keyOverflow)
	return keySize
}

func (kf keyFactory) allocateKeyOverflow(keyOverflow []byte) int64 {
	keyOverflowRawSize := make([]byte, binary.MaxVarintLen64)
	keyOverflowRawSize = keyOverflowRawSize[:binary.PutUvarint(keyOverflowRawSize, uint64(len(keyOverflow)))]
	keyOverflowAddr, buffer := kf.FileStorage.AllocateSpace(len(keyOverflowRawSize) + len(keyOverflow))
	j := copy(buffer, keyOverflowRawSize)
	copy(buffer[j:], keyOverflow)
	return keyOverflowAddr
}

func (kf keyFactory) destroyKeyOverflow(keyOverflowAddr int64) {
	kf.FileStorage.FreeSpace(keyOverflowAddr)
}

func (kf keyFactory) getKeyOverflow(key key) (int, int64, []byte) {
	i := maxKeySize - 8
	keyOverflowAddr := int64(binary.BigEndian.Uint64(key[i:]))
	data := kf.FileStorage.AccessSpace(keyOverflowAddr)
	n, j := binary.Uvarint(data)

	if j <= 0 {
		panic(errCorrupted)
	}

	keyOverflowSize := int(n)
	return i, keyOverflowAddr, data[j : j+keyOverflowSize]
}

func isMaxKey(rawKey []byte) bool {
	return unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&rawKey)).Data) ==
		unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&MaxKey)).Data)
}

func isMinKey(rawKey []byte) bool {
	return unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&rawKey)).Data) ==
		unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&MinKey)).Data)
}
