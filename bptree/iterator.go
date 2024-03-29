package bptree

import (
	"errors"

	"github.com/roy2220/fsm"
)

// Iterator represents an iteration over records in a B+ Tree.
type Iterator interface {
	// IsAtEnd indicates if the iteration has no more records.
	IsAtEnd() (hasNoMoreRecords bool)

	// Advance advances the iteration to the next record and returns the
	// iterator self.
	// If the iteration has no more records it does nothing.
	Advance() Iterator

	// GetKeySize returns the key size of the current record in the iteration.
	GetKeySize() (keySize int, err error)

	// ReadKey reads data of the key of the current record in the iteration
	// at the given offset into the given buffer and then returns the number
	// of bytes read.
	// If the iteration has no more records it returns an error.
	ReadKey(dataOffset int, buffer []byte) (numberOfBytesRead int, err error)

	// ReadKeyAll reads and returns all data of the key of the current record
	// in the iteration.
	// If the iteration has no more records it returns an error.
	ReadKeyAll() (key []byte, err error)

	// GetValueSize returns the value size of the current record in the iteration.
	GetValueSize() (valueSize int, err error)

	// ReadValue reads data of the value of the current record in the iteration
	// at the given offset into the given buffer and then returns the number
	// of bytes read.
	// If the iteration has no more records it returns an error.
	ReadValue(dataOffset int, buffer []byte) (numberOfBytesRead int, err error)

	// ReadValueAll reads and returns all data of the value of the current
	// record in the iteration.
	// If the iteration has no more records it returns an error.
	ReadValueAll() (value []byte, err error)

	// ReadRecordAll reads and returns all data of the key and all data of the
	// value of the current record in the iteration.
	// If the iteration has no more records it returns an error.
	ReadRecordAll() (key, value []byte, err error)
}

type forwardIterator struct{ iterator }

var _ = Iterator((*forwardIterator)(nil))

func (fi *forwardIterator) Init(
	fileStorage *fsm.FileStorage,
	firstLeafAddr int64,
	firstRecordIndex int,
	lastLeafAddr int64,
	lastRecordIndex int,
	isAtEnd bool,
) *forwardIterator {
	fi.init(fileStorage, firstLeafAddr, firstRecordIndex, lastLeafAddr, lastRecordIndex, isAtEnd)
	return fi
}

func (fi *forwardIterator) Advance() Iterator {
	fi.preAdvance()

	if !fi.isAtEnd {
		leafController := fi.makeCurrentLeafController()

		if fi.currentRecordIndex < leafController.NumberOfRecords()-1 {
			fi.currentRecordIndex++
		} else {
			fi.currentLeafAddr = leafHeader(leafController).NextAddr()
			fi.currentRecordIndex = 0
		}
	}

	return fi
}

type backwardIterator struct{ iterator }

var _ = Iterator((*backwardIterator)(nil))

func (bi *backwardIterator) Init(
	fileStorage *fsm.FileStorage,
	firstLeafAddr int64,
	firstRecordIndex int,
	lastLeafAddr int64,
	lastRecordIndex int,
	isAtEnd bool,
) *backwardIterator {
	bi.init(fileStorage, firstLeafAddr, firstRecordIndex, lastLeafAddr, lastRecordIndex, isAtEnd)
	return bi
}

func (bi *backwardIterator) Advance() Iterator {
	bi.preAdvance()

	if !bi.isAtEnd {
		if bi.currentRecordIndex >= 1 {
			bi.currentRecordIndex--
		} else {
			leafController := bi.makeCurrentLeafController()
			bi.currentLeafAddr = leafHeader(leafController).PrevAddr()
			leafController = bi.makeCurrentLeafController()
			bi.currentRecordIndex = leafController.NumberOfRecords() - 1
		}
	}

	return bi
}

type iterator struct {
	fileStorage        *fsm.FileStorage
	currentLeafAddr    int64
	currentRecordIndex int
	lastLeafAddr       int64
	lastRecordIndex    int
	isAtEnd            bool
}

func (i *iterator) GetKeySize() (int, error) {
	if i.isAtEnd {
		return 0, errEndOfIteration
	}

	leafController := i.makeCurrentLeafController()
	key := leafController.GetKey(i.currentRecordIndex)
	return keyFactory{i.fileStorage}.GetRawKeySize(key), nil
}

func (i *iterator) ReadKey(dataOffset int, buffer []byte) (int, error) {
	if i.isAtEnd {
		return 0, errEndOfIteration
	}

	leafController := i.makeCurrentLeafController()
	key := leafController.GetKey(i.currentRecordIndex)
	return keyFactory{i.fileStorage}.ReadKey(key, dataOffset, buffer), nil
}

func (i *iterator) ReadKeyAll() ([]byte, error) {
	if i.isAtEnd {
		return nil, errEndOfIteration
	}

	leafController := i.makeCurrentLeafController()
	key := leafController.GetKey(i.currentRecordIndex)
	return keyFactory{i.fileStorage}.ReadKeyAll(key), nil
}

func (i *iterator) GetValueSize() (int, error) {
	if i.isAtEnd {
		return 0, errEndOfIteration
	}

	leafController := i.makeCurrentLeafController()
	value := leafController.GetValue(i.currentRecordIndex)
	return valueFactory{i.fileStorage}.GetRawValueSize(value), nil
}

func (i *iterator) ReadValue(dataOffset int, buffer []byte) (int, error) {
	if i.isAtEnd {
		return 0, errEndOfIteration
	}

	leafController := i.makeCurrentLeafController()
	value := leafController.GetValue(i.currentRecordIndex)
	return valueFactory{i.fileStorage}.ReadValue(value, dataOffset, buffer), nil
}

func (i *iterator) ReadValueAll() ([]byte, error) {
	if i.isAtEnd {
		return nil, errEndOfIteration
	}

	leafController := i.makeCurrentLeafController()
	value := leafController.GetValue(i.currentRecordIndex)
	return valueFactory{i.fileStorage}.ReadValueAll(value), nil
}

func (i *iterator) ReadRecordAll() ([]byte, []byte, error) {
	if i.isAtEnd {
		return nil, nil, errEndOfIteration
	}

	leafController := i.makeCurrentLeafController()
	key := leafController.GetKey(i.currentRecordIndex)
	value := leafController.GetValue(i.currentRecordIndex)
	return keyFactory{i.fileStorage}.ReadKeyAll(key), valueFactory{i.fileStorage}.ReadValueAll(value), nil
}

func (i *iterator) IsAtEnd() bool {
	return i.isAtEnd
}

func (i *iterator) init(
	fileStorage *fsm.FileStorage,
	firstLeafAddr int64,
	firstRecordIndex int,
	lastLeafAddr int64,
	lastRecordIndex int,
	isAtEnd bool,
) {
	i.fileStorage = fileStorage
	i.currentLeafAddr = firstLeafAddr
	i.currentRecordIndex = firstRecordIndex
	i.lastLeafAddr = lastLeafAddr
	i.lastRecordIndex = lastRecordIndex
	i.isAtEnd = isAtEnd
}

func (i *iterator) preAdvance() {
	if i.currentLeafAddr == i.lastLeafAddr && i.currentRecordIndex == i.lastRecordIndex {
		*i = iterator{isAtEnd: true}
	}
}

func (i *iterator) makeCurrentLeafController() leafController {
	return leafFactory{i.fileStorage}.GetLeafController(i.currentLeafAddr)
}

var errEndOfIteration = errors.New("bptree: end of iteration")
