package bptree

import "github.com/roy2220/fsm"

// Iterator represents an iteration over records in a B+ Tree.
type Iterator interface {
	// IsAtEnd indicates if the iteration has no more records.
	IsAtEnd() (hasNoMoreRecords bool)

	// Record reads the key and value of the current record in the iteration.
	ReadRecord() (key, value []byte)

	// Key reads the key of the current record in the iteration.
	ReadKey() []byte

	// Value reads the value of the current record in the iteration.
	ReadValue() []byte

	// Advance advances the iteration to the next record.
	Advance()
}

type forwardIterator struct{ iterator }

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

func (fi *forwardIterator) Advance() {
	fi.advance()

	if fi.isAtEnd {
		return
	}

	leafController := fi.makeCurrentLeafController()

	if fi.currentRecordIndex < leafController.NumberOfRecords()-1 {
		fi.currentRecordIndex++
	} else {
		fi.currentLeafAddr = leafHeader(leafController).NextAddr()
		fi.currentRecordIndex = 0
	}
}

type backwardIterator struct{ iterator }

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

func (bi *backwardIterator) Advance() {
	bi.advance()

	if bi.isAtEnd {
		return
	}

	if bi.currentRecordIndex >= 1 {
		bi.currentRecordIndex--
	} else {
		leafController := bi.makeCurrentLeafController()
		bi.currentLeafAddr = leafHeader(leafController).PrevAddr()
		leafController = bi.makeCurrentLeafController()
		bi.currentRecordIndex = leafController.NumberOfRecords() - 1
	}
}

type iterator struct {
	fileStorage        *fsm.FileStorage
	currentLeafAddr    int64
	currentRecordIndex int
	lastLeafAddr       int64
	lastRecordIndex    int
	isAtEnd            bool
}

func (i *iterator) ReadRecord() ([]byte, []byte) {
	i.checkEnd()
	leafController := i.makeCurrentLeafController()
	key := leafController.GetKey(i.currentRecordIndex)
	value := leafController.GetValue(i.currentRecordIndex)
	return keyFactory{i.fileStorage}.ReadKey(key), valueFactory{i.fileStorage}.ReadValue(value)
}

func (i *iterator) ReadKey() []byte {
	i.checkEnd()
	leafController := i.makeCurrentLeafController()
	key := leafController.GetKey(i.currentRecordIndex)
	return keyFactory{i.fileStorage}.ReadKey(key)
}

func (i *iterator) ReadValue() []byte {
	i.checkEnd()
	leafController := i.makeCurrentLeafController()
	value := leafController.GetValue(i.currentRecordIndex)
	return valueFactory{i.fileStorage}.ReadValue(value)
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

func (i *iterator) checkEnd() {
	if i.isAtEnd {
		panic(errEndOfIteration)
	}

}

func (i *iterator) advance() {
	if i.currentLeafAddr == i.lastLeafAddr && i.currentRecordIndex == i.lastRecordIndex {
		*i = iterator{isAtEnd: true}
	}
}

func (i *iterator) makeCurrentLeafController() leafController {
	return leafFactory{i.fileStorage}.GetLeafController(i.currentLeafAddr)
}
