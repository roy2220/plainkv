package bptree

import (
	"os"
	"testing"

	"github.com/roy2220/fsm"
	"github.com/stretchr/testify/assert"
)

func TestValueFactory(t *testing.T) {
	const fn = "../testdata/bptree_value.tmp"

	fs := new(fsm.FileStorage).Init()
	err := fs.Open(fn, true)

	if !assert.NoError(t, err) {
		t.FailNow()
	}

	defer func() {
		fs.Close()
		os.Remove(fn)
	}()

	buf := make([]byte, leafSize)
	for i := range buf {
		buf[i] = '0' + byte(i%10)
	}

	{
		v := valueFactory{fs}.CreateValue(buf[:maxValueSize-1])
		v2 := valueFactory{fs}.ReadValueAll(v)
		assert.Equal(t, buf[:maxValueSize-1], v2)

		vs := valueFactory{fs}.GetRawValueSize(v)
		assert.Equal(t, len(v2), vs)

		buf2 := make([]byte, maxValueSize-1)
		n := valueFactory{fs}.ReadValue(v, 0, buf2)
		assert.Equal(t, len(buf2), n)
		assert.Equal(t, buf[:maxValueSize-1], []byte(buf2))

		assert.Equal(t, 0, fs.Stats().AllocatedSpaceSize)
		valueFactory{fs}.DestroyValue(v)
	}

	{
		v := valueFactory{fs}.CreateValue(buf[:2*maxValueSize])
		v2 := valueFactory{fs}.ReadValueAll(v)
		assert.Equal(t, buf[:2*maxValueSize], v2)

		vs := valueFactory{fs}.GetRawValueSize(v)
		assert.Equal(t, len(v2), vs)

		buf2 := make([]byte, maxValueSize-8)
		n := valueFactory{fs}.ReadValue(v, 0, buf2)
		assert.Equal(t, len(buf2), n)
		assert.Equal(t, buf[:maxValueSize-8], []byte(buf2))
		buf2 = make([]byte, maxValueSize)
		n = valueFactory{fs}.ReadValue(v, maxValueSize/2, buf2)
		assert.Equal(t, len(buf2), n)
		assert.Equal(t, buf[maxValueSize/2:maxValueSize/2+maxValueSize], []byte(buf2))

		assert.Less(t, 0, fs.Stats().AllocatedSpaceSize)
		valueFactory{fs}.DestroyValue(v)
		assert.Equal(t, 0, fs.Stats().AllocatedSpaceSize)
	}

	{
		v := valueFactory{fs}.CreateValue(buf[:2*maxValueSize])
		buf2 := make([]byte, maxValueSize)
		n := valueFactory{fs}.ReadValue(v, maxValueSize/2, buf2)
		assert.Equal(t, len(buf2), n)
		assert.Equal(t, buf[maxValueSize/2:maxValueSize/2+maxValueSize], []byte(buf2))
	}
}
