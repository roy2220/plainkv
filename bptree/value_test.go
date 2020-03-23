package bptree

import (
	"os"
	"testing"

	"github.com/roy2220/fsm"
	"github.com/stretchr/testify/assert"
)

func TestValueFactory(t *testing.T) {
	const fn = "../test/bptree_value.tmp"

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
		v2 := valueFactory{fs}.ReadValue(v)
		assert.Equal(t, buf[:maxValueSize-1], v2)
		assert.Equal(t, 0, fs.Stats().AllocatedSpaceSize)
		valueFactory{fs}.DestroyValue(v)
	}

	{
		v := valueFactory{fs}.CreateValue(buf[:2*maxValueSize])
		v2 := valueFactory{fs}.ReadValue(v)
		assert.Equal(t, buf[:2*maxValueSize], v2)
		assert.Less(t, 0, fs.Stats().AllocatedSpaceSize)
		valueFactory{fs}.DestroyValue(v)
		assert.Equal(t, 0, fs.Stats().AllocatedSpaceSize)
	}
}
