package bptree

import (
	"os"
	"testing"

	"github.com/roy2220/fsm"
	"github.com/stretchr/testify/assert"
)

func TestKeyComparerAndFactory(t *testing.T) {
	const fn = "../test/bptree_key.tmp"

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
		k := keyFactory{fs}.CreateKey(buf[:maxKeySize-1])
		d := keyComparer{fs}.CompareKey(k, buf[:maxKeySize-8])
		assert.Greater(t, d, 0)
		d = keyComparer{fs}.CompareKey(k, buf[:maxKeySize-1])
		assert.Equal(t, d, 0)
		k2 := keyFactory{fs}.ReadKeyAll(k)
		assert.Equal(t, buf[:maxKeySize-1], k2)
		d = keyComparer{fs}.CompareKey(k, buf[:maxKeySize])
		assert.Less(t, d, 0)

		ks := keyFactory{fs}.GetRawKeySize(k)
		assert.Equal(t, len(k2), ks)

		buf2 := make([]byte, maxKeySize-1)
		n := keyFactory{fs}.ReadKey(k, 0, buf2)
		assert.Equal(t, len(buf2), n)

		assert.Equal(t, 0, fs.Stats().AllocatedSpaceSize)
		keyFactory{fs}.DestroyKey(k)
	}

	{
		k := keyFactory{fs}.CreateKey(buf[:2*maxKeySize])
		d := keyComparer{fs}.CompareKey(k, buf[:maxKeySize-8])
		assert.Greater(t, d, 0)
		d = keyComparer{fs}.CompareKey(k, buf[:2*maxKeySize-1])
		assert.Greater(t, d, 0)
		d = keyComparer{fs}.CompareKey(k, buf[:2*maxKeySize])
		assert.Equal(t, d, 0)
		k2 := keyFactory{fs}.ReadKeyAll(k)
		assert.Equal(t, buf[:2*maxKeySize], k2)
		d = keyComparer{fs}.CompareKey(k, buf[:2*maxKeySize+1])
		assert.Less(t, d, 0)

		ks := keyFactory{fs}.GetRawKeySize(k)
		assert.Equal(t, len(k2), ks)

		buf2 := make([]byte, maxKeySize-8)
		n := keyFactory{fs}.ReadKey(k, 0, buf2)
		assert.Equal(t, len(buf2), n)
		assert.Equal(t, buf[:maxKeySize-8], []byte(buf2))
		buf2 = make([]byte, maxKeySize)
		n = keyFactory{fs}.ReadKey(k, maxKeySize/2, buf2)
		assert.Equal(t, len(buf2), n)
		assert.Equal(t, buf[maxKeySize/2:maxKeySize/2+maxKeySize], []byte(buf2))

		assert.Less(t, 0, fs.Stats().AllocatedSpaceSize)
		keyFactory{fs}.DestroyKey(k)
		assert.Equal(t, 0, fs.Stats().AllocatedSpaceSize)
	}

	{
		k := keyFactory{fs}.CreateKey(buf[:2*maxKeySize])
		buf2 := make([]byte, maxKeySize)
		n := keyFactory{fs}.ReadKey(k, maxKeySize/2, buf2)
		assert.Equal(t, len(buf2), n)
		assert.Equal(t, buf[maxKeySize/2:maxKeySize/2+maxKeySize], []byte(buf2))
	}
}
