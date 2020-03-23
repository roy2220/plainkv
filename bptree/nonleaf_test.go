package bptree

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNonLeafInsertChildren(t *testing.T) {
	nlc := nonLeafController(make([]byte, nonLeafSize))
	v2k := [...]key{
		[]byte("a"),
		[]byte("bb"),
		[]byte("ccc"),
		[]byte("dddd"),
		[]byte("eeeee"),
		[]byte("ffffff"),
	}

	assert.Equal(t, "", dumpNonLeaf(nlc))
	nlc.InsertChildren(0, []nonLeafChild{
		{v2k[0], 0},
	})
	nlc.InsertChildren(1, []nonLeafChild{
		{v2k[3], 3},
		{v2k[4], 4},
		{v2k[5], 5},
	})
	nlc.InsertChildren(1, []nonLeafChild{
		{v2k[1], 1},
		{v2k[2], 2},
	})
	assert.Equal(t, len(v2k), nlc.NumberOfChildren())
	assert.Equal(t, "a:0,bb:1,ccc:2,dddd:3,eeeee:4,ffffff:5", dumpNonLeaf(nlc))
}

func TestNonLeafDeleteChildren(t *testing.T) {
	nlc := nonLeafController(make([]byte, nonLeafSize))
	v2k := [...]key{
		[]byte("a"),
		[]byte("bb"),
		[]byte("ccc"),
		[]byte("dddd"),
		[]byte("eeeee"),
		[]byte("ffffff"),
	}
	for v, k := range v2k {
		nlc.InsertChildren(v, []nonLeafChild{
			{k, int64(v)},
		})
	}

	assert.Equal(t, len(v2k), nlc.NumberOfChildren())
	assert.Equal(t, "a:0,bb:1,ccc:2,dddd:3,eeeee:4,ffffff:5", dumpNonLeaf(nlc))

	c := nlc.RemoveChildren(0, 1)
	assert.Equal(t, "a:0", dumpNonLeafChildren(c))
	assert.Equal(t, "bb:1,ccc:2,dddd:3,eeeee:4,ffffff:5", dumpNonLeaf(nlc))

	c = nlc.RemoveChildren(1, 2)
	assert.Equal(t, "ccc:2,dddd:3", dumpNonLeafChildren(c))
	assert.Equal(t, "bb:1,eeeee:4,ffffff:5", dumpNonLeaf(nlc))

	c = nlc.RemoveChildren(2, 1)
	assert.Equal(t, "ffffff:5", dumpNonLeafChildren(c))
	assert.Equal(t, "bb:1,eeeee:4", dumpNonLeaf(nlc))

	c = nlc.RemoveChildren(0, 2)
	assert.Equal(t, "bb:1,eeeee:4", dumpNonLeafChildren(c))
	assert.Equal(t, "", dumpNonLeaf(nlc))
}

func TestNonLeafLocateChild(t *testing.T) {
	nlc := nonLeafController(make([]byte, nonLeafSize))
	v2k := [...]key{
		[]byte("a"),
		[]byte("bb"),
		[]byte("ccc"),
		[]byte("dddd"),
		[]byte("eeeee"),
		[]byte("ffffff"),
	}
	for v, k := range v2k {
		nlc.InsertChildren(v, []nonLeafChild{
			{k, int64(v)},
		})
	}
	_, ok := nlc.LocateChild([]byte("a"), keyComparer{})
	assert.False(t, ok)
	for v, k := range v2k {
		if v == 0 {
			continue
		}
		i, ok := nlc.LocateChild(k, keyComparer{})
		if assert.True(t, ok) {
			assert.Equal(t, v, i)
		}
	}
	for v, k := range v2k {
		if v == 0 {
			continue
		}
		i, ok := nlc.LocateChild(k[:len(k)-1], keyComparer{})
		if assert.False(t, ok) {
			assert.Equal(t, v, i)
		}
	}
	for v, k := range v2k {
		if v == 0 {
			continue
		}
		i, ok := nlc.LocateChild(key(string(k)+string(k[len(k)-1:])), keyComparer{})
		if assert.False(t, ok) {
			assert.Equal(t, v+1, i)
		}
	}
}

func TestLeafSetKey(t *testing.T) {
	nlc := nonLeafController(make([]byte, nonLeafSize))
	v2k := [...]key{
		[]byte("a"),
		[]byte("bb"),
		[]byte("ccc"),
		[]byte("dddd"),
		[]byte("eeeee"),
		[]byte("ffffff"),
	}
	for v, k := range v2k {
		nlc.InsertChildren(v, []nonLeafChild{
			{k, int64(v)},
		})
	}
	for v, k := range v2k {
		nlc.SetKey(v, key(string(k)+string(k)))
	}
	assert.Equal(t, "aa:0,bbbb:1,cccccc:2,dddddddd:3,eeeeeeeeee:4,ffffffffffff:5", dumpNonLeaf(nlc))
}

func TestNonLeafGetLoadSize(t *testing.T) {
	nlc := nonLeafController(make([]byte, nonLeafSize))
	assert.Equal(t, 0, nlc.GetLoadSize())
	nlc.InsertChildren(0, []nonLeafChild{
		{[]byte("123"), int64(123)},
	})
	assert.Equal(t, nonLeafChildHeaderSize+3, nlc.GetLoadSize())
	nlc.RemoveChildren(0, 1)
	assert.Equal(t, 0, nlc.GetLoadSize())
}

func dumpNonLeaf(nlc nonLeafController) string {
	b := bytes.NewBuffer(nil)
	n := nlc.NumberOfChildren()
	for i := 0; i < n; i++ {
		k := nlc.GetKey(i)
		v := nlc.GetChildAddr(i)
		if i < n-1 {
			fmt.Fprintf(b, "%s:%d,", string(k), v)
		} else {
			fmt.Fprintf(b, "%s:%d", string(k), v)
		}
	}
	return b.String()
}

func dumpNonLeafChildren(c []nonLeafChild) string {
	b := bytes.NewBuffer(nil)
	for i, r := range c {
		if i < len(c)-1 {
			fmt.Fprintf(b, "%s:%d,", string(r.Key), r.Addr)
		} else {
			fmt.Fprintf(b, "%s:%d", string(r.Key), r.Addr)
		}
	}
	return b.String()
}
