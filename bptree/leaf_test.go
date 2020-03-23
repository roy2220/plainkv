package bptree

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLeafInsertRecords(t *testing.T) {
	lc := leafController(make([]byte, leafSize))
	kvs := [][2][]byte{
		{[]byte("a"), []byte("1")},
		{[]byte("bb"), []byte("22")},
		{[]byte("ccc"), []byte("333")},
		{[]byte("dddd"), []byte("4444")},
		{[]byte("eeeee"), []byte("55555")},
		{[]byte("ffffff"), []byte("666666")},
	}

	assert.Equal(t, "", dumpLeaf(lc))
	lc.InsertRecords(0, []record{
		{kvs[0][0], kvs[0][1]},
	})
	lc.InsertRecords(1, []record{
		{kvs[3][0], kvs[3][1]},
		{kvs[4][0], kvs[4][1]},
		{kvs[5][0], kvs[5][1]},
	})
	lc.InsertRecords(1, []record{
		{kvs[1][0], kvs[1][1]},
		{kvs[2][0], kvs[2][1]},
	})
	assert.Equal(t, len(kvs), lc.NumberOfRecords())
	assert.Equal(t, "a:1,bb:22,ccc:333,dddd:4444,eeeee:55555,ffffff:666666", dumpLeaf(lc))
}

func TestLeafDeleteRecords(t *testing.T) {
	lc := leafController(make([]byte, leafSize))
	kvs := [][2][]byte{
		{[]byte("a"), []byte("1")},
		{[]byte("bb"), []byte("22")},
		{[]byte("ccc"), []byte("333")},
		{[]byte("dddd"), []byte("4444")},
		{[]byte("eeeee"), []byte("55555")},
		{[]byte("ffffff"), []byte("666666")},
	}
	for i, kv := range kvs {
		lc.InsertRecords(i, []record{
			{kv[0], kv[1]},
		})
	}

	assert.Equal(t, len(kvs), lc.NumberOfRecords())
	assert.Equal(t, "a:1,bb:22,ccc:333,dddd:4444,eeeee:55555,ffffff:666666", dumpLeaf(lc))

	rs := lc.RemoveRecords(0, 1)
	assert.Equal(t, "a:1", dumpRecords(rs))
	assert.Equal(t, "bb:22,ccc:333,dddd:4444,eeeee:55555,ffffff:666666", dumpLeaf(lc))

	rs = lc.RemoveRecords(1, 2)
	assert.Equal(t, "ccc:333,dddd:4444", dumpRecords(rs))
	assert.Equal(t, "bb:22,eeeee:55555,ffffff:666666", dumpLeaf(lc))

	rs = lc.RemoveRecords(2, 1)
	assert.Equal(t, "ffffff:666666", dumpRecords(rs))
	assert.Equal(t, "bb:22,eeeee:55555", dumpLeaf(lc))

	rs = lc.RemoveRecords(0, 2)
	assert.Equal(t, "bb:22,eeeee:55555", dumpRecords(rs))
	assert.Equal(t, "", dumpLeaf(lc))
}

func TestLeafLocateRecord(t *testing.T) {
	lc := leafController(make([]byte, leafSize))
	kvs := [][2][]byte{
		{[]byte("a"), []byte("1")},
		{[]byte("bb"), []byte("22")},
		{[]byte("ccc"), []byte("333")},
		{[]byte("dddd"), []byte("4444")},
		{[]byte("eeeee"), []byte("55555")},
		{[]byte("ffffff"), []byte("666666")},
	}
	for i, kv := range kvs {
		lc.InsertRecords(i, []record{
			{kv[0], kv[1]},
		})
	}

	for _, kv := range kvs {
		k, v := kv[0], kv[1]
		i, ok := lc.LocateRecord(k, keyComparer{})
		if assert.True(t, ok) {
			assert.Equal(t, int(v[0]-'1'), i)
		}
	}
	for _, kv := range kvs {
		k, v := kv[0], kv[1]
		i, ok := lc.LocateRecord(k[:len(k)-1], keyComparer{})
		if assert.False(t, ok) {
			assert.Equal(t, int(v[0]-'1'), i)
		}
	}
	for _, kv := range kvs {
		k, v := kv[0], kv[1]
		i, ok := lc.LocateRecord(key(string(k)+string(k[len(k)-1:])), keyComparer{})
		if assert.False(t, ok) {
			assert.Equal(t, int(v[0]-'0'), i)
		}
	}
}

func TestLeafSetValue(t *testing.T) {
	lc := leafController(make([]byte, leafSize))
	kvs := [][2][]byte{
		{[]byte("a"), []byte("1")},
		{[]byte("bb"), []byte("22")},
		{[]byte("ccc"), []byte("333")},
		{[]byte("dddd"), []byte("4444")},
		{[]byte("eeeee"), []byte("55555")},
		{[]byte("ffffff"), []byte("666666")},
	}
	for i, kv := range kvs {
		lc.InsertRecords(i, []record{
			{kv[0], kv[1]},
		})
	}

	for i, kv := range kvs {
		lc.SetValue(i, kv[0])
	}
	assert.Equal(t, "a:a,bb:bb,ccc:ccc,dddd:dddd,eeeee:eeeee,ffffff:ffffff", dumpLeaf(lc))
}

func TestLeafGetLoadSize(t *testing.T) {
	lc := leafController(make([]byte, leafSize))
	assert.Equal(t, 0, lc.GetLoadSize())
	lc.InsertRecords(0, []record{
		{key("123"), value("4567")},
	})
	assert.Equal(t, recordHeaderSize+7, lc.GetLoadSize())
	lc.RemoveRecords(0, 1)
	assert.Equal(t, 0, lc.GetLoadSize())
}

func dumpLeaf(lc leafController) string {
	b := bytes.NewBuffer(nil)
	n := lc.NumberOfRecords()
	for i := 0; i < n; i++ {
		k := lc.GetKey(i)
		v := lc.GetValue(i)
		if i < n-1 {
			fmt.Fprintf(b, "%s:%s,", string(k), string(v))
		} else {
			fmt.Fprintf(b, "%s:%s", string(k), string(v))
		}
	}
	return b.String()
}

func dumpRecords(rs []record) string {
	b := bytes.NewBuffer(nil)
	for i, r := range rs {
		if i < len(rs)-1 {
			fmt.Fprintf(b, "%s:%s,", string(r.Key), string(r.Value))
		} else {
			fmt.Fprintf(b, "%s:%s", string(r.Key), string(r.Value))
		}
	}
	return b.String()
}
