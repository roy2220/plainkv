# plainkv

[![Build Status](https://travis-ci.com/roy2220/plainkv.svg?branch=master)](https://travis-ci.com/roy2220/plainkv) [![Coverage Status](https://codecov.io/gh/roy2220/plainkv/branch/master/graph/badge.svg)](https://codecov.io/gh/roy2220/plainkv)

A simple key-value storage library for Go

## Example

```go
package main

import (
        "fmt"

        "github.com/roy2220/plainkv"
)

func main() {
        func() {
                dict, err := plainkv.OpenDict("./test/dict.tmp", true)

                if err != nil {
                        panic(err)
                }

                defer dict.Close()

                dict.Set([]byte("foo"), []byte("bar"))

                v, ok := dict.SetIfNotExists([]byte("hello"), []byte("word"))
                fmt.Printf("%v %q\n", ok, string(v))

                v, ok = dict.SetIfExists([]byte("hello"), []byte("world"))
                fmt.Printf("%v %q\n", ok, string(v))
        }()

        func() {
                dict, err := plainkv.OpenDict("./test/dict.tmp", false)

                if err != nil {
                        panic(err)
                }

                defer dict.Close()

                v, ok := dict.Get([]byte("foo"))
                fmt.Printf("%v %q\n", ok, string(v))

                v, ok = dict.Clear([]byte("hello"))
                fmt.Printf("%v %q\n", ok, string(v))

                v, ok = dict.Get([]byte("hello"))
                fmt.Printf("%v %q\n", ok, string(v))
        }()
        // Output:
        // true ""
        // true "word"
        // true "bar"
        // true "world"
        // false ""
}
```

## Documentation

See https://godoc.org/github.com/roy2220/plainkv
