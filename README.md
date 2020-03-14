# plainkv

[![GoDoc](https://godoc.org/github.com/roy2220/plainkv?status.svg)](https://godoc.org/github.com/roy2220/plainkv) [![Build Status](https://travis-ci.com/roy2220/plainkv.svg?branch=master)](https://travis-ci.com/roy2220/plainkv) [![Coverage Status](https://codecov.io/gh/roy2220/plainkv/branch/master/graph/badge.svg)](https://codecov.io/gh/roy2220/plainkv)

A simple key-value storage library for Go

## Architecture

![Architecture](./docs/architecture.svg)

## Example

```go
package main

import (
        "fmt"

        "github.com/roy2220/plainkv"
)

func main() {
        func() {
                d, err := plainkv.OpenDict("./test/dict.tmp", true)
                if err != nil {
                        panic(err)
                }
                defer d.Close()

                d.Set([]byte("foo"), []byte("bar"), false /* don't return the replaced value */)

                _, ok := d.SetIfNotExists([]byte("hello"), []byte("w0rd"), false /* don't return the present value */)
                fmt.Printf("%v\n", ok)

                v, ok := d.SetIfExists([]byte("hello"), []byte("world"), true /* return the replaced value */)
                fmt.Printf("%v %q\n", ok, v)
        }()

        func() {
                d, err := plainkv.OpenDict("./test/dict.tmp", false)
                if err != nil {
                        panic(err)
                }
                defer d.Close()

                dc := plainkv.DictCursor{}
                for {
                        k, v, ok := d.Scan(&dc)
                        if !ok {
                                break
                        }
                        fmt.Printf("%q %q\n", k, v)
                }

                v, ok := d.Test([]byte("foo"), true /* return the present value */)
                fmt.Printf("%v %q\n", ok, v)

                v, ok = d.Clear([]byte("hello"), true /* return the removed value */)
                fmt.Printf("%v %q\n", ok, v)
        }()
        // Output:
        // true
        // true "w0rd"
        // "foo" "bar"
        // "hello" "world"
        // true "bar"
        // true "world"
}
```
