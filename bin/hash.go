package main

import (
	"os"
	"io"
	"fmt"
	"crypto/sha1"
)

func main() {
	blockSize := 4 * 1024 * 1024

	for {
		b := make([]byte, blockSize)
		n, err := os.Stdin.Read(b)
		if err != io.EOF && err != nil {
			panic(err)
		}

		h := sha1.New()
		h.Write(b)
		fmt.Printf("%x\n", h.Sum(nil))

		if err == io.EOF || n == 0 {
			break
		}
	}
}
