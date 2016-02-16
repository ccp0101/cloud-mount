package main

import (
	"os"
	"io"
	"fmt"
	"crypto/md5"
)

func main() {
	blockSize := 4 * 1024 * 1024

	for {
		b := make([]byte, blockSize)
		n, err := io.ReadFull(os.Stdin, b)
		if err != io.ErrUnexpectedEOF && err != nil {
			panic(err)
		}

		fmt.Printf("%x\n", md5.Sum(b))

		if err == io.ErrUnexpectedEOF || n == 0 {
			break
		}
	}
}
