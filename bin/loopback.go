package main

import (
	"flag"
	"fmt"
	"os"
	"blockmap"
	"io/ioutil"
	"github.com/hashicorp/golang-lru"
	"github.com/ccp0101/go-nbd"
)

type Loopback struct {
	folder string
	writeEnabled bool
	blockSize int64
}

func (self Loopback) MapFilename(id int64) (string) {
	return fmt.Sprintf("%s/%d.block", self.folder, id)
}

func (self Loopback) Read(id int64) (b []byte, err error) {
	filename := self.MapFilename(id)
	file, err := os.Open(filename)
	if os.IsNotExist(err) {
		b = make([]byte, self.blockSize)
		err = nil
		return
	}
	if err != nil {
		return
	}
	b, err = ioutil.ReadAll(file)
	file.Close()
	return
}

func (self Loopback) Write(id int64, b []byte) (err error) {
	filename := self.MapFilename(id)
	err = ioutil.WriteFile(filename, b, os.FileMode(0666))
	return
}

func main() {
	folder := flag.String("folder", "", "folder for blocks")
	size := flag.Int("size", 128 * 1024 * 1024, "size in bytes")
	write := flag.Bool("write", false, "use true for read-write mode")
	flag.Parse()
	if *folder == "" || *size == 0 {
		flag.Usage()
		os.Exit(2)
	}
	bs := int64(4 * 1024 * 1024)

	loopback := Loopback {
		folder: *folder,
		writeEnabled: *write,
		blockSize: bs,
	}
	
	cache, err := lru.New(4)
	bm := blockmap.NewBlockMap(loopback, bs, int64(*size) / bs, cache)

	device := nbd.Create(bm, int64(*size))
	dev, err := device.Connect()

	if err != nil {
		panic(err)
	}

	fmt.Println("NBD device: ", dev)
	if bm.Loop(device); err != nil {
		panic(err)
	}
}
