// Copyright (C) 2014 Andreas Klauer <Andreas.Klauer@metamorpher.de>
// License: GPL

// nbdsetup is an alternative to losetup using network block devices.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/ccp0101/go-nbd"
)

type deviceProxy struct {
	nbdFile *nbd.NBD
	device nbd.Device
}

func (self deviceProxy) ReadAt(b []byte, off int64) (n int, err error) {
	fmt.Printf("ReadAt(): off = %d, len = %d\n", off, len(b))
	return self.device.ReadAt(b, off)
}

func (self deviceProxy) WriteAt(b []byte, off int64) (n int, err error) {
	fmt.Printf("Write(): off = %d, len = %d\n", off, len(b))
	return self.device.WriteAt(b, off)
}

func (self deviceProxy) Sync() (err error) {
	fmt.Printf("Sync()\n")
	return self.device.Sync()
}

func main() {
	filename := flag.String("file", "", "regular file or block device")
	write := flag.Bool("write", false, "use true for read-write mode")
	flag.Parse()
	if *filename == "" {
		flag.Usage()
		os.Exit(2)
	}
	fmt.Printf("Using %s in read", *filename)
	file, err := os.Open(*filename)
	if *write {
		fmt.Printf("-write")
		file, err = os.OpenFile(*filename, os.O_RDWR, os.FileMode(0666))
	}
	fmt.Printf(" mode.\n")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	stat, _ := file.Stat()

	device := new(deviceProxy)
	device.device = file
	device.nbdFile = nbd.Create(device, stat.Size())
	dev, err := device.nbdFile.Connect()

	if err != nil {
		panic(err)
	}

	fmt.Println("NBD device: ", dev)

	if device.nbdFile.Loop(); err != nil {
		panic(err)
	}
}
