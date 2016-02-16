package blockmap

import (
	"fmt"
	"sync"
	"time"
	"errors"
	"github.com/ccp0101/go-nbd"
)

type BlockProvider interface {
	Read(int64) ([]byte, error)
	Write(int64, []byte) (error)
}

type Cache interface {
	Get(interface{}) (interface{}, bool)
	Add(interface{}, interface{}) (bool)
	Remove(interface{})
}

type BlockMap struct {
	ioLock *sync.Mutex
	folder string
	size int64
	blockSize int64
	blockCount int64
	blockCache Cache
	maxWriteQueue int
	writeQueue map[int64][]byte
	concurrentReads int
	concurrentWrites int
	lastFlush time.Time
	syncInterval time.Duration
	blockProvider BlockProvider
}

type UploadTask struct {
	blockId int64
	block []byte
}

func NewBlockMap(provider BlockProvider, blockSize int64,
		blockCount int64, cache Cache) (bm *BlockMap) {
	bm = &BlockMap{}
	bm.blockProvider = provider
	bm.ioLock = &sync.Mutex{}
	bm.size = blockSize * blockCount
	bm.blockSize = blockSize
	bm.blockCount = blockCount
	bm.blockCache = cache
	bm.writeQueue = make(map[int64][]byte)
	bm.concurrentWrites = 4
	bm.concurrentReads = 4
	bm.maxWriteQueue = 16
	bm.syncInterval = 5 * time.Second
	bm.lastFlush = time.Now().UTC()
	return
}

func (self BlockMap) ReadBlockAt(blockId int64, b []byte, off int64, length int64) (err error) {
	// off is offset within this block
	block, ok := self.writeQueue[blockId]
	if !ok {
		cachedBlock, ok := self.blockCache.Get(blockId)
		if !ok {
			block, err = self.blockProvider.Read(blockId)
			if err != nil {
				return
			}
			self.blockCache.Add(blockId, block)
		} else {
			block = cachedBlock.([]byte)
		}
	}

	if int64(len(b)) != length {
		err = errors.New(fmt.Sprintf("ReadBlockAt(): len(b) = %d, length = %d, len(b) != length",
			len(b), length))
		return
	}

	if int64(len(block)) < (off + length) {
		err = errors.New(fmt.Sprintf("ReadBlockAt(): len(block) = %d, off + length = %d, len(block) < (off + length)",
			len(block), off + length))
		return
	}

	copy(b[:], block[off:(off + length)])
	return
}

func (self BlockMap) ReadAt(b []byte, off int64) (n int, err error) {
	fmt.Printf("ReadAt(): off = %d, len = %d\n", off, len(b))
	self.ioLock.Lock()
	defer self.ioLock.Unlock()

	left := off
	right := left + int64(len(b))
	p := off

	var wg sync.WaitGroup

	for p < right {
		blockId := p / self.blockSize
		blockOffset := p - blockId * self.blockSize
		blockCopyLen := right - p

		if blockOffset + blockCopyLen > self.blockSize {
			blockCopyLen = self.blockSize - blockOffset
		}

		wg.Add(1)
		func(blockId int64, block []byte, blockOffset int64, blockCopyLen int64) {
			err := self.ReadBlockAt(blockId, block, blockOffset, blockCopyLen)
			if err != nil {
				panic(err)
			}
			wg.Done()
		}(blockId, b[(p-off):(p-off+blockCopyLen)], blockOffset, blockCopyLen)

		p = p + blockCopyLen
	}
	wg.Wait()

	return
}

func (self BlockMap) WriteBlockAt(blockId int64, b []byte, off int64, length int64) (err error) {
	// off is offset within this block
	block, ok := self.writeQueue[blockId]
	if !ok {
		cachedBlock, ok := self.blockCache.Get(blockId)
		if !ok {
			block, err = self.blockProvider.Read(blockId)
			if err != nil {
				return
			}
		} else {
			block = cachedBlock.([]byte)
		}
	}

	copy(block[off:(off + length)], b[:])
	self.writeQueue[blockId] = block
	self.blockCache.Remove(blockId)
	return
}


func (self BlockMap) WriteAt(b []byte, off int64) (n int, err error) {
	fmt.Printf("Write(): off = %d, len = %d\n", off, len(b))
	self.ioLock.Lock()
	defer self.ioLock.Unlock()

	left := off
	right := left + int64(len(b))
	p := off

	var wg sync.WaitGroup

	for p < right {
		blockId := p / self.blockSize
		blockOffset := p - blockId * self.blockSize
		blockCopyLen := right - p

		if blockOffset + blockCopyLen > self.blockSize {
			blockCopyLen = self.blockSize - blockOffset
		}

		wg.Add(1)
		func(blockId int64, block []byte, blockOffset int64, blockCopyLen int64) {
			err := self.WriteBlockAt(blockId, block, blockOffset, blockCopyLen)
			if err != nil {
				panic(err)
			}
			wg.Done()
		}(blockId, b[(p-off):(p-off+blockCopyLen)], blockOffset, blockCopyLen)

		p = p + blockCopyLen
	}
	wg.Wait()

	if len(self.writeQueue) > self.maxWriteQueue {
		err = self.Flush()
	}

	return
}

func (self BlockMap) Flush() (err error) {
	err = nil
	fmt.Printf("Flush()\n")

	var wg sync.WaitGroup
	c := make(chan UploadTask)

	for i := 0; i < self.concurrentWrites; i++ {
        wg.Add(1)
        go func(c chan UploadTask) {
        	for task := range c {
        		err := self.blockProvider.Write(task.blockId, task.block)
				if err != nil {
					panic(err)
				}
        	}
        	wg.Done()
    	}(c)
    }

	for blockId, block := range self.writeQueue {
		c <- UploadTask{blockId: blockId, block: block}
		delete(self.writeQueue, blockId)
	}
	close(c)
	wg.Wait()
	self.lastFlush = time.Now().UTC()

	return

}

func (self BlockMap) Sync() (err error) {
	fmt.Printf("Sync()\n")
	self.ioLock.Lock()
	defer self.ioLock.Unlock()

	err = self.Flush()
	return
}

func (self BlockMap) SyncIfTimeout() (err error) {
	fmt.Printf("SyncIfTimeout()\n")
	self.ioLock.Lock()
	defer self.ioLock.Unlock()
	err = nil

	if time.Now().UTC().Sub(self.lastFlush) > self.syncInterval && len(self.writeQueue) > 0 {
		err = self.Flush()
	}
	return
}

func (self BlockMap) Loop(device *nbd.NBD) (err error) {
	go func(device *nbd.NBD) {
		if device.Loop(); err != nil {
			panic(err)
		}
	}(device)

	for {
		err := self.SyncIfTimeout()
		if err != nil {
			panic(err)
		}
		time.Sleep(5 * time.Second)
	}
}
