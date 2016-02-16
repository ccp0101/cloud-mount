
package main

import (
	"flag"
	"fmt"
	"os"
	"errors"
	"sync"
	"time"
	"strings"
	"bytes"
	"net/http"
	"encoding/json"
	"io/ioutil"
	"golang.org/x/build/internal/lru"
	"github.com/ccp0101/go-nbd"
)

type DropboxDevice struct {
	client http.Client
	ioLock *sync.Mutex
	token string
	folder string
	size int64
	blockSize int64
	blockCount int64
	blockCache interface {
		Get(interface{}) (interface{}, bool)
		Add(interface{}, interface{})
	}
	maxWriteQueue int
	writeQueue map[int64][]byte
	concurrentWrites int
	lastFlush time.Time
}

type UploadTask struct {
	blockId int64
	block []byte
}

func NewDropboxDevice(token string, folder string, blockSize int64,
	blockCount int64, cache *lru.Cache) (dropbox DropboxDevice, err error) {
	dropbox = DropboxDevice{}
	dropbox.client = http.Client{}
	dropbox.token = token
	dropbox.folder = folder
	dropbox.size = blockSize * blockCount
	dropbox.blockSize = blockSize
	dropbox.blockCount = blockCount
	dropbox.blockCache = cache
	dropbox.writeQueue = make(map[int64][]byte)
	dropbox.concurrentWrites = 1
	dropbox.maxWriteQueue = 4
	dropbox.ioLock = &sync.Mutex{}
	dropbox.lastFlush = time.Now().UTC()

	err = dropbox.VerifyToken()
	return
}

func (self DropboxDevice) AddAuthHeader(req *http.Request) () {
	bearer := fmt.Sprintf("Bearer %s", self.token)
	req.Header.Set("Authorization", bearer)
	return
}

func (self DropboxDevice) GetBlockPath(blockId int64) (string) {
	return fmt.Sprintf("/%s/%d.block", self.folder, blockId)
}

func (self DropboxDevice) VerifyToken() (err error) {
	// {"account_id": "dbid:AACkeCpR5QaPcUdHQfeqIaRlkj0ngu6wy7w", "name": {"given_name": "Changping", "surname": "Chen", "familiar_name": "Changping", "display_name": "Changping Chen"}, "email": "i@ccp.li", "email_verified": true, "country": "US", "locale": "en", "referral_link": "https://db.tt/5Cbpg2f3", "is_paired": true, "account_type": {".tag": "basic"}}
	fmt.Printf("Verifying Dropbox account...")
	url := "https://api.dropboxapi.com/2/users/get_current_account"
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return err
	}
	self.AddAuthHeader(req)

	resp, err := self.client.Do(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var data map[string]interface{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		return err
	}

	fmt.Printf("Dropbox account ID: %s\n", data["account_id"])

	return err
}

func (self DropboxDevice) DownloadBlock(blockId int64) (b []byte, err error) {
	url := "https://content.dropboxapi.com/2/files/download"

	args := make(map[string]string)
	args["path"] = self.GetBlockPath(blockId)
	argsHeader, err := json.Marshal(&args)
	if err != nil {
		panic(err)
		return
	}

	fmt.Printf("DownloadBlock(): %s\n", args["path"])

	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return
	}

	req.Header.Set("Dropbox-API-Arg", string(argsHeader))
	self.AddAuthHeader(req)

	resp, err := self.client.Do(req)
	if err != nil {
		return
	}

	var data map[string]interface{}

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	if resp.StatusCode == 200 {
		err = json.Unmarshal([]byte(resp.Header.Get("Dropbox-API-Result")), &data)
		if err != nil {
			return
		}

		if int64(len(b)) != self.blockSize {
			err = errors.New(fmt.Sprintf("%s has the wrong size. ", args["path"]))
			return
		}
	} else {
		err = json.Unmarshal(b, &data)
		if err != nil {
			return
		}

		if errorSummary, ok := data["error_summary"]; ok {
			if strings.Contains(errorSummary.(string), "not_found") {
				b = make([]byte, self.blockSize)
				return
			}
			err = errors.New(errorSummary.(string))
		} else {
			err = errors.New("Dropbox API error.")
		}
	}
	return
}


func (self DropboxDevice) UploadBlock(blockId int64, b []byte) (err error) {
	url := "https://content.dropboxapi.com/2/files/upload"

	args := make(map[string]interface{})
	args["path"] = self.GetBlockPath(blockId)
	args["mode"] = "overwrite"
	args["mute"] = true
	argsHeader, err := json.Marshal(&args)
	if err != nil {
		panic(err)
		return
	}

	fmt.Printf("UploadBlock(): %s\n", args["path"])

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(b))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Dropbox-API-Arg", string(argsHeader))
	self.AddAuthHeader(req)

	resp, err := self.client.Do(req)
	if err != nil {
		return
	}

	var data map[string]interface{}

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	err = json.Unmarshal(b, &data)
	if err != nil {
		return
	}

	if errorSummary, ok := data["error_summary"]; ok {
		err = errors.New(errorSummary.(string))
	}
	return
}

func (self DropboxDevice) ReadBlockAt(blockId int64, b []byte, off int64, length int64) (err error) {
	// off is offset within this block
	block, ok := self.writeQueue[blockId]
	if !ok {
		cachedBlock, ok := self.blockCache.Get(blockId)
		if !ok {
			block, err = self.DownloadBlock(blockId)
			if err != nil {
				return
			}
			self.blockCache.Add(blockId, block)
		} else {
			block = cachedBlock.([]byte)
		}
	}

	copy(b[:], block[off:(off + length)])
	return
}

func (self DropboxDevice) ReadAt(b []byte, off int64) (n int, err error) {
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
		func() {
			err := self.ReadBlockAt(blockId, b[(p-off):(p-off+blockCopyLen)], blockOffset, blockCopyLen)
			if err != nil {
				panic(err)
			}
			wg.Done()
		}()

		p = p + blockCopyLen
	}
	wg.Wait()

	return
}

func (self DropboxDevice) WriteBlockAt(blockId int64, b []byte, off int64, length int64) (err error) {
	// off is offset within this block
	cachedBlock, ok := self.blockCache.Get(blockId)
	var block []byte
	if !ok {
		block, err = self.DownloadBlock(blockId)
		if err != nil {
			return
		}
	} else {
		block = cachedBlock.([]byte)
	}
	newBlock := make([]byte, self.blockSize)
	copy(newBlock[:off], block[:off])
	copy(newBlock[off:(off + length)], b[:])
	copy(newBlock[(off + length):], block[(off + length):])
	self.writeQueue[blockId] = newBlock
	self.blockCache.Add(blockId, newBlock)
	return
}


func (self DropboxDevice) WriteAt(b []byte, off int64) (n int, err error) {
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
		func() {
			err := self.WriteBlockAt(blockId, b[(p-off):(p-off+blockCopyLen)], blockOffset, blockCopyLen)
			if err != nil {
				panic(err)
			}
			wg.Done()
		}()

		p = p + blockCopyLen
	}
	wg.Wait()

	if len(self.writeQueue) > self.maxWriteQueue {
		err = self.Flush()
	}

	return
}

func (self DropboxDevice) Flush() (err error) {
	err = nil
	fmt.Printf("Flush()\n")

	var wg sync.WaitGroup
	c := make(chan UploadTask)

	for i := 0; i < self.concurrentWrites; i++ {
        wg.Add(1)
        go func(c chan UploadTask) {
        	for task := range c {
        		err := self.UploadBlock(task.blockId, task.block)
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

func (self DropboxDevice) Sync() (err error) {
	fmt.Printf("Sync()\n")
	self.ioLock.Lock()
	defer self.ioLock.Unlock()

	err = self.Flush()
	return
}

func (self DropboxDevice) SyncIfTimeout() (err error) {
	fmt.Printf("SyncIfTimeout()\n")
	self.ioLock.Lock()
	defer self.ioLock.Unlock()
	err = nil

	if time.Now().UTC().Sub(self.lastFlush).Seconds() > 5.0 && len(self.writeQueue) > 0 {
		err = self.Flush()
	}
	return
}

func main() {
	token := flag.String("token", "", "oauth2 access token")
	folder := flag.String("folder", "", "dropbox folder")
	size := flag.Int("size", 0, "total storage size in bytes")
	blockSize := flag.Int("bs", 4 * 1024 * 1024, "block size")
	cacheSize := flag.Int("cache", 128 * 1024 * 1024, "cache size in bytes")

	flag.Parse()

	if *token == "" || *folder == "" || *size == 0 {
		flag.Usage()
		os.Exit(1)
	}

	cache := lru.New(*cacheSize / *blockSize)
	dropbox, err := NewDropboxDevice(*token, *folder, int64(*blockSize),
		int64(*size / *blockSize), cache)

	if err != nil {
		panic(err)
	}

	ndbDevice := nbd.Create(dropbox, dropbox.size)
	dev, err := ndbDevice.Connect()

	if err != nil {
		panic(err)
	}

	fmt.Println("NBD device: ", dev)

	go func(ndbDevice *nbd.NBD) {
		if ndbDevice.Loop(); err != nil {
			panic(err)
		}
	}(ndbDevice)

	for {
		err := dropbox.SyncIfTimeout()
		if err != nil {
			panic(err)
		}
		time.Sleep(5 * time.Second)
	}	
}
