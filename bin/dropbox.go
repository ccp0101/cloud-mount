
package main

import (
	"flag"
	"fmt"
	"os"
	"errors"
	"strings"
	"bytes"
	"strconv"
	"net/http"
	"encoding/json"
	"io/ioutil"
	"github.com/youtube/vitess/go/cache"
	"github.com/ccp0101/go-nbd"
)

type DropboxDevice struct {
	client http.Client
	token string
	folder string
	size int64
	cacheSize int64
	blockSize int64
	blockCount int64
	blockCache *cache.LRUCache
}

func (self DropboxDevice) Initialize() (err error) {
	self.client = http.Client{}
	self.blockCache = cache.NewLRUCache(self.cacheSize / self.blockSize)
	err = self.VerifyToken()
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

	args := make(map[string]string)
	args["path"] = self.GetBlockPath(blockId)
	args["mode"] = "overwrite"
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

func (self DropboxDevice) ReadAt(b []byte, off int64) (n int, err error) {
	fmt.Printf("ReadAt(): off = %d, len = %d\n", off, len(b))

	length := int64(len(b))

	blockId := off / self.blockSize
	off = off - blockId * self.blockSize

	var block []byte
	cacheKey := strconv.Itoa(int(blockId))
	cachedBlock, ok := self.blockCache.Get(cacheKey)
	if !ok {
		block, err = self.DownloadBlock(blockId)
		if err != nil {
			panic(err)
		}
		self.blockCache.Set(cacheKey, block)
	} else {
		block = cachedBlock.([]byte)
	}
	copy(b[:], block[off:(off + length)])
	return
}

func (self DropboxDevice) WriteAt(b []byte, off int64) (n int, err error) {
	fmt.Printf("Write(): off = %d, len = %d\n", off, len(b))

	length := int64(len(b))

	blockId := off / self.blockSize
	off = off - blockId * self.blockSize

	block, err := self.DownloadBlock(blockId)
	if err != nil {
		panic(err)
	}
	copy(block[off:(off + length)], b)
	err = self.UploadBlock(blockId, block)
	if err != nil {
		panic(err)
	}
	return
}

func (self DropboxDevice) Sync() (err error) {
	fmt.Printf("Sync()\n")
	err = nil
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

	dropbox := new(DropboxDevice)
	dropbox.token = *token
	dropbox.folder = *folder
	dropbox.size = int64(*size)
	dropbox.blockSize = int64(*blockSize)
	dropbox.cacheSize = int64(*cacheSize)
	dropbox.blockCount = int64(*size / *blockSize)

	if err := dropbox.Initialize(); err != nil {
		panic(err)
	}

	ndbDevice := nbd.Create(dropbox, dropbox.size)
	dev, err := ndbDevice.Connect()

	if err != nil {
		panic(err)
	}

	fmt.Println("NBD device: ", dev)

	if ndbDevice.Loop(); err != nil {
		panic(err)
	}
}
