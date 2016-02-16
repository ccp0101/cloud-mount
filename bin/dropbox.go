
package main

import (
	"flag"
	"fmt"
	"os"
	"errors"
	"strings"
	"bytes"
	"net/http"
	"encoding/json"
	"io/ioutil"
	"blockmap"
	"github.com/hashicorp/golang-lru"
	"github.com/ccp0101/go-nbd"
)

type Dropbox struct {
	client http.Client
	token string
	folder string
	blockSize int64
}

func NewDropbox(token string, folder string, blockSize int64) (dropbox Dropbox, err error) {
	dropbox = Dropbox{}
	dropbox.client = http.Client{}
	dropbox.token = token
	dropbox.folder = folder
	dropbox.blockSize = blockSize
	err = dropbox.VerifyToken()
	return
}

func (self Dropbox) AddAuthHeader(req *http.Request) () {
	bearer := fmt.Sprintf("Bearer %s", self.token)
	req.Header.Set("Authorization", bearer)
	return
}

func (self Dropbox) GetBlockPath(id int64) (string) {
	return fmt.Sprintf("/%s/%d.block", self.folder, id)
}

func (self Dropbox) VerifyToken() (err error) {
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

func (self Dropbox) DownloadBlock(id int64) (b []byte, err error) {
	url := "https://content.dropboxapi.com/2/files/download"

	args := make(map[string]string)
	args["path"] = self.GetBlockPath(id)
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


func (self Dropbox) UploadBlock(id int64, b []byte) (err error) {
	url := "https://content.dropboxapi.com/2/files/upload"

	args := make(map[string]interface{})
	args["path"] = self.GetBlockPath(id)
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

func (self Dropbox) Read(id int64) (b []byte, err error) {
	b, err = self.DownloadBlock(id)
	return
}

func (self Dropbox) Write(id int64, b []byte) (err error) {
	err = self.UploadBlock(id, b)
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

	dropbox, err := NewDropbox(*token, *folder, int64(*blockSize))

	if err != nil {
		panic(err)
	}

	cache, err := lru.New(*cacheSize / *blockSize)
	bm := blockmap.NewBlockMap(dropbox, int64(*blockSize),
		int64(*size) / int64(*blockSize), cache)

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
