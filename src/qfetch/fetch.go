package qfetch

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/qiniu/api.v6/auth/digest"
	"github.com/qiniu/api.v6/rs"
	"github.com/qiniu/log"
	"github.com/qiniu/rpc"
	"github.com/syndtr/goleveldb/leveldb"
)

var once sync.Once
var fetchTasks chan func()

func doFetch(tasks chan func()) {
	for {
		task := <-tasks
		task()
	}
}

func Fetch(mac *digest.Mac, job string, checkExists bool, fileListPath, bucket, accessKey, secretKey string,
	worker int, logFile string) {
	//open file list to fetch
	fh, openErr := os.Open(fileListPath)
	if openErr != nil {
		fmt.Println("open resource file error,", openErr)
		return
	}
	defer fh.Close()

	//try open log file
	if logFile != "" {
		logFh, openErr := os.Create(logFile)
		if openErr != nil {
			log.SetOutput(os.Stdout)
		} else {
			log.SetOutput(logFh)
			defer logFh.Close()
		}
	} else {
		log.SetOutput(os.Stdout)
		defer os.Stdout.Sync()
	}

	//open leveldb success and not found
	successLdbPath := fmt.Sprintf(".%s.job", job)
	notFoundLdbPath := fmt.Sprintf(".%s.404.job", job)

	successLdb, lerr := leveldb.OpenFile(successLdbPath, nil)
	if lerr != nil {
		fmt.Println("open fetch progress file error,", lerr)
		return
	}
	defer successLdb.Close()

	notFoundLdb, lerr := leveldb.OpenFile(notFoundLdbPath, nil)
	if lerr != nil {
		fmt.Println("open fetch not found file error,", lerr)
		return
	}
	defer notFoundLdb.Close()

	client := rs.New(mac)

	//init work group
	once.Do(func() {
		fetchTasks = make(chan func(), worker)
		for i := 0; i < worker; i++ {
			go doFetch(fetchTasks)
		}
	})

	fetchWaitGroup := sync.WaitGroup{}

	//scan each line and add task
	bReader := bufio.NewScanner(fh)
	bReader.Split(bufio.ScanLines)
	for bReader.Scan() {
		line := strings.TrimSpace(bReader.Text())
		if line == "" {
			continue
		}

		items := strings.Split(line, "\t")
		if !(len(items) == 1 || len(items) == 2) {
			log.Errorf("invalid resource line %s", line)
			continue
		}

		m3u8Url := items[0]
		m3u8Key := ""

		if len(items) == 1 {
			resUri, pErr := url.Parse(m3u8Url)
			if pErr != nil {
				log.Errorf("invalid resource url %s", m3u8Url)
				continue
			}
			m3u8Key = resUri.Path
			if strings.HasPrefix(m3u8Key, "/") {
				m3u8Key = m3u8Key[1:]
			}
		} else if len(items) == 2 {
			m3u8Key = items[1]
		}

		//check from leveldb success whether it is done
		val, exists := successLdb.Get([]byte(m3u8Url), nil)
		if exists == nil && string(val) == m3u8Key {
			log.Infof("skip m3u8 fetched %s => %s", m3u8Url, m3u8Key)
			continue
		}

		//check from leveldb not found whether it meet 404
		nfVal, nfExists := notFoundLdb.Get([]byte(m3u8Url), nil)
		if nfExists == nil && string(nfVal) == m3u8Key {
			log.Infof("skip m3u8 404 %s => %s", m3u8Url, m3u8Key)
			continue
		}

		//check whether file already exists in bucket
		if checkExists {
			if entry, err := client.Stat(nil, bucket, m3u8Key); err == nil && entry.Hash != "" {
				successLdb.Put([]byte(m3u8Url), []byte(m3u8Key), nil)
				log.Infof("skip m3u8 exists %s => %s", m3u8Url, m3u8Key)
				continue
			}
		}

		//otherwise fetch it
		fetchWaitGroup.Add(1)
		fetchTasks <- func() {
			defer fetchWaitGroup.Done()
			FetchM3u8(bucket, m3u8Key, m3u8Url, checkExists, &client, successLdb, notFoundLdb)
		}
	}

	//wait for all the fetch done
	fetchWaitGroup.Wait()
}

//fetch ts first and m3u8 later
func FetchM3u8(bucket, m3u8Key, m3u8Url string, checkExists bool, client *rs.Client,
	successLdb *leveldb.DB, notFoundLdb *leveldb.DB) {
	m3u8Uri, pErr := url.Parse(m3u8Url)
	if pErr != nil {
		log.Errorf("invalid m3u8 url, %s", pErr)
		return
	}

	m3u8Resp, respErr := http.Get(m3u8Url)
	if respErr != nil {
		fmt.Printf("get m3u8 error, %s", respErr)
		return
	}

	m3u8Data, readErr := ioutil.ReadAll(m3u8Resp.Body)
	if readErr != nil {
		m3u8Resp.Body.Close()
		log.Errorf("read m3u8 body error, %s", readErr)
		return
	}

	//close body
	m3u8Resp.Body.Close()

	//scan ts
	tsKeyList := make([]string, 0, 1000)
	var tsDomain string

	//scan m3u8 content
	bScanner := bufio.NewScanner(bytes.NewReader(m3u8Data))
	for bScanner.Scan() {
		m3u8Line := bScanner.Text()

		if !strings.HasPrefix(m3u8Line, "#") {
			//this is ts line

			var tsKey string
			if strings.HasPrefix(m3u8Line, "http://") || strings.HasPrefix(m3u8Line, "https://") {
				tsUri, pErr := url.Parse(m3u8Line)
				if pErr != nil {
					log.Errorf("invalid ts line, %s", m3u8Line)
					continue
				}

				tsKey = strings.TrimPrefix(tsUri.Path, "/")
				tsDomain = fmt.Sprintf("%s://%s", tsUri.Scheme, tsUri.Host)
			} else {
				if strings.HasPrefix(m3u8Line, "/") {
					tsKey = strings.TrimPrefix(m3u8Line, "/")
				} else {
					//check m3u8 url to find ts prefix
					tsKeyPrefix := strings.TrimPrefix(filepath.Dir(m3u8Uri.Path), "/")
					if tsKeyPrefix != "" {
						//Dir function removes the last / of the path, so there is a new / here
						tsKey = fmt.Sprintf("%s/%s", tsKeyPrefix, m3u8Line)
					} else {
						tsKey = m3u8Line
					}
				}
				tsDomain = fmt.Sprintf("%s://%s", m3u8Uri.Scheme, m3u8Uri.Host)
			}
			//append
			tsKeyList = append(tsKeyList, tsKey)
		}
	}

	//fetch all the ts files firsts
	var tsFetchHasError bool
	var tsFetchErrorCount int
	for _, tsKey := range tsKeyList {
		tsUrl := fmt.Sprintf("%s/%s", tsDomain, tsKey)
		log.Infof("fetch ts %s => %s doing", tsUrl, tsKey)

		//check from leveldb success whether it is done
		val, exists := successLdb.Get([]byte(tsUrl), nil)
		if exists == nil && string(val) == tsKey {
			log.Infof("skip ts fetched %s => %s", tsUrl, tsKey)
			continue
		}

		//check exists
		if checkExists {
			if entry, err := client.Stat(nil, bucket, tsKey); err == nil && entry.Hash != "" {
				successLdb.Put([]byte(tsUrl), []byte(tsKey), nil)
				log.Infof("skip ts exists %s => %s", tsUrl, tsKey)
				continue
			}
		}

		//fetch each ts
		_, fErr := client.Fetch(nil, bucket, tsKey, tsUrl)
		if fErr != nil {
			tsFetchHasError = true
			tsFetchErrorCount += 1
			log.Errorf("fetch ts %s error, %s", tsUrl, fErr)
		} else {
			log.Infof("fetch ts %s => %s success", tsUrl, tsKey)
			successLdb.Put([]byte(tsUrl), []byte(tsKey), nil)
		}
	}

	//fetch m3u8
	if tsFetchHasError {
		log.Errorf("fetch ts of m3u8 %s has %d errors", m3u8Url, tsFetchErrorCount)
		return
	}

	log.Infof("fetch m3u8 %s => %s doing", m3u8Url, m3u8Key)
	_, fErr := client.Fetch(nil, bucket, m3u8Key, m3u8Url)
	if fErr == nil {
		log.Infof("fetch m3u8 %s => %s success", m3u8Url, m3u8Key)
		successLdb.Put([]byte(m3u8Url), []byte(m3u8Key), nil)
	} else {
		if v, ok := fErr.(*rpc.ErrorInfo); ok {
			if v.Code == 404 {
				notFoundLdb.Put([]byte(m3u8Url), []byte(m3u8Key), nil)
			}
			log.Errorf("fetch m3u8 %s error, %s", m3u8Url, v.Err)
		} else {
			log.Errorf("fetch m3u8 %s error, %s", m3u8Url, fErr)
		}
	}

	return
}
