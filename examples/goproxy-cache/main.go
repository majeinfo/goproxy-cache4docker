package main

import (
	"flag"
	"log"
	"net/http"
	_ "bytes"
	"io/ioutil"
	"regexp"
	"strings"
	"os"
	"io"
	"fmt"
	"sync"
	"errors"
	"time"
	"github.com/elazarl/goproxy"
)

const (
	// Constants for cache entry status
	EMPTY = 0
	AVAILABLE = 1
	IN_PROGRESS = 2
)

var (
	re = regexp.MustCompile("/registry-v2/docker/registry/v2/blobs/sha256/../(?P<shaname>[^/]+)/")
	cachedir string
	cache map[string]int
	m sync.RWMutex
)

type cacheTeeReader struct {
	shaname string
	fname string
	f *os.File
	body io.ReadCloser
	ctx *goproxy.ProxyCtx
	nbread int
	nbwritten int
}

func newCacheTeeReader(shaname string, body io.ReadCloser, ctx *goproxy.ProxyCtx) (*cacheTeeReader, error) {
	ctx.Logf("Create new TEE for %s", shaname)
	tee := cacheTeeReader{
		shaname: shaname,
		fname: cachedir + shaname,
		ctx: ctx,
		body: body,
		f: nil,
		nbread: 0,
		nbwritten: 0,
	}

	f, err := os.Create(tee.fname)
	if err != nil {
		ctx.Warnf("Could not open file %s inwrite mode", tee.fname)
		ctx.Warnf("%s", err)
		return nil, errors.New("Could not open file")
	}
	tee.f = f

	return &tee, nil
}

func (tee *cacheTeeReader) Read(p []byte) (n int, err error) {
	//tee.ctx.Logf("cacheTeeReader/Read for %s", tee.shaname)
	nread, err := tee.body.Read(p)
	tee.nbread += nread
	if nread > 0 {
		nbytes, err2 := tee.f.Write(p[:nread])
		//tee.ctx.Logf("nread=%d, nwritten=%d", nread, nbytes)
		tee.nbwritten += nbytes
		if err2 != nil {
			tee.ctx.Warnf("Error writing in file %s", tee.fname)
		}
	}

	return nread, err
}

func (tee *cacheTeeReader) Close() error {
	tee.ctx.Logf("cacheTeeReader/Close for %s (nbread=%d, nbwritten=%d)", tee.shaname, tee.nbread, tee.nbwritten)
	tee.f.Close()
	m.Lock()
	cache[tee.shaname] = AVAILABLE
	defer m.Unlock()

	return nil
}

func cacheInit() {
	// Assume cachedir ends with a /
	if !strings.HasSuffix(cachedir, "/") {
		cachedir = cachedir + "/"
	}

	if stat, err := os.Stat(cachedir); err != nil || !stat.IsDir() {
		fmt.Printf("Directory %s does not exists - try to create it\n", cachedir)
		if err := os.Mkdir(cachedir, 0755); err != nil {
			fmt.Printf("Cannot create directory %s\n", cachedir)
			log.Fatal(err)
		}
	} else {
		fmt.Printf("Directory %s exists\n", cachedir)
	}

	// Load the cache
	cache = make(map[string]int)
	files, err := ioutil.ReadDir(cachedir)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		fmt.Printf("cache: %s\n", file.Name())
		cache[file.Name()] = AVAILABLE
	}
}

// Returns "" or the name of the layer
func shouldBeCached(urlpath string, ctx *goproxy.ProxyCtx) string {
	ctx.Logf("shouldBeCached: %s", urlpath)
	if res := re.FindStringSubmatch(urlpath); res != nil && len(res) > 1 {
		ctx.Logf("....yes....: %s", res[1])
		return res[1]
	}

	ctx.Logf("....no....")
	return ""
}

func cacheExistsFor(blob string, ctx *goproxy.ProxyCtx) bool {
	fname := cachedir + blob
	if _, err := os.Stat(fname); os.IsNotExist(err) {
		ctx.Logf("File %s does not exist", fname)
		return false
	}

	return true
}

func CacheReqHandler(req *http.Request, ctx *goproxy.ProxyCtx) (*http.Request, *http.Response) {
	ctx.Logf("CacheReqHandler")

    	if shaname := shouldBeCached(req.URL.Path, ctx); shaname != "" {
		ctx.Logf("Check Cache for %s", shaname)
		m.Lock()
		in_cache := cache[shaname]

		// Wait for other download
		for in_cache == IN_PROGRESS { 
			ctx.Logf("Locked on cache in progress")
			m.Unlock()
			time.Sleep(2 * time.Second)
			m.Lock()
			in_cache = cache[shaname]
		}

		if in_cache == AVAILABLE {
			m.Unlock()
			ctx.Logf("Cache Exists: return it !")

			f, err := os.Open(cachedir + shaname)
			if err != nil {
				ctx.Warnf("Cannot open and read file %s", cachedir + shaname)
				return req, nil
			}
			fi, err := os.Stat(cachedir + shaname)
			if err != nil {
				ctx.Warnf("Cannot stat file %s", cachedir + shaname)
				return req, nil
			}

			resp := &http.Response{}
			resp.Request = req
			resp.TransferEncoding = req.TransferEncoding
			resp.Header = make(http.Header)
			resp.Header.Add("Content-Type", "application/octet-stream")
			resp.StatusCode = 200
			resp.ContentLength = fi.Size()
			resp.Body = ioutil.NopCloser(f)
			return req, resp
		} else {
			ctx.Logf("Not in cache")
			cache[shaname] = IN_PROGRESS
			m.Unlock()
		}
	}

	return req, nil
}

func CacheRespHandler(resp *http.Response, ctx *goproxy.ProxyCtx) *http.Response {
	ctx.Logf("CacheRespHandler")
	fmt.Println(resp.Header)

	// Note: resp contains resp.request
	if resp.StatusCode != 200 {
		return resp
	}

    	if shaname := shouldBeCached(resp.Request.URL.Path, ctx); shaname != "" {
		m.Lock()
		in_cache := cache[shaname]
		m.Unlock()

		if in_cache == AVAILABLE {
			ctx.Logf("%s already in cache", shaname)
		} else {
			ctx.Logf("Should set in Cache: %s", resp.Request.URL.Path)
			ctx.Logf("shaname=%s", shaname)

			tee, err := newCacheTeeReader(shaname, resp.Body, ctx)
			if err == nil {
				resp.Body = tee
			} else {
				ctx.Warnf("newCacheTeeReader failed")
			}
		}
	}
	
	return resp
}

func main() {
	verbose := flag.Bool("v", false, "should every proxy request be logged to stdout")
	addr := flag.String("addr", ":8080", "proxy listen address")
	flag.StringVar(&cachedir, "d", "/tmp/proxy", "directory where to store cache")
	flag.Parse()
	cacheInit()
	setCA(caCert, caKey)
	proxy := goproxy.NewProxyHttpServer()
	proxy.OnRequest(goproxy.ReqHostMatches(regexp.MustCompile("^index.docker.io$"))).HandleConnect(goproxy.AlwaysMitm)
	proxy.OnRequest().Do(goproxy.FuncReqHandler(CacheReqHandler))
	proxy.OnRequest().HandleConnect(goproxy.AlwaysMitm)
	proxy.OnResponse().Do(goproxy.FuncRespHandler(CacheRespHandler))
	proxy.Verbose = *verbose
	log.Fatal(http.ListenAndServe(*addr, proxy))
}
