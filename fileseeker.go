package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"time"

	cparser "github.com/cartabinaria/config-parser-go"
	"github.com/charmbracelet/log"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/net/webdav"
)

type Config struct {
	UpdateFrequency    time.Duration
	UpdateFrequencyStr string `toml:"update_frequency"`
	Port               int    `toml:"port"`
	DataPath           string `toml:"data_path"`
	BaseUrl            string `toml:"base_url"`
	CartaBinariaUrl    string `toml:"carta_binaria_url"`
	TeachingsPath      string `toml:"teachings_path"`
}

/* load toml config file */
func loadConfig() (*Config, error) {
	file, err := os.Open(*configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file %s: %w", *configPath, err)
	}

	var cfg Config
	err = toml.NewDecoder(file).Decode(&cfg)
	if err != nil {
		return nil, fmt.Errorf("error while parsing config file: %w", err)
	}

	// Parse period
	cfg.UpdateFrequency, err = time.ParseDuration(cfg.UpdateFrequencyStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse update_frequency: %w", err)
	} else if cfg.UpdateFrequency <= 0 {
		return nil, errors.New("update_frequency must be greater than 0")
	}

	return &cfg, nil
}

type (
	statikNode struct {
		statikDirectory
		Directories []statikDirectory `json:"directories"`
		Files       []statikFile      `json:"files"`
	}

	statikDirectory struct {
		Url         string    `json:"url"`
		Time        time.Time `json:"time"`
		GeneratedAt time.Time `json:"generated_at"`
		Name        string    `json:"name"`
		Path        string    `json:"path"`
		SizeRaw     string    `json:"size"`
	}

	statikFile struct {
		Name    string    `json:"name"`
		Path    string    `json:"path"`
		Url     string    `json:"url"`
		Mime    string    `json:"mime"`
		SizeRaw string    `json:"size"`
		Time    time.Time `json:"time"`
	}
)

type lockedFs struct {
	fs webdav.FileSystem
}

func (lfs *lockedFs) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	_, ok := mutexMap[name]
	if !ok {
		var lock sync.RWMutex
		mutexMap[name] = &lock
	}
	mutexMap[name].RLock()

	file, err := lfs.fs.OpenFile(ctx, name, flag, perm)
	if err != nil {
		mutexMap[name].RUnlock()
		return nil, err
	}

	return &lockedFile{file: file, name: name}, nil
}

func (lfs *lockedFs) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	return errors.New("read-only filesystem: Mkdir is not allowed")
}

func (lfs *lockedFs) Rename(ctx context.Context, oldName, newName string) error {
	return errors.New("read-only filesystem: Rename is not allowed")
}

func (lfs *lockedFs) RemoveAll(ctx context.Context, name string) error {
	return errors.New("read-only filesystem: RemoveAll is not allowed")
}

func (lfs *lockedFs) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	return lfs.fs.Stat(ctx, name)
}

// needed to release RWMutex ()
type lockedFile struct {
	file webdav.File
	name string
}

func (lf *lockedFile) Close() error {
	_, ok := mutexMap[lf.name]
	if !ok {
		var lock sync.RWMutex
		mutexMap[lf.name] = &lock
	}
	err := lf.file.Close()
	mutexMap[lf.name].RUnlock()
	return err
}

func (lf *lockedFile) Read(p []byte) (n int, err error) { return lf.file.Read(p) }

func (lf *lockedFile) Write(p []byte) (n int, err error) {
	return 0, errors.New("read-only filesystem: Write is not allowed")
}

func (lf *lockedFile) Seek(offset int64, whence int) (int64, error) {
	return lf.file.Seek(offset, whence)
}

func (lf *lockedFile) Stat() (fs.FileInfo, error) { return lf.file.Stat() }

func (lf *lockedFile) Readdir(count int) ([]fs.FileInfo, error) { return lf.file.Readdir(count) }

/* middleware to allow only read-only requests */
func readonlyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET", "PROPFIND", "HEAD", "OPTIONS":
			next.ServeHTTP(w, r)
		default:
			http.Error(w, "405: Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

var (
	configPath = flag.String("c", "config.toml", "config path")
	verbose    = flag.Bool("v", false, "verbose logging")
	mutexMap   = make(map[string]*sync.RWMutex)
)

func main() {
	flag.Parse()

	if *verbose {
		log.SetLevel(log.DebugLevel)
	}

	log.Info("Starting fileseeker...")

	globalCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)

	/* load config */
	config, err := loadConfig()
	if err != nil {
		log.Errorf("Failed to load config: %v", err)
		os.Exit(1)
	}

	log.Debug("Loading teachings...")
	teachingData, err := cparser.ParseTeachings()
	if err != nil {
		log.Errorf("Failed to load teachings: %v", err)
		os.Exit(1)
	}

	/* run statikBFS */
	go func() {

		ticker := time.NewTicker(config.UpdateFrequency)
		defer ticker.Stop()

		log.Info("Starting periodic updates...", "frequency", config.UpdateFrequency)

		// first run
		statikBFS(config, teachingData)

		for {
			select {
			case <-globalCtx.Done():
				log.Info("Shutting down periodic updates...")
				return
			case <-ticker.C:
				statikBFS(config, teachingData)
			}
		}
	}()

	/* file system */
	lfs := &lockedFs{fs: webdav.Dir(config.DataPath)}

	log.Info("Setting up webdav server...")

	/* webdav server setup */
	webdavHandler := &webdav.Handler{
		FileSystem: lfs,
		LockSystem: webdav.NewMemLS(),
		Prefix:     "/",
		Logger: func(r *http.Request, err error) {
			if err != nil {
				log.Printf("WEBDAV [%s]: %s, ERROR: %s", r.Method, r.URL, err)
			} else {
				log.Printf("WEBDAV [%s]: %s", r.Method, r.URL)
			}
		},
	}

	mux := http.NewServeMux()
	mux.Handle("/", readonlyMiddleware(webdavHandler))

	log.Info("Starting server", "port", config.Port)

	srv := &http.Server{Addr: fmt.Sprintf(":%d", config.Port), Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("Failed to start server: %v", err)
			cancel() // cancel the context to stop the server
		}
	}()

	// wait for shutdown signal, then clean up
	<-globalCtx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)

	log.Info("Shutting down server...")
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Errorf("Error shutting down server: %v", err)
	}
	shutdownCancel()
	log.Info("Server shutdown complete")
}

func statikBFS(config *Config, teachingData []cparser.Teaching) {

	urlQueue := make([]string, 0)

	rootUrl := config.CartaBinariaUrl

	// enqueue teachings
	for _, teaching := range teachingData {
		url := fmt.Sprintf("%s/%s", rootUrl, teaching.Url)
		urlQueue = append(urlQueue, url)
	}
	log.Debug("Enqueued teachings", "len", len(urlQueue))

	// walk the tree
	for len(urlQueue) > 0 {

		// breath-first search (FIFO)
		statikUrl := urlQueue[0]
		urlQueue = urlQueue[1:]

		// get statik.json
		node, err := getStatik(fmt.Sprintf("%s/statik.json", statikUrl))
		if err != nil {
			log.Errorf("Failed to get statik.json: %v", err)
			continue
		}

		// enqueue directories
		for _, d := range node.Directories {
			subUrl := fmt.Sprintf("%s/%s", statikUrl, d.Name)
			urlQueue = append(urlQueue, subUrl)
		}

		// download files
		for _, f := range node.Files {
			time.Sleep(2 * time.Millisecond)

			url := fmt.Sprintf("%s/%s", statikUrl, f.Name)

			path := strings.TrimPrefix(url, rootUrl)
			path = filepath.Join(config.DataPath, path)

			pathLogger := log.With("queued_urls", len(urlQueue), "path", path)

			pathLogger.Debug("Downloading", "url", url)

			// create folder if not exists
			// write file
			// if file exists, check if remote file is newer
			// create file
			err := downloadStatikFile(path, url, f.Time)

			if err == upToDate {
				pathLogger.Debug("Up to date")
			} else if err != nil {
				pathLogger.Debug("Failed", "err", err)
			} else {
				pathLogger.Debug("Downloaded")
			}
		}
	}
}

var upToDate = errors.New("up to date")

func downloadStatikFile(localPath string, url string, lastModified time.Time) error {
	_, ok := mutexMap[localPath]

	if !ok {
		var lock sync.RWMutex
		mutexMap[localPath] = &lock
	}
	mutexMap[localPath].Lock()

	// if file already exists, check if remote file is newer. if not, return
	stat, err := os.Stat(localPath)
	if err == nil {
		localModTime := stat.ModTime()

		if lastModified.Before(localModTime) {
			mutexMap[localPath].Unlock()
			return upToDate
		}
	}

	// create directory if not exists
	dir := filepath.Dir(localPath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			mutexMap[localPath].Unlock()
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// download file
	resp, err := http.Get(url)
	if err != nil {
		mutexMap[localPath].Unlock()
		return fmt.Errorf("failed to fetch %s: %w", url, err)
	}

	rBody := resp.Body

	fp, err := os.Create(localPath)
	if err != nil {
		mutexMap[localPath].Unlock()
		return fmt.Errorf("failed to create file %s: %w", localPath, err)
	}

	_, err = fp.ReadFrom(rBody)
	if err != nil {
		mutexMap[localPath].Unlock()
		return fmt.Errorf("failed to write file %s: %w", localPath, err)
	}

	err = fp.Close()
	if err != nil {
		mutexMap[localPath].Unlock()
		return fmt.Errorf("failed to close file %s: %w", localPath, err)
	}

	err = rBody.Close()
	if err != nil {
		mutexMap[localPath].Unlock()
		return fmt.Errorf("failed to close response body: %w", err)
	}

	mutexMap[localPath].Unlock()

	return nil
}

func getStatik(url string) (statikNode, error) {
	resp, err := http.Get(url)
	if err != nil {
		return statikNode{}, fmt.Errorf("failed to fetch statik.json %s: %w", url, err)
	}

	if resp.StatusCode != http.StatusOK {
		return statikNode{}, fmt.Errorf("failed to fetch statik.json %s: %s", url, resp.Status)
	}

	var statik statikNode
	err = json.NewDecoder(resp.Body).Decode(&statik)
	if err != nil {
		return statikNode{}, fmt.Errorf("failed to decode statik.json: %w", err)
	}

	err = resp.Body.Close()
	if err != nil {
		return statikNode{}, fmt.Errorf("failed to close response body: %w", err)
	}

	return statik, nil
}
