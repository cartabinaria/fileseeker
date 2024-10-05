package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/charmbracelet/log"
)

type teachings []struct {
	Url string `json:"url"`
}

type statikNode struct {
	statikDirectory
	Directories []statikDirectory `json:"directories"`
	Files       []statikFile      `json:"files"`
}

type statikDirectory struct {
	Url         string    `json:"url"`
	Time        time.Time `json:"time"`
	GeneratedAt time.Time `json:"generated_at"`
	Name        string    `json:"name"`
	Path        string    `json:"path"`
	SizeRaw     string    `json:"size"`
}

type statikFile struct {
	Name    string    `json:"name"`
	Path    string    `json:"path"`
	Url     string    `json:"url"`
	Mime    string    `json:"mime"`
	SizeRaw string    `json:"size"`
	Time    time.Time `json:"time"`
}

var (
	dataDir    = flag.String("d", "data", "data directory")
	configFile = flag.String("c", "config/teachings.json", "config file")
)

func main() {
	flag.Parse()

	log.Info("Starting fileseeker...")

	log.Debug("Loading teachings...")

	teachingData, err := loadTeachings(*configFile)
	if err != nil {
		log.Errorf("Failed to load teachings: %v", err)
		os.Exit(1)
	}

	urlQueue := make([]string, 0)

	const rootUrl = "https://csunibo.github.io"

	// enqueue teachings
	for _, teaching := range teachingData {
		url := fmt.Sprintf("%s/%s", rootUrl, teaching.Url)
		urlQueue = append(urlQueue, url)
	}
	log.Debug("Enqueued teachings", "len", len(urlQueue))

	// walk the tree

	for len(urlQueue) > 0 {
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
			path = filepath.Join(*dataDir, path)

			pathLogger := log.With("path", path)

			pathLogger.Debug("Downloading", "url", url)

			// create folder if not exists
			// write file
			// if file exists, check if remote file is newer
			// create file
			err := downloadStatikFile(path, url, f.Time)

			if err == upToDate {
				pathLogger.Info("Up to date")
			} else if err != nil {
				pathLogger.Info("Failed", "err", err)
			} else {
				pathLogger.Info("Downloaded")
			}
		}
	}
}

var upToDate = errors.New("up to date")

func downloadStatikFile(localPath string, url string, lastModified time.Time) error {

	// if file already exists, check if remote file is newer. if not, return
	stat, err := os.Stat(localPath)
	if err == nil {
		localModTime := stat.ModTime()

		if lastModified.Before(localModTime) {
			return upToDate
		}
	}

	// create directory if not exists
	dir := filepath.Dir(localPath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// download file
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to fetch %s: %w", url, err)
	}

	rBody := resp.Body

	fp, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", localPath, err)
	}

	_, err = fp.ReadFrom(rBody)
	if err != nil {
		return fmt.Errorf("failed to write file %s: %w", localPath, err)
	}

	err = fp.Close()
	if err != nil {
		return fmt.Errorf("failed to close file %s: %w", localPath, err)
	}

	err = rBody.Close()
	if err != nil {
		return fmt.Errorf("failed to close response body: %w", err)
	}

	return nil
}

func loadTeachings(teachingsFile string) (teachings, error) {
	f, err := os.Open(teachingsFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", teachingsFile, err)
	}

	var config teachings
	if err := json.NewDecoder(f).Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to decode file %s: %w", teachingsFile, err)
	}
	return config, nil
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
