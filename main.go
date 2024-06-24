package config

import (
	"embed"
	"io/fs"
)

//go:embed *.json
var content embed.FS

func Open(path string) (fs.File, error) {
	return content.Open(path)
}

func ReadFile(path string) ([]byte, error) {
	return content.ReadFile(path)
}
