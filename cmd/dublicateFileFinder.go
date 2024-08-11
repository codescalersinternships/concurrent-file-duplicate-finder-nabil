package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"hash"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
)

func readWorker(idx int, bufferSize int, file *os.File, hash hash.Hash) (string, error) {
	if _, err := file.Seek(int64(idx)*int64(bufferSize), io.SeekStart); err != nil {
		return "", fmt.Errorf("error seeking to start byte: %e", err)
	}
	buffer := make([]byte, bufferSize)
	_, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		return "", fmt.Errorf("error reading file: %e", err)
	}
	return string(hash.Sum(buffer[:bufferSize])), nil
}

func readFile(filePath string) (string, error) {
	bufferSize := 4096
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("cann't open file err: %e", err)
	}
	defer file.Close()
	hash := md5.New()
	fileInfo, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("error getting file stats: %e", err)
	}
	fileSize := fileInfo.Size()
	numberOfIterations := int(math.Ceil(float64(fileSize) / float64(bufferSize)))
	var hashChunks = make([]string, numberOfIterations)
	var readFileWG sync.WaitGroup
	for i := 0; i < numberOfIterations; i++ {
		readFileWG.Add(1)
		var errF error
		go func(idx int) {
			defer readFileWG.Done()
			hashChunks[idx], err = readWorker(idx, bufferSize, file, hash)
			if err != nil {
				errF = err
			}
		}(i)
		if errF != nil {
			return "", errF
		}
	}
	readFileWG.Wait()
	fileHash := ""
	for _, h := range hashChunks {
		fileHash += h
	}
	return fileHash, nil
}

var wg sync.WaitGroup

func traverse(dirPath string, freqHash map[string][]string, isRoot bool) error {

	content, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}
	for _, entry := range content {
		nxtPath := filepath.Join(dirPath, entry.Name())
		if entry.IsDir() {
			wg.Add(1)
			go func(path string) {
				defer wg.Done()
				errlocal := traverse(path, freqHash, false)
				if errlocal != nil {
					err = errlocal
				}
			}(nxtPath)
			if err != nil {
				return err
			}
		} else {
			hash, err := readFile(nxtPath)
			if err != nil {
				return err
			}
			freqHash[hash] = append(freqHash[hash], nxtPath)
		}
	}
	if isRoot {
		wg.Done()
	}
	return nil
}
func main() {
	flag.Parse()
	if len(os.Args) <= 1 {
		log.Fatal("No directory passed")
		os.Exit(1)
	}
	var dirPath string = os.Args[len(os.Args)-1]
	files := make(map[string][]string)
	wg.Add(1)
	err := traverse(dirPath, files, true)
	if err != nil {
		fmt.Print(err)
	}
	wg.Wait()
	fmt.Println(files)
}
