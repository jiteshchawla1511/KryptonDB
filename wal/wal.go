package wal

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	lsmtree "github.com/jiteshchawla1511/KryptonDB/LSM_Tree"
)

type Entry struct {
	Key    string `json:"k"`
	Value  string `json:"v"`
	Delete bool   `json:"-"`
}

const DefaultWalPath = "wal.aof"

type WAL struct {
	filepath string
	file     *os.File
	writer   *bufio.Writer
	lock     sync.Mutex
}

func InitWal(path string) *WAL {
	var file *os.File

	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		file, err = os.Create(path)
		if err != nil {
			panic(err)
		}
	} else {
		file, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
	}

	writer := bufio.NewWriter(file)
	wal := &WAL{
		filepath: path,
		file:     file,
		writer:   writer,
	}
	return wal
}

func (w *WAL) Write(data ...[]byte) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if len(data) > w.writer.Available() {
		err := w.writer.Flush()
		if err != nil {
			fmt.Println(err)
			return err
		}
	}

	limit := []byte("|")

	for _, d := range data {
		d = append(d, limit...)
		_, err := w.writer.Write(d)
		if err != nil {
			return err
		}
	}
	w.writer.WriteString("\n")
	return nil
}

func (w *WAL) Persist() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	err := w.writer.Flush()
	if err != nil {
		return err
	}

	err = w.file.Sync()
	if err != nil {
		return err
	}

	w.writer.Reset(w.file)
	return nil
}

func (w *WAL) ReadEntries() []Entry {
	w.lock.Lock()
	defer w.lock.Unlock()

	file, err := os.OpenFile(w.filepath, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}

	reader := bufio.NewReader(file)

	if err != nil {
		panic(err)
	}

	data, err := io.ReadAll(reader)

	if err != nil {
		panic(err)
	}

	cmds := strings.Split(string(data), "\n")

	entries := make([]Entry, 0, len(cmds))

	for _, cmd := range cmds {
		if cmd == "" {
			continue
		}

		args := strings.Split(cmd, "|")

		switch args[0] {
		case "+":
			if len(args) != 4 {
				continue
			}
			entries = append(entries, Entry{Key: args[1], Value: args[2], Delete: false})
		case "-":
			if len(args) != 3 {
				continue
			}
			entries = append(entries, Entry{Key: args[1], Delete: true})
		}
	}

	return entries
}

func (w *WAL) InitDB(lsmTree *lsmtree.LSMTree) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	file, err := os.OpenFile(w.filepath, os.O_RDONLY, 0644)

	if err != nil {
		return err
	}

	reader := bufio.NewReader(file)

	if err != nil {
		return err
	}

	data, err := io.ReadAll(reader)

	if err != nil {
		return err
	}

	cmds := strings.Split(string(data), "\n")

	for _, cmd := range cmds {
		if cmd == "" {
			continue
		}

		args := strings.Split(cmd, "|")

		switch args[0] {
		case "+":
			lsmTree.Put(args[1], args[2])
		case "-":
			lsmTree.Del(args[1])
		}
	}

	return nil
}

func (w *WAL) Truncate() {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.file.Truncate(0)
	w.file.Seek(0, 0)
}
