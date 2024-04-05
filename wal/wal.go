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
