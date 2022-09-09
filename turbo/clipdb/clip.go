package clipdb

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/bodgit/sevenzip"
	"github.com/ledgerwatch/erigon/common"
	"golang.org/x/sync/semaphore"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

func Uncompress(ctx context.Context, from, to string) ([]string, error) {
	if !common.FileExist(from) {
		return nil, errors.New(from + " not exist!")
	}
	if err := os.MkdirAll(to, 0744); nil != err {
		return nil, fmt.Errorf("create dir failed, %v", err)
	}

	r, err := sevenzip.OpenReader(from)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var errStr string
	errCH := make(chan error)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-errCH:
				if nil == err {
					return
				}
				errStr += e.Error()
			}
		}
	}()

	var files []string
	concur := runtime.NumCPU()
	seg := semaphore.NewWeighted(int64(concur))
	for i, f := range r.File {
		if err := seg.Acquire(ctx, 1); nil != err {
			return nil, err
		}
		go func(i int, f *sevenzip.File) {
			defer seg.Release(1)
			rc, err := f.Open()
			if err != nil {
				errCH <- err
				return
			}
			defer rc.Close()

			tmpF, err := os.Create(filepath.Join(to, f.Name))
			if nil != err {
				errCH <- err
				return
			}
			defer tmpF.Close()
			w := bufio.NewWriter(tmpF)
			if _, err := io.Copy(w, rc); err != nil {
				errCH <- err
				return
			}
			w.Flush()
		}(i, f)

		files = append(files, f.Name)
	}
	_ = seg.Acquire(ctx, int64(concur))
	close(errCH)

	if len(errStr) > 0 {
		return nil, errors.New(errStr)
	}
	return files, nil
}
