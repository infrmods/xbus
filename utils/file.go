package utils

import (
	"os"
)

func WriteFile(path string, perm os.FileMode, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	return err
}
