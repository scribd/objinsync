package sync

import (
	"os"
	"path/filepath"
	"strings"
)

// file list contains absolute path
// it won't include directories in the returned map
func listAndPruneDir(dirname string) (map[string]bool, error) {
	files := make(map[string]bool)
	dirsToDelete := make(map[string]bool)

	err := filepath.Walk(dirname, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if strings.Contains(path, "__pycache__") {
				// ignore python cache files for sync
				return filepath.SkipDir
			}
			// don't delete root even if it's empty
			if path != dirname {
				dirsToDelete[path] = true
			}
		} else {
			parentDir := filepath.Dir(path)
			if _, ok := dirsToDelete[parentDir]; ok {
				// mark dir as not empty
				delete(dirsToDelete, parentDir)
			}
			files[path] = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// delete empty dirs
	for d, _ := range dirsToDelete {
		os.Remove(d)
	}

	return files, nil
}
