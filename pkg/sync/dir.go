package sync

import (
	"os"
	"path/filepath"

	"github.com/bmatcuk/doublestar"
	"go.uber.org/zap"
)

// This function finds all files in a given directory and return them in a map.
// It also purges empty directories.
//
// file map contains absolute path
// it won't include directories in the returned map
func listAndPruneDir(dirname string, exclude []string) (map[string]bool, error) {
	l := zap.S()
	files := make(map[string]bool)
	dirsToDelete := make(map[string]bool)

	err := filepath.Walk(dirname, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// ignore file that matches exclude rules
		shouldSkip := false
		relPath, err := filepath.Rel(dirname, path)
		if err != nil {
			l.Errorf("Got invalid path from filepath.Walk: %s, err: %s", path, err)
			shouldSkip = true
		} else {
			if info.IsDir() {
				// this is so that pattern `foo/**` also matches `foo`
				relPath += "/"
			}
			for _, pattern := range exclude {
				matched, _ := doublestar.Match(pattern, relPath)
				if matched {
					shouldSkip = true
					break
				}
			}
		}

		if info.IsDir() {
			if shouldSkip {
				return filepath.SkipDir
			}
			// don't delete root even if it's empty
			if path != dirname {
				dirsToDelete[path] = true
			}
		} else {
			if shouldSkip {
				return nil
			}
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
