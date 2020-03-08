package sync

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWalkAndDeleteEmptyDir(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	assert.Equal(t, nil, err)
	defer os.RemoveAll(dir)

	emptyDir := filepath.Join(dir, "empty")
	os.MkdirAll(emptyDir, os.ModePerm)

	nonEmptyDir := filepath.Join(dir, "bar")
	os.MkdirAll(nonEmptyDir, os.ModePerm)
	fileA := filepath.Join(nonEmptyDir, "a.go")
	err = ioutil.WriteFile(fileA, []byte("test"), 0644)
	assert.Equal(t, nil, err)

	fileB := filepath.Join(dir, "b.file")
	err = ioutil.WriteFile(fileB, []byte("test2"), 0644)
	assert.Equal(t, nil, err)

	files, err := listAndPruneDir(dir, nil)
	assert.Equal(t, nil, err)

	for _, f := range []string{fileA, fileB} {
		// make sure files are not delted
		_, err = os.Stat(f)
		assert.Equal(t, nil, err)

		_, hasF := files[f]
		assert.True(t, hasF)
	}

	// make sure empty dirs are deleted
	_, err = os.Stat(emptyDir)
	assert.NotEqual(t, nil, err)
}

func TestWalkAndExcludeDir(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	assert.Equal(t, nil, err)
	defer os.RemoveAll(dir)

	// __pycache__ in root dir
	emptyDir := filepath.Join(dir, "__pycache__")
	os.MkdirAll(emptyDir, os.ModePerm)

	// __pycache__ in sub dir
	nonEmptyDir := filepath.Join(dir, "foo")
	os.MkdirAll(nonEmptyDir, os.ModePerm)
	emptyDir2 := filepath.Join(nonEmptyDir, "__pycache__")
	os.MkdirAll(emptyDir2, os.ModePerm)

	nonEmptyDir = filepath.Join(dir, "bar")
	os.MkdirAll(nonEmptyDir, os.ModePerm)
	cacheDir := filepath.Join(nonEmptyDir, "__pycache__")
	os.MkdirAll(cacheDir, os.ModePerm)
	pycFile := filepath.Join(cacheDir, "foo.pyc")
	err = ioutil.WriteFile(pycFile, []byte("test2"), 0644)

	files, err := listAndPruneDir(dir, []string{"__pycache__/**"})
	assert.Equal(t, nil, err)
	assert.Equal(t, true, files[pycFile])

	// make sure empty pycache dir is ignored for deletion
	_, err = os.Stat(emptyDir)
	// should match and exclude __pycache__/
	assert.Equal(t, nil, err)
	// should not match and exclude foo/__pycache__/
	_, err = os.Stat(emptyDir2)
	assert.NotEqual(t, nil, err)
}

func TestWalkAndExcludeNestedDirs(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	assert.Equal(t, nil, err)
	defer os.RemoveAll(dir)

	// __pycache__ in root dir
	emptyDir := filepath.Join(dir, "__pycache__")
	os.MkdirAll(emptyDir, os.ModePerm)

	// __pycache__ in sub dir
	nonEmptyDir := filepath.Join(dir, "foo")
	os.MkdirAll(nonEmptyDir, os.ModePerm)
	emptyDir2 := filepath.Join(nonEmptyDir, "__pycache__")
	os.MkdirAll(emptyDir2, os.ModePerm)

	nonEmptyDir = filepath.Join(dir, "bar")
	os.MkdirAll(nonEmptyDir, os.ModePerm)
	cacheDir := filepath.Join(nonEmptyDir, "__pycache__")
	os.MkdirAll(cacheDir, os.ModePerm)
	pycFile := filepath.Join(cacheDir, "foo.pyc")
	err = ioutil.WriteFile(pycFile, []byte("test2"), 0644)

	files, err := listAndPruneDir(dir, []string{"**/__pycache__/**"})
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(files))

	// make sure empty pycache dir is ignored for deletion
	_, err = os.Stat(emptyDir)
	// should match and exclude __pycache__/
	assert.Equal(t, nil, err)
	// should also match and exclude foo/__pycache__/
	_, err = os.Stat(emptyDir2)
	assert.Equal(t, nil, err)
}

func TestWalkAndExcludeFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	assert.Equal(t, nil, err)
	defer os.RemoveAll(dir)

	nonEmptyDir := filepath.Join(dir, "foo")
	os.MkdirAll(nonEmptyDir, os.ModePerm)
	cacheDir := filepath.Join(nonEmptyDir, "__pycache__")
	os.MkdirAll(cacheDir, os.ModePerm)
	pycFile := filepath.Join(cacheDir, "foo.pyc")
	err = ioutil.WriteFile(pycFile, []byte("test2"), 0644)
	pyFile := filepath.Join(cacheDir, "foo.py")
	err = ioutil.WriteFile(pyFile, []byte("test2"), 0644)
	pyFile2 := filepath.Join(cacheDir, "bar.py")
	err = ioutil.WriteFile(pyFile2, []byte("test2"), 0644)

	files, err := listAndPruneDir(dir, []string{"foo/**/*.py"})
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(files))
	// all *.py file should be excluded
	assert.Equal(t, true, files[pycFile])
}
