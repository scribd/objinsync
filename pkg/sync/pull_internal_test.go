package sync

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/assert"
)

func TestSkipParentDir(t *testing.T) {
	p := NewPuller()
	p.taskQueue = make(chan DownloadTask, 10)
	p.handlePageList(
		&s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				&s3.Object{
					Key:  aws.String("home"),
					ETag: aws.String("1"),
				},
				&s3.Object{
					Key:  aws.String("home/"),
					ETag: aws.String("1"),
				},
			},
		},
		false,
		"foo",
		"home",
		"abc",
	)
	close(p.taskQueue)

	cnt := 0
	for _ = range p.taskQueue {
		cnt += 1
	}
	assert.Equal(t, 0, cnt)
}

func TestDeleteStaleFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	nonEmptyDir := filepath.Join(dir, "bar")
	os.MkdirAll(nonEmptyDir, os.ModePerm)
	fileA := filepath.Join(nonEmptyDir, "a.go")
	err = ioutil.WriteFile(fileA, []byte("test"), 0644)
	assert.Equal(t, nil, err)
	deletedFileA := filepath.Join(nonEmptyDir, "a.deleted.go")
	err = ioutil.WriteFile(deletedFileA, []byte("test"), 0644)
	assert.Equal(t, nil, err)

	fileB := filepath.Join(dir, "b.file")
	err = ioutil.WriteFile(fileB, []byte("test2"), 0644)
	assert.Equal(t, nil, err)
	deletedFileB := filepath.Join(dir, "b.delted.file")
	err = ioutil.WriteFile(deletedFileB, []byte("test2"), 0644)
	assert.Equal(t, nil, err)

	p := NewPuller()
	p.taskQueue = make(chan DownloadTask, 10)
	p.filesToDelete, err = listAndPruneDir(dir, nil)
	assert.Equal(t, nil, err)

	cnt := 0
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for _ = range p.taskQueue {
			cnt += 1
		}
		wg.Done()
	}()

	p.handlePageList(
		&s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				&s3.Object{
					Key:  aws.String("home/dags/b.file"),
					ETag: aws.String("1"),
				},
				&s3.Object{
					Key:  aws.String("home/dags/bar/a.go"),
					ETag: aws.String("1"),
				},
			},
		},
		false,
		"foo",
		"home/dags",
		dir,
	)
	close(p.taskQueue)
	wg.Wait()

	assert.Equal(t, 2, cnt)
	for f, _ := range p.filesToDelete {
		os.Remove(f)
	}

	for _, f := range []string{deletedFileA, deletedFileB} {
		_, err = os.Stat(f)
		assert.NotEqual(t, nil, err)
	}
	for _, f := range []string{dir, fileA, fileB} {
		_, err = os.Stat(f)
		assert.Equal(t, nil, err)
	}

	assert.Equal(t, 2, p.fileListedCnt)
	assert.Equal(t, 2, p.filePulledCnt)
}

func TestSkipObjectsWithoutChange(t *testing.T) {
	p := NewPuller()
	p.taskQueue = make(chan DownloadTask, 10)
	p.uidCache["s3://foo/home/dags/b.file"] = "1"

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// drain queue
		for _ = range p.taskQueue {
		}
		wg.Done()
	}()

	p.handlePageList(
		&s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				&s3.Object{
					Key:  aws.String("home/dags/b.file"),
					ETag: aws.String("1"),
				},
				&s3.Object{
					Key:  aws.String("home/dags/bar/a.go"),
					ETag: aws.String("1"),
				},
			},
		},
		false,
		"foo",
		"home/dags",
		"bar",
	)
	close(p.taskQueue)
	wg.Wait()

	assert.Equal(t, 2, p.fileListedCnt)
	assert.Equal(t, 1, p.filePulledCnt)
}

func TestSkipExcludedObjects(t *testing.T) {
	p := NewPuller()
	p.taskQueue = make(chan DownloadTask, 10)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// drain queue
		for _ = range p.taskQueue {
		}
		wg.Done()
	}()

	p.AddExcludePattern("airflow.cfg")
	p.AddExcludePattern("webserver_config.py")
	p.AddExcludePattern("config/**")
	p.handlePageList(
		&s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				&s3.Object{
					Key:  aws.String("home/dags/b.file"),
					ETag: aws.String("1"),
				},
				&s3.Object{
					Key:  aws.String("home/airflow.cfg"),
					ETag: aws.String("2"),
				},
				&s3.Object{
					Key:  aws.String("home/config/a.file"),
					ETag: aws.String("3"),
				},
				&s3.Object{
					Key:  aws.String("home/config/subdir/a.file"),
					ETag: aws.String("4"),
				},
				&s3.Object{
					Key:  aws.String("home/webserver_config.py"),
					ETag: aws.String("5"),
				},
			},
		},
		false,
		"foo",
		"home",
		"bar",
	)
	close(p.taskQueue)
	wg.Wait()

	assert.Equal(t, 1, p.fileListedCnt)
	assert.Equal(t, 1, p.filePulledCnt)
}

func TestSkipDirectories(t *testing.T) {
	p := NewPuller()
	p.taskQueue = make(chan DownloadTask, 10)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// drain queue
		for _ = range p.taskQueue {
		}
		wg.Done()
	}()

	p.handlePageList(
		&s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				&s3.Object{
					Key:  aws.String("home/dags/foo/bar/"),
					ETag: aws.String("1"),
				},
				&s3.Object{
					Key:  aws.String("home/dags/foo/bar/a.go"),
					ETag: aws.String("1"),
				},
			},
		},
		false,
		"foo",
		"home/dags",
		"bar",
	)
	close(p.taskQueue)
	wg.Wait()

	assert.Equal(t, 1, p.fileListedCnt)
	assert.Equal(t, 1, p.filePulledCnt)
}

type MockDownloader struct{}

func (self MockDownloader) Download(w io.WriterAt, input *s3.GetObjectInput, options ...func(*s3manager.Downloader)) (int64, error) {
	return 1, nil
}

func TestNestedPathDownload(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	assert.Equal(t, nil, err)
	defer os.RemoveAll(dir)

	mockDownloader := MockDownloader{}

	p := NewPuller()
	p.errMsgQueue = make(chan string, 30)

	p.downloadHandler(
		DownloadTask{
			Uri:       "s3://abc/efg/123/foo/",
			LocalPath: filepath.Join(dir, "123", "foo"),
			Uid:       "uid",
		},
		mockDownloader)
	p.downloadHandler(
		DownloadTask{
			Uri:       "s3://abc/efg/123/foo/bar",
			LocalPath: filepath.Join(dir, "123", "foo", "bar"),
			Uid:       "uid",
		},
		mockDownloader)
	close(p.errMsgQueue)

	messages := []string{}
	for msg := range p.errMsgQueue {
		messages = append(messages, msg)
	}

	assert.Equal(t, []string{}, messages)

	fi, err := os.Stat(filepath.Join(dir, "123", "foo", "bar"))
	assert.Equal(t, nil, err)
	assert.Equal(t, false, fi.IsDir())

	fi, err = os.Stat(filepath.Join(dir, "123"))
	assert.Equal(t, nil, err)
	assert.Equal(t, true, fi.IsDir())

	fi, err = os.Stat(filepath.Join(dir, "123", "foo"))
	assert.Equal(t, true, fi.IsDir())
	assert.Equal(t, nil, err)
}
