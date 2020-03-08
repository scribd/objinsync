package sync

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/bmatcuk/doublestar"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	metricsFileListed = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "objinsync",
		Subsystem: "pull",
		Name:      "files_listed",
		Help:      "Number of files checked in each pull cycle.",
	})

	metricsFilePulled = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "objinsync",
		Subsystem: "pull",
		Name:      "files_pulled",
		Help:      "Number of files pulled in each pull cycle.",
	})

	metricsFileDeleted = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "objinsync",
		Subsystem: "pull",
		Name:      "files_deleted",
		Help:      "Number of files deleted in each pull cycle.",
	})
)

func init() {
	prometheus.MustRegister(metricsFileListed)
	prometheus.MustRegister(metricsFilePulled)
	prometheus.MustRegister(metricsFileDeleted)
}

type GenericDownloader interface {
	Download(io.WriterAt, *s3.GetObjectInput, ...func(*s3manager.Downloader)) (int64, error)
}

type DownloadTask struct {
	Uri       string
	LocalPath string
	Uid       string
}

func parseObjectUri(uri string) (string, string, error) {
	parts := strings.SplitN(uri, "//", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("URL is not a valid object URL")
	}

	path := parts[1]
	pathParts := strings.SplitN(path, "/", 2)
	if len(pathParts) != 2 {
		return "", "", fmt.Errorf("URL is not a valid object URL")
	}

	return pathParts[0], pathParts[1], nil
}

func (self *Puller) downloadHandler(task DownloadTask, downloader GenericDownloader) {
	l := zap.S()

	if strings.HasSuffix(task.Uri, "/") {
		// skip directories from S3
		return
	}

	bucket, key, err := parseObjectUri(task.Uri)
	if err != nil {
		self.errMsgQueue <- fmt.Sprintf("Got invalid remote uri %s: %v", task.Uri, err)
		return
	}

	// create parent dir if not exists
	parentDir := filepath.Dir(task.LocalPath)
	if _, err := os.Stat(parentDir); os.IsNotExist(err) {
		err = os.MkdirAll(parentDir, os.ModePerm)
		if err != nil {
			self.errMsgQueue <- fmt.Sprintf(
				"Failed to create directory %s for %s: %v", parentDir, task.LocalPath, err)
			return
		}
	}

	// create file
	file, err := os.OpenFile(task.LocalPath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		self.errMsgQueue <- fmt.Sprintf("Failed to create file %s for download: %v", task.LocalPath, err)
		return
	}
	defer file.Close()
	file.Truncate(0)
	file.Seek(0, 0)

	downloader.Download(file, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	// update cache with new object ID
	self.uidLock.Lock()
	l.Debugw("Updaing uid cache", "key", task.Uri, "val", task.Uid)
	self.uidCache[task.Uri] = task.Uid
	defer self.uidLock.Unlock()
}

func (self *Puller) handlePageList(
	page *s3.ListObjectsV2Output,
	lastPage bool,
	bucket string,
	remoteDirPath string,
	localDir string,
) bool {
	l := zap.S()

	l.Infof("Object list page contains %d objects.", len(page.Contents))
	for _, obj := range page.Contents {
		key := *(obj.Key)
		// For directories, S3 returns keys with / suffix
		if strings.HasSuffix(key, "/") {
			l.Debugf("Skipping directory: %s", key)
			continue
		}

		newUid := *(obj.ETag)
		uri := fmt.Sprintf("s3://%s/%s", bucket, key)
		l.Debugf("Processing obj(%s): %s", newUid, uri)

		relPath, err := filepath.Rel(remoteDirPath, key)
		if err != nil {
			l.Errorf("skipped %s, %s is not the parent of %s!", uri, remoteDirPath, key)
			continue
		}
		// ignore file that matches exclude rules
		shouldSkip := false
		for _, pattern := range self.exclude {
			matched, _ := doublestar.Match(pattern, relPath)
			if matched {
				l.Debugf("skipped %s due to exclude pattern: %s", uri, pattern)
				shouldSkip = true
				break
			}
		}
		if shouldSkip {
			continue
		}

		// remove file from purge list
		localPath := filepath.Join(localDir, relPath)
		l.Debugf("Remove %s from files to delete", localPath)
		delete(self.filesToDelete, localPath)

		if relPath == "" || relPath == "/" || relPath == "." {
			// skip parent dir itself
			continue
		}

		self.fileListedCnt += 1

		self.uidLock.Lock()
		oldUid, ok := self.uidCache[uri]
		self.uidLock.Unlock()
		l.Debugf("Comparing object UID: %s <> %s = %v", oldUid, newUid, oldUid == newUid)
		if ok && oldUid == newUid {
			// skip update if uid is the same
			continue
		}

		self.filePulledCnt += 1
		self.taskQueue <- DownloadTask{
			Uri:       uri,
			LocalPath: localPath,
			Uid:       newUid,
		}
	}
	return true
}

type Puller struct {
	exclude     []string
	workerCnt   int
	uidCache    map[string]string
	uidLock     *sync.Mutex
	taskQueue   chan DownloadTask
	errMsgQueue chan string
	// Here is how filesToDelete is being used:
	//
	// 1. before each pull action, we populate filesToDelete with all files
	// (without dirs) from local target directory. During this process, we also
	// delete local empty directories.
	//
	// 2. we list S3 bucket, for any file in the bucket, we remove related
	// entry from the delete list
	//
	// 3. at the end of the pull, we delete files from the list
	filesToDelete map[string]bool
	fileListedCnt int
	filePulledCnt int
}

func (self *Puller) AddExcludePattern(pattern string) {
	self.exclude = append(self.exclude, pattern)
}

func (self *Puller) Pull(remoteUri string, localDir string) string {
	l := zap.S()

	filesToDelete, err := listAndPruneDir(localDir, self.exclude)
	if err != nil {
		return fmt.Sprintf("Failed to list and prune local dir %s: %v", localDir, err)
	}
	// handlePageList method will remove files existed in remote source from this list
	self.filesToDelete = filesToDelete
	defer func() {
		self.filesToDelete = nil
	}()

	bucket, remoteDirPath, err := parseObjectUri(remoteUri)
	if err != nil {
		return fmt.Sprintf("Invalid remote uri %s: %v", remoteUri, err)
	}

	self.taskQueue = make(chan DownloadTask, 30)
	self.errMsgQueue = make(chan string, 30)

	sess := session.Must(session.NewSession())

	region := os.Getenv("AWS_REGION")
	if region == "" {
		var err error
		metaSvc := ec2metadata.New(sess)
		region, err = metaSvc.Region()
		if err != nil {
			return fmt.Sprintf("Failed to detect AWS region: %v", err)
		}
	}

	svc := s3.New(sess, aws.NewConfig().WithRegion(region))
	downloader := s3manager.NewDownloaderWithClient(svc)

	// spawn worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < self.workerCnt; i++ {
		wg.Add(1)
		go func(id int) {
			l.Debugf("Worker %d started", id)
			for task := range self.taskQueue {
				self.downloadHandler(task, downloader)
			}
			l.Debugf("Worker %d exited", id)
			wg.Done()
		}(i)
	}

	// spawn error message collector goroutine
	pullErrMsg := ""
	var errMsgWg sync.WaitGroup
	errMsgWg.Add(1)
	go func() {
		var messages []string
		for msg := range self.errMsgQueue {
			messages = append(messages, msg)
		}
		pullErrMsg = strings.Join(messages, "; ")
		errMsgWg.Done()
	}()

	l.Infow("Listing objects", "bucket", bucket, "dirpath", remoteDirPath)
	listParams := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(remoteDirPath),
	}
	self.fileListedCnt = 0
	self.filePulledCnt = 0

	err = svc.ListObjectsV2Pages(listParams,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			return self.handlePageList(page, lastPage, bucket, remoteDirPath, localDir)
		})
	close(self.taskQueue)
	wg.Wait()
	close(self.errMsgQueue)

	metricsFileListed.Set(float64(self.fileListedCnt))
	metricsFilePulled.Set(float64(self.filePulledCnt))

	if err != nil {
		return fmt.Sprintf("Failed to list remote uri %s: %v", remoteUri, err)
	} else {
		errMsgWg.Wait()

		l.Debugf("Files to delete: %s", self.filesToDelete)
		metricsFileDeleted.Set(float64(len(self.filesToDelete)))
		// delete files not exist in remote source
		for f, _ := range self.filesToDelete {
			os.Remove(f)
		}

		return pullErrMsg
	}
}

func NewPuller() *Puller {
	return &Puller{
		workerCnt: 5,
		uidCache:  map[string]string{},
		uidLock:   &sync.Mutex{},
	}
}
