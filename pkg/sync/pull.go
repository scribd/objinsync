package sync

import (
	"crypto/md5"
	"encoding/hex"
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
	// uid key is common suffix between local path and remote uri
	UidKey string
}

// parse bucket and key out of remote object URI
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

func uidKeyFromLocalPath(localDir string, localPath string) (string, error) {
	return filepath.Rel(localDir, localPath)
}

func uidFromLocalPath(localPath string) (string, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return "", fmt.Errorf("Invalid file path for checksum calculation: %s, err: %s", localPath, err)
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("Failed to calculate checksum for file: %s, err: %s", localPath, err)
	}

	uid := hex.EncodeToString(h.Sum(nil))
	// AWS S3 ETag is a quoted hex string
	return fmt.Sprintf("\"%s\"", uid), nil
}

type Puller struct {
	RemoteUri  string
	LocalDir   string
	DisableSSL bool
	S3Endpoint string

	workingDir  string
	defaultMode os.FileMode
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
	tmpfileName := fmt.Sprintf("%x", md5.Sum([]byte(task.LocalPath)))
	tmpfilePath := filepath.Join(self.workingDir, tmpfileName)
	tmpfile, err := os.OpenFile(tmpfilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, self.defaultMode)
	if err != nil {
		self.errMsgQueue <- fmt.Sprintf("Failed to create temp file for download: %v", err)
		return
	}
	defer tmpfile.Close()

	downloader.Download(tmpfile, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	// use rename to make file update atomic
	err = os.Rename(tmpfilePath, task.LocalPath)
	if err != nil {
		self.errMsgQueue <- fmt.Sprintf("Failed to replace file %s for download: %v", task.LocalPath, err)
		return
	}

	// update cache with new object ID
	self.uidLock.Lock()
	l.Debugw("Updaing uid cache", "key", task.UidKey, "val", task.Uid)
	self.uidCache[task.UidKey] = task.Uid
	self.uidLock.Unlock()
}

func (self *Puller) isPathExcluded(path string) bool {
	for _, pattern := range self.exclude {
		matched, _ := doublestar.Match(pattern, path)
		if matched {
			return true
		}
	}
	return false
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
		shouldSkip := self.isPathExcluded(relPath)
		if shouldSkip {
			l.Debugf("skipped %s due to exclude pattern", uri)
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

		uidKey := relPath
		self.uidLock.Lock()
		oldUid, ok := self.uidCache[uidKey]
		self.uidLock.Unlock()
		l.Debugf("Comparing object UID: %s <> %s", oldUid, newUid)
		if ok && oldUid == newUid {
			// skip update if uid is the same
			continue
		}

		self.filePulledCnt += 1
		self.taskQueue <- DownloadTask{
			Uri:       uri,
			LocalPath: localPath,
			Uid:       newUid,
			UidKey:    uidKey,
		}
	}
	return true
}

func (self *Puller) AddExcludePatterns(patterns []string) {
	for _, pattern := range patterns {
		self.exclude = append(self.exclude, pattern)
	}
}

func (self *Puller) SetupWorkingDir() error {
	// create temporary working directory to hold downloads for atomic rename
	// TmpDir won't work because it could be in a different partition, which
	// will lead to invalid cross-device link error
	if _, err := os.Stat(self.workingDir); os.IsNotExist(err) {
		err = os.MkdirAll(self.workingDir, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *Puller) Pull() string {
	l := zap.S()

	filesToDelete, err := listAndPruneDir(self.LocalDir, self.exclude)
	if err != nil {
		return fmt.Sprintf("Failed to list and prune local dir %s: %v", self.LocalDir, err)
	}
	// handlePageList method will remove files existed in remote source from this list
	self.filesToDelete = filesToDelete
	defer func() {
		self.filesToDelete = nil
	}()

	bucket, remoteDirPath, err := parseObjectUri(self.RemoteUri)
	if err != nil {
		return fmt.Sprintf("Invalid remote uri %s: %v", self.RemoteUri, err)
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

	s3Config := &aws.Config{Region: aws.String(region)}
	if self.DisableSSL {
		s3Config.DisableSSL = aws.Bool(true)
	}
	if self.S3Endpoint != "" {
		s3Config.Endpoint = aws.String(self.S3Endpoint)
		s3Config.S3ForcePathStyle = aws.Bool(true)
	}
	svc := s3.New(sess, s3Config)

	downloader := s3manager.NewDownloaderWithClient(svc)

	if err := self.SetupWorkingDir(); err != nil {
		return fmt.Sprintf("Failed to create working directory %s: %v", self.workingDir, err)
	}
	defer os.RemoveAll(self.workingDir) // purge working dir when downlaods are done

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
			return self.handlePageList(page, lastPage, bucket, remoteDirPath, self.LocalDir)
		})
	close(self.taskQueue)
	wg.Wait()
	close(self.errMsgQueue)

	metricsFileListed.Set(float64(self.fileListedCnt))
	metricsFilePulled.Set(float64(self.filePulledCnt))

	if err != nil {
		return fmt.Sprintf("Failed to list remote uri %s: %v", self.RemoteUri, err)
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

func (self *Puller) PopulateChecksum() {
	l := zap.S()

	setFileChecksum := func(path string) {
		f, err := os.Open(path)
		if err != nil {
			l.Errorf("Invalid file path for checksum calculation: %s, err: %s", path, err)
		}
		defer f.Close()

		h := md5.New()
		if _, err := io.Copy(h, f); err != nil {
			l.Errorf("Failed to calculate checksum for file: %s, err: %s", path, err)
		}

		uidKey, err := uidKeyFromLocalPath(self.LocalDir, path)
		if err != nil {
			l.Errorf("Failed to calculate uidKey for file: %s under dir: %s, err: %s", path, self.LocalDir, err)
			return
		}

		uid, err := uidFromLocalPath(path)
		if err != nil {
			l.Errorf("Failed to calculate UID: %s", err)
			return
		}

		self.uidLock.Lock()
		self.uidCache[uidKey] = uid
		self.uidLock.Unlock()
	}

	err := filepath.Walk(self.LocalDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// ignore file that matches exclude rules
		shouldSkip := false
		relPath, err := filepath.Rel(self.LocalDir, path)
		if err != nil {
			l.Errorf("Got invalid path from filepath.Walk: %s, err: %s", path, err)
			shouldSkip = true
		} else {
			if info.IsDir() {
				// this is so that pattern `foo/**` also matches `foo`
				relPath += "/"
			}
			shouldSkip = self.isPathExcluded(relPath)
		}

		if info.IsDir() {
			if shouldSkip {
				return filepath.SkipDir
			}
		} else {
			if shouldSkip {
				return nil
			}

			setFileChecksum(path)
		}
		return nil
	})

	if err != nil {
		l.Errorf("Failed to walk directory for populating file checksum, err: %s", err)
	}
}

func (self *Puller) SetDefaultFileMode(mode os.FileMode) {
	self.defaultMode = mode
}

func NewPuller(remoteUri string, localDir string) (*Puller, error) {
	if _, err := os.Stat(localDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("local directory `%s` does not exist: %v", localDir, err)
	}

	return &Puller{
		RemoteUri:   remoteUri,
		LocalDir:    localDir,
		DisableSSL:  false,
		workingDir:  filepath.Join(localDir, ".objinsync"),
		defaultMode: 0664,
		workerCnt:   5,
		uidCache:    map[string]string{},
		uidLock:     &sync.Mutex{},
	}, nil
}
