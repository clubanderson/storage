package file

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kubescape/storage/pkg/apis/softwarecomposition"
	"github.com/kubescape/storage/pkg/apis/softwarecomposition/v1beta1"
	"github.com/kubescape/storage/pkg/generated/clientset/versioned/scheme"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitemigration"
	"zombiezen.com/go/sqlite/sqlitex"
)

const (
	releaseTestBound       = 3 * time.Second
	releaseResponseBound   = 500 * time.Millisecond
	releaseRequestDeadline = 250 * time.Millisecond
	releaseStackCapacity   = 1 << 20
	releaseResourceRoot    = "/spdx.softwarecomposition.kubescape.io/sbomsyfts"
	releaseExistingKey     = releaseResourceRoot + "/test/existing"
)

func releaseObject(name string) *v1beta1.SBOMSyft {
	return &v1beta1.SBOMSyft{
		TypeMeta: metav1.TypeMeta{APIVersion: softwarecomposition.GroupName + "/v1beta1", Kind: "SBOMSyft"},
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "test", UID: types.UID("release-test-" + name),
			Labels: map[string]string{"revision": "original"},
		},
	}
}

func releaseFixture(t *testing.T, size int) (*StorageImpl, *sqlitemigration.Pool) {
	t.Helper()
	pool := NewPool(filepath.Join(t.TempDir(), "metadata.sq3"), size)
	require.NotNil(t, pool)
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	require.NoError(t, softwarecomposition.AddToScheme(scheme.Scheme))
	s, ok := NewStorageImpl(afero.NewMemMapFs(), DefaultStorageRoot, pool, nil, scheme.Scheme).(*StorageImpl)
	require.True(t, ok)
	ctx, cancel := context.WithTimeout(context.Background(), releaseTestBound)
	defer cancel()
	require.NoError(t, s.Create(ctx, releaseExistingKey, releaseObject("existing"), &v1beta1.SBOMSyft{}, 0))
	return s, pool
}

// Observe the actual blocked library call, rather than cancelling before the method starts.
func releaseAwaitFrame(t *testing.T, method, blocked string) {
	t.Helper()
	deadline := time.Now().Add(releaseTestBound)
	buf := make([]byte, releaseStackCapacity)
	for time.Now().Before(deadline) {
		n := goruntime.Stack(buf, true)
		for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
			if strings.Contains(stack, "(*StorageImpl)."+method+"(") && strings.Contains(stack, blocked) {
				return
			}
		}
		goruntime.Gosched()
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s did not reach %s", method, blocked)
}

func releaseUpdate(input runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
	obj, ok := input.(*v1beta1.SBOMSyft)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected update type %T", input)
	}
	next := obj.DeepCopy()
	next.Labels["revision"] = "updated"
	return next, nil, nil
}

func TestReleaseBackportBlockedAcquisitionCancellation(t *testing.T) {
	for _, method := range []string{"GetList", "Create", "GuaranteedUpdate"} {
		for _, deadline := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/deadline=%t", method, deadline), func(t *testing.T) {
				s, pool := releaseFixture(t, 1)
				held, err := pool.Take(context.Background())
				require.NoError(t, err)
				defer func() {
					if held != nil {
						pool.Put(held)
					}
				}()
				duration := releaseTestBound
				want := context.Canceled
				if deadline {
					duration = releaseRequestDeadline
					want = context.DeadlineExceeded
				}
				ctx, cancel := context.WithTimeout(context.Background(), duration)
				defer cancel()
				done := make(chan error, 1)
				go func() {
					switch method {
					case "GetList":
						done <- s.GetList(ctx, releaseResourceRoot, storage.ListOptions{Recursive: true}, &metav1.PartialObjectMetadataList{})
					case "Create":
						done <- s.Create(ctx, releaseResourceRoot+"/test/new", releaseObject("new"), &v1beta1.SBOMSyft{}, 0)
					case "GuaranteedUpdate":
						done <- s.GuaranteedUpdate(ctx, releaseExistingKey, &v1beta1.SBOMSyft{}, false, nil, releaseUpdate, nil)
					}
				}()
				releaseAwaitFrame(t, method, "sqlitemigration.(*Pool).Take(")
				if !deadline {
					cancel()
				}
				<-ctx.Done()
				select {
				case err = <-done:
					require.ErrorIs(t, err, want)
				case <-time.After(releaseResponseBound):
					pool.Put(held)
					held = nil
					select {
					case err = <-done:
					case <-time.After(releaseTestBound):
						t.Fatal("request remained blocked after returning the held connection")
					}
					t.Fatalf("already-blocked %s ignored %v; eventual error=%v", method, want, err)
				}
				pool.Put(held)
				held = nil
				background, backgroundCancel := context.WithTimeout(context.Background(), releaseTestBound)
				defer backgroundCancel()
				require.NoError(t, s.Create(background, releaseResourceRoot+"/test/background",
					releaseObject("background"), &v1beta1.SBOMSyft{}, 0))
				list := &metav1.PartialObjectMetadataList{}
				require.NoError(t, s.GetList(background, releaseResourceRoot, storage.ListOptions{Recursive: true}, list))
				require.Len(t, list.Items, 2)
				for _, item := range list.Items {
					require.NotEmpty(t, item.UID)
					require.NotEmpty(t, item.ResourceVersion)
				}
			})
		}
	}
}

func TestReleaseBackportKeyWaitDoesNotPinConnection(t *testing.T) {
	s, pool := releaseFixture(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), releaseTestBound)
	defer cancel()
	require.NoError(t, s.locks.Lock(ctx, releaseExistingKey))
	locked := true
	defer func() {
		if locked {
			s.locks.Unlock(releaseExistingKey)
		}
	}()
	done := make(chan error, 1)
	go func() { done <- s.Get(ctx, releaseExistingKey, storage.GetOptions{}, &v1beta1.SBOMSyft{}) }()
	releaseAwaitFrame(t, "Get", ".RLock(")
	writerCtx, writerCancel := context.WithTimeout(context.Background(), releaseResponseBound)
	defer writerCancel()
	conn, err := pool.Take(writerCtx)
	if err != nil {
		s.locks.Unlock(releaseExistingKey)
		locked = false
		select {
		case <-done:
		case <-time.After(releaseTestBound):
			t.Fatal("reader did not complete after key release")
		}
		t.Fatalf("reader waiting on the writer's key pinned the writer's only connection: %v", err)
	}
	pool.Put(conn)
	s.locks.Unlock(releaseExistingKey)
	locked = false
	require.NoError(t, <-done)
}

func TestReleaseBackportConnectionOwnerCanFinish(t *testing.T) {
	s, pool := releaseFixture(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), releaseTestBound)
	defer cancel()
	held, err := pool.Take(ctx)
	require.NoError(t, err)
	defer func() {
		if held != nil {
			pool.Put(held)
		}
	}()
	done := make(chan error, 1)
	go func() {
		done <- s.GuaranteedUpdate(ctx, releaseExistingKey, &v1beta1.SBOMSyft{}, false, nil, releaseUpdate, nil)
	}()
	releaseAwaitFrame(t, "GuaranteedUpdate", "sqlitemigration.(*Pool).Take(")
	require.NoError(t, s.GetWithConn(ctx, held, releaseExistingKey, storage.GetOptions{}, &v1beta1.SBOMSyft{}),
		"connection owner must be able to obtain the key and finish")
	pool.Put(held)
	held = nil
	require.NoError(t, <-done)
}

func TestReleaseBackportCancelledKeyWaitReturnsCause(t *testing.T) {
	s, _ := releaseFixture(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), releaseTestBound)
	defer cancel()
	require.NoError(t, s.locks.Lock(ctx, releaseExistingKey))
	defer s.locks.Unlock(releaseExistingKey)
	done := make(chan error, 1)
	go func() { done <- s.Get(ctx, releaseExistingKey, storage.GetOptions{}, &v1beta1.SBOMSyft{}) }()
	releaseAwaitFrame(t, "Get", ".RLock(")
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(releaseResponseBound):
		t.Fatal("cancelled key wait did not stop promptly")
	}
}

func TestReleaseBackportMetadataFailurePreservesPayload(t *testing.T) {
	s, pool := releaseFixture(t, 1)
	beforePayload, err := afero.ReadFile(s.appFs, makePayloadPath(filepath.Join(s.root, releaseExistingKey)))
	require.NoError(t, err)
	conn, err := pool.Take(context.Background())
	require.NoError(t, err)
	defer pool.Put(conn)
	beforeMetadata, err := ReadMetadata(conn, releaseExistingKey)
	require.NoError(t, err)
	require.NoError(t, sqlitex.Execute(conn, `CREATE TRIGGER reject_metadata BEFORE INSERT ON metadata
		BEGIN SELECT RAISE(ABORT, 'injected metadata failure'); END`, nil))
	next := releaseObject("existing")
	next.ResourceVersion = "1"
	next.Labels["revision"] = "new"
	require.Error(t, s.saveObject(conn, releaseExistingKey, next, &v1beta1.SBOMSyft{}, ""))
	afterPayload, err := afero.ReadFile(s.appFs, makePayloadPath(filepath.Join(s.root, releaseExistingKey)))
	require.NoError(t, err)
	require.Equal(t, beforePayload, afterPayload, "failed metadata write must not publish a new payload")
	afterMetadata, err := ReadMetadata(conn, releaseExistingKey)
	require.NoError(t, err)
	require.Equal(t, beforeMetadata, afterMetadata)
}

func TestReleaseBackportInvalidMetadataReturnsError(t *testing.T) {
	s, pool := releaseFixture(t, 1)
	conn, err := pool.Take(context.Background())
	require.NoError(t, err)
	require.NoError(t, sqlitex.Execute(conn, `UPDATE metadata SET metadata = 'not-json'`, nil))
	pool.Put(conn)
	ctx, cancel := context.WithTimeout(context.Background(), releaseTestBound)
	defer cancel()
	list := &metav1.PartialObjectMetadataList{}
	require.Error(t, s.GetList(ctx, releaseResourceRoot, storage.ListOptions{Recursive: true}, list),
		"bad metadata must not produce a success-shaped partial list")
}

var releaseInjectedIO = errors.New("injected payload I/O failure")

type releaseFaultFs struct {
	afero.Fs
	renameErr   bool
	closeErr    bool
	cancel      context.CancelFunc
	closeCancel context.CancelFunc
}

func (f *releaseFaultFs) Rename(old, next string) error {
	if f.renameErr {
		return releaseInjectedIO
	}
	if err := f.Fs.Rename(old, next); err != nil {
		return err
	}
	if f.cancel != nil {
		f.cancel()
	}
	return nil
}

func (f *releaseFaultFs) OpenFile(name string, flag int, mode os.FileMode) (afero.File, error) {
	file, err := f.Fs.OpenFile(name, flag, mode)
	if err != nil || (!f.closeErr && f.closeCancel == nil) {
		return file, err
	}
	return &releaseFaultFile{File: file, fail: f.closeErr, cancel: f.closeCancel}, nil
}

type releaseFaultFile struct {
	afero.File
	fail   bool
	cancel context.CancelFunc
}

func (f *releaseFaultFile) Close() error {
	err := f.File.Close()
	if f.cancel != nil {
		f.cancel()
	}
	if f.fail {
		return errors.Join(err, releaseInjectedIO)
	}
	return err
}

func TestReleaseBackportCancelledStagedWriteDoesNotPublish(t *testing.T) {
	for _, update := range []bool{false, true} {
		t.Run(fmt.Sprintf("update=%t", update), func(t *testing.T) {
			s, pool := releaseFixture(t, 1)
			conn, err := pool.Take(context.Background())
			require.NoError(t, err)
			metadata, err := ReadMetadata(conn, releaseExistingKey)
			require.NoError(t, err)
			pool.Put(conn)
			payloadPath := makePayloadPath(filepath.Join(s.root, releaseExistingKey))
			payload, err := afero.ReadFile(s.appFs, payloadPath)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), releaseTestBound)
			defer cancel()
			s.appFs = &releaseFaultFs{Fs: s.appFs, closeCancel: cancel}
			if update {
				err = s.GuaranteedUpdate(ctx, releaseExistingKey, &v1beta1.SBOMSyft{}, false, nil, releaseUpdate, nil)
			} else {
				err = s.Create(ctx, releaseResourceRoot+"/test/new", releaseObject("new"), &v1beta1.SBOMSyft{}, 0)
			}
			require.ErrorIs(t, err, context.Canceled)
			actualPayload, err := afero.ReadFile(s.appFs, payloadPath)
			require.NoError(t, err)
			require.Equal(t, payload, actualPayload)
			conn, err = pool.Take(context.Background())
			require.NoError(t, err)
			defer pool.Put(conn)
			actualMetadata, err := ReadMetadata(conn, releaseExistingKey)
			require.NoError(t, err)
			require.Equal(t, metadata, actualMetadata)
			if !update {
				_, err := ReadMetadata(conn, releaseResourceRoot+"/test/new")
				require.ErrorIs(t, err, ErrMetadataNotFound)
			}
		})
	}
}

func TestReleaseBackportBackgroundTransactionOwnership(t *testing.T) {
	s, pool := releaseFixture(t, 2)
	conn, err := pool.Take(context.Background())
	require.NoError(t, err)
	defer pool.Put(conn)
	observer, err := pool.Take(context.Background())
	require.NoError(t, err)
	defer pool.Put(observer)
	release := sqlitex.Transaction(conn)
	var txnErr error
	defer release(&txnErr)
	key := releaseResourceRoot + "/test/transaction"
	require.NoError(t, s.saveObject(conn, key, releaseObject("transaction"), &v1beta1.SBOMSyft{}, ""))
	_, err = ReadMetadata(observer, key)
	require.ErrorIs(t, err, ErrMetadataNotFound, "inner save must not commit its caller's transaction")
	release(&txnErr)
	require.NoError(t, txnErr)
	metadata, err := ReadMetadata(observer, key)
	require.NoError(t, err)
	require.Contains(t, string(metadata), "release-test-transaction")
}

func TestReleaseBackportPayloadFailurePreservesMetadata(t *testing.T) {
	for _, failure := range []string{"rename", "close", "commit-cancellation"} {
		t.Run(failure, func(t *testing.T) {
			s, pool := releaseFixture(t, 1)
			ctx, cancel := context.WithTimeout(context.Background(), releaseTestBound)
			defer cancel()
			conn, err := pool.Take(ctx)
			require.NoError(t, err)
			defer pool.Put(conn)
			payloadPath := makePayloadPath(filepath.Join(s.root, releaseExistingKey))
			beforePayload, err := afero.ReadFile(s.appFs, payloadPath)
			require.NoError(t, err)
			beforeMetadata, err := ReadMetadata(conn, releaseExistingKey)
			require.NoError(t, err)
			fault := &releaseFaultFs{Fs: s.appFs, renameErr: failure == "rename", closeErr: failure == "close"}
			if failure == "commit-cancellation" {
				fault.cancel = cancel
			}
			s.appFs = fault
			next := releaseObject("existing")
			next.ResourceVersion = "1"
			next.Labels["revision"] = "new"
			err = s.saveObject(conn, releaseExistingKey, next, &v1beta1.SBOMSyft{}, "")
			if failure == "commit-cancellation" {
				require.Equal(t, sqlite.ResultInterrupt, sqlite.ErrCode(err))
			} else {
				require.ErrorIs(t, err, releaseInjectedIO)
			}
			conn.SetInterrupt(nil)
			fault.closeErr = false
			afterPayload, err := afero.ReadFile(s.appFs, payloadPath)
			require.NoError(t, err)
			require.Equal(t, beforePayload, afterPayload)
			afterMetadata, err := ReadMetadata(conn, releaseExistingKey)
			require.NoError(t, err)
			require.Equal(t, beforeMetadata, afterMetadata)
		})
	}
}

func TestReleaseBackportConcurrentMetadataListsAndWatches(t *testing.T) {
	s, pool := releaseFixture(t, 2)
	resources := []string{"applicationprofiles", "configurationscansummaries", "containerprofiles",
		"generatednetworkpolicies", "knownservers", "networkneighborhoods", "openvulnerabilityexchangecontainers",
		"sbomsyftfiltereds", "sbomsyfts", "vulnerabilitymanifests", "vulnerabilitymanifestsummaries",
		"vulnerabilitysummaries", "workloadconfigurationscans", "workloadconfigurationscansummaries"}
	ctx, cancel := context.WithTimeout(context.Background(), releaseTestBound)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, len(resources))
	var sbomWatch watch.Interface
	for _, resource := range resources {
		key := "/spdx.softwarecomposition.kubescape.io/" + resource
		w, err := s.Watch(ctx, key, storage.ListOptions{Recursive: true})
		require.NoError(t, err)
		defer w.Stop()
		if resource == "sbomsyfts" {
			sbomWatch = w
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			list := &metav1.PartialObjectMetadataList{}
			if err := s.GetList(ctx, key, storage.ListOptions{Recursive: true}, list); err != nil {
				errs <- err
				return
			}
			if list.ResourceVersion == "" {
				errs <- fmt.Errorf("%s list has no initial resourceVersion", resource)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.NotNil(t, sbomWatch)
	require.NoError(t, s.GuaranteedUpdate(ctx, releaseExistingKey, &v1beta1.SBOMSyft{}, false, nil, releaseUpdate, nil))
	select {
	case event := <-sbomWatch.ResultChan():
		require.Equal(t, watch.Modified, event.Type)
		meta, ok := event.Object.(metav1.Object)
		require.True(t, ok)
		require.Equal(t, types.UID("release-test-existing"), meta.GetUID())
		require.Equal(t, "2", meta.GetResourceVersion())
	case <-ctx.Done():
		t.Fatal("normal metadata watch did not deliver the committed update")
	}
	cancel()
	select {
	case _, open := <-sbomWatch.ResultChan():
		require.False(t, open)
	case <-time.After(releaseResponseBound):
		t.Fatal("cancelled watch did not close")
	}
	probeCtx, probeCancel := context.WithTimeout(context.Background(), releaseResponseBound)
	defer probeCancel()
	conn, err := pool.Take(probeCtx)
	require.NoError(t, err, "watch/list initialization must return pooled resources")
	pool.Put(conn)
}
