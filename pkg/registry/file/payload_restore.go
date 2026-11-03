package file

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/afero"
)

const legacyPayloadMode = 0644

// Keep the previous open inode until SQLite commits, allowing a failed commit
// to restore the original Gob without copying every successful write twice.
func (s *StorageImpl) restorePayload(previous afero.File, path string) (err error) {
	if previous == nil {
		return s.appFs.Remove(path)
	}
	if _, err := previous.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind previous payload: %w", err)
	}
	restore, err := afero.TempFile(s.appFs, filepath.Dir(path), ".restore-")
	if err != nil {
		return fmt.Errorf("stage previous payload: %w", err)
	}
	name := restore.Name()
	defer func() { err = errors.Join(err, s.removeStage(name)) }()
	_, copyErr := io.Copy(restore, previous)
	modeErr := s.appFs.Chmod(name, legacyPayloadMode)
	closeErr := restore.Close()
	if err := errors.Join(copyErr, modeErr, closeErr); err != nil {
		return fmt.Errorf("restore previous payload: %w", err)
	}
	return s.appFs.Rename(name, path)
}

func (s *StorageImpl) removeStage(path string) error {
	if err := s.appFs.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove staged payload: %w", err)
	}
	return nil
}
