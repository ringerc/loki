package storage

import (
	"context"
	"errors"
	"io"
	"path"
	"slices"
)

var (
	ErrUserIDMustNotBeEmpty = errors.New("userID must not be empty")
	ErrUserIDMustBeEmpty    = errors.New("userID must be empty")
)

// IndexSet provides storage operations for user or common index tables.
type IndexSet interface {
	RefreshIndexTableCache(ctx context.Context, tableName string)
	ListFiles(ctx context.Context, tableName, userID string, bypassCache bool) ([]IndexFile, error)
	GetFile(ctx context.Context, tableName, userID, fileName string) (io.ReadCloser, error)
	PutFile(ctx context.Context, tableName, userID, fileName string, file io.ReadSeeker) error
	DeleteFile(ctx context.Context, tableName, userID, fileName string) error
	IsFileNotFoundErr(err error) bool
	IsUserBasedIndexSet() bool
}

type indexSet struct {
	client         Client
	userBasedIndex bool
}

// NewIndexSet handles storage operations based on the value of indexSet.userBasedIndex
func NewIndexSet(client Client, userBasedIndex bool) IndexSet {
	return indexSet{
		client:         client,
		userBasedIndex: userBasedIndex,
	}
}

func (i indexSet) validateUserID(userID string) error {
	if i.userBasedIndex && userID == "" {
		return ErrUserIDMustNotBeEmpty
	} else if !i.userBasedIndex && userID != "" {
		return ErrUserIDMustBeEmpty
	}

	return nil
}

func (i indexSet) RefreshIndexTableCache(ctx context.Context, tableName string) {
	i.client.RefreshIndexTableCache(ctx, tableName)
}

// The Azure Blob Storage client (azblob) will return directory entries as files
// when listing objects in an Azure Data Lake Storage (azure storage gen2) account.
// We need to filter these out, otherwise lower levels of the stack will try to download
// and open them as files. Since Loki will never create an object with the same name
// as a path-prefix used for other objects, we can safely filter out directory entries
// without knowing their sizes or attributes by filtering out all entries that form the
// prefix of another entry.
//
// This returns the modified slice but may mutate the input slice too.
func filterOutDirectories(files []IndexFile) []IndexFile {
	dirs := make(map[string]struct{})
	for _, file := range files {
		// Find the path before the last slash, if any, and add it to the set of known
		// "directories". Trailing slashes will not be added.
		dir := path.Dir(file.Name)
		if dir != "." {
			dirs[dir] = struct{}{}
		}
	}
	// Now filter out any file-paths that are known to be directories.
	files = slices.DeleteFunc(files, func(f IndexFile) bool {
		_, isDir := dirs[path.Clean(f.Name)]
		return isDir
	})
	return files
}

func (i indexSet) ListFiles(ctx context.Context, tableName, userID string, bypassCache bool) ([]IndexFile, error) {
	err := i.validateUserID(userID)
	if err != nil {
		return nil, err
	}

	var files []IndexFile
	if i.userBasedIndex {
		files, err = i.client.ListUserFiles(ctx, tableName, userID, bypassCache)
	} else {
		files, _, err = i.client.ListFiles(ctx, tableName, bypassCache)
	}
	if err != nil {
		return files, err
	}

	return filterOutDirectories(files), err
}

func (i indexSet) GetFile(ctx context.Context, tableName, userID, fileName string) (io.ReadCloser, error) {
	err := i.validateUserID(userID)
	if err != nil {
		return nil, err
	}

	if i.userBasedIndex {
		return i.client.GetUserFile(ctx, tableName, userID, fileName)
	}

	return i.client.GetFile(ctx, tableName, fileName)
}

func (i indexSet) PutFile(ctx context.Context, tableName, userID, fileName string, file io.ReadSeeker) error {
	err := i.validateUserID(userID)
	if err != nil {
		return err
	}

	if i.userBasedIndex {
		return i.client.PutUserFile(ctx, tableName, userID, fileName, file)
	}

	return i.client.PutFile(ctx, tableName, fileName, file)
}

func (i indexSet) DeleteFile(ctx context.Context, tableName, userID, fileName string) error {
	err := i.validateUserID(userID)
	if err != nil {
		return err
	}

	if i.userBasedIndex {
		return i.client.DeleteUserFile(ctx, tableName, userID, fileName)
	}

	return i.client.DeleteFile(ctx, tableName, fileName)
}

func (i indexSet) IsFileNotFoundErr(err error) bool {
	return i.client.IsFileNotFoundErr(err)
}

func (i indexSet) IsUserBasedIndexSet() bool {
	return i.userBasedIndex
}
