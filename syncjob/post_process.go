package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/jlaffaye/ftp"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (c *Context) processChanges(new []string, modified []string,
	tempDir string) error {
	// Open db
	var err error
	c.db, err = sql.Open("sqlite3", "../versionDB.db")
	defer c.db.Close()
	if err != nil {
		return err
	}

	// Move replaced or deleted file versions to archive
	err = c.archiveOldVersions(tempDir)
	if err != nil {
		return err
	}

	// Add new or modified files as db entries
	err = c.handleNewVersions(new)
	if err != nil {
		return err
	}
	err = c.handleNewVersions(modified)
	if err != nil {
		return err
	}

	// Delete temp folder after handling files
	path := fmt.Sprintf("%s/%s", c.LocalPath, tempDir)
	err = os.RemoveAll(path)

	return err
}

func (c *Context) archiveOldVersions(tempDir string) error {
	var err error

	// Return if rsync didn't make a modified folder
	_, err = os.Stat(fmt.Sprintf("%s/%s", c.LocalPath, tempDir))
	if err != nil {
		return nil
	}

	// Make archive folder
	dest := fmt.Sprintf("%s/%s", c.LocalPath, tempDir)
	os.MkdirAll(c.LocalTop+"/archive", os.ModePerm)

	// Walk through each modified file
	if _, err := os.Stat(dest); err == nil {
		err = filepath.Walk(dest, c.archiveFile(tempDir))
	}

	return err
}

// Handle a list of files with new versions
func (c *Context) handleNewVersions(files []string) error {
	for _, file := range files {
		err := c.handleNewVersion(file)
		if err != nil {
			return err
		}
	}
	return nil
}

// Handle one file with a new version on disk
func (c *Context) handleNewVersion(file string) error {
	var err error

	// Set version number
	var versionNum int = 1
	prevNum := c.lastVersionNum(file, true)
	if prevNum > -1 { // Some version already exists
		versionNum = prevNum + 1
	}

	// Set datetime modified
	modTime, err := c.getServerTime(file)
	if err != nil {
		return err
	}

	// Insert into database
	query := fmt.Sprintf("insert into entries(PathName, VersionNum, "+
		"DateModified) values('%s', %d, '%s')", file, versionNum, modTime)
	_, err = c.db.Exec(query)

	return err
}

// Find the latest version number of the file
func (c *Context) lastVersionNum(file string, includeArchived bool) int {
	var num int = -1
	var archive string = ""
	if !includeArchived { // Specify not archived entries
		archive = "and ArchiveKey is null "
	}

	query := fmt.Sprintf("select VersionNum from entries where "+
		"PathName='%s' %sorder by VersionNum desc", file, archive)
	rows, err := c.db.Query(query)
	defer rows.Close()
	if err != nil {
		return num
	}

	rows.Next()
	err = rows.Scan(&num)
	return num
}

// Generate a folder name from the current datetime
func timeName() string {
	t := time.Now()
	result := fmt.Sprintf("backup-%d-%02d-%02d-%02d-%02d-%02d",
		t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
	return result
}

// Handle each changed file
func (c *Context) archiveFile(tempDir string) filepath.WalkFunc {
	return func(origPath string, f os.FileInfo, err error) error {
		if f.IsDir() {
			return nil
		}

		// Setup
		newPath := origPath[len(c.LocalTop)-2:]                // Remove first part of newPath
		newPath = strings.Replace(newPath, tempDir+"/", "", 1) // Remove tempDir
		num := c.lastVersionNum(newPath, false)
		key, err := generateHash(origPath, newPath, num)
		if err != nil {
			return err
		}

		// Move to archive folder
		dest := fmt.Sprintf("%s/archive/%s", c.LocalTop[2:], key)
		err = os.Rename(origPath, dest)

		// Update the old entry with archiveKey blob
		query := fmt.Sprintf("update entries set ArchiveKey='%s' "+
			"where PathName='%s' and VersionNum=%d;", key, newPath, num)
		_, err = c.db.Exec(query)

		return err
	}
}

// Hash for archiveKey
func generateHash(origPath string, path string, num int) (string, error) {
	// Add a header
	key := fmt.Sprintf("%s -- Version %d -- ", path, num)
	hash := md5.New()
	io.WriteString(hash, key)

	// Add the file contents
	var result string
	file, err := os.Open(origPath)
	if err != nil {
		return result, err
	}
	defer file.Close()
	if _, err := io.Copy(hash, file); err != nil {
		return result, err
	}

	// Generate checksum
	hashInBytes := hash.Sum(nil)[:16]
	result = hex.EncodeToString(hashInBytes)
	return result, nil
}

// Get the date time modified from the FTP server
func (c *Context) getServerTime(input string) (string, error) {
	folder := filepath.Dir(input)
	file := filepath.Base(input)

	// Open FTP connection
	client, err := c.connectToServer()
	defer client.Quit()
	entries, err := client.List(folder)
	if err != nil {
		return "", err
	}

	// Find the right entry
	for _, entry := range entries {
		if entry.Name == file {
			return fmt.Sprintf("%s", entry.Time.Format(time.RFC3339)), err
		}
	}
	return "", err
}

// Connect to the FTP server
func (c *Context) connectToServer() (*ftp.ServerConn, error) {
	addr := c.Server + ":" + c.Port
	client, err := ftp.Dial(addr)
	if err != nil {
		return nil, err
	}
	err = client.Login(c.Username, c.Password)
	if err != nil {
		return nil, err
	}
	return client, err
}
