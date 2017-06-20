package main

import (
	"time"
	"fmt"
	"crypto/md5"
	"io"
	"os"
	"encoding/hex"
	"io/ioutil"
	"gopkg.in/yaml.v2"
)

// Generate a folder name from the current datetime
func timeName() string {
	t := time.Now()
	result := fmt.Sprintf("backup-%d-%02d-%02d-%02d-%02d-%02d",
	t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
	return result
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

// Load the configuration file
func (c *Context) loadConfig() *Context {
	file, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		panic(err)
	}

	err = yaml.Unmarshal(file, c)
	if err != nil {
		panic(err)
	}

	return c
}
