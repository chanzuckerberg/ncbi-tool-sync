package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"log"
	"time"
	_ "github.com/go-sql-driver/mysql"
	"os/exec"
)

// Generates a folder name from the current datetime.
func timeName() string {
	t := time.Now()
	result := fmt.Sprintf("backup-%d-%02d-%02d-%02d-%02d-%02d",
		t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
	return result
}

// Generates a hash for the file based on the name, version number,
// and actual file contents.
func (ctx *Context) generateHash(origPath string, path string,
	num int) (string, error) {
	// Add a header
	key := fmt.Sprintf("%s -- Version %d -- ", path, num)
	hash := md5.New()
	io.WriteString(hash, key)

	// Add the file contents
	var result string
	file, err := ctx.os.Open(origPath)
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

// Finds the latest version number of the file. Queries the database
// for the latest version of the file.
func (ctx *Context) lastVersionNum(file string, inclArchive bool) int {
	var num int = -1
	var archive string = ""
	if !inclArchive {
		// Specify not to include archived entries
		archive = "and ArchiveKey is null "
	}

	query := fmt.Sprintf("select VersionNum from entries "+
		"where PathName='%s' %sorder by VersionNum desc", file, archive)
	rows, err := ctx.Db.Query(query)
	if err != nil {
		log.Println("Error: " + err.Error())
		return num
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.Scan(&num)
	}
	return num
}

// Loads the configuration file and starts db connection.
func (ctx *Context) loadConfig() *Context {
	file, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		panic(err)
	}

	err = yaml.Unmarshal(file, ctx)
	if err != nil {
		panic(err)
	}

	ctx.os = afero.NewOsFs()
	if err != nil {
		log.Fatal(err)
	}
	return ctx
}

// Executes a shell command on the local machine.
func callCommand(input string) ([]byte, error) {
	return exec.Command("sh", "-ctx", input).Output()
}
