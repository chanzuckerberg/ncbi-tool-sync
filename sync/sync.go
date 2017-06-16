package sync

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/jlaffaye/ftp"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type Context struct {
	db         *sql.DB
	Server     string `yaml:"Server"`
	Port       string `yaml:"Port"`
	Username   string `yaml:"Username"`
	Password   string `yaml:"Password"`
	SourcePath string `yaml:"SourcePath"`
	LocalPath  string `yaml:"LocalPath"`
	LocalTop   string `yaml:"LocalTop"`
	Bucket     string `yaml:"Bucket"`
}

func Main() {
	var c Context
	var err error
	c.loadConfig()
	if err != nil {
		fmt.Println(err)
	}

	err = c.callRsyncFlow(c.SourcePath)
	if err != nil {
		fmt.Println(err)
	}
}

func callCommand(input string) ([]byte, error) {
	return exec.Command("sh", "-c", input).Output()
}

// Parse the Rsync itemized output for new, modified, and deleted files
func parseChanges(out []byte, base string) ([]string, []string, []string) {
	changes := strings.Split(string(out[:]), "\n")

	changes = changes[1 : len(changes)-4] // Remove junk lines

	var new, modified, deleted []string

	for _, line := range changes {
		col := strings.SplitN(line, " ", 2)
		change := col[0]
		file := col[1]
		path := base + "/" + file
		last := file[len(file)-1:]
		if strings.HasPrefix(change, ">f+++++++") {
			new = append(new, path)
		} else if strings.HasPrefix(change, ">f") {
			modified = append(modified, path)
		} else if strings.HasPrefix(change, "*deleting") && last != "/" {
			// Exclude folders
			deleted = append(deleted, path)
		}
	}
	return new, modified, deleted
}

func (c *Context) callRsyncFlow(input string) error {
	var err error
	var cmd string

	// Construct Rsync parameters
	source := fmt.Sprintf("rsync://%s%s/", c.Server, input)
	tempDir := curTimeName()
	template := "rsync -abrzv %s --itemize-changes --delete --size-only --no-motd " +
		"--exclude='.*' --backup-dir='%s' %s %s"

	// Dry run
	cmd = fmt.Sprintf(template, "-n", tempDir, source, c.LocalPath)
	fmt.Println(cmd)
	out, err := callCommand(cmd)
	if err != nil {
		fmt.Printf("%s, %s", out, err)
		return err
	}
	new, modified, deleted := parseChanges(out, input)
	fmt.Printf("\nNew from remote: %s", new)
	fmt.Printf("\nModified from remote: %s", modified)
	fmt.Printf("\nDeleted from remote: %s", deleted)

	// Actual run
	fmt.Println("\nGOING TO START REAL RUN")
	os.MkdirAll(c.LocalPath, os.ModePerm)
	cmd = fmt.Sprintf(template, "", tempDir, source, c.LocalPath)
	out, err = callCommand(cmd)
	fmt.Printf("\n%s%s\n", out, err)
	if err != nil {
		return err
	}

	// Process changes
	fmt.Println("\nGOING TO PROCESS CHANGES")
	err = c.processChanges(new, modified, tempDir)
	return err
}

func (c *Context) processChanges(new []string, modified []string, tempDir string) error {
	// Open db
	var err error
	c.db, err = sql.Open("sqlite3", "./versionDB.db")
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
	prevNum := c.findPrevVersionNum(file, true)
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
func (c *Context) findPrevVersionNum(file string, includeArchived bool) int {
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
func curTimeName() string {
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
		num := c.findPrevVersionNum(newPath, false)
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
