package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/afero"
	"log"
	"os"
	"os/exec"
	"strings"
)

type Context struct {
	db         *sql.DB
	os         afero.Fs
	Server     string `yaml:"Server"`
	Port       string `yaml:"Port"`
	Username   string `yaml:"Username"`
	Password   string `yaml:"Password"`
	SourcePath string `yaml:"SourcePath"`
	LocalPath  string `yaml:"LocalPath"`
	LocalTop   string `yaml:"LocalTop"`
	Bucket     string `yaml:"Bucket"`
}

func init() {
	log.SetOutput(os.Stderr)
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// Entry point for the entire sync workflow with remote server.
func main() {
	var c Context
	var err error

	// Load configuration
	c.configure()
	defer c.db.Close()

	// Mount FUSE directory
	//c.UnmountFuse()
	//err = c.MountFuse()
	//defer c.UmountFuse()
	if err != nil {
		log.Fatal(err)
	}

	// Call Rsync flow
	err = c.callRsyncFlow()
	if err != nil {
		log.Fatal(err)
	}
}

// Executes a shell command on the local machine.
func callCommand(input string) ([]byte, error) {
	return exec.Command("sh", "-c", input).Output()
}

// Parses the Rsync itemized output for new, modified, and deleted
// files. Follows the Rsync itemized changes syntax that specify
// exactly which changes occurred or will occur.
func parseChanges(out []byte, base string) ([]string,
	[]string, []string) {
	changes := strings.Split(string(out[:]), "\n")
	changes = changes[1 : len(changes)-4] // Remove junk lines

	var newNow, modified, deleted []string

	for _, line := range changes {
		col := strings.SplitN(line, " ", 2)
		change := col[0]
		file := col[1]
		path := base + "/" + file
		last := file[len(file)-1:]
		if strings.HasPrefix(change, ">f+++++++") {
			newNow = append(newNow, path)
		} else if strings.HasPrefix(change, ">f") {
			modified = append(modified, path)
		} else if strings.HasPrefix(change, "*deleting") &&
			last != "/" {
			// Exclude folders
			deleted = append(deleted, path)
		}
	}
	return newNow, modified, deleted
}

// Calls the Rsync workflow. Executes a dry run first for processing.
// Then runs the real sync. Finally processes the changes.
func (c *Context) callRsyncFlow() error {
	var err error
	var cmd string

	// Construct Rsync parameters
	source := fmt.Sprintf("rsync://%s%s/", c.Server,
		c.SourcePath)
	tempDir := timeName()
	template := "rsync -abrzv %s --itemize-changes --delete " +
		"--size-only --no-motd --exclude='.*' --backup-dir='%s' %s %s"

	// Dry run
	cmd = fmt.Sprintf(template, "-n", tempDir, source, c.LocalPath)
	log.Println("Command: " + cmd)
	out, err := callCommand(cmd)
	if err != nil {
		log.Fatal(out, err)
		return err
	}
	newNow, modified, deleted := parseChanges(out, c.SourcePath)
	log.Printf("New on remote: %s", newNow)
	log.Printf("Modified on remote: %s", modified)
	log.Printf("Deleted on remote: %s", deleted)

	// Actual run
	log.Println("Actual sync run...")
	os.MkdirAll(c.LocalPath, os.ModePerm)
	cmd = fmt.Sprintf(template, "", tempDir, source, c.LocalPath)
	out, err = callCommand(cmd)
	if err != nil {
		log.Fatal(out, err)
		return err
	}

	// Process changes
	log.Println("Processing changes...")
	err = c.processChanges(newNow, modified, tempDir)
	return err
}
