package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	"os/exec"
	"strings"
	"github.com/spf13/afero"
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

func main() {
	var c Context
	var err error

	// Load configuration
	c.configure()
	if err != nil {
		log.Fatal(err)
	}

	// Mount FUSE directory
	c.MountFuse()
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

func callCommand(input string) ([]byte, error) {
	return exec.Command("sh", "-c", input).Output()
}

// Parse the Rsync itemized output for new, modified, and deleted files
func parseChanges(out []byte, base string) ([]string, []string, []string) {
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
		} else if strings.HasPrefix(change, "*deleting") && last != "/" {
			// Exclude folders
			deleted = append(deleted, path)
		}
	}
	return newNow, modified, deleted
}

func (c *Context) callRsyncFlow() error {
	var err error
	var cmd string

	// Construct Rsync parameters
	source := fmt.Sprintf("rsync://%s%s/", c.Server, c.SourcePath)
	tempDir := timeName()
	template := "rsync -abrzv %s --itemize-changes --delete " +
		"--size-only --no-motd --exclude='.*' --backup-dir='%s' %s %s"

	// Dry run
	cmd = fmt.Sprintf(template, "-n", tempDir, source, c.LocalPath)
	fmt.Println("COMMAND: " + cmd)
	out, err := callCommand(cmd)
	if err != nil {
		printIfErr(out, err)
		return err
	}
	newNow, modified, deleted := parseChanges(out, c.SourcePath)
	fmt.Printf("\nNew on remote: %s", newNow)
	fmt.Printf("\nModified on remote: %s", modified)
	fmt.Printf("\nDeleted on remote: %s", deleted)

	// Actual run
	fmt.Println("\n\nACTUAL RUN:")
	os.MkdirAll(c.LocalPath, os.ModePerm)
	cmd = fmt.Sprintf(template, "", tempDir, source, c.LocalPath)
	out, err = callCommand(cmd)
	//fmt.Printf("\n%s\n", out)
	if err != nil {
		printIfErr(out, err)
		return err
	}

	// Process changes
	fmt.Println("\nPROCESS CHANGES:")
	err = c.processChanges(newNow, modified, tempDir)
	return err
}
