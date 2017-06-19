package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
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

func init() {
	log.SetOutput(os.Stderr)
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	var c Context
	var err error
	c.loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	err = c.callRsyncFlow(c.SourcePath)
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

	changes = changes[1 : len(changes)-3] // Remove junk lines

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
	tempDir := timeName()
	template := "rsync -abrzv %s --itemize-changes --delete " +
		"--size-only --no-motd --exclude='.*' --backup-dir='%s' %s %s"

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
