package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"log"
	"os/exec"
	"time"
	"os/user"
	"bytes"
	"bufio"
	"os"
	"errors"
)

// Generates a folder name from the current datetime.
func timeName() string {
	t := time.Now()
	result := fmt.Sprintf("backup-%d-%02d-%02d-%02d-%02d-%02d",
		t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
	return result
}

// Gets the full path of the user's home directory
func getUserHome() string {
	usr, err := user.Current()
	if err != nil {
		log.Print("Couldn't get user's home directory.")
		log.Fatal(err)
	}
	return usr.HomeDir
}

// Generates a hash for the file based on the name, version number,
// and actual file contents.
func (ctx *Context) generateHash(origPath string, path string,
	num int) (string, error) {
	// Add a header
	var err error
	var result string
	key := fmt.Sprintf("%s -- Version %d -- ", path, num)
	hash := md5.New()
	_, err = io.WriteString(hash, key)
	if err != nil {
		return result, err
	}

	// Add the file contents
	//file, err := ctx.os.Open(origPath)
	//if err != nil {
	//	return result, err
	//}
	//defer file.Close()
	//if _, err := io.Copy(hash, file); err != nil {
	//	return result, err
	//}

	// Generate checksum
	hashInBytes := hash.Sum(nil)[:16]
	result = hex.EncodeToString(hashInBytes)
	return result, nil
}

// Finds the latest version number of the file. Queries the database
// for the latest version of the file.
func (ctx *Context) lastVersionNum(file string,
	inclArchive bool) int {
	num := -1
	archive := ""
	if !inclArchive {
		// Specify not to include archived entries
		archive = "and ArchiveKey is null "
	}

	query := fmt.Sprintf("select VersionNum from entries "+
		"where PathName='%s' %sorder by VersionNum desc", file, archive)
	rows, err := ctx.Db.Query(query)
	if err != nil {
		err = newErr("Error in getting VersionNum.", err)
		log.Print(err)
		return num
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.Scan(&num)
		if err != nil {
			err = newErr("Error scanning row.", err)
			log.Print(err)
		}
	}
	return num
}

// Loads the configuration file and starts db connection.
func (ctx *Context) setupConfig() *Context {
	file, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		panic(err)
	}

	// Load from config file
	err = yaml.Unmarshal(file, ctx)
	if err != nil {
		panic(err)
	}

	// Interface for file system
	ctx.os = afero.NewOsFs()
	if err != nil {
		log.Fatal(err)
	}

	ctx.LocalTop = ctx.UserHome + "/remote"
	ctx.LocalPath = ctx.LocalTop + ctx.SourcePath
	ctx.Archive = ctx.LocalTop + "/archive"
	ctx.TempNew = ctx.UserHome + "/tempNew"
	ctx.TempOld = ctx.UserHome + "/tempOld"
	os.MkdirAll(ctx.TempNew, os.ModePerm)
	os.MkdirAll(ctx.TempOld, os.ModePerm)

	serv := os.Getenv("SERVER")
	if serv != "" {
		ctx.Server = serv
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		os.Setenv("AWS_REGION", "us-west-2")
	}

	return ctx
}

func newErr(input string, err error) error {
	return errors.New(input + " " + err.Error())
}

// Executes a shell command on the local machine.
func callCommand(input string) ([]byte, error) {
	return exec.Command("sh", "-cx", input).CombinedOutput()
}

// Executes a shell command and returns the stdout, stderr, and err
func commandWithOutput(input string) (string, string, error) {
	cmd := exec.Command("sh", "-cx", input)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	outResp := stdout.String()
	errResp := stderr.String()
	return outResp, errResp, err
}

func commandVerbose(input string) (string, string, error) {
	log.Print("Command: " + input)
	stdout, stderr, err := commandWithOutput(input)
	if stdout != "" {
		log.Print(stdout)
	}
	if stderr != "" {
		log.Print(stderr)
	}
	if err != nil {
		err = newErr("Error in running command.", err)
		log.Print(err)
	} else {
		log.Print("Command ran with no errors.")
	}
	return stdout, stderr, err
}

func commandVerboseOnErr(input string) (string, string, error) {
	log.Print("Command: " + input)
	stdout, stderr, err := commandWithOutput(input)
	if err != nil {
		if stdout != "" {
			log.Print(stdout)
		}
		if stderr != "" {
			log.Print(stderr)
		}
		err = newErr("Error in running command.", err)
		log.Print(err)
	} else {
		log.Print("Command ran with no errors.")
	}
	return stdout, stderr, err
}

func commandStreaming(input string) {
	var err error
	log.Print("Command: " + input)
	cmd := exec.Command("bash", "-c", input)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		err = newErr("Couldn't get from stdout.", err)
		log.Print(err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		err = newErr("Couldn't get from stderr.", err)
		log.Print(err)
	}
	scanner := bufio.NewScanner(io.MultiReader(stdout, stderr))
	go func() {
		for scanner.Scan() {
			log.Print(scanner.Text())
		}
	}()

	err = cmd.Start()
	if err != nil {
		err = newErr("Error in starting command.", err)
		log.Print(err)
	}
	err = cmd.Wait()
	if err != nil {
		err = newErr("Error in command execution.", err)
		log.Print(err)
	}
	log.Print("Command finished executing.")
}
