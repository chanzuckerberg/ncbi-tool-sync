package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/user"
	"strings"
)

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
func generateHash(path string, num int) (string, error) {
	// Add a header
	var result string
	key := fmt.Sprintf("%s -- Version %d -- ", path, num)
	hash := md5.New()
	_, err := io.WriteString(hash, key)
	if err != nil {
		err = newErr("Error in generating md5 hash.", err)
		log.Print(err)
		return result, err
	}

	// Generate checksum
	hashInBytes := hash.Sum(nil)[:16]
	result = hex.EncodeToString(hashInBytes)
	return result, nil
}

// Finds the latest version number of the file. Queries the database for the
// latest version of the file.
func lastVersionNum(ctx *Context, file string, inclArchive bool) int {
	num := -1
	var err error
	var rows *sql.Rows

	// Query
	if inclArchive {
		rows, err = ctx.Db.Query("select VersionNum from entries "+
			"where PathName=? order by VersionNum desc", file)
	} else {
		// Specify not to include archived entries
		rows, err = ctx.Db.Query("select VersionNum from entries "+
			"where PathName=? and ArchiveKey is null order by VersionNum desc",
			file)
	}
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
func setupConfig(ctx *Context) {
	file, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// Load from config file
	err = yaml.Unmarshal(file, ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Interface for file system
	ctx.os = afero.NewOsFs()
	if err != nil {
		log.Fatal(err)
	}

	ctx.LocalTop = ctx.UserHome + "/remote"
	ctx.LocalPath = ctx.LocalTop + ctx.SourcePath
	ctx.TempNew = ctx.UserHome + "/tempNew"
	ctx.os.MkdirAll(ctx.TempNew, os.ModePerm)

	serv := os.Getenv("SERVER")
	if serv != "" {
		ctx.Server = serv
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		os.Setenv("AWS_REGION", "us-west-2")
	}
}

// Formats a custom string and error message into one error.
func newErr(input string, err error) error {
	return errors.New(input + " " + err.Error())
}

// Outputs AWS response if not empty.
func awsOutput(input string) {
	// Skip if empty response
	snip := strings.Replace(input, " ", "", -1)
	if strings.Replace(snip, "\n", "", -1) == "{}" {
		return
	}
	log.Print("AWS response: " + input)
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

// Outputs a system command to log with stdout, stderr, and err output.
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

// Outputs a system command to log with all output on error.
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

// Outputs a system command to a streaming log from stdout and stderr pipes.
func commandStreaming(input string) {
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
