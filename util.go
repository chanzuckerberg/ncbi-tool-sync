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
	"io"
	"log"
	"os"
	"os/exec"
	"os/user"
	"strings"
	"github.com/smallfish/simpleyaml"
	"io/ioutil"
	"menteslibres.net/gosexy/to"
)

// Gets the full path of the user's home directory
func getUserHome() string {
	usr, err := user.Current()
	if err != nil {
		log.Print("Couldn't get user's home directory.")
		log.Fatal(err)
	}
	//fmt.Println(usr)

	return usr.HomeDir
	//return "/home/ubuntu"
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
	loadFromYaml(ctx)

	ctx.os = afero.NewOsFs() // Interface for file system
	ctx.LocalTop = ctx.UserHome + "/remote"
	ctx.TempNew = ctx.UserHome + "/tempNew"
	ctx.os.MkdirAll(ctx.TempNew, os.ModePerm)

	if serv := os.Getenv("SERVER"); serv != "" {
		ctx.Server = serv
	}
	if region := os.Getenv("AWS_REGION"); region == "" {
		os.Setenv("AWS_REGION", "us-west-2")
	}

	// Set up credentials for s3fs
	id := os.Getenv("AWS_ACCESS_KEY_ID")
	pass := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if id != "" && pass != "" {
		cmd := fmt.Sprintf("echo %s:%s > /etc/passwd-s3fs", id, pass)
		if _, _, err := commandVerbose(cmd); err != nil {
			log.Fatal(err)
		}
		cmd = fmt.Sprintf("chmod 600 /etc/passwd-s3fs")
		if _, _, err := commandVerbose(cmd); err != nil {
			log.Fatal(err)
		}
	}
}

func loadFromYaml(ctx *Context) {
	// Load from config file
	source, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		log.Fatal("Error in opening config. ", err)
	}
	yml, err := simpleyaml.NewYaml(source)
	if err != nil {
		log.Fatal("Error in parsing config. ", err)
	}

	var str string
	if str, err = yml.Get("server").String(); err != nil {
		log.Print("No server set in config.yaml. Will try to set from env.")
	} else {
		ctx.Server = str
	}
	if str, err = yml.Get("bucket").String(); err != nil {
		log.Fatal("Error in setting bucket. ", err)
	}
	ctx.Bucket = str

	loadSyncFolders(ctx, yml)
}

func loadSyncFolders(ctx *Context, yml *simpleyaml.Yaml) {
	// Load sync folder details
	size, err := yml.Get("syncFolders").GetArraySize()
	if err != nil {
		log.Fatal("Error in loading syncFolders. ", err)
	}
	for i := 0; i < size; i++ {
		folder := yml.Get("syncFolders").GetIndex(i)
		name, err := folder.Get("name").String()
		if err != nil {
			log.Fatal("Error in loading folder name. ", err)
		}
		flagsYml, err := folder.Get("flags").Array()
		if err != nil {
			log.Fatal("Error in loading sync flags. ", err)
		}
		flags := []string{}
		for _, v := range flagsYml {
			flags = append(flags, to.String(v))
		}
		res := syncFolder{name, flags}
		ctx.syncFolders = append(ctx.syncFolders, res)
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
		log.Print("Command ran successfully.")
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
		log.Print("Command ran successfully.")
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
