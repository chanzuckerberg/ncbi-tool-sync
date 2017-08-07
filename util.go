package main

import (
	"bytes"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"log"
	"os/exec"
	"runtime"
	"strings"
)

// generateChecksum generates a checksum for the file based on the name and
// version number.
func generateChecksum(path string, num int) (string, error) {
	var result string
	key := fmt.Sprintf("%s -- Version %d -- ", path, num)
	hash := md5.New()
	_, err := io.WriteString(hash, key)
	if err != nil {
		return result, handle("Error in generating md5 hash.", err)
	}

	// Generate checksum
	hashInBytes := hash.Sum(nil)[:16]
	result = hex.EncodeToString(hashInBytes)
	return result, nil
}

var lastVersionNum = lastVersionNumDb

// lastVersionNumDb finds the latest version number of the file in the db.
func lastVersionNumDb(ctx *context, file string, inclArchive bool) int {
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
		errOut("Error in getting VersionNum.", err)
		return num
	}
	defer func() {
		if err = rows.Close(); err != nil {
			errOut("Error in closing rows", err)
		}
	}()

	if rows.Next() {
		err = rows.Scan(&num)
		if err != nil {
			errOut("Error scanning row.", err)
		}
	}
	return num
}

// awsOutput outputs the AWS operation response if not empty.
func awsOutput(input string) {
	// Skip if empty response
	snip := strings.Replace(input, " ", "", -1)
	if strings.Replace(snip, "\n", "", -1) == "{}" {
		return
	}
	log.Print("AWS response: " + input)
}

var commandWithOutput = commandWithOutputFunc

// commandWithOutputFunc executes a shell command and returns the stdout,
// stderr, and err.
func commandWithOutputFunc(input string) (string, string, error) {
	cmd := exec.Command("sh", "-cx", input)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	outResp := stdout.String()
	errResp := stderr.String()
	return outResp, errResp, err
}

// commandVerbose outputs a system command to log with stdout, stderr, and
// err output.
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
		errOut("Error in running command.", err)
	} else {
		log.Print("Command ran successfully.")
	}
	return stdout, stderr, err
}

// commandVerboseOnErr outputs a system command to log with all output on
// error.
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
		errOut("Error in running command.", err)
	} else {
		log.Print("Command ran successfully.")
	}
	return stdout, stderr, err
}

// handle logs errors and information at runtime. Used for easier error
// tracing up the call stack.
func handle(input string, err error) error {
	if err == nil {
		return err
	}
	pc, fn, line, ok := runtime.Caller(1)
	if input[len(input)-1:] != "." { // Add a period.
		input += "."
	}
	input += " " + err.Error()
	if !ok {
		log.Printf("[error] %s", input)
		return errors.New(input)
	}
	p := strings.Split(fn, "/")
	fn = p[len(p)-1]
	log.Printf("[error] in %s[%s:%d] %s",
		runtime.FuncForPC(pc).Name(), fn, line, input)
	return errors.New(input)
}

// errOut outputs error messages but doesn't create a new error.
func errOut(input string, err error) {
	if err == nil {
		return
	}
	pc, fn, line, ok := runtime.Caller(1)
	if input[len(input)-1:] != "." {
		input += "."
	}
	input += " " + err.Error()
	if !ok {
		log.Printf("[error] %s", input)
		return
	}
	p := strings.Split(fn, "/")
	fn = p[len(p)-1]
	log.Printf("[error] in %s[%s:%d] %s",
		runtime.FuncForPC(pc).Name(), fn, line, input)
}
