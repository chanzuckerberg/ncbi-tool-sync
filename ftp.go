package main

import (
	"errors"
	"github.com/jlaffaye/ftp"
	"path/filepath"
	"time"
)

// Variable assignments for testing
var clientList = clientListFtp
var connectToServer = connectToServerFtp
var getModTime = getModTimeFTP

// getServerListing gets a listing of files and modified times from the FTP
// server. Returns a map of the file pathName to the modTime.
func getServerListing(dir string) (map[string]string, error) {
	// Open FTP connection
	FileToTime := make(map[string]string)
	client, err := connectToServer()
	if err != nil {
		return FileToTime, handle("Error in connecting to FTP server.", err)
	}
	defer func() {
		if err = client.Quit(); err != nil {
			errOut("Error in quitting FTP connection", err)
		}
	}()
	entries, err := clientList(client, dir)
	if err != nil {
		return FileToTime, handle("Error in FTP listing.", err)
	}

	for _, entry := range entries {
		res := entry.Time.Format(time.RFC3339)
		res = res[:len(res)-1]
		FileToTime[entry.Name] = res
	}
	return FileToTime, err
}

// clientListFtp calls the list command on the FTP client. Dependency
// injection to aid in testing.
func clientListFtp(client *ftp.ServerConn, dir string) ([]*ftp.Entry, error) {
	return client.List(dir)
}

// connectToServerFtp connects to the FTP server and returns the client
// connection.
func connectToServerFtp() (*ftp.ServerConn, error) {
	addr := "ftp.ncbi.nih.gov:21"
	client, err := ftp.Dial(addr)
	if err != nil {
		return nil, handle("Error in dialing FTP server.", err)
	}
	err = client.Login("anonymous", "test@test.com")
	if err != nil {
		return nil, handle("Error in logging in to FTP server.", err)
	}
	return client, err
}

// getModTimeFTP gets the date modified times from the FTP server utilizing a
// directory listing cache.
func getModTimeFTP(path string, cache map[string]map[string]string) string {
	var err error
	dir := filepath.Dir(path)
	file := filepath.Base(path)
	_, present := cache[dir]
	if !present {
		// Get listing from server
		cache[dir], err = getServerListing(dir)
		if err != nil {
			errOut("Error in getting listing from FTP server.", err)
		}
	} else {
		_, present = cache[dir][file]
		if !present {
			err = errors.New("")
			errOut("Error in getting FTP listing. Expected to find file in cached "+
				"listing.", err)
			return ""
		}
	}
	return cache[dir][file]
}
