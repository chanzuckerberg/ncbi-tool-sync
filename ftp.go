package main

import (
	"github.com/jlaffaye/ftp"
	"time"
)

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

var clientList = clientListFtp

// clientListFtp calls the list command on the FTP client. Dependency
// injection to aid in testing.
func clientListFtp(client *ftp.ServerConn, dir string) ([]*ftp.Entry, error) {
	return client.List(dir)
}

var connectToServer = connectToServerFtp

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
