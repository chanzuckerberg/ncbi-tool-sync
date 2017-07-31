package main

import (
	"github.com/jlaffaye/ftp"
	"time"
)

// Gets a listing of files and modified times from the FTP server.
// Returns a map of the file pathName to the modTime.
func getServerListing(dir string) (map[string]string,
	error) {
	// Open FTP connection
	FileToTime := make(map[string]string)
	client, err := connectToServer()
	if err != nil {
		return FileToTime, handle("Error in connecting to FTP server.", err)
	}
	defer client.Quit()
	entries, err := client.List(dir)
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

// Connects to the FTP server and returns the client.
func connectToServer() (*ftp.ServerConn, error) {
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
