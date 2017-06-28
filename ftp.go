package main

import (
	"fmt"
	"github.com/jlaffaye/ftp"
	"time"
)

// Gets a listing of files and modified times from the FTP server.
// Returns a map of the file pathName to the modTime.
func (ctx *Context) getServerListing(dir string) (map[string]string,
	error) {
	// Open FTP connection
	FileToTime := make(map[string]string)
	client, err := ctx.connectToServer()
	if err != nil {
		return FileToTime, err
	}
	defer client.Quit()
	entries, err := client.List(dir)
	if err != nil {
		return FileToTime, err
	}

	for _, entry := range entries {
		res := fmt.Sprintf("%s",
			entry.Time.Format(time.RFC3339))
		res = res[:len(res)-1]
		FileToTime[entry.Name] = res
	}
	return FileToTime, err
}

// Connects to the FTP server and returns the client.
func (ctx *Context) connectToServer() (*ftp.ServerConn, error) {
	addr := ctx.Server + ":" + ctx.Port
	client, err := ftp.Dial(addr)
	if err != nil {
		return nil, err
	}
	err = client.Login(ctx.Username, ctx.Password)
	if err != nil {
		return nil, err
	}
	return client, err
}
