package main

import (
	"fmt"
	"github.com/jlaffaye/ftp"
	"path/filepath"
	"time"
)

// Get the date time modified from the FTP server
func (c *Context) getServerTime(input string) (string, error) {
	folder := filepath.Dir(input)
	file := filepath.Base(input)

	// Open FTP connection
	client, err := c.connectToServer()
	defer client.Quit()
	entries, err := client.List(folder)
	if err != nil {
		return "", err
	}

	// Find the right entry
	for _, entry := range entries {
		if entry.Name == file {
			return fmt.Sprintf("%s", entry.Time.Format(time.RFC3339)), err
		}
	}
	return "", err
}

// Connect to the FTP server
func (c *Context) connectToServer() (*ftp.ServerConn, error) {
	addr := c.Server + ":" + c.Port
	client, err := ftp.Dial(addr)
	if err != nil {
		return nil, err
	}
	err = client.Login(c.Username, c.Password)
	if err != nil {
		return nil, err
	}
	return client, err
}
