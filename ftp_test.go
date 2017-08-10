package main

import (
	"github.com/jlaffaye/ftp"
	"time"
)

func FakeClientList(client *ftp.ServerConn, dir string) ([]*ftp.Entry, error) {
	t, _ := time.Parse(time.RFC3339, "2017-08-04T22:08:41+00:00")
	res := ftp.Entry{Name: "testFile", Size: uint64(4000), Time: t}
	return []*ftp.Entry{&res}, nil
}

func FakeGetModTime(pathName string,
	cache map[string]map[string]string) string {
	return "2017-08-02T22:20:26"
}
