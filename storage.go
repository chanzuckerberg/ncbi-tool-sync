package main

import (
	"fmt"
	"log"
	"os"
)

// Mounts the virtual directory. Uses goofys tool to mount S3 as a
// local folder for syncing operations.
func (c *Context) MountFuse() error {
	_ = c.os.Mkdir("remote", os.ModePerm)
	cmd := fmt.Sprintf("goofys %s remote", c.Bucket)
	out, err := callCommand(cmd)
	if err != nil {
		log.Fatal("Error in mounting FUSE.")
		log.Println(out)
		log.Println(err.Error())
	}
	return err
}

// Unmounts the virtual directory. Ignores errors since directory may
// already be unmounted.
func (c *Context) UnmountFuse() {
	cmd := fmt.Sprintf("umount remote")
	callCommand(cmd)
}

// Creates the table and schema in the db if needed.
func (c *Context) SetupDb() {
	query := "CREATE TABLE IF NOT EXISTS entries (" +
		"PathName VARCHAR(700) NOT NULL, " +
		"VersionNum INT NOT NULL, " +
		"DateModified VARCHAR(40), " +
		"ArchiveKey VARCHAR(50), " +
		"PRIMARY KEY (PathName, VersionNum));"
	c.db.Exec(query)
}
