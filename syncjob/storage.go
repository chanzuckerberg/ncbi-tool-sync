package main

import (
	"fmt"
	"os"
	"log"
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
