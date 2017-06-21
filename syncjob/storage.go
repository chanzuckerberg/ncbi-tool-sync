package main

import (
	"fmt"
	"os"
)

// Mount the virtual directory
func (c *Context) MountFuse() error {
	_ = c.os.Mkdir("remote", os.ModePerm)
	cmd := fmt.Sprintf("goofys %s remote", c.Bucket)
	out, err := callCommand(cmd)
	fmt.Printf("%s%s\n", out, err)
	return err
}

// Unmount the virtual directory
func (c *Context) UmountFuse() error {
	cmd := fmt.Sprintf("umount remote")
	out, err := callCommand(cmd)
	fmt.Printf("%s%s\n", out, err)
	return err
}
