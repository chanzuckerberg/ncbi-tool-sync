package sync

import (
    "fmt"
    "io/ioutil"
    "gopkg.in/yaml.v2"
    "os"
)

// Load the configuration file
func (c *Context) LoadConfig() *Context {
    file, err := ioutil.ReadFile("config.yaml")
    if err != nil { panic(err) }

    err = yaml.Unmarshal(file, c)
    if err != nil { panic(err) }

    return c
}

// Mount the virtual directory
func (c *Context) MountFuse() error {
    _ = os.Mkdir("remote", os.ModePerm)
    cmd := fmt.Sprintf("goofys %s remote", c.Bucket)
    out, err := callCommand(cmd)
    fmt.Printf("%s%s\n", out, err)
    return nil
}

// Unmount the virtual directory
func (c *Context) UmountFuse() error {
    cmd := fmt.Sprintf("umount remote")
    out, err := callCommand(cmd)
    fmt.Printf("%s%s\n", out, err)
    return err
}
