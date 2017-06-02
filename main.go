package main

import (
    "fmt"
    "github.com/jlaffaye/ftp"
    "bytes"
    "io/ioutil"
    "gopkg.in/yaml.v2"
    "os"
    "io"
    "strings"
)

var config conf
var ftpClient *ftp.ServerConn

func main() {
    var err error
    ftpClient, err = connectToServer()
    ftpClient.DisableEPSV = false
    if err != nil {
        fmt.Println(err)
    }

    err = traverseFolder(config.RemotePath)
    if err != nil {
        fmt.Println(err)
    }
}

func connectToServer() (*ftp.ServerConn, error) {
    config.loadFTPconfig()
    addr := config.Server + ":" + config.Port
    client, err := ftp.Dial(addr)
    if err != nil {
        return nil, err
    }

    err = client.Login(config.Username, config.Password)
    if err != nil {
        return nil, err
    }

    return client, nil
}

func traverseFolder(path string) error {
    // Get directory listing
    entries, err := ftpClient.List(path)
    if err != nil {
        return err
    }

    // Traverse each entry
    for _, entry := range entries {
        name := entry.Name
        if name == "." || name == ".." || string(name[0]) == "." {
            continue
        }

        fmt.Println("NAME IS: " + name)
        newPath := path + "/" + name

        if entry.Type == 0 {            // Is a file
            err = handleFile(newPath)
            if err != nil {
                return err
            }
        } else if entry.Type == 1 {     // Is a folder
            fmt.Println("GOING TO: " + path + "/" + name)
            traverseFolder(newPath)
        }
    }

    return nil
}

func handleFile(remotePath string) error {
    localPath := config.LocalPath + remotePath
    fmt.Println("LOCAL PATH IS: " + localPath)
    fmt.Println("REMOTE PATH IS: " + remotePath)

    // File exists remotely but not locally
    if _, err := os.Stat(localPath); os.IsNotExist(err) {
        fmt.Println("DOES NOT EXIST: " + remotePath)
        err := downloadFile(remotePath)
        if err != nil {
            return err
        }
    }

    return nil
}

func downloadFile(remotePath string) error {
    //fmt.Println("GOING TO DOWNLOAD: " + remotePath)
    localPath := config.LocalPath[1:] + remotePath
    fmt.Println("LOCAL IS: " + localPath)
    fmt.Println("BLAH")
    reader, err := ftpClient.Retr(remotePath)
    if err != nil {
        fmt.Println(err)
        panic(err)
    }
    fmt.Println("DONE")

    buf := new(bytes.Buffer)
    buf.ReadFrom(reader)
    //s := buf.String()
    //fmt.Println(s)
    //data := []byte("hello")
    fo, err := os.Create(localPath)
    if err != nil {
        return err
    }
    defer fo.Close()
    reader.Close()

    _, err = io.Copy(fo, strings.NewReader("hello"))
    if err != nil {
        return err
    }

    return err
}

type conf struct {
    Server     string `yaml:"Server"`
    Port       string `yaml:"Port"`
    Username   string `yaml:"Username"`
    Password   string `yaml:"Password"`
    RemotePath string `yaml:"RemotePath"`
    LocalPath  string `yaml:"LocalPath"`
}

func (c *conf) loadFTPconfig() *conf {
    file, err := ioutil.ReadFile("config.yaml")
    if err != nil {
        fmt.Println(err)
    }

    err = yaml.Unmarshal(file, c)
    if err != nil {
        fmt.Println(err)
    }

    return c
}

func viewDirectory() error {
    fmt.Println("In viewDirectory")
    client, err := ftp.Dial("ftp.ncbi.nlm.nih.gov:21")
    if err != nil {
        fmt.Println(err)
        return err
    }
    if err := client.Login("anonymous", "test@test.com"); err != nil {
        return err
    }

    fmt.Println("bob1")
    reader, err := client.Retr("README.ftp")

    buf := new(bytes.Buffer)
    buf.ReadFrom(reader)
    s := buf.String()
    fmt.Println(s)
    d1 := []byte("hello")
    ioutil.WriteFile("./TEST", d1, 0644)


    fmt.Println("END")
    return err
}
