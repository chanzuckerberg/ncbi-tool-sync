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
    //"gopkg.in/dutchcoders/goftp.v1"
    "os/exec"
    //"log"
    "path/filepath"
    "time"
)

var config conf
var ftpClient *ftp.ServerConn

func main() {
    //var err error
    //ftpClient, err = connectToServer()
    //ftpClient.DisableEPSV = false
    //if err != nil {
    //    fmt.Println(err)
    //}

    //err = traverseFolder(config.RemotePath)
    //if err != nil {
    //    fmt.Println(err)
    //}

    config.loadFTPconfig()
    //listAllFiles("")
    rsyncSimple("")
}

type SyncConfig struct {
    Args     []string
    From     string
    To       string
}

func trimPath(input string) string {
    pieces := strings.Split(input, "/")
    last := pieces[len(pieces)-1]
    result := input[:len(input)-len(last)]
    return result
}

func callCommand(input string) ([]byte, error) {
    return exec.Command("sh","-c", input).Output()
}

// List all files recursively in a directory. Files only, full paths on server.
func listAllFiles(input string) []string {
    input = "genomes/all/GCF/001/696/305"

    // Call rsync and parse out list of files
    source := fmt.Sprintf("rsync://%s/%s", config.Server, input)
    fmt.Println(source)
    cmd := fmt.Sprintf("rsync -nr --list-only %s | tail -n+18 | tr -s ' ' | grep -v '^d' | cut -d ' ' -f5", source)
    out, _ := callCommand(cmd)

    fileList := strings.Split(string(out[:]), "\n")
    fileList = fileList[:len(fileList)-1]  // Remove last empty elem

    // Append input path to beginning of each file path output
    trimmed := trimPath(input)
    var resultList []string
    for _, value := range fileList {
        resultList = append(resultList, trimmed + value)
    }
    return resultList
}

func rsyncSimple(input string) {
    input = "blast/demo"

    // Call rsync
    source := fmt.Sprintf("rsync://%s/%s", config.Server, input)
    changedDir := curTimeName()
    template := "rsync -abrz --delete --exclude='.*' --backup-dir='%s' %s %s"
    cmd := fmt.Sprintf(template, changedDir, source, config.LocalPath)
    _, err := callCommand(cmd)
    //fmt.Printf("%s %s", out, err)

    // Handle changed files
    dest := fmt.Sprintf("%s/%s", config.LocalPath, changedDir)
    if _, err := os.Stat(dest); err == nil {
        err = filepath.Walk(dest, visitFile)
    }
    fmt.Printf("filepath.Walk() returned %v\n", err)

    // Delete temp folder after handling files
    
}

func curTimeName() string {
    t := time.Now()
    result := fmt.Sprintf("backup-%d-%02d-%02d-%02d-%02d-%02d",
        t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
    return result
}

// Handle each changed file
func visitFile(path string, f os.FileInfo, err error) error {
    if f.IsDir() || f.Name()[0] == '.' {
        return nil
    }
    fmt.Printf("Visited: %s\n", path)
    return err
}

func checksumChecks(input string) {
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
    ftpClient.DisableEPSV = false
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