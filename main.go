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
    listAllFiles("")
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

// List all files recursively in a directory. Files only, full paths on server.
func listAllFiles(input string) []string {
    input = "genomes/all/GCF/001/696/305"

    // Call rsync and parse out list of files
    source := "rsync://" + config.Server + "/" + input
    cmd := "rsync -nr --list-only " + source + " | tail -n+18 | tr -s ' ' | grep -v '^d' | cut -d ' ' -f5"
    out, _ := exec.Command("sh","-c", cmd).Output()

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

func rsyncTest() {
    c1 := exec.Command("rsync", "-nr", "--list-only", "rsync://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/001/696/305/GCF_001696305.1_UCN72.1/", "| head -n 5")

    c2 := exec.Command("tail", "-n+18")
    c3 := exec.Command("awk", "-d' '", "-f 3")

    c2.Stdin, _ = c1.StdoutPipe()
    c3.Stdin, _ = c2.StdoutPipe()
    c3.Stdout = os.Stdout
    _ = c3.Start()
    _ = c2.Start()
    _ = c1.Run()
    _ = c3.Wait()
    _ = c2.Wait()
    stdoutStderr, _ := c3.CombinedOutput()

    fmt.Printf("%s", stdoutStderr)
}

func callRsync(cfg SyncConfig) {
    cfg.Args = append(cfg.Args, cfg.From, cfg.To)
    cmd := exec.Command("rsync", "")

    cmd.Stdout = os.Stdout
    cmd.Stderr = os.Stderr
    fmt.Println(cmd.Stdout)
    fmt.Println(cmd.Stderr)
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