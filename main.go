package main

import (
    "fmt"
    "io/ioutil"
    "gopkg.in/yaml.v2"
    "os"
    "strings"
    "os/exec"
    "path/filepath"
    "time"
)

var config conf

func main() {
    config.loadConfig()
    //listAllFiles("")
    rsyncSimple(config.RemotePath)
}

type SyncConfig struct {
    Args     []string
    From     string
    To       string
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
    cmd := fmt.Sprintf("rsync -anr --list-only --no-motd %s | tail -n+1 | tr -s ' ' | grep -v '^d' | cut -d ' ' -f5", source)
    out, _ := callCommand(cmd)

    fileList := strings.Split(string(out[:]), "\n")
    fileList = fileList[:len(fileList)-1]       // Remove last empty elem

    // Append input path to beginning of each file path output
    trimmed := filepath.Dir(input)
    var resultList []string
    for _, value := range fileList {
        resultList = append(resultList, trimmed + value)
    }
    return resultList
}

func parseChanges(cmd string, inputPath string) ([]string, []string, []string) {
    out, _ := callCommand(cmd)
    fmt.Printf("%s", out)
    changeList := strings.Split(string(out[:]), "\n")
    changeList = changeList[:len(changeList)-4]     // Remove last junk lines

    var newL []string
    var modifiedL []string
    var deletedL []string
    trimmed := filepath.Dir(inputPath)

    for _, line := range changeList {
        result := strings.SplitN(line, " ", 2)
        change := result[0]
        file := result[1]
        if strings.HasPrefix(change, ">f+++++++") {
            newL = append(newL, trimmed + "/" + file)
        } else if strings.HasPrefix(change, ">f") {
            modifiedL = append(modifiedL, trimmed + "/" + file)
        } else if strings.HasPrefix(line, "*deleting") &&
            file[len(file)-1:] != "/" {
            // Exclude deleted folders
            deletedL = append(deletedL, trimmed + "/" + file)
        }
    }
    return newL, modifiedL, deletedL
}

func rsyncSimple(input string) {
    var err error
    var cmd string

    // Construct Rsync parameters
    source := fmt.Sprintf("rsync://%s%s/", config.Server, input)
    tempDir := curTimeName()
    template := "rsync -abrzv %s --itemize-changes --delete --no-motd --exclude='.*' --backup-dir='%s' %s %s | tail -n+2"

    // Dry run
    cmd = fmt.Sprintf(template, "-n", tempDir, source, config.LocalPath)
    newL, modifiedL, deletedL := parseChanges(cmd, input)
    fmt.Printf("\nNEW: %s", newL)
    fmt.Printf("\nMODIFIED: %s", modifiedL)
    fmt.Printf("\nDELETED: %s", deletedL)

    // Actual run
    //cmd = fmt.Sprintf(template, "", tempDir, source, config.LocalPath)
    //out, _ := callCommand(cmd)
    //fmt.Printf("%s", out)

    // Handle changes
    handleNewFiles(newL)
    handleModifiedFiles(modifiedL)
    handleDeletedFiles(deletedL)
    return

    // Handle replaced or deleted files
    dest := fmt.Sprintf("%s/%s", config.LocalPath, tempDir)
    if _, err := os.Stat(dest); err == nil {
        err = filepath.Walk(dest, visitFile)
    }
    fmt.Printf("filepath.Walk() returned %v\n", err)

    // Delete temp folders after handling files
}

func handleNewFiles(newL []string) {

}

func handleModifiedFiles(modifiedL []string) {

}

func handleDeletedFiles(deletedL []string) {

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

type conf struct {
    Server     string `yaml:"Server"`
    Port       string `yaml:"Port"`
    Username   string `yaml:"Username"`
    Password   string `yaml:"Password"`
    RemotePath string `yaml:"RemotePath"`
    LocalPath  string `yaml:"LocalPath"`
}

func (c *conf) loadConfig() *conf {
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
