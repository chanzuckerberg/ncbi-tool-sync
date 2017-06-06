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
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
)

var config conf
var db *sql.DB

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

func parseChanges(cmd string, inputPath string) ([][]string, error) {
    fmt.Println(cmd)
    out, err := callCommand(cmd)
    if err != nil {
        return nil, err
    }
    changes := strings.Split(string(out[:]), "\n")
    changes = changes[:len(changes)-4]      // Remove last junk lines

    var new, modified, deleted []string
    base := inputPath

    for _, line := range changes {
        col := strings.SplitN(line, " ", 2)
        change := col[0]
        file := col[1]
        if strings.HasPrefix(change, ">f+++++++") {
            new = append(new, base + "/" + file)
        } else if strings.HasPrefix(change, ">f") {
            modified = append(modified, base + "/" + file)
        } else if strings.HasPrefix(line, "*deleting") &&
            file[len(file)-1:] != "/" {
            // Exclude deleted folders
            deleted = append(deleted, base + "/" + file)
        }
    }
    var results [][]string
    results = append(results, new, modified, deleted)
    return results, err
}

func rsyncSimple(input string) error {
    var err error
    var cmd string

    // Construct Rsync parameters
    source := fmt.Sprintf("rsync://%s%s/", config.Server, input)
    tempDir := curTimeName()
    template := "rsync -abrzv %s --itemize-changes --delete --no-motd --exclude='.*' --backup-dir='%s' %s %s | tail -n+2"

    // Dry run
    cmd = fmt.Sprintf(template, "-n", tempDir, source, config.LocalPath)
    results, err := parseChanges(cmd, input)
    if err != nil {
        return err
    }
    new := results[0]
    modified := results[1]
    deleted := results[2]

    fmt.Printf("\nNEW: %s", new)
    fmt.Printf("\nMODIFIED: %s", modified)
    fmt.Printf("\nDELETED: %s", deleted)

    // Actual run
    cmd = fmt.Sprintf(template, "", tempDir, source, config.LocalPath)
    out, err := callCommand(cmd)
    if err != nil {
        return err
    }
    fmt.Printf("\n%s%s\n", out, err)

    // Handle changes
    db, err = sql.Open("sqlite3", "./versionDB.db")
    defer db.Close()
    if err != nil {
        return err
    }

    handleNewFiles(new)
    return nil
    handleModifiedFiles(modified)
    handleDeletedFiles(deleted)

    // Handle replaced or deleted files
    dest := fmt.Sprintf("%s/%s", config.LocalPath, tempDir)
    if _, err := os.Stat(dest); err == nil {
        err = filepath.Walk(dest, visitFile)
    }
    fmt.Printf("filepath.Walk() returned %v\n", err)

    // Delete temp folders after handling files
    return nil
}

func handleNewFiles(new []string) {
    handleNewVersions(new)
}

func handleModifiedFiles(modified []string) {

}

func handleDeletedFiles(deleted []string) {

}

// Handle a list of files with new versions
func handleNewVersions(files []string) error {
    for _, file := range files {
        handleNewVersion(file)
    }
    return nil
}

// Handle one file with a new version on disk
func handleNewVersion(file string) error {
    // Set version number
    var versionNum int = 1
    lastNum, err := findLastVersionNum(db, file)
    if err != nil {
        return err
    }
    if lastNum > -1 {       // Some version already exists
        versionNum = lastNum + 1
    }

    // Set datetime modified
    localPath := fmt.Sprintf("%s%s", config.LocalTop, file)
    fmt.Println(localPath)
    info, err := os.Stat(localPath)
    if err != nil {
        return err
    }
    modTime := fmt.Sprintf("%s", info.ModTime())
    fmt.Println("Last modified time : ", modTime)

    // Insert into database
    query := fmt.Sprintf("insert into entries(PathName, VersionNum, DateModified) values('%s', %d, '%s')", file, versionNum, modTime)
    _, err = db.Exec(query)
    if err != nil {
        return err
    }

    return nil
}

func findLastVersionNum(db *sql.DB, file string) (int, error) {
    query := fmt.Sprintf("select VersionNum from entries where PathName='%s' order by VersionNum desc", file)
    rows, err := db.Query(query)
    defer rows.Close()
    if err != nil {
        return -1, err
    }

    for rows.Next() {
        var VersionNum int
        err = rows.Scan(&VersionNum)
        if err != nil {
            return -1, err
        }
        return VersionNum, nil
    }
    return -1, nil
}

func curTimeName() string {
    t := time.Now()
    result := fmt.Sprintf("backup-%d-%02d-%02d-%02d-%02d-%02d",
        t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
    fmt.Println(result)
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
    LocalTop   string `yaml:"LocalTop"`
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