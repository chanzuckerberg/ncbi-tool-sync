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
    "crypto/md5"
    "io"
    "encoding/hex"
)

var config conf
var db *sql.DB

func main() {
    config.loadConfig()
    err := callRsyncFlow(config.RemotePath)
    if err != nil {
        fmt.Println(err)
    }
}

func callCommand(input string) ([]byte, error) {
    return exec.Command("sh","-c", input).Output()
}

// Parse the Rsync itemized output for new, modified, and deleted files
func parseChanges(out []byte, base string) ([]string, []string, []string) {
    changes := strings.Split(string(out[:]), "\n")
    changes = changes[:len(changes)-4]      // Remove last junk lines

    var new, modified, deleted []string

    for _, line := range changes {
        col := strings.SplitN(line, " ", 2)
        change := col[0]
        file := col[1]
        path := base + "/" + file
        last := file[len(file)-1:]
        if strings.HasPrefix(change, ">f+++++++") {
            new = append(new, path)
        } else if strings.HasPrefix(change, ">f") {
            modified = append(modified, path)
        } else if strings.HasPrefix(change, "*deleting") && last != "/" {
            // Exclude folders
            deleted = append(deleted, path)
        }
    }
    return new, modified, deleted
}

func callRsyncFlow(input string) error {
    var err error
    var cmd string

    // Construct Rsync parameters
    source := fmt.Sprintf("rsync://%s%s/", config.Server, input)
    tempDir := curTimeName()
    template := "rsync -abrzv %s --itemize-changes --delete --no-motd " +
        "--exclude='.*' --backup-dir='%s' %s %s | tail -n+2"

    // Dry run
    cmd = fmt.Sprintf(template, "-n", tempDir, source, config.LocalPath)
    fmt.Println(cmd)
    out, err := callCommand(cmd)
    if err != nil { return err }
    new, modified, deleted := parseChanges(out, input)
    fmt.Printf("\nNEW: %s", new)
    fmt.Printf("\nMODIFIED: %s", modified)
    fmt.Printf("\nDELETED: %s", deleted)

    // Actual run
    os.MkdirAll(config.LocalPath, os.ModePerm)
    cmd = fmt.Sprintf(template, "", tempDir, source, config.LocalPath)
    out, err = callCommand(cmd)
    if err != nil { return err }
    fmt.Printf("\n%s%s\n", out, err)

    // Process changes
    err = processChanges(new, modified, tempDir)
    return err
}

func processChanges(new []string, modified []string, tempDir string) error {
    // Open db
    var err error
    db, err = sql.Open("sqlite3", "./versionDB.db")
    defer db.Close()
    if err != nil { return err }

    // Move replaced or deleted file versions to archive
    err = archiveOldVersions(tempDir)
    if err != nil { return err }

    // Add new or modified files as db entries
    err = handleNewVersions(new)
    if err != nil { return err }
    err = handleNewVersions(modified)
    if err != nil { return err }

    // Delete temp folder after handling files
    path := fmt.Sprintf("%s/%s", config.LocalPath, tempDir)
    err = os.RemoveAll(path)

    return err
}

func archiveOldVersions(tempDir string) error {
    var err error

    // Return if rsync didn't make a modified folder
    _, err = os.Stat(fmt.Sprintf("%s/%s", config.LocalPath, tempDir))
    if err != nil { return nil }

    // Make archive folder
    dest := fmt.Sprintf("%s/%s", config.LocalPath, tempDir)
    os.MkdirAll(config.LocalTop + "/archive", os.ModePerm)

    // Walk through each modified file
    if _, err := os.Stat(dest); err == nil {
        err = filepath.Walk(dest, archiveFile(tempDir))
    }

    return err
}

// Handle a list of files with new versions
func handleNewVersions(files []string) error {
    for _, file := range files {
        err := handleNewVersion(file)
        if err != nil { return err }
    }
    return nil
}

// Handle one file with a new version on disk
func handleNewVersion(file string) error {
    // Set version number
    var versionNum int = 1
    prevNum := findPrevVersionNum(file, true)
    if prevNum > -1 { // Some version already exists
        versionNum = prevNum + 1
    }

    // Set datetime modified
    path := fmt.Sprintf("%s%s", config.LocalTop, file)
    info, err := os.Stat(path)
    if err != nil { return err }
    modTime := fmt.Sprintf("%s", info.ModTime())

    // Insert into database
    query := fmt.Sprintf("insert into entries(PathName, VersionNum, " +
        "DateModified) values('%s', %d, '%s')", file, versionNum, modTime)
    _, err = db.Exec(query)

    return err
}

// Find the latest version number of the file
func findPrevVersionNum(file string, includeArchived bool) int {
    var num int = -1
    var archive string = ""
    if !includeArchived {      // Specify not archived entries
        archive = "and ArchiveKey is null "
    }

    query := fmt.Sprintf("select VersionNum from entries where " +
        "PathName='%s' %sorder by VersionNum desc", file, archive)
    rows, err := db.Query(query)
    defer rows.Close()
    if err != nil {
        return num
    }

    rows.Next()
    err = rows.Scan(&num)
    return num
}

// Generate a folder name from the current datetime
func curTimeName() string {
    t := time.Now()
    result := fmt.Sprintf("backup-%d-%02d-%02d-%02d-%02d-%02d",
        t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
    return result
}

// Handle each changed file
func archiveFile(tempDir string) filepath.WalkFunc {
    return func(origPath string, f os.FileInfo, err error) error {
        if f.IsDir() { return nil }

        // Setup
        newPath := origPath[len(config.LocalTop)-2:]             // Remove first part of newPath
        newPath = strings.Replace(newPath, tempDir + "/", "", 1) // Remove tempDir
        num := findPrevVersionNum(newPath, false)
        key, err := generateHash(origPath, newPath, num)
        if err != nil { return err }

        // Move to archive folder
        dest := fmt.Sprintf("%s/archive/%s", config.LocalTop[2:], key)
        err = os.Rename(origPath, dest)

        // Update the old entry with archiveKey blob
        query := fmt.Sprintf("update entries set ArchiveKey='%s' " +
            "where PathName='%s' and VersionNum=%d;", key, newPath, num)
        _, err = db.Exec(query)

        return err
    }
}

// Hash for archiveKey
func generateHash(origPath string, path string, num int) (string, error) {
    // Add a header
    key := fmt.Sprintf("%s -- Version %d -- ", path, num)
    hash := md5.New()
    io.WriteString(hash, key)

    // Add the file contents
    var result string
    file, err := os.Open(origPath)
    if err != nil {
        return result, err
    }
    defer file.Close()
    if _, err := io.Copy(hash, file); err != nil {
        return result, err
    }

    // Generate checksum
    hashInBytes := hash.Sum(nil)[:16]
    result = hex.EncodeToString(hashInBytes)
    return result, nil
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

// Load the configuration file
func (c *conf) loadConfig() *conf {
    file, err := ioutil.ReadFile("config.yaml")
    if err != nil { panic(err) }

    err = yaml.Unmarshal(file, c)
    if err != nil { panic(err) }

    return c
}
