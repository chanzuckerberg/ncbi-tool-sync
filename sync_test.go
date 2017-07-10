package main

import (
	"database/sql"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"os/user"
	"log"
	"time"
)

// Metadata about a file version from the db
type Metadata struct {
	Path       string
	Version    int
	ModTime    sql.NullString
	ArchiveKey sql.NullString
}

func TestCallCommand(t *testing.T) {
	_, err := callCommand("ls")
	if err != nil {
		t.Error("Couldn't call ls")
	}
}

func TestParseChanges(t *testing.T) {
	out := "receiving file list ... done\n.d..tp... ./\n>f+++++++ blast_programming.ppt\n>f....... ieee_blast.final.ppt\n>f....... edited.ppt\n*deleting ieee_talk.pdf\n*deleting folder/\n.f..t.... mt_tback.tgz\n.f..t.... openmp_test.tar.gz\n>f+++++++ bingbong.ppt\n\nsent 414 bytes  received 2452 bytes  1910.67 bytes/sec\ntotal size is 6943964334  speedup is 2422876.60\n"
	newNow, mod, del := parseChanges(out, "")
	assert.Equal(t, "/blast_programming.ppt", newNow[0])
	assert.Equal(t, "/bingbong.ppt", newNow[1])
	assert.Equal(t, "/ieee_blast.final.ppt", mod[0])
	assert.Equal(t, "/edited.ppt", mod[1])
	assert.Equal(t, "/ieee_talk.pdf", del[0])
	assert.Len(t, del, 1)
}

func TestProcessChangesTrivial(t *testing.T) {
	ctx := new(Context)
	ctx.os = afero.NewMemMapFs()
	ctx.LocalPath = "local/sub"
	ctx.LocalTop = "local"

	err := ctx.processChanges([]string{}, []string{}, "temp")
	if err != nil {
		t.Logf(err.Error())
	}
}

func TestCurTimeName(t *testing.T) {
	res := timeName()
	assert.Contains(t, res, "backup")
}

// Touches the actual disk
func TestGenerateHash(t *testing.T) {
	ctx := Context{os: afero.NewOsFs()}

	fo, err := ctx.os.Create("temp")
	if err != nil {
		t.Logf(err.Error())
	}
	defer fo.Close()
	_, err = io.Copy(fo, strings.NewReader("testing"))
	if err != nil {
		t.Logf(err.Error())
	}

	res, err := ctx.generateHash("temp", "tempHello", 1)
	assert.Equal(t, "f761c73b4cefb84b02e0b7f1576ca395", res)
	ctx.os.Remove("temp")
}

func SetupInitialState(t *testing.T) (Context, error) {
	db, err := sql.Open("mysql",
		"dev:password@tcp(127.0.0.1:3306)/testdb")
	db.Exec("drop table entries")
	db.Exec("create table if not exists entries")

	query := "CREATE TABLE IF NOT EXISTS entries (" +
		"PathName VARCHAR(500) NOT NULL, " +
		"VersionNum INT NOT NULL, " +
		"DateModified DATETIME, " +
		"ArchiveKey VARCHAR(50), " +
		"PRIMARY KEY (PathName, VersionNum));"
	_, err = db.Exec(query)

	if err != nil {
		t.Errorf(err.Error())
	}
	ctx := Context{
		Db:         db,
		os:         afero.NewOsFs(),
		Server:     "rsync://ftp.ncbi.nih.gov",
		SourcePath: "/blast/demo/igblast",
	}
	usr, _ := user.Current()
	ctx.UserHome = usr.HomeDir
	ctx.LocalTop = ctx.UserHome + "/test/remote"
	ctx.LocalPath = ctx.LocalTop + ctx.SourcePath
	ctx.Archive = ctx.LocalTop + "/archive"
	ctx.Bucket = "czbiohub-jsheu-test"
	ctx.TempNew = ctx.UserHome + "/test/tempNew"
	err = ctx.os.MkdirAll(ctx.LocalTop+"/blast/demo/igblast", os.ModePerm)
	if err != nil {
		t.Errorf(err.Error())
	}
	log.Println("Calling setup sync...")
	cmd := "rsync -abrzv --itemize-changes --delete --size-only --no-motd --exclude='.*' rsync://ftp.ncbi.nlm.nih.gov/blast/demo/igblast/ " + ctx.LocalTop + "/blast/demo/igblast"
	commandVerbose(cmd)
	return ctx, err
}

func cleanup(ctx Context) {
	os.RemoveAll(ctx.UserHome + "/test")
	ctx.Db.Exec("drop table entries")
}

// Full-flow acceptance test for new files.
// Happy path for new files from remote server.
func TestSyncNewAcceptance(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// Set up state
	ctx, err := SetupInitialState(t)
	if err != nil {
		t.Errorf(err.Error())
	}
	err = os.Remove(ctx.LocalTop + "/blast/demo/igblast/readme")
	if err != nil {
		t.Errorf(err.Error())
	}

	// Call our function to test
	if err = ctx.callRsyncFlow(); err != nil {
		t.Errorf("Unexpected: %s", err)
	}

	// Verify expectations
	md := Metadata{}
	rows, err := ctx.Db.Query("select * from entries;")
	rows.Next()
	rows.Scan(&md.Path, &md.Version, &md.ModTime, &md.ArchiveKey)
	assert.Equal(t, "/blast/demo/igblast/readme", md.Path)
	assert.Equal(t, 1, md.Version)
	assert.Equal(t, "2011-09-16 16:33:49", md.ModTime.String)
	_, err = os.Stat(ctx.LocalTop + "/blast/demo/igblast/readme")

	if err != nil {
		t.Errorf("Unexpected: %s", err)
	}

	cleanup(ctx)
}

// Full-flow acceptance test for modified files.
// Happy path for modified files from remote server.
func TestSyncModifiedAcceptance(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// Set up state
	ctx, err := SetupInitialState(t)
	if err != nil {
		t.Errorf(err.Error())
	}
	ctx.Db.Exec("insert into entries(PathName, VersionNum, DateModified) values('/blast/demo/igblast/readme', 1, '2010-09-16 16:33:49')")
	out, err := callCommand("echo 'FILE WAS MODIFIED' >> " + ctx.LocalTop + "/blast/demo/igblast/readme")
	if err != nil {
		t.Errorf("%s, %s", out, err.Error())
	}

	// Call our function to test
	if err = ctx.callRsyncFlow(); err != nil {
		t.Errorf("Unexpected: %s", err)
	}

	// Verify expectations
	md := Metadata{}
	rows, err := ctx.Db.Query("select * from entries order by VersionNum desc;")
	rows.Next()
	rows.Scan(&md.Path, &md.Version, &md.ModTime, &md.ArchiveKey)
	assert.Equal(t, "/blast/demo/igblast/readme", md.Path)
	assert.Equal(t, 2, md.Version)
	assert.Equal(t, "2011-09-16 16:33:49", md.ModTime.String)
	_, err = os.Stat(ctx.LocalTop+"/blast/demo/igblast/readme")
	rows.Next()
	rows.Scan(&md.Path, &md.Version, &md.ModTime, &md.ArchiveKey)
	assert.Equal(t, "/blast/demo/igblast/readme", md.Path)
	assert.Equal(t, 1, md.Version)
	assert.Equal(t, "2010-09-16 16:33:49", md.ModTime.String)
	assert.Equal(t, "e0942d53c65bb568d200f25b3042fbc7", md.ArchiveKey.String)
	_, err = os.Stat(ctx.LocalTop+"/archive/e0942d53c65bb568d200f25b3042fbc7")

	b, err := ioutil.ReadFile(ctx.LocalTop+"/archive/e0942d53c65bb568d200f25b3042fbc7")
	if !strings.Contains(string(b), "FILE WAS MODIFIED") {
		t.Errorf("Archive copy doesn't contain string.")
	}
	b, err = ioutil.ReadFile(ctx.LocalTop+"/blast/demo/igblast/readme")
	if strings.Contains(string(b), "FILE WAS MODIFIED") {
		t.Errorf("New version is wrong.")
	}

	if err != nil {
		t.Errorf("Unexpected: %s", err)
	}

	cleanup(ctx)
}

// Full-flow acceptance test for deleted files
// Happy path for deleted files from remote server.
func TestSyncDeletedAcceptance(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// Set up state
	ctx, err := SetupInitialState(t)
	if err != nil {
		t.Errorf(err.Error())
	}
	time.Sleep(time.Duration(5)*time.Second)
	_, err = callCommand("touch " + ctx.LocalTop+ "/blast/demo/igblast/testfile")
	ctx.Db.Exec("insert into entries(PathName, VersionNum, DateModified) values('/blast/demo/igblast/testfile', 1, '2010-09-16 16:33:49')")

	// Call our function to test
	if err = ctx.callRsyncFlow(); err != nil {
		t.Errorf("Unexpected: %s", err)
	}

	// Verify expectations
	md := Metadata{}
	rows, err := ctx.Db.Query("select * from entries order by VersionNum desc;")
	rows.Next()
	rows.Scan(&md.Path, &md.Version, &md.ModTime, &md.ArchiveKey)
	if err != nil {
		t.Errorf("Unexpected: %s", err)
	}
	assert.Equal(t, "/blast/demo/igblast/testfile", md.Path)
	assert.Equal(t, 1, md.Version)
	assert.Equal(t, "2010-09-16 16:33:49", md.ModTime.String)
	assert.Equal(t, "d37650ecfee9f1acdb11699503407acf", md.ArchiveKey.String)
	time.Sleep(time.Duration(10)*time.Second)
	_, err = os.Stat(ctx.LocalTop+"/blast/demo/igblast/testfile")
	if err == nil {
		t.Errorf("File wasn't deleted from current folder properly.")
		t.FailNow()
	}
	_, err = os.Stat(ctx.LocalTop+"/archive/d37650ecfee9f1acdb11699503407acf")
	if err != nil {
		t.Errorf("File isn't in archive properly.")
		t.FailNow()
	}

	cleanup(ctx)
}
