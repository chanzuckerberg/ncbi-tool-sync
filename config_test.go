package main

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestLoadSyncFolders(t *testing.T) {
	_, ctx := ctxTesting(t)
	ioutilReadFile = FakeIoutilReadFile
	loadConfigFile(ctx)
	assert.Equal(t, "rsync://ftp.ncbi.nih.gov", ctx.Server)
	assert.Equal(t, "czbiohub-ncbi-store", ctx.Bucket)
	assert.Equal(t, 2, len(ctx.syncFolders))
	ac := assert.Contains
	ae := assert.Equal
	f := ctx.syncFolders
	ae(t, f[0].sourcePath, "/blast/db")
	ac(t, f[0].flags, "exclude 'cloud/*'")
	ac(t, f[0].flags, "exclude 'other_genomic.gz'")
	ae(t, f[1].sourcePath, "/pub/taxonomy")
	ac(t, f[1].flags, "exclude '.*'")
}

func FakeIoutilReadFile(input string) ([]byte, error) {
	out := `server: rsync://ftp.ncbi.nih.gov
bucket: czbiohub-ncbi-store

syncFolders:
  - name: /blast/db
    flags:
      - exclude '.*'
      - exclude 'cloud/*'
      - exclude 'nr.??.tar.gz'
      - exclude 'nt.??.tar.gz'
      - exclude 'other_genomic.gz'
  - name: /pub/taxonomy
    flags:
      - exclude '.*'`
	return []byte(out), nil
}
