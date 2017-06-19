package sync

import (
	"testing"
	"ncbi_proj/sync"
	"github.com/stretchr/testify/assert"
	"fmt"
	"os"
	"io"
	"strings"
)

func TestCallCommand(t *testing.T) {
	_, err := sync.CallCommand("ls")
	if err != nil {
		t.Error("Couldn't call ls")
	}
}

func TestParseChanges(t *testing.T) {
	var out []byte
	out = []byte("receiving file list ... done\n.d..tp... ./\n>f+++++++ blast_programming.ppt\n>f....... ieee_blast.final.ppt\n>f....... edited.ppt\n*deleting ieee_talk.pdf\n*deleting folder/\n.f..t.... mt_tback.tgz\n.f..t.... openmp_test.tar.gz\n>f+++++++ bingbong.ppt\n\nsent 414 bytes  received 2452 bytes  1910.67 bytes/sec\ntotal size is 6943964334  speedup is 2422876.60")
	new, mod, del := sync.ParseChanges(out, "")
	assert.Equal(t, "/blast_programming.ppt", new[0])
	assert.Equal(t, "/bingbong.ppt", new[1])
	assert.Equal(t, "/ieee_blast.final.ppt", mod[0])
	assert.Equal(t, "/edited.ppt", mod[1])
	assert.Equal(t, "/ieee_talk.pdf", del[0])
	assert.Len(t, del, 1)
}

func TestProcessChangesTrivial(t *testing.T) {
	ctx := new(sync.Context)
	ctx.LocalPath = "local/sub"
	ctx.LocalTop = "local"

	err := ctx.ProcessChanges([]string{}, []string{}, "temp")
	if err != nil {
		t.FailNow()
	}
}

func TestCurTimeName(t *testing.T) {
	res := sync.CurTimeName()
	assert.Contains(t, res, "backup")
}

func TestGenerateHash(t *testing.T) {
	fo, err := os.Create("temp")
	if err != nil {
		t.FailNow()
	}
	defer fo.Close()
	_, err = io.Copy(fo, strings.NewReader("testing"))
	if err != nil {
		t.FailNow()
	}

	res, err := sync.GenerateHash("temp", "tempHello", 1)
	fmt.Println(res)
	assert.Equal(t, "4da1b90d8dcea849087d2df445df67ff", res)
	os.Remove("temp")
}
