package main

import (
	"github.com/jasonlvhit/gocron"
	"log"
	"time"
)

var callSyncFlow = callSyncFlowRepeat

// callSyncFlowRepeat calls the Rsync workflow. Executes a dry run first for
// identifying changes. Then runs the actual file sync operations. Finally
// updates the db with changes. Gocron schedules repeating runs.
func callSyncFlowRepeat(ctx *context, repeat bool) error {
	log.Print("Start of sync flow...")
	var err error

	// Check db
	if err = ctx.db.Ping(); err != nil {
		return handle("Failed to ping database. Aborting run.", err)
	}

	// Offset scheduling of next run so it'll only schedule after this one
	// finishes.
	gocron.Clear()
	defer func() {
		if !repeat {
			return
		}
		gocron.Every(12).Hours().Do(callSyncFlowRepeat, ctx, true)
		log.Print("Next run has been scheduled...")
		<-gocron.Start()
	}()

	// Dry run analysis stage for identifying file changes.
	toSync, err := dryRunStage(ctx)
	if err != nil {
		// If listing from NCBI fails, wait some time and try again. Gocron
		// scheduling will still be in effect.
		defer func() {
			time.Sleep(5 * time.Minute)
			callSyncFlowRepeat(ctx, false)
		}()
		return handle("Error in dry run stage.", err)
	}

	// File operation stage. Moving actual files around.
	fileOperationStage(ctx, toSync)

	log.Print("Finished processing changes.")
	log.Print("End of sync flow...")
	return err
}

// An fInfo represents file path name, modified time, and size in bytes.
type fInfo struct {
	name    string
	modTime string
	size    int
}

// A syncResult represents lists of new, modified, and deleted files.
type syncResult struct {
	newF     []string
	modified []string
	deleted  []string
}
