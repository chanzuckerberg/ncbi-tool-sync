* NCBI-replica with version history data storage platform. Phase 1 of creating a service for accessing old versions of NCBI data.

* See planning document: https://docs.google.com/document/d/1y9Y6Q5HgPHT5CfIPCMtkK2gIINtzcTEhdNzEWwqIIw4/edit

* Testing:
  - To avoid running some of the acceptance tests, run go test with -short, e.g.
    - ```go test -short ./...```

- Folder structure for sync component:
  - config.yaml (Config file)
  - sync.go
    - Actual synchronization step
  - post_process.go
    - Processing new, modified, and deleted files
  - archive.go
    - Archiving modified and deleted files
  - ftp.go
    - FTP utility functions
  - storage.go
    - Storage utility functions
  - util.go
  - remote/ (Folder mount point for AWS S3 with goofys (FUSE))
