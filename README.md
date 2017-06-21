* NCBI-replica with version history data storage platform. Phase 1 of creating a service for accessing old versions of NCBI data.

* See planning document: https://docs.google.com/document/d/1y9Y6Q5HgPHT5CfIPCMtkK2gIINtzcTEhdNzEWwqIIw4/edit

- Folder structure:
    - server/ (Server component)
      - config.yaml (Config file)
      - models/
      - controllers/
      - utils/
      - web/
    - syncjob/ (Rsync-based synchronization tool)
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
    - cliclient/ (Command line client)
    - remote/ (Folder mount point for AWS S3 with goofys (FUSE))