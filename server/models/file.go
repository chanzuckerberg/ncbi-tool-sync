package models

import "database/sql"

type File struct {

}

func NewFile() *File {
    return &File{}
}

func (f *File) Get(pathName string, versionNum int, db *sql.DB) {
}

func (f *File) GetLatest(pathName string) {

}

func (f *File) GetHistory(pathName string) {

}