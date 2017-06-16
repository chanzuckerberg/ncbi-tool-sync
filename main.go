package main

import (
	//"ncbi_proj/sync"
	"ncbi_proj/server"
	"fmt"
)

func main() {
	err := server.Main()
	if err != nil {
		fmt.Println("ERROR")
		fmt.Println(err)
	}
	//sync.Main()
}
