/*
Usage: go run node.go [ip:port] [id]
	•[ip:port] : address of the key-value service
	•[id] : a unique string identifier for the node
*/

package main

import (
	"fmt"
	"os"
	"strconv"
	"net/rpc"
)

// args in get(args)
type GetArgs struct {
	Key string // key to look up
}

// args in put(args)
type PutArgs struct {
	Key string // key to associate value with
	Val string // value
}

// args in testset(args)
type TestSetArgs struct {
	Key     string // key to test
	TestVal string // value to test against actual value
	NewVal  string // value to use if testval equals to actual value
}

// Reply from service for all three API calls above.
type ValReply struct {
	Val string // value; depends on the call
}

type KeyValService int

func main(){
	usage := fmt.Sprintf("Usage: %s ip:port id\n", os.Args[0])
	if len(os.Args) != 3 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	kvAddr := os.Args[1]
	id := os.Args[2]

	println(kvAddr)
	// Connect to the KV-service via RPC.
	kvService, err := rpc.Dial("tcp", kvAddr)
	checkError(err)

	// Use kvVal for all RPC replies.
	var kvVal ValReply

	// Put(id, 2016)
	putArgs := PutArgs{
		Key: strconv.Itoa(0),
		Val: id}
	err = kvService.Call("KeyValService.Put", putArgs, &kvVal)
	checkError(err)
	fmt.Println("KV.put(" + putArgs.Key + "," + putArgs.Val + ") = " + kvVal.Val)

	// Get("my-key")
	getArgs := GetArgs{strconv.Itoa(0)}
	err = kvService.Call("KeyValService.Get", getArgs, &kvVal)
	checkError(err)
	fmt.Println("KV.get(" + getArgs.Key + ") = " + kvVal.Val)

}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		os.Exit(1)
	}
}