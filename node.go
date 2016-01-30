/*
Usage: go run node.go [ip:port] [id]
	•[ip:port] : address of the key-value service
	•[id] : a unique string identifier for the node
*/

package main

import (
	"fmt"
	"net/rpc"
	"os"
	"strconv"

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
	
	// parse args
	usage := fmt.Sprintf("Usage: %s ip:port id\n", os.Args[0])
	if len(os.Args) != 3 {
		fmt.Printf(usage)
		os.Exit(1)
	}
   
   kvAddr := os.Args[1]
   nodeId := os.Args[2]
   var key string

    // Connect to the KV-service via RPC.
	kvService, err := rpc.Dial("tcp", kvAddr)
	checkError(err)

	//Set a key to equal nodeId
	key = grabKey(nodeId, kvService)
    fmt.Println("key we grabbed is: " + key)

    //check to see if we're the leader
    checkHandleLeader(nodeId, key, kvService)

	//Update Key and handle unavailability
	key = updateKey(key, nodeId, kvService)
	fmt.Println("key we updated: " + key)



}

// If error is non-nil, print it out and halt.
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		os.Exit(1)
	}
}

//grabs a key value
func grabKey(nodeId string, kvService *rpc.Client) (key string){
	// Use kvVal for all RPC replies
	var kvVal ValReply
    var initKey int
    initKey = 0

	   for initKey > -1 {
	   	myKey := strconv.Itoa(initKey)

		tsArgs := TestSetArgs{
			Key:     myKey,
			TestVal: "",
			NewVal:  nodeId}
		err := kvService.Call("KeyValService.TestSet", tsArgs, &kvVal)
		checkError(err)
		fmt.Println("KV.get(" + tsArgs.Key + "," + tsArgs.TestVal + "," + tsArgs.NewVal + ") = " + kvVal.Val)
			if(kvVal.Val != nodeId || kvVal.Val == "unavailable"){
             initKey++
			}else{
				fmt.Println("We wrote " + kvVal.Val + " to a key: "+ myKey)
				key = myKey
				break
			}
		}
		return
}

//update your key
func updateKey(keyValue string, myId string, kvService *rpc.Client)(newKey string){
	var kvVal ValReply
	tsArgs := TestSetArgs{
		Key:	keyValue,
		TestVal: myId,
		NewVal: myId}
	err := kvService.Call("KeyValService.TestSet", tsArgs, &kvVal)
	checkError(err)
	fmt.Println("KV.get(" + tsArgs.Key + "," + tsArgs.TestVal + "," + tsArgs.NewVal + ") = " + kvVal.Val)
  if(kvVal.Val == "can't touch this"){
  	var kvValUpdate ValReply
  		tsArgsUpdate := TestSetArgs{
		Key:	keyValue,
		TestVal: "can't touch this",
		NewVal: myId}
  	err = kvService.Call("KeyValService.TestSet", tsArgsUpdate, &kvValUpdate)
	checkError(err)

   fmt.Println("Key value was updated: " + kvValUpdate.Val)
   newKey = kvValUpdate.Val

  } else if kvVal.Val == "unavailable" {
  	newKey = grabKey(myId, kvService)
  	 fmt.Println("Key was updated: " + newKey)
  } else {
  	fmt.Println("Key is still available")
    newKey = keyValue
  }

  return

}


//checks if this node is the leader, if it is it handles it.
func checkHandleLeader(myId string, myKey string, kvService *rpc.Client){
	var initKey int
	initKey = 0

	for initKey > -1 {
	leader := strconv.Itoa(initKey)
	var kvVal ValReply
	getArgs := GetArgs{leader}
	err := kvService.Call("KeyValService.Get", getArgs, &kvVal)
	checkError(err)
	fmt.Println("KV.get(" + getArgs.Key + ") = " + kvVal.Val)
		if(kvVal.Val == "unavailable"){
			initKey++
		} else if (kvVal.Val == myId && leader == myKey) {
				fmt.Println("I'm the leader! I should do leader things.")
				break
			} else {
				fmt.Println("I'm not the leader")
				break
			}
	}

}

//read Acvtive nodes in KVServer, in increasing order
func readActiveNodes(){

}

//read values of keys from KVServer in increasing order until final active node
func readNodes(){

}

//if this node is leader node, reset all values to 1
func resetValues(){

}

