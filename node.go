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
	"strings"
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

type MapVal struct {
	value string // the underlying value representation
}
//map of known nodes
var kvmap map[string]*MapVal
var id string
var leaderKey int
var leaderID string
var myRegisteredKey int
var kvService *rpc.Client
var firstNode int

func main(){
	usage := fmt.Sprintf("Usage: %s ip:port id\n", os.Args[0])
	if len(os.Args) != 3 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	kvAddr := os.Args[1]
	id = os.Args[2]

	firstNode = 0

	kvmap = make(map[string]*MapVal)
	// Connect to the KV-service via RPC.
	var err error
	kvService, err = rpc.Dial("tcp", kvAddr)
	checkError(err)


	//just joining the party
	//Pair id with first availible key

	joinTheParty(id)
		



	//iterate through KV server and see which nodes are also connected
	lastValue := "NotNil"

	
	var j int
	for {
		j = 0
		toPrint := 0
		for lastValue != ""{
			
			var replyForGets ValReply
			getArgs := GetArgs{strconv.Itoa(j)}
			err = kvService.Call("KeyValService.Get", getArgs, &replyForGets)
			checkError(err)

			lastValue = replyForGets.Val

			if(replyForGets.Val==id && myRegisteredKey != j){
				myRegisteredKey = j
			}
			
			change := didValChange(replyForGets.Val, j)
			if (toPrint == 0 && change == 1){
				toPrint = 1
			}
			j = j + 1
		}
		
		if toPrint == 1{
		 	printAllNodes()
		}

		if amILeader(){
			leaderTasks()
		} else if checkingLeader(){
			seeIfLeaderIsStillActive()
		}


		lastValue = "NotNil"
		
	}
}

func didValChange(checkVal string, key int) int{
	if (checkVal == "") {return 0}

	val := kvmap[strconv.Itoa(key)]
	if (val == nil) {
		val = &MapVal{
			value: checkVal,
		}
		kvmap[strconv.Itoa(key)] = val
		return 1
	} else if (checkVal == "unavailable"){
		//TODO set in map the unavailability
		val.value = checkVal

	} else if (val.value == checkVal) {
		return 0
	}else if (val.value != checkVal) {
		val.value = checkVal
		return 1
	} 

	return 0
}

func printAllNodes(){
	hasNext := true
	var i int = 0
	for hasNext{
		val := kvmap[strconv.Itoa(i)]
		if val == nil {
			hasNext = false
		} else if (val.value == "unavailable" || val.value == "Offline") {
			//Do Not Print
		} else  {
			print(val.value + " ")
		}
		i = i+2
	}
	println()
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		os.Exit(1)
	}
}



func leaderTasks(){
	//set self to leader on the KV service
	var kvVal ValReply
	putArgs := PutArgs{
		Key: strconv.Itoa(myRegisteredKey+1),
		Val: "Leader"}
	err := kvService.Call("KeyValService.Put", putArgs, &kvVal)
	checkError(err)
	if(kvVal.Val == "unavailable"){return}

	hasNext := true
	var i int = 0
	for hasNext{
		
		if checkIfKeyBecomesUnavailable(leaderKey){
			return
		}
		//stayActive
		putArgs := PutArgs{
			Key: strconv.Itoa(myRegisteredKey+1),
			Val: "Leader"}
		err := kvService.Call("KeyValService.Put", putArgs, &kvVal)
		checkError(err)

		//check if a key is unavailable
		val := kvmap[strconv.Itoa(i)]
		stat := kvmap[strconv.Itoa(i+1)]
		
		if (val!=nil) {
			if (val.value != "unavailable" && val.value != "Offline"){
				if (checkIfKeyBecomesUnavailable(i)){
					joinTheParty(val.value)
				} else if (stat.value != "PendingDisconnect"){
					//set test var for node
					putPendingDis := PutArgs{
						Key: strconv.Itoa(i+1),
						Val: "PendingDisconnect"}
					err := kvService.Call("KeyValService.Put", putPendingDis, &kvVal)
					checkError(err)
				} else if (stat.value == "PendingDisconnect"){
					//set set node offline
					putOffline := PutArgs{
						Key: strconv.Itoa(i+1),
						Val: "Offline"}
					err := kvService.Call("KeyValService.Put", putOffline, &kvVal)
					checkError(err)
				}

			}
		}

		i=i+2

		//reset counter to 0
		if(val==nil){
			hasNext = false
		}
	}

}

func checkIfKeyBecomesUnavailable(keyToCheck int) bool{
	// Get("my-key")
	var replyVal ValReply
	getValArgs := GetArgs{strconv.Itoa(keyToCheck)}
	err := kvService.Call("KeyValService.Get", getValArgs, &replyVal)
	checkError(err)

	var replyStatus ValReply
	getKeyArgs := GetArgs{strconv.Itoa(keyToCheck+1)}
	err = kvService.Call("KeyValService.Get", getKeyArgs, &replyStatus)
	checkError(err)

	if (replyVal.Val == "unavailable" || replyStatus.Val == "unavailable"){
		return true
	}
	return false
}


func seeIfLeaderIsStillActive(){
	// Get("my-key")
	var leaderReplyVal ValReply
	getValArgs := GetArgs{strconv.Itoa(leaderKey)}
	err := kvService.Call("KeyValService.Get", getValArgs, &leaderReplyVal)
	checkError(err)

	var leaderReplyStatus ValReply
	getKeyArgs := GetArgs{strconv.Itoa(leaderKey+1)}
	err = kvService.Call("KeyValService.Get", getKeyArgs, &leaderReplyStatus)
	checkError(err)


	if(leaderReplyVal.Val == "unavailable" || leaderReplyStatus.Val == "unavailable"){
		//leader key has gone down therefore I am leader and need to move the
		//last leader to a new position
		//TODO:

		if(leaderReplyStatus.Val == "unavailable"){
			var kvVal ValReply
			putArgs := PutArgs{
				Key: strconv.Itoa(leaderKey),
				Val: "Offline"}
			err = kvService.Call("KeyValService.Put", putArgs, &kvVal)
			checkError(err)
		}

		joinTheParty(leaderID)


	}else if(leaderReplyStatus.Val == "Test 5"){
		//set leader offline if 5 tests have failed
		//after five cycles we will assume leader has gone offline
		var kvVal ValReply
		putArgs := PutArgs{
			Key: strconv.Itoa(leaderKey),
			Val: "Offline"}
		err = kvService.Call("KeyValService.Put", putArgs, &kvVal)
		checkError(err)

	} else if (leaderReplyStatus.Val == "Leader"){
		var kvVal ValReply
		putArgs := PutArgs{
			Key: strconv.Itoa(leaderKey+1),
			Val: "Test 1"}
		err = kvService.Call("KeyValService.Put", putArgs, &kvVal)
		checkError(err)

	} else {
		// increment the test counter by 1
		numberPrior := strings.TrimPrefix(leaderReplyStatus.Val, "Test ")
		intPrior, _ := strconv.Atoi(numberPrior)
		testString := "Test " + strconv.Itoa(intPrior+1)

		//println(numberPrior)
		//println("TestString: -" + testString + "-")

		//set the new test value
		var kvVal ValReply
		putArgs := PutArgs{
			Key: strconv.Itoa(leaderKey+1),
			Val: testString}
		err = kvService.Call("KeyValService.Put", putArgs, &kvVal)
		checkError(err)

	}
	return
}

//function to determine if I am checking on the leader
func checkingLeader() bool{
	var nodeNo int = 0
	var i int = 0
	for {
		val := kvmap[strconv.Itoa(i)]
		if (val.value != "unavailable" && val.value != "Offline"){
			if (nodeNo == 0) {
				//learn which node is the leader
				leaderKey = i
				leaderID = val.value
			} else if(nodeNo == 1){
				if(val.value == id){
					return true
				} else {
					return false
				}
			}
			i = i+2
			nodeNo++
		} else {
			i = i+2
		}
		if (nodeNo==2){return false}
	}
	println("LEADER VAR ERROR")
	return false
}

func amILeader() bool {
	var nodeNo int = 0
		var i int = 0
		for{
			val := kvmap[strconv.Itoa(i)]
			if (val == nil){
				return false
			}
			if (val.value != "unavailable" && val.value != "Offline"){
				if(nodeNo == 0){
					if(val.value == id){
						return true
					} else {
						return false
					}
				}
				i = i+2
				nodeNo++
			} else {
				i=i+2
			}
		}
	println("LEADERCHECK ERROR")
	return false
}

func joinTheParty(toAddID string){
	var kvVal ValReply
	var err error

	i := 0
	var foundAvail int = 0
	for foundAvail == 0{ 
		tsArgs := TestSetArgs{
			Key:     strconv.Itoa(i),
			TestVal: "",
			NewVal:  toAddID}
		err = kvService.Call("KeyValService.TestSet", tsArgs, &kvVal)
		checkError(err)


		//add self to node's map
		if kvVal.Val == toAddID {

			myRegisteredKey = i
			putArgs := PutArgs{
				Key: strconv.Itoa(i+1),
				Val: "Active"}
			err = kvService.Call("KeyValService.Put", putArgs, &kvVal)
			checkError(err)
			

			foundAvail = 1
		}
		i = i+2
		
	}
}