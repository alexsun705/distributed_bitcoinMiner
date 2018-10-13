package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"github.com/cmu440/lsp"
	"github.com/cmu440/bitcoin"

)

// *** important *** : 
// in the whole system assume lower inclusive upper exclusive

const maxUint = ^uint64(0) 
type server struct {
	lspServer lsp.Server
	eClientRequestChan chan *clientRequest // NewRequest
	eMinerJoinChan chan *miner // NewJoin
	eMinerResultChan chan *minerResult // NewResult
	dropChan chan int //drop from either miner or client
	requestWaitingArray []*clientRequest // all queueing requests that has not been processed
	currRequest *clientRequest // the current request being processed
	minersArray []*miner
}

type clientRequest struct {
	// both arrays are of size n+1
	connID int
	data string 
	responsibleMiners []int // id of miners, shrinks when result comes
	lower uint64
	upper uint64
	minHash uint64
	minNonce uint64
	totalResponses uint64 // the number of responses received so far
	dropped bool // if the client is being dropped
}

type minerResult struct {
	minerID int 
	hash uint64
	nonce uint64
}

type miner struct{
	minerID int
	data string // the data at this moment
	lower uint64 // the range of the job at this moment
	upper uint64 // the range of the job at this moment
	hash uint64 // the hash of the job at this moment
	available bool
}

func inList(l []int, num int) bool {
	for i := 0; i < len(l); i++ {
		if l[i] == num {
			return true
		}
	}
	return false
}

func indexInArray(l []int, num int) int {
	// requires num must be in l
	for i := 0; i < len(l); i++ {
		if l[i] == num {
			return i
		}
	}
	// shouldn't really be here
	return -1
}


func (S *server) readRoutine(){
	for {
		connID, bytes, err = S.Read()
		if (err != nil){
			// one client or miner must be dropped
			S.dropChan <- connID
		}
		var msg Message
		lsp.unmarshal(bytes, &msg) // is it right?
		if (msg.Type == bitcoin.Request){
			// we have a new client request
			arrayLength := msg.Upper - msg.Lower + 1
			newRequest = &clientRequest{
				connID: connID,
				data: msg.data,
				responsibleMiners: make([]int, 0)
				lower: msg.Lower,
				upper: msg.Upper,
				minHash: maxUint, // same as uninitialized
				minNonce: maxUint, // same as uninitialized
				responses: 0,
				dropped: false,
			}
			S.eClientRequestChan <- newRequest
		} else if (msg.Type == bitcoin.Join){
			newMiner = &miner{
				minerID: connID,
				available: true,
			}
			S.eMinerJoinChan <- newMiner
		} else { // must be result
			res = &minerResult{
				minerID: connID,
				hash: msg.hash,
				nonce: msg.nonce
			}
			S.eMinerResultChan <- res
		}
	}
}

// do the load balancing. All miners must be available
func (S *server) loadBalance(request *clientRequest) {
	S.currRequest = request
	data := request.Data
	num := len(S.minersArray)
	request.Upper +=1 //add one since upperbound inclusive, calculation later assuming upperbound exclusive
	totalLoad := request.Upper - request.Lower // because of the exclusive, inclusive rule
	individualLoad := totalLoad / num
	leftoverLoad := totalLoad - individualLoad * num 
	if individualLoad == 0{//miner amount more than range of nonce
		individualLoad = 1
		leftoverLoad = 0
		num = totalLoad
	}

	start := request.lower
	for i := 0; i < num; i++ {
		end := start + individualLoad
		miner := S.minersArray[i]
		miner.lower = start
		miner.upper = end
		miner.data = data
		miner.available = false
		if (i == 0){
			// give the leftover load to the first miner
			end += leftoverLoad
			miner.upper = end
			
		}
		// write to the miner
		connID := miner.minerID
		msg := bitcoin.NewRequest(data, miner.lower, miner.upper)
		payload, err := lsp.marshal(msg)
		_ = err
		lsp.Write(connID, payload)
		// hold this miner responsible for the request
		request.responsibleMiners = append(request.responsibleMiners, miner.minerID)
		// update start for next loop
		start = end
	}
}

func (S *server) mainRoutine() {
	for {

		select{
		case request := <- S.eClientRequestChan:
			if len(S.requestWaitingArray) == 0 && S.currRequest == nil {
				//set curRequest and loadBalance + write to all miners
				S.loadBalance(request)
			} else {
				S.requestWaitingArray = append(S.requestWaitingArray, request)
			}
		

		case miner := <- S.eMinerJoinChan:
			// this is the timing to check the dropped miner array
			if (len(S.droppedMinerArray) != 0){
				droppedMiner := S.droppedMinerArray[0]
				curr := S.currRequest
				miner.data = droppedMiner.data // although not necessary
				miner.lower = droppedMiner.lower 
				miner.upper = droppedMiner.upper
				miner.available = false
				// write to the miner
				minerID := miner.minerID
				msg := bitcoin.NewRequest(miner.data, miner.lower, miner.upper)
				payload, _ := lsp.marshal(msg)
				S.lspServer.Write(minerID, payload)
				// change the responsible miner in the request
				// must be in the request
				for i := 0; i < len(curr.responsibleMiners); i++ {
					if curr.responsibleMiners[i] == droppedMiner.minerID{
						curr.responsibleMiners[i] = miner.minerID
					}
				}
				S.droppedMinerArray = S.droppedMinerArray[1:]
			}
			append(S.minersArray, miner)

		case result := <- S.eMinerResultChan:
			curr := S.currRequest
			if (curr == nil){
				continue
			}
			id := result.minerID
			hash := result.hash
			nonce := result.nonce
			// if the message is from a dropped miner, then don't need to 
			// consider the result. 
			// check two places to ensure safety. It really should be that these
			// two places are in sync. 
			if !inList(curr.responsibleMiners, id) || !inList(S.minersArray, num){
				continue
			}
			// if be here then id is both valid and responsible for the curr request
			if hash < curr.minHash{
				curr.minHash = hash
				curr.minNonce = nonce
			}
			curr.responses += 1
			// now go to miners array to resume availability of that miner
			for i := 0; i < len(minersArray); i++ {
				miner = S.minersArray[i]
				if miner.minerID == id {
					// we have found the miner
					miner.available = true
					// check the dropped miner chan
					if (len(S.droppedMinerArray) != 0){
						droppedMiner = S.droppedMinerArray[0]
						miner.data = droppedMiner.data // although not necessary
						miner.lower = droppedMiner.lower 
						miner.upper = droppedMiner.upper
						miner.available = false
						// write to the miner
						minerID := miner.minerID
						msg := bitcoin.NewRequest(miner.data, miner.lower, miner.upper)
						payload, _ := lsp.marshal(msg)
						S.lspServer.Write(minerID, payload)
						// change the responsible miner in the request
						// must be in the request
						for i := 0; i < len(curr.responsibleMiners); i++ {
							if curr.responsibleMiners[i] == droppedMiner.minerID{
								curr.responsibleMiners[i] = miner.minerID
							}
						}
						S.droppedMinerArray = S.droppedMinerArray[1:]
					}
					break
				}
			}
			// now need to send the final result back to the client
			if curr.responses == len(curr.responsibleMiners) { 
				// now should send the result back to the client
				result := bitcoin.NewResult(curr.minHash, curr.minNonce)
				payload, _ := lsp.marshal(result)
				// only write back to the client if the current resquest is not dropped
				if (!curr.dropped){
					lsp.Write(curr.connID, payload)
				}
				// close this request
				S.currRequest = nil
				// add a new client in if there is any pending request to deal with
				if len(S.requestWaitingArray) != 0{
					request := S.requestWaitingArray[0]
					S.requestWaitingArray = S.requestWaitingArray[1:]
					S.loadBalance(request)
				}
			}			
		case connID := <- S.dropChan:
			if inList(S.minersArray, connID) {//if miner dropped
				minerID := connID
				curr := S.currRequest
				index := indexInArray(S.minersArray, connID)
				droppedMiner := minersArray[index] 
				S.minersArray = append(S.minersArray[:index], S.minersArray[index+1:]...)
				// no need to do the following steps if there is no curr or curr is dropped
				if (curr == nil || curr.dropped){
					continue
				}
				// check if there is available miner
				availableID := -1
				for i := 0; i < len(S.minersArray); i++ {
					miner = S.minersArray[i]
					if miner.available {
						// we have found an available miner
						availableID = miner.minerID
						break
					}
				}

				if availableID != -1 {
					miner := S.minersArray[availableID]
					// change this miner's job to dropped miner's job
					miner.data = droppedMiner.data // although not necessary
					miner.lower = droppedMiner.lower 
					miner.upper = droppedMiner.upper
					miner.available = false
					// write to the miner
					connID := miner.minerID
					msg := bitcoin.NewRequest(miner.data, miner.lower, miner.upper)
					payload, _ := lsp.marshal(msg)
					lsp.Write(connID, payload)
					// change the responsible miner in the request
					// must be in the request
					for i := 0; i < len(curr.responsibleMiners); i++ {
						if curr.responsibleMiners[i] == droppedMiner.minerID{
							curr.responsibleMiners[i] = miner.minerID
						}
					}
				} else {
					// just append to the dropped miner array
					// wait for later times when a miner frees up or a new miner joins
					S.droppedMinerArray = append(S.droppedMinerArray, droppedMiner)
				}
			} else { //if client dropped
				requestID := connID
				// if curr request is being dropped
				if (S.currRequest != nil && requestID == S.currRequest.connID){
					// need to drop this request
					S.currRequest.dropped = true
					// make all miners available
					for i := 0; i < len(S.minersArray); i++ {
						miner := S.minersArray[i]
						miner.available = true
					}
					// empty the dropped miner array
					S.droppedMinerArray = make([]*miner, 0)
				}
				// if a waiting request is being dropped
				for i, request := range S.requestWaitingArray {
					if request.connID == requestID {
						// we need to throw this request away
						S.requestWaitingArray = append(S.requestWaitingArray[:i], S.requestWaitingArray[i+1:]...)
						//break
					}
				}
			}
		}
	}
}


func StartServer(port int) (*server, error) {
	// TODO: implement this!
	params := NewParams()
	s, err = NewServer(port, params)
	if (err != nil){
		return nil, err
	}
	S := &server{
		lspServer: s,
		eClientRequestChan: make(chan *Message),
		eMinerJoinChan: make(chan *miner),
		eMinerResultChan: make(chan *Message),
		requestWaitingArray: make([]*clientRequest, 0),
		dropChan: make(chan int),
		currRequest:nil,
		minersArray: make([]*miner, 0),
	}
	go S.mainRoutine()
	go S.readRoutine()
	return S, nil
}

var LOGF *log.Logger
func main() {
	// You may need a logger for debug purpose
	const (
		name = "log.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// Usage: LOGF.Println() or LOGF.Printf()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := StartServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)

	defer srv.lspServer.Close()

	// TODO: implement this!
	
}
