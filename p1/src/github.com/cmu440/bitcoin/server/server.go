package main

import (
    "fmt"
    "log"
    "os"
    "strconv"
    "encoding/json"
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
    processRequestChan chan *clientRequest
    dropChan chan int
    requestWaitingArray []*clientRequest // all queueing requests that has not been processed
    currRequest *clientRequest // the current request being processed
    minersArray []*miner // miners not dropped
    droppedMinersArray []*miner // miners being dropped
}

type clientRequest struct {
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

func inListMinersArray(l []*miner, num int) bool {
    for i := 0; i < len(l); i++ {
        if l[i].minerID == num {
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

func indexInMinersArray(l []*miner, num int) int {
    // requires num must be in l
    for i := 0; i < len(l); i++ {
        if l[i].minerID == num {
            return i
        }
    }
    // shouldn't really be here
    return -1
}
func marshal(msg *bitcoin.Message) ([]byte, error) {
    res, err := json.Marshal(msg)
    return res, err
}

func unmarshal(data []byte, v *bitcoin.Message) error {
    err := json.Unmarshal(data, v)
    // if (err != nil) {
    //     fmt.Println("unmarshal: incorrect message")
    // }
    return err
}

// func (S *server) printMinerArray() {
//     l = S.minersArray
//     d = S.droppedMinersArray
//     fmt.Printf("Printing minersArray: ")
//     for i := 0

// }

func (S *server) readRoutine(){
    for {
        // fmt.Println("inside read")
        connID, bytes, err := S.lspServer.Read()
        if (err != nil){
            // one client or miner must be dropped
            S.dropChan <- connID
        }
        var msg bitcoin.Message
        err2 := unmarshal(bytes, &msg)
        if (err2 != nil){
            continue
        }
        if (msg.Type == bitcoin.Request){
            // we have a new client request
            // arrayLength := msg.Upper - msg.Lower + 1
            newRequest := &clientRequest{
                connID: connID,
                data: msg.Data,
                responsibleMiners: make([]int, 0),
                lower: msg.Lower,
                upper: msg.Upper,
                minHash: maxUint, // same as uninitialized
                minNonce: maxUint, // same as uninitialized
                totalResponses: 0,
                dropped: false,
            }
            S.eClientRequestChan <- newRequest
        } else if (msg.Type == bitcoin.Join){
            newMiner := &miner{
                minerID: connID,
                available: true,
            }
            fmt.Printf("server: new miner's connID : %d \n", connID)
            S.eMinerJoinChan <- newMiner
        } else { // must be result
            res := &minerResult{
                minerID: connID,
                hash: msg.Hash,
                nonce: msg.Nonce,
            }
            S.eMinerResultChan <- res
        }
    }
}

// do the load balancing. All miners must be available
func (S *server) loadBalance(request *clientRequest) {
    S.currRequest = request
    data := request.data
    num := uint64(len(S.minersArray))
    request.upper +=1 //add one since upperbound inclusive, calculation later assuming upperbound exclusive
    totalLoad := request.upper - request.lower // because of the exclusive, inclusive rule
    individualLoad := totalLoad / num
    leftoverLoad := totalLoad - individualLoad * num 
    if individualLoad == 0{//miner amount more than range of nonce
        individualLoad = 1
        leftoverLoad = 0
        num = totalLoad
    }

    start := request.lower
    var i uint64
    for i = 0; i < num; i++ { 
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
        payload, err := marshal(msg)
        _ = err
        S.lspServer.Write(connID, payload)
        // hold this miner responsible for the request
        request.responsibleMiners = append(request.responsibleMiners, miner.minerID)
        // update start for next loop
        start = end
    }
}

func (S *server) mainRoutine() {
    for {
        // fmt.Println("inside main")
        select{
        case request := <- S.eClientRequestChan:
            if len(S.requestWaitingArray) == 0 && S.currRequest == nil && len(S.minersArray) != 0 {
                //set curRequest and loadBalance + write to all miners
                fmt.Printf("server: client %d joined, gonna do load balance \n", request.connID)
                S.loadBalance(request)
            } else {
                fmt.Printf("server: client %d joined, add to wait array \n", request.connID)
                S.requestWaitingArray = append(S.requestWaitingArray, request)
            }
        

        case miner := <- S.eMinerJoinChan:
            // this is the timing to check the dropped miner array
            if (len(S.droppedMinersArray) != 0){
                droppedMiner := S.droppedMinersArray[0]
                curr := S.currRequest
                miner.data = droppedMiner.data // although not necessary
                miner.lower = droppedMiner.lower 
                miner.upper = droppedMiner.upper
                miner.available = false
                // write to the miner
                minerID := miner.minerID
                msg := bitcoin.NewRequest(miner.data, miner.lower, miner.upper)
                payload, _ := marshal(msg)
                S.lspServer.Write(minerID, payload)
                // change the responsible miner in the request
                // must be in the request
                for i := 0; i < len(curr.responsibleMiners); i++ {
                    if curr.responsibleMiners[i] == droppedMiner.minerID{
                        curr.responsibleMiners[i] = miner.minerID
                    }
                }
                S.droppedMinersArray = S.droppedMinersArray[1:]
            }
            S.minersArray = append(S.minersArray, miner)
            if (S.currRequest == nil){
                // the requests are waiting for the miners to join
                // need to load balance right now
                if (len(S.requestWaitingArray) != 0){
                    request := S.requestWaitingArray[0]
                    S.requestWaitingArray = S.requestWaitingArray[1:]
                    S.loadBalance(request)
                }
            }
            fmt.Printf("server: miner join %v, %v \n", S.minersArray, S.droppedMinersArray)

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
            if !inList(curr.responsibleMiners, id) || !inListMinersArray(S.minersArray, id){
                continue
            }
            // if be here then id is both valid and responsible for the curr request
            if hash < curr.minHash{
                curr.minHash = hash
                curr.minNonce = nonce
            }
            curr.totalResponses += 1
            // now go to miners array to resume availability of that miner
            for i := 0; i < len(S.minersArray); i++ {
                miner := S.minersArray[i]
                if miner.minerID == id {
                    // we have found the miner
                    miner.available = true
                    // check the dropped miner chan
                    if (len(S.droppedMinersArray) != 0){
                        droppedMiner := S.droppedMinersArray[0]
                        miner.data = droppedMiner.data // although not necessary
                        miner.lower = droppedMiner.lower 
                        miner.upper = droppedMiner.upper
                        miner.available = false
                        // write to the miner
                        minerID := miner.minerID
                        msg := bitcoin.NewRequest(miner.data, miner.lower, miner.upper)
                        payload, _ := marshal(msg)
                        S.lspServer.Write(minerID, payload)
                        // change the responsible miner in the request
                        // must be in the request
                        for i := 0; i < len(curr.responsibleMiners); i++ {
                            if curr.responsibleMiners[i] == droppedMiner.minerID{
                                curr.responsibleMiners[i] = miner.minerID
                            }
                        }
                        S.droppedMinersArray = S.droppedMinersArray[1:]
                    }
                    break
                }
            }
            // now need to send the final result back to the client
            if curr.totalResponses == uint64(len(curr.responsibleMiners)) { 
                // now should send the result back to the client
                result := bitcoin.NewResult(curr.minHash, curr.minNonce)
                payload, _ := marshal(result)
                // only write back to the client if the current resquest is not dropped
                if (!curr.dropped){
                    S.lspServer.Write(curr.connID, payload)
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
            if inListMinersArray(S.minersArray, connID) {
                // minerID := connID
                fmt.Printf("server: miner %d dropped \n", connID)
                curr := S.currRequest
                index := indexInMinersArray(S.minersArray, connID)
                droppedMiner := S.minersArray[index] 
                S.minersArray = append(S.minersArray[:index], S.minersArray[index+1:]...)
                // no need to do the following steps if there is no curr or curr is dropped
                fmt.Printf("server: miner in array %v, %v \n", S.minersArray, S.droppedMinersArray)
                if (curr == nil || curr.dropped){
                    continue
                }
                // check if there is available miner
                availableID := -1
                for i := 0; i < len(S.minersArray); i++ {
                    miner := S.minersArray[i]
                    if miner.available {
                        // we have found an available miner
                        availableID = i
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
                    payload, _ := marshal(msg)
                    S.lspServer.Write(connID, payload)
                    // change the responsible miner in the request
                    // must be in the request
                    for i := 0; i < len(curr.responsibleMiners); i++ {
                        if curr.responsibleMiners[i] == droppedMiner.minerID{
                            curr.responsibleMiners[i] = miner.minerID
                        }
                    }
                    fmt.Printf("server: there is available miner %v, %v \n", S.minersArray, S.droppedMinersArray)
                } else {
                    // just append to the dropped miner array
                    // wait for later times when a miner frees up or a new miner joins
                    S.droppedMinersArray = append(S.droppedMinersArray, droppedMiner)
                    fmt.Printf("server: no available miner %v, %v \n", S.minersArray, S.droppedMinersArray)
                }
            } else{
                requestID := connID
                fmt.Printf("server: client %d dropped\n", connID)
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
                    S.droppedMinersArray = make([]*miner, 0)
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
    params := lsp.NewParams()
    s, err := lsp.NewServer(port, params)
    if (err != nil){
        return nil, err
    }
    S := &server{
        lspServer: s,
        eClientRequestChan: make(chan *clientRequest),
        eMinerJoinChan: make(chan *miner),
        eMinerResultChan: make(chan *minerResult),
        processRequestChan: make(chan *clientRequest),
        requestWaitingArray: make([]*clientRequest, 0),
        dropChan: make(chan int),
        currRequest:nil,
        minersArray: make([]*miner, 0),
        droppedMinersArray: make([]*miner, 0),
    }
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

    // TODO: implement this!
    go srv.readRoutine()
    srv.mainRoutine()

    defer srv.lspServer.Close()
    
}
