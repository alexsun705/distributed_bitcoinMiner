package main

import (
    "fmt"
    "os"
    "encoding/json"
    "github.com/cmu440/bitcoin"
    "github.com/cmu440/lsp"
)

func marshal(msg *bitcoin.Message) ([]byte, error) {
    res, err := json.Marshal(msg)
    return res, err
}

func unmarshal(data []byte, v *bitcoin.Message) error {
    err := json.Unmarshal(data, v)
    return err
}

const maxUint = ^uint64(0)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {
    // TODO: implement this!
    miner, err := lsp.NewClient(hostport, lsp.NewParams())
    if err != nil {
        return miner, err
    }
    join := bitcoin.NewJoin()
    payload, _ := marshal(join)
    miner.Write(payload)
    return miner, nil
}

func evalRoutine(miner lsp.Client) {
    for {
        payload, err := miner.Read()
        // fmt.Println("miner: got job")
        if (err != nil){
            // should shut itself down in this case
            return
        }
        var request bitcoin.Message
        unmarshal(payload, &request)
        var result uint64
        var index uint64
        result = maxUint // defined in server.go
        index = 0
        data := request.Data 
        lower := request.Lower // inclusive
        upper := request.Upper // exclusive
        for i := lower; i < upper; i ++ {
            hash := bitcoin.Hash(data, i)
            if (hash < result){
                result = hash
                index = i
            }
        }
        newResult := bitcoin.NewResult(result, index)
        payloadResult, _ := marshal(newResult)
        newErr := miner.Write(payloadResult)
        if (newErr != nil){
            // should shut itself down in this case
            return
        }
    }
}
func main() {
    const numArgs = 2
    if len(os.Args) != numArgs {
        fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
        return
    }

    hostport := os.Args[1]
    miner, err := joinWithServer(hostport)
    if err != nil {
        fmt.Println("Failed to join with server:", err)
        return
    }

    defer miner.Close()

    // TODO: implement this!
    evalRoutine(miner)
}

