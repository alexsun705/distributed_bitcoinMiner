package main

import (
    "fmt"
    "os"
    "strconv"
    "encoding/json"
    "github.com/cmu440/bitcoin"
    "github.com/cmu440/lsp"
)

const maxUint = ^uint64(0)

func marshal(msg *bitcoin.Message) ([]byte, error) {
    res, err := json.Marshal(msg)
    return res, err
}

func unmarshal(data []byte, v *bitcoin.Message) error {
    err := json.Unmarshal(data, v)
    return err
}

func main() {
    const numArgs = 4
    if len(os.Args) != numArgs {
        fmt.Printf("Usage: ./%s <hostport> <message> <maxNonce>", os.Args[0])
        return
    }
    hostport := os.Args[1]
    message := os.Args[2]
    maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)
    if err != nil {
        fmt.Printf("%s is not a number.\n", os.Args[3])
        return
    }

    client, err := lsp.NewClient(hostport, lsp.NewParams())
    if err != nil {
        fmt.Println("Failed to connect to server:", err)
        return
    }

    request := bitcoin.NewRequest(message, 0, maxNonce)
    payload, _ := marshal(request)
    client.Write(payload)
    // this read will block
    resultPayload, err := client.Read()
    if (err != nil){
        printDisconnected()
    } else {
        var msg bitcoin.Message
        unmarshal(resultPayload, &msg)
        printResult(msg.Hash, msg.Nonce)
    }

    defer client.Close()
}

// printResult prints the final result to stdout.
func printResult(hash, nonce uint64) {
    fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
    fmt.Println("Disconnected")
}
