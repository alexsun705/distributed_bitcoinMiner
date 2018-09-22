// Contains the implementation of a LSP client.

package lsp

import "errors"

type client struct {
    serverConn *UDPConn
    serverAddr *UDPAddr
    connID int
    currSeqNum int
    writeChan chan []byte // write request sends to this channel
    writeBackChan chan error // the chan sent back from main routine
    readChan chan int  // read request sends to this channel
    payloadChan chan []byte // where payload is sent from main routine
    writeAckChan chan int // ack is going to be sent
    writeConnChan chan int // connect is going to be sent
    connIDRequestChan chan int // when function connID() calls send data to this channel
    connIDReturnChan chan int // the function returns value from this channel
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
    serverAddr := lspnet.ResolveUDPAddr("UDP", hostport)
    serverConn := lspnet.DialUDP("UDP", nil, serverAddr)
    // to do: wait to receive ack from server
    c := &client{
        serverConn: serverConn,
        serverAddr: serverAddr,
        connID: connID, 
        currSeqNum: 1, 
        writeChan: make(chan []byte),
        writeBackChan: make(chan error),
        readChan: make(chan int),
        payloadChan: make(chan []byte),
        writeAckChan: make(chan int),
        writeConnChan: make(chan int),
        connIDChan: make(chan int),
        connIDRequestChan: make(chan int),
        connIDReturnChan: make(chan int),
    }
    c.writeConnChan <- 1
    err := <- c.writeBackChan
    return (c, err)
}

func (c *client) ConnID() int {
    connIDRequestChan <- 1
    res := <- connIDReturnChan
    return res 
}

func (c *client) Read() ([]byte, error) {
    // TODO: remove this line when you are ready to begin implementing this method.
    select {} // Blocks indefinitely.
    return nil, errors.New("not yet implemented")
}

func (c *client) Write(payload []byte) error {
    c.writeChan <- payload
    res := <- c.writeBackChan
    return res
}

func (c *client) Close() error {
    return errors.New("not yet implemented")
}


// other functions defined below
func (c *client) sendMessage(original *Message){
    msg, err = marshal(original)
    if (err != nil){
        c.writeBackChan <- err
        continue
    }
    _, err := WriteToUDP(msg, c.serverAddr)
    if (err != nil){
        c.writeBackChan <- err
        continue
    }
    c.currSeqNum += 1
    c.writeBackChan <- nil
}

func marshal(msg *Message) ([]byte, error) {
    res, err := json.Marshal(msg)
    return res, err
}

func unmarshal(data []byte, v *Message) error {
    err := json.Unmarshal(data, v)
    return err
}

func makeCheckSum(connID, seqNum, size int, payload []byte) uint16 {
    connIDSum := Int2Checksum(connID)
    seqNumSum := Int2Checksum(seqNum)
    sizeSum := Int2Checksum(size)
    payloadSum := ByteArray2CheckSum(payload)
    // all of these are uint32
    sum := connIDSum + seqNumSum + sizeSum + payloadSum
    carry := sum >> 16
    primary := 0x0000ffff & sum
    result := uint16 (carry + primary)
    return result
}

func (c *client) mainRoutine(){
    for{
        select{
            case payload <- writeChan:
                checksum := makeCheckSum(c.connID, c.currSeqNum, len(payload), payload)
                originalMsg := NewData(c.connID, c.currSeqNum, len(payload), payload, checksum)
                c.sendMessage(originalMsg)
            
            case <- writeAckChan:
                ack = NewAck(c.connID, c.currSeqNum)
                c.sendMessage(ack)

            case <- writeConnChan:
                conn = NewConnect()
                c.sendMessage(conn)

            case <- connIDRequestChan:
                c.connIDRequestChan <- c.connID
        }   
    }
}




