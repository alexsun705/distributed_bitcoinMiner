// Contains the implementation of a LSP server.

package lsp

import (
    "errors"
    "lspnet"
    "encoding/json"
    "strconv"
)

type readReturn struct{
    connID int
    seqNum int
    payload []byte
    err error
}
type connectRequest struct{
    message *Message
    addr *UDPAddr
}
type s_client struct{//server side client structure
    addr *UDPAddr
    seqExpected int//start with one
    connID int
    writeSeqNum int // used for writing, start with 1
    messageToPush *readReturn
    //received data messages that is not read yet, no duplicates
    // seq number of messages in pendingMessages >= seqExpected
    //no corrupted messages as well
    pendingMessages []*Message

    appendChan chan *Message //signal clientMain append to pendingMessages

    //signal clientMain to stage the push message, so clientMain can try to push
    //message to server.readReturnChan in future looping 
    stagePushChan chan *Message 
    
    // writeChan chan *Message
    writeBackChan chan *Message//used to send signal to client through UDP
}

type writeAckRequest struct{
    ack *Message
    c *s_client
}

type writeRequest struct{
    connID int
    payload []byte
}

type server struct {
    // TODO: implement this!
    serverConn *UDPConn
    serverAddr *UDPAddr
    connectedClients []*s_client
    //start at 1, sequence number of data messages sent from server
    curDataSeqNum int 
    //start at 1, connID to be assigned when next new connection is made
    curClientConnID int 

    connectChan chan connectRequest // channel to set up new connections
    readReturnChan chan readReturn //channel to send message to Read() back
    params *Params
    writeRequestChan chan *writeRequest
    writeAckChan chan *writeAckRequest
    writeBackChan chan error

}
// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
    s := server{
        serverConn: nil,
        serverAddr: nil,
        connectedClient: make([]*s_client,5),
        curDataSeqNum: 1,
        curClientConnID: 1,
        readReturnChan: make(chan readReturn,500),
        connectChan: make(chan connectRequest),
        params: params,
    }
    adr,err := lspnet.ResolveUDPAddr("udp",strconv.Itoa(port))
    if err != nil {
        return (nil,err)
    }
    s.serverAddr = adr
    conn, err := lspnet.ListenUDP("udp", s.serverAddr)
    if err != nil {
        return (nil,err)
    }
    s.serverConn=conn
    go s.mainRoutine()
    return &s,nil
}


func (s *server) Read() (int, []byte, error) {
    // TODO: remove this line when you are ready to begin implementing this method.
    message := <- s.readReturnChan
    return message.connID,message.payload,message.err
    
}

func (s *server) Write(connID int, payload []byte) error {
    request := &writeRequest{
        connID:connID,
        payload:payload,
    }
    s.writeRequestChan <- request
    err := <- s.writeBackChan
    return err
}

func (s *server) CloseConn(connID int) error {
    return errors.New("not yet implemented")
}

func (s *server) Close() error {
    return errors.New("not yet implemented")
}

//check if a connection is
func (s *server) stillConnected(conn int) bool {
    n := len(s.connectedClients)
    for i := 0; i < n; i++ {
        if (s.connectedClients[i].conn == conn){
            return true
        }
    }
    return false
}

func (c *s_client) clientWrite(s *server){
    for {
        select{
        case original:= <- c.writeBackChan:
            msg, err := marshal(original)
            if (err != nil){
                s.writeBackChan <- err
                continue
            }
            _, err := s.serverConn.WriteToUDP(msg, c.addr)
            s.writeBackChan <- err
        }
    }
}

func (s *server) mainRoutine() {
    for {
        select {
        case request := <- s.connectChan://set up connection
            message := request.message
            if (message.Type == MsgConnect){//start a new server side client
                c := &s_client{//need to adapt to new struct
                    addr: request.addr,
                    seqExpected: 1,
                    writeSeqNum: 1,
                    connID: s.curClientConn,
                    messageToPush: nil,
                    pendingMessages: make([]*Message, 5),
                    appendChan: make(chan *Message),
                    stagePushChan: make(chan *Message),
                    writeChan: make(chan *Message),
                    writeBackChan: make(chan *Message),
                }
                s.curClientConn += 1;
                s.connectedClients = append(s.connectedClients, c)
                go c.clientRead(&s)
                go c.clientMain(&s)
            }
        
        case request := <- s.writeRequestChan:
            // write data to client
            connID := request.connID
            payload := request.payload
            client := nil
            for (i := 0; i < len(s.connectedClients); i++){
                if s.connectedClients[i].ConnID == connID{
                    client = s.connectedClients[i]
                    break
                }
            }
            if (client != nil){
                seqNum = c.writeSeqNum
                size = len(payload)
                checksum = makeCheckSum(connID, seqNum, size, payload)
                original := NewData(connID, seqNum, size, payload, checksum)
                msg, err := marshal(original)
                if (err != nil){
                    s.writeBackChan <- err
                    continue
                }
                _, err := s.serverConn.WriteToUDP(msg, client.addr)
                if (err == nil){
                    client.writeSeqNum += 1
                }
                s.writeBackChan <- err
            } else {
                err := errors.New("This client does not exist")
                s.writeBackChan <- err
            }
        }

        // write ack to client
        case ackRequest := <- s.writeAckChan:
            ack := ackRequest.ack
            c := ackRequest.c
            connID := ack.ConnID
            _, err := s.serverConn.WriteToUDP(ack, c.addr)
    }
}
func (c *s_client) alreadyReceived(seq int) bool {
    n := len(c.pendingMessages)
    for i := 0; i < n; i++ {
        if (c.pendingMessages[i].SeqNum == seq){
            return true
        }
    }
    return false
}
//would block until Read() is called once
func (client *s_client) clientMain(s *server){
    for {
        //if messageToPush is good, we would try to push to s.readReturnChan
        if (client.messageToPush!=nil && client.messageToPush.seqNum==client.seqExpected){
            readReturnChan := s.readReturnChan
        } else{
            readReturnChan := nil
        }

        select {
        case message:= <- client.appendChan:// append out of order message
            if !client.alreadyReceived(message.SeqNum) {
                client.pendingMessages = append(client.pendingMessages,message)
            }
        case message:= <- client.stagePushChan: //
            if (message.SeqNum == client.seqExpected){
                wrapMessage := &readReturn{
                    connID: message.ConnID,
                    seqNum: message.SeqNum,
                    payload: message.PayLoad,
                    err: nil,
                }
                client.messageToPush = wrapMessage
            }
        case readReturnChan <- client.messageToPush:
            //if entered here, means we just pushed the message with seqNum
            //client.seqExpected to the main readReturnChan, thus need to update
            //and check whether we have pendingMessages that can be 
            client.seqExpected +=1
            //go through pending messages and check if already received the next  
            //message in order, check againt client.seqExpected
            for i := 0; i < len(client.pendingMessages); i++ {
                message := client.pendingMessages[i]
                if (message.SeqNum == client.seqExpected){
                    //make sure sending messages out in order
                    wrapMessage := &readReturn{
                        connID: message.ConnID,
                        seqNum: message.SeqNum,
                        payload: message.PayLoad,
                        err: nil,
                    }
                    client.messageToPush = wrapMessage
                    //cut this message off pendingMessages
                    client.pendingMessages = append(client.pendingMessages[:i], client.pendingMessages[i+1:]...)
                   
                    break //make sure only push one message to the read()
                }
            }
        }
    }
}
func (client *s_client) clientRead(s *server) {
    for {
        serverConn := s.serverConn
        var b [2000]byte
        size,addr,err := serverConn.ReadFromUDP(b)
        if err != nil {//deal with error later
            v := &Message{}
            unmarshal(b,v)//unMarshall returns *Message
            if integrityCheck(v){//check integrity here with checksum and size 
                if (message.Type == MsgData){
                    seq := message.SeqNum
                    if (seq > client.seqExpected){//out of order, pending
                        client.appendChan <- message
                    }
                    if (seq == client.seqExpected){
                        //let clientMain try pushing message to s.readReturnChan
                        client.stagePushChan <- message
                    }
                    //else if seq <seqExpected, then don't worry about returning it
                    ack := NewAck(v.ConnID, v.SeqNum)
                    ackRequest = &writeAckRequest{
                        ack: ack,
                        c: client
                    }
                    s.writeAckChan <- ackRequest
                } else if (message.Type == MsgConnect){
                    request := connectRequest{
                        message,
                        addr,
                    }
                    s.connectChan <- &request//make new server side client struct
                    ack := NewAck(c.connID, 0)
                    ackRequest = &writeAckRequest{
                        ack: ack,
                        c: client
                    }
                    s.writeAckChan <- ackRequest
                //if its ACK, do sth later for epoch
            }

        }
    }
}




