// Contains the implementation of a LSP server.

package lsp

import (
    "errors"
    "github.com/cmu440/lspnet"
    
    "strconv"
    "strings"
)

type readReturn struct{
    connID int
    seqNum int
    payload []byte
    err error
}
type connectRequest struct{
    message *Message
    addr *lspnet.UDPAddr
}
type s_client struct{//server side client structure
    addr *lspnet.UDPAddr
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
    clientCloseChan chan int

}

type writeAckRequest struct{
    ack *Message
    client *s_client
}

type writeRequest struct{
    connID int
    payload []byte
}

type server struct {
    // TODO: implement this!
    serverConn *lspnet.UDPConn
    serverAddr *lspnet.UDPAddr
    connectedClients []*s_client
    //start at 1, sequence number of data messages sent from server
    curDataSeqNum int 
    //start at 1, connID to be assigned when next new connection is made
    curClientConnID int 

    newClientChan chan *s_client
    connectChan chan *connectRequest // channel to set up new connections
    readReturnChan chan *readReturn //channel to send message to Read() back
    params *Params
    writeRequestChan chan *writeRequest
    writeAckChan chan *writeAckRequest
    writeBackChan chan error

    clientRemoveChan chan *s_client
    mainCloseChan chan int
    readCloseChan chan int


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
        connectedClients: make([]*s_client,5),
        curDataSeqNum: 1,
        curClientConnID: 1,
        newClientChan: make(chan *s_client),
        connectChan: make(chan *connectRequest),
        readReturnChan: make(chan *readReturn,500),
        params: params,
        writeRequestChan: make(chan *writeRequest),
        writeAckChan: make(chan *writeAckRequest),
        writeBackChan: make(chan error),
        clientRemoveChan: make(chan *s_client),
        mainCloseChan: make(chan int),
        readCloseChan: make(chan int),
    }
    adr,err := lspnet.ResolveUDPAddr("udp","localhost:"+strconv.Itoa(port))
    if err != nil {
        return nil, err
    }
    s.serverAddr = adr
    conn, err := lspnet.ListenUDP("udp", s.serverAddr)
    if err != nil {
        return nil, err
    }
    s.serverConn=conn
    go s.mainRoutine()
    go s.readRoutine()
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

    sClient := s.searchClientToClose(connID)
    if sClient!=nil {
        sClient.clientCloseChan <- 1
        s.clientRemoveChan <- sClient //could have issues, if removing client
        return nil
    }
    return errors.New("connection already closed")
}

func (s *server) Close() error {
    s.mainCloseChan <-1
    s.readCloseChan <-1
    return nil
}
func (s *server) searchClientToClose (connID int) *s_client {
    for i := 0; i < len(s.connectedClients); i++ {
        sClient :=  s.connectedClients[i]
        if (sClient.connID == connID) {
            return sClient
        }
    }
    return nil
}
func (s *server) mainRoutine() {
    for {
        select {
        case <- s.mainCloseChan:
            for i := 0; i < len(s.connectedClients); i++ {
                s.connectedClients[i].clientCloseChan <- 1
            }
            return
        case sClient := <- s.clientRemoveChan:
            for i := 0; i < len(s.connectedClients); i++ {
                if (s.connectedClients[i].connID == sClient.connID) {
                    s.connectedClients = append(s.connectedClients[:i], s.connectedClients[i+1:]...)
                    break
                }
            }
        case request := <- s.connectChan://set up connection
            message := request.message
            if (message.Type == MsgConnect){//start a new server side client
                c := &s_client{//need to adapt to new struct
                    addr: request.addr,
                    seqExpected: 1,
                    writeSeqNum: 1,
                    connID: s.curClientConnID,
                    messageToPush: nil,
                    pendingMessages: make([]*Message, 5),
                    appendChan: make(chan *Message),
                    stagePushChan: make(chan *Message),
                    clientCloseChan: make(chan int),
                }
                s.curClientConnID += 1;
                s.connectedClients = append(s.connectedClients, c)
                s.newClientChan <- c//let read routine create ack request
                go c.clientMain(s)
            }
        
        case request := <- s.writeRequestChan:
            // write data to client
            connID := request.connID
            payload := request.payload
            var sClient *s_client = nil
            for i := 0; i < len(s.connectedClients); i++{
                if s.connectedClients[i].connID == connID{
                    sClient = s.connectedClients[i]
                    break
                }
            }
            if (sClient != nil){
                seqNum := sClient.writeSeqNum
                size := len(payload)
                checksum := makeCheckSum(connID, seqNum, size, payload)
                original := NewData(connID, seqNum, size, payload, checksum)
                msg, err := marshal(original)
                if (err != nil){
                    s.writeBackChan <- err
                    continue
                }
                num, err := s.serverConn.WriteToUDP(msg, sClient.addr)
                _ = num
                if (err == nil){
                    sClient.writeSeqNum += 1
                }
                s.writeBackChan <- err
            } else {
                err := errors.New("This client does not exist")
                s.writeBackChan <- err
            }

        // write ack to client
        case ackRequest := <- s.writeAckChan:
            ack := ackRequest.ack
            sClient := ackRequest.client
            byteMessage,err := marshal(ack)
            _ = err//deal with later?
            num, err := s.serverConn.WriteToUDP(byteMessage, sClient.addr)
            _ = num
            _ = err//deal with later?
        }
    }
}
func (s *server) searchClient(addr *lspnet.UDPAddr) *s_client {
    for i := 0; i < len(s.connectedClients); i++ {
        sClient :=  s.connectedClients[i]
        if (strings.Compare(sClient.addr.String(),addr.String())==0) {
            return sClient
        }
    }
    return nil
}
func (s *server) readRoutine() {
    for {
        select{
        case <-s.readCloseChan:
            return
        default:
            serverConn := s.serverConn
            var b []byte
            size,addr,err := serverConn.ReadFromUDP(b)
            _ = size
            if err != nil {//deal with error later
                message := &Message{} //store message
                unmarshal(b,message)//unMarshall returns *Message
                if integrityCheck(message){//check integrity here with checksum and size 
                    if (message.Type == MsgData){
                        sClient := s.searchClient(addr)//make sure client connected
                        seq := message.SeqNum
                        if (sClient != nil){
                            if (seq > sClient.seqExpected){//out of order, pending
                                sClient.appendChan <- message
                            }
                            if (seq == sClient.seqExpected){
                                //let clientMain try pushing message to s.readReturnChan
                                sClient.stagePushChan <- message
                            }

                            //else if seq <seqExpected, then don't worry about returning it to Read()
                            ack := NewAck(message.ConnID, message.SeqNum)
                            ackRequest := &writeAckRequest{
                                ack: ack,
                                client: sClient,
                            }
                            s.writeAckChan <- ackRequest
                        }
                    } else if (message.Type == MsgConnect){
                        request := &connectRequest{
                            message,
                            addr,
                        }
                        //check if the client is already connected on the server end
                        newClient :=s.searchClient(addr)
                        var sClient *s_client = nil
                        if (newClient==nil){//first connect message
                            s.connectChan <- request
                            sClient = <- s.newClientChan//wait for new client from main
                        } else{
                            sClient = newClient
                        }
                        //make new server side client struct in mainRoutine
                        ack := NewAck(sClient.connID, 0)
                        ackRequest := &writeAckRequest{
                            ack: ack,
                            client: sClient,
                        }
                        s.writeAckChan <- ackRequest
                    //if its ACK, do sth later for epoch
                    }

                }
            }
        }    
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
//would block until Read() is called
//mainly deal with out of order messages on each client
//append out of order messages to pendingMessages, try to push the correct 
//message to s.readReturnChan when have one
func (sClient *s_client) clientMain(s *server){
    for {
        //if messageToPush is good, we would try to push to s.readReturnChan
        var readReturnChan chan *readReturn = nil
        if (sClient.messageToPush!=nil && sClient.messageToPush.seqNum==sClient.seqExpected){
            readReturnChan := s.readReturnChan
            _ = readReturnChan
        }

        select {
        case <- sClient.clientCloseChan: //CloseConn or Close called
            return
        case message:= <- sClient.appendChan:// append out of order message
            if !sClient.alreadyReceived(message.SeqNum) {
                sClient.pendingMessages = append(sClient.pendingMessages,message)
            }
        case message:= <- sClient.stagePushChan: //
            if (message.SeqNum == sClient.seqExpected){
                wrapMessage := &readReturn{
                    connID: message.ConnID,
                    seqNum: message.SeqNum,
                    payload: message.Payload,
                    err: nil,
                }
                sClient.messageToPush = wrapMessage
            }
        case readReturnChan <- sClient.messageToPush:
            //if entered here, means we just pushed the message with seqNum
            //client.seqExpected to the main readReturnChan, thus need to update
            //and check whether we have pendingMessages that can be 
            sClient.seqExpected +=1
            //go through pending messages and check if already received the next  
            //message in order, check againt client.seqExpected
            for i := 0; i < len(sClient.pendingMessages); i++ {
                message := sClient.pendingMessages[i]
                if (message.SeqNum == sClient.seqExpected){
                    //make sure sending messages out in order
                    wrapMessage := &readReturn{
                        connID: message.ConnID,
                        seqNum: message.SeqNum,
                        payload: message.Payload,
                        err: nil,
                    }
                    sClient.messageToPush = wrapMessage
                    //cut this message off pendingMessages
                    sClient.pendingMessages = append(sClient.pendingMessages[:i], sClient.pendingMessages[i+1:]...)
                   
                    break //make sure only push one message to the read()
                }
            }
        }
    }
}





