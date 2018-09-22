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
    payload []byte
    err error
}

type s_client struct{
    conn *UDPConn
    addr *UDPAddr
    seqExpected int
    connID int
    pendingMessages []*Message
    readChan chan int //used to signal Read() is called
    writeChan chan *Message
}
type writeRequest struct{
    conn int
    payload []byte
}

type server struct {
    // TODO: implement this!
    conn *UDPConn
    addr *UDPAddr
    connectedClients []s_client
    curDataSeqNum int //start at 1
    curClientConn int //start at 1
    connID int

    readReturnChan chan readReturn //channel to send message to Read() back
    clientWriteChan chan *Message //channel to marshal and send out message to clients
    apiReadChan chan int //channel in read routine to deal with API Read() call
    apiWriteChan chan writeRequest//channel in main routine to deal with Write()

}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
    s := server{
        nil,
        nil,
        nil,
        1,
        1,
        0,
        make(chan readReturn,500),
        make(chan *Message),
        make(chan int),
        make(chan writeRequest),
    }
    adr,err := lspnet.ResolveUDPAddr("udp",strconv.Itoa(port))
    if err != nil {
        return (nil,err)
    }
    s.addr = adr
    conn, err := lspnet.ListenUDP("udp", s.addr)
    if err != nil {
        return (nil,err)
    }
    s.conn=conn
    go mainRoutine(&s)
    go readRoutine(&s)
    return &s,nil
}


func (s *server) Read() (int, []byte, error) {
    // TODO: remove this line when you are ready to begin implementing this method.
    s.apiReadChan <- 1
    message := <- s.readReturnChan
    return message.conn,message.payload,message.err
}

func (s *server) Write(connID int, payload []byte) error {
    return errors.New("not yet implemented")
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

func readRoutine(s *server) {
    for {
        select {
            case  <- s.apiReadChan://get request from Read()
                //signal all clients first and tell them there is a Read()
                
                //Potentially have multiple messages from the same client
                //blocking in their read routine?????????????????
                //also when this readRoutine is blocked to check Message with 
                //Data type, are ACK messages being blocked? any harm?
                for i := 0; i < len(s.connectedClients); i++ {
                    client := s.connectedClients[i]
                    client.readChan <- 1
                }
            default://normally reading connnection message from clients
                    var b [2000]byte
                    conn := ListenUDP() ??????????
                    size,addr,err := conn.ReadFromUDP(b)
                    if (err!=nil){
                        //do something
                    }
                    ***********message := unMarshal(b,size)//change to unmarshal function
                    s.connectChan <- message
        }
    }
}
func mainRoutine(s *server) {
    for {
        select {
        case message := <- s.connectChan:
            if (message.Type == MsgConnect){
                c := s_client{
                    conn,
                    nil,
                    1,
                    s.curClientConn,
                    nil,
                    make([]*Message, 5),
                    make(chan int),
                    make(chan *Message),
                }
                s.curClientConn +=1;
                s.connectedClients = append(s.connectedClients, &c)
                go clientRead(&c,&s)
                ********** go clientWrite(&c)
            }
        *********** case //write, ack, whatever channel
        }
    }
}
func (c *s_client) alreadyReceived(ConnID int) bool {
    n := len(c.pendingMessages)
    for i := 0; i < n; i++ {
        if (c.pendingMessages[i].ConnID == ConnID){
            return true
        }
    }
    return false
}
func clientRead(client *s_client,s *server) {
    for {
        select{
        case <- client.readChan:
            //find whether there is pendingMessages that matches seqExpected
            for i := 0; i < len(client.pendingMessages); i++ {
                message := client.pendingMessages[i]
                if (message.connID == client.seqExpected){
                    m := readReturn{//generate message to return
                        message.ConnID,
                        message.Payload,
                        nil,
                    }
                    //make sure sending messages out in order
                    client.seqExpected +=1
                    //cut this message off pendingMessages
                    client.pendingMessages = append(client.pendingMessages[:i], client.pendingMessages[i+1:]...)
                    s.returnReadChan <- &m
                    break //make sure only push one message to the read()
                }
            }
            

        default:
            conn,addr := client.conn,client.addr
            var b [2000]byte
            size,addr,err := conn.ReadFromUDP(b)
            if err != nil {//deal with error later
                *************message := unMarshal(b,size)//unMarshall returns *Message
                if (message.Type == MsgData){
                    seq := message.SeqNum
                    if (seq >= client.seqExpected){
                        if (client.alreadyReceived==false)
                        client.pendingMessages = append(client.pendingMessages,message)
                    }
                    *********** //send ack back
                }
            }

        }
    }
}




