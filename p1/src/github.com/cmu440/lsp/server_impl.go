// Contains the implementation of a LSP server.

package lsp

import (
	"errors"
	"github.com/cmu440/lspnet"
	//"github.com/cmu440/lspnet" ********* need to use this on autolab
	"fmt"
	"strconv"
	"strings"
)

type readReturn struct {
	connID  int
	seqNum  int
	payload []byte
	err     error
}
type connectRequest struct {
	message *Message
	addr    *lspnet.UDPAddr
}

type s_client struct { //server side client structure
	addr          *lspnet.UDPAddr
	seqExpected   int //start with one
	connID        int
	writeSeqNum   int // used for writing, start with 1
	messageToPush *readReturn
	//received data messages that is not read yet, no duplicates
	// seq number of messages in pendingMessages >= seqExpected
	//no corrupted messages as well
	pendingMessages []*Message
	messageChan     chan *Message //receive message from readRoutine
	clientCloseChan chan int

	// this is for the rest of partA
	window [] *windowElem
	windowStart int
	addToWindowChan chan *windowElem
	buffer [] *windowElem
}

type writeAckRequest struct {
	ack    *Message
	client *s_client
}

type windowElem struct {
	seqNum int
	ackChan chan *Message
	msg []byte
	finishedSetInWindowChan chan int // the chan tells the client main that 
									 // the elem has been put in window
}

type writeRequest struct {
	connID  int
	payload []byte
}

type server struct {
	// TODO: implement this!
	serverConn       *lspnet.UDPConn
	serverAddr       *lspnet.UDPAddr
	connectedClients []*s_client
	//start at 1, sequence number of data messages sent from server
	curDataSeqNum int
	//start at 1, connID to be assigned when next new connection is made
	curClientConnID int

	newClientChan    chan *s_client
	connectChan      chan *connectRequest // channel to set up new connections
	readReturnChan   chan *readReturn     //channel to send message to Read() back
	params           *Params
	writeRequestChan chan *writeRequest
	writeAckChan     chan *writeAckRequest
	writeBackChan    chan error

	clientRemoveChan chan *s_client
	mainCloseChan    chan int
	readCloseChan    chan int

	// below is for the rest of partA
	clientWriteErrorChan chan error
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	s := server{
		serverConn:       nil,
		serverAddr:       nil,
		connectedClients: make([]*s_client, 0),
		curDataSeqNum:    1,
		curClientConnID:  1,
		newClientChan:    make(chan *s_client),
		connectChan:      make(chan *connectRequest),
		readReturnChan:   make(chan *readReturn, 500),
		params:           params,
		writeRequestChan: make(chan *writeRequest),
		writeAckChan:     make(chan *writeAckRequest),
		writeBackChan:    make(chan error),
		clientRemoveChan: make(chan *s_client),
		mainCloseChan:    make(chan int),
		readCloseChan:    make(chan int),
	}
	adr, err := lspnet.ResolveUDPAddr("udp", "localhost:"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}
	s.serverAddr = adr
	// fmt.Println(s.serverAddr)
	conn, err := lspnet.ListenUDP("udp", s.serverAddr)
	if err != nil {
		fmt.Println("server: error")
		return nil, err
	}
	s.serverConn = conn
	go s.mainRoutine()
	go s.readRoutine()
	return &s, nil
}

func (s *server) Read() (int, []byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	message := <-s.readReturnChan
	return message.connID, message.payload, message.err

}

func (s *server) Write(connID int, payload []byte) error {
	request := &writeRequest{
		connID:  connID,
		payload: payload,
	}
	// fmt.Println("server: before send write request")
	s.writeRequestChan <- request
	err := <-s.writeBackChan
	return err
}

func (s *server) CloseConn(connID int) error {

	sClient := s.searchClientToClose(connID)
	if sClient != nil {
		sClient.clientCloseChan <- 1
		s.clientRemoveChan <- sClient //could have issues, if removing client
		return nil
	}
	return errors.New("connection already closed")
}

func (s *server) Close() error {
	s.mainCloseChan <- 1
	s.readCloseChan <- 1
	return nil
}
func (s *server) searchClientToClose(connID int) *s_client {
	for i := 0; i < len(s.connectedClients); i++ {
		sClient := s.connectedClients[i]
		if sClient.connID == connID {
			return sClient
		}
	}
	return nil
}
func (s *server) mainRoutine() {
	for {
		select {
		case <-s.mainCloseChan:
			for i := 0; i < len(s.connectedClients); i++ {
				s.connectedClients[i].clientCloseChan <- 1
			}
			return
		case sClient := <-s.clientRemoveChan:
			for i := 0; i < len(s.connectedClients); i++ {
				if s.connectedClients[i].connID == sClient.connID {
					s.connectedClients = append(s.connectedClients[:i], s.connectedClients[i+1:]...)
					break
				}
			}
		case request := <-s.connectChan: //set up connection
			//fmt.Println("server: got request to connect")
			message := request.message
			if message.Type == MsgConnect { //start a new server side client
				c := &s_client{ //need to adapt to new struct
					addr:            request.addr,
					seqExpected:     1,
					writeSeqNum:     1,
					connID:          s.curClientConnID,
					messageToPush:   nil,
					pendingMessages: make([]*Message, 0),
					messageChan:     make(chan *Message),
					clientCloseChan: make(chan int),

					window: make([]*windowElem, DefaultWindowSize)
					windowStart: 1
				}
				s.curClientConnID += 1
				s.connectedClients = append(s.connectedClients, c)
				// fmt.Printf("%v", s.connectedClients)
				//fmt.Println("server: send new client struct back")
				s.newClientChan <- c //let read routine create ack request
				//fmt.Println("server: start client Main routine")
				go c.clientMain(s)
			}

		case request := <-s.writeRequestChan:
			// write data to client
			connID := request.connID
			payload := request.payload
			var sClient *s_client = nil
			for i := 0; i < len(s.connectedClients); i++ {
				if s.connectedClients[i].connID == connID {
					sClient = s.connectedClients[i]
					break
				}
			}
			if sClient != nil {
				seqNum := sClient.writeSeqNum
				size := len(payload)
				checksum := makeCheckSum(connID, seqNum, size, payload)
				original := NewData(connID, seqNum, size, payload, checksum)
				msg, err := marshal(original)

				elem = &windowElem{
					seqNum: seqNum,
					ackChan: make(chan *Message),
					msg: msg, 
				}
				sClient.addToWindowChan <- elem
				// ********** go resendRoutine // the first time sending is also done in resendRoutine
				// if err != nil {
				// 	s.writeBackChan <- err
				// 	continue
				// }
				// num, err := s.serverConn.WriteToUDP(msg, sClient.addr)
				// _ = num
				// if err == nil {
				// 	sClient.writeSeqNum += 1
				// }
				err := <- clientWriteErrorChan
				s.writeBackChan <- err

			} else {
				err := errors.New("This client does not exist")
				s.writeBackChan <- err
			}

		// write ack to client
		case ackRequest := <-s.writeAckChan:
			// fmt.Println("server: got ack sending request")
			ack := ackRequest.ack
			sClient := ackRequest.client

			byteMessage, err := marshal(ack)
			// fmt.Println("server: marshalled it")
			_ = err                //deal with later?
			message1 := &Message{} //store message

			unmarshal(byteMessage, message1) //unMarshall returns *Message
			// fmt.Println(byteMessage)
			num, err := s.serverConn.WriteToUDP(byteMessage, sClient.addr)
			// fmt.Println("server: finished sending ack")
			_ = num
			_ = err //deal with later?
		}
	}
}
func (s *server) searchClient(addr *lspnet.UDPAddr) *s_client {
	// fmt.Println("server: entered search")
	for i := 0; i < len(s.connectedClients); i++ {
		sClient := s.connectedClients[i]
		// fmt.Println("server: client not nil")
		if strings.Compare(sClient.addr.String(), addr.String()) == 0 {
			// fmt.Println("server: this is received client connecting again")
			return sClient
		}
	}
	// fmt.Println("server: newClient!")
	return nil
}
func (s *server) readRoutine() {
	for {
		select {
		case <-s.readCloseChan:
			return
		default:
			serverConn := s.serverConn
			b := make([]byte, 2000)
			//fmt.Println("server: before read")

			size, addr, err := serverConn.ReadFromUDP(b)

			//fmt.Println("server: got message")
			if err == nil { //deal with error later
				var message Message           //store message
				unmarshal(b[:size], &message) //unMarshall returns *Message
				// fmt.Println("server: get message type is "+strconv.Itoa(int(message.Type))+" and connID: "+strconv.Itoa(int(message.ConnID)))
				//fmt.Println("server: after unmarshal")
				if integrityCheck(&message) { //check integrity here with checksum and size
					//fmt.Println("server: integrity check passed")
					if message.Type == MsgData {
						sClient := s.searchClient(addr) //make sure client connected
						if sClient != nil {
							sClient.messageChan <- &message
							//else if seq <seqExpected, then don't worry about returning it to Read()
							ack := NewAck(message.ConnID, message.SeqNum)
							// fmt.Println("server: ack made")
							ackRequest := &writeAckRequest{
								ack:    ack,
								client: sClient,
							}
							s.writeAckChan <- ackRequest
						}
					} else if message.Type == MsgConnect {
						request := &connectRequest{
							&message,
							addr,
						}
						//fmt.Println("server: before searchClient")
						//check if the client is already connected on the server end
						newClient := s.searchClient(addr)
						var sClient *s_client = nil
						if newClient == nil { //first connect message
							//fmt.Println("server: send message to connectChan")
							s.connectChan <- request
							//fmt.Println("server: wait for newClient struct")
							sClient = <-s.newClientChan //wait for new client from main
							//fmt.Println("server: got newClient struct!")
						} else {
							//fmt.Println("server: already a received client")
							sClient = newClient
						}
						//make new server side client struct in mainRoutine
						// fmt.Println("server: making ack request back for connID:"+strconv.Itoa(sClient.connID))
						ack := NewAck(sClient.connID, 0)
						ackRequest := &writeAckRequest{
							ack:    ack,
							client: sClient,
						}
						// fmt.Println("server: send ackRequest to mainRoutine")
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
		if c.pendingMessages[i].SeqNum == seq {
			return true
		}
	}
	return false
}

//would block until Read() is called
//mainly deal with out of order messages on each client
//append out of order messages to pendingMessages, try to push the correct
//message to s.readReturnChan when have one
func (sClient *s_client) clientMain(s *server) {
	for {
		//if messageToPush is good, we would try to push to s.readReturnChan
		// var readReturnChan chan *readReturn = nil
		// if (sClient.messageToPush != nil && sClient.messageToPush.seqNum==sClient.seqExpected){
		//     fmt.Println("server: readReturnChan is not Nil")
		//     readReturnChan := s.readReturnChan
		//     _ = readReturnChan
		// }
		if sClient.messageToPush != nil && sClient.messageToPush.seqNum == sClient.seqExpected {
			select {
			case <-sClient.clientCloseChan: //CloseConn or Close called
				return
			case message := <-sClient.messageChan:
				if message.SeqNum > sClient.seqExpected {
					if !sClient.alreadyReceived(message.SeqNum) {
						sClient.pendingMessages = append(sClient.pendingMessages, message)
					}
				} else if message.SeqNum == sClient.seqExpected {
					wrapMessage := &readReturn{
						connID:  message.ConnID,
						seqNum:  message.SeqNum,
						payload: message.Payload,
						err:     nil,
					}
					sClient.messageToPush = wrapMessage
				}
			case s.readReturnChan <- sClient.messageToPush:
				//if entered here, means we just pushed the message with seqNum
				//client.seqExpected to the main readReturnChan, thus need to update
				//and check whether we have pendingMessages that can be
				// fmt.Println("server: has pushed the message")
				sClient.seqExpected += 1
				//go through pending messages and check if already received the next
				//message in order, check againt client.seqExpected
				for i := 0; i < len(sClient.pendingMessages); i++ {
					message := sClient.pendingMessages[i]
					if message.SeqNum == sClient.seqExpected {
						//make sure sending messages out in order
						wrapMessage := &readReturn{
							connID:  message.ConnID,
							seqNum:  message.SeqNum,
							payload: message.Payload,
							err:     nil,
						}
						sClient.messageToPush = wrapMessage
						//cut this message off pendingMessages
						sClient.pendingMessages = append(sClient.pendingMessages[:i], sClient.pendingMessages[i+1:]...)

						break //make sure only push one message to the read()
					}
				}
			// below two cases are for partA
			// to avoid bug, the two cases are not copied in the else clause
			case elem := <- sClient.addToWindowChan:
				seqNum = elem.seqNum 
				// the below condition is ** key ** 
				if (seqNum < sClient.windowStart + DefaultWindowSize && sClient.window[seqNum - windowStart] == nil){
					// can be put into the window
					sClient.window[seqNum - windowStart] = elem
					******* go resendRoutine // NOTE: the first time sending is also done in resendRoutine
				} else {
					append(sClient.buffer, elem)
				}

			case index := <- sClient.resendSuccessChan:
				sClient.window[index] = nil
				window := sClient.window
				if index == 0{
					offset := 0
					for int i = 0; i < DefaultWindowSize; i ++ {
						if window[i] == nil{
							offset += 1
						} else {
							break
						}
					}
					// for cleaniness and garbage recollection purpose, remake 
					// the window every time we slide the window
					new_window := make([] *windowElem, DefaultWindowSize)
					for i := offset; i < DefaultWindowSize; i++{
						new_window[i - offset] = window[i]
					}
					// add element in the buffer to the window
					emptyStartIndex := DefaultWindowSize - offset
					for i := 0; i < offset; i ++ {
						new_window[i + emptyStartIndex] = buffer[i]
					}
					// shrink the buffer
					new_buffer := buffer[offset:]
					sClient.windowStart += offset
					sClient.window = new_window
					sClient.buffer = new_buffer
				}
			}

		} else {
			select {
			case <-sClient.clientCloseChan: //CloseConn or Close called
				return
			case message := <-sClient.messageChan:
				if message.SeqNum > sClient.seqExpected {
					if !sClient.alreadyReceived(message.SeqNum) {
						sClient.pendingMessages = append(sClient.pendingMessages, message)
					}
				} else if message.SeqNum == sClient.seqExpected {
					wrapMessage := &readReturn{
						connID:  message.ConnID,
						seqNum:  message.SeqNum,
						payload: message.Payload,
						err:     nil,
					}
					sClient.messageToPush = wrapMessage
				}
			}
			}
		}
	}
}
