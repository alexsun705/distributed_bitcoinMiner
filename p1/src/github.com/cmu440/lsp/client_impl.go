// Contains the implementation of a LSP client.

package lsp

import (
	"github.com/cmu440/lspnet"
	//"github.com/cmu440/lspnet" ********* need to use this on autolab
	"encoding/json"
	"fmt"
	// "strconv"
)

type client struct {
	clientConn  *lspnet.UDPConn
	serverAddr  *lspnet.UDPAddr
	connID      int
	curSeqNum   int
	seqExpected int

	//Read
	messageToPush   *readReturn   //save the one message to return to Read()
	pendingMessages []*Message    //save out of order messages
	appendChan      chan *Message //signal clientMain append to pendingMessages
	//signal clientMain to stage the push message, so clientMain can try to push
	//message to server.readReturnChan in future looping
	stagePushChan  chan *Message
	readReturnChan chan *readReturn //channel to send message to Read() back

	//Write

	writeChan         chan []byte // write request sends to this channel
	writeBackChan     chan error  // the chan sent back from main routine
	readChan          chan int    // read request sends to this channel
	payloadChan       chan []byte // where payload is sent from main routine
	writeAckChan      chan int    // ack is going to be sent
	writeConnChan     chan int    // connect is going to be sent
	connIDChan        chan int
	connIDRequestChan chan int // when function connID() calls send data to this channel
	connIDReturnChan  chan int // the function returns value from this channel
	closeChan         chan int
	mainCloseChan     chan int
	readCloseChan     chan int

	// below is for partA
	window [] *windowElem // the window that contains all the elements that are trying to resend
    windowStart int
    addToWindowChan chan *windowElem
    resendSuccessChan chan int // index := <- chan, which index from the window start has succeeded
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
	serverAddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	// fmt.Println(serverAddr)
	if err != nil {
		return nil, err
	}
	clientConn, err := lspnet.DialUDP("udp", nil, serverAddr)
	if err != nil {
		return nil, err
	}
	// to do: wait to receive ack from server
	c := &client{
		clientConn:        clientConn,
		serverAddr:        serverAddr,
		connID:            -1,
		curSeqNum:         1,
		seqExpected:       1,
		messageToPush:     nil, //save the one message to return to Read()
		appendChan:        make(chan *Message),
		stagePushChan:     make(chan *Message),
		readReturnChan:    make(chan *readReturn), //channel to send message to Read() back
<<<<<<< HEAD
		pendingMessages:   make([]*Message,0),
=======
		pendingMessages:   make([]*Message, 0),
>>>>>>> 3dfdff50e371ea95e546e8c7fdb72194d115a75e
		writeChan:         make(chan []byte),
		writeBackChan:     make(chan error),
		readChan:          make(chan int),
		payloadChan:       make(chan []byte),
		writeAckChan:      make(chan int),
		writeConnChan:     make(chan int),
		connIDChan:        make(chan int),
		connIDRequestChan: make(chan int),
		connIDReturnChan:  make(chan int),
		mainCloseChan:     make(chan int),
		readCloseChan:     make(chan int),

	}

	go c.mainRoutine()
	go c.readRoutine()

	c.writeConnChan <- 1
	//assume gonna get ack back

	//insert routine to wait for ack and block later
	// fmt.Println("client: waiting for Ack")
	connID := <-c.connIDChan
	c.connID = connID
	// fmt.Println("client: newClient() setup done")
	return c, nil
}

func (c *client) ConnID() int {
	c.connIDRequestChan <- 1
	res := <-c.connIDReturnChan
	return res
}

func (c *client) Read() ([]byte, error) {
	message := <-c.readReturnChan
	return message.payload, message.err
}

func (c *client) Write(payload []byte) error {
	// fmt.Println("client: before send write request")
	c.writeChan <- payload
	res := <-c.writeBackChan
	return res
}

func (c *client) Close() error {
	c.mainCloseChan <- 1
	c.readCloseChan <- 1
	return nil
}

// other functions defined below
//send ACK,CONN or DATA message to server
//send err back to Write() call when sending out Data message
func (c *client) sendMessage(original *Message) {
	msg, err := marshal(original)
	// fmt.Println("client: finished marshal message")
	if err != nil && original.Type == MsgData {
		c.writeBackChan <- err
		// fmt.Println("client: whoops")
		return
	}
	num, err := c.clientConn.Write(msg)
	// fmt.Println("client: conn sent")
	_ = num
	if err != nil && original.Type == MsgData {
		// fmt.Println("client: there is error")
		c.writeBackChan <- err
		return
	}
	if original.Type == MsgData {
		c.writeBackChan <- nil
	}
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

	payloadSum := ByteArray2Checksum(payload)
	// all of these are uint32
	sum := connIDSum + seqNumSum + sizeSum + payloadSum
	for sum > 0xffff {
		carry := sum >> 16
		primary := 0x0000ffff & sum
		sum = carry + primary

	}
	return uint16(sum)
}

func integrityCheck(msg *Message) bool {
	if msg.Type == MsgConnect || msg.Type == MsgAck {
		return true
	}
	actualLen := len(msg.Payload)
	expectedLen := msg.Size
	actualChecksum := makeCheckSum(msg.ConnID, msg.SeqNum, msg.Size, msg.Payload)
	expectedChecksum := msg.Checksum
	return (actualLen == expectedLen) && (actualChecksum == expectedChecksum)

}
func (c *client) received(seq int) bool {
	n := len(c.pendingMessages)
	for i := 0; i < n; i++ {
		if c.pendingMessages[i].SeqNum == seq {
			return true
		}
	}
	return false
}

func (c *client) mainRoutine() {
	for {
		// var readReturnChan chan *readReturn = nil
		// if (c.messageToPush!=nil && c.messageToPush.seqNum==c.seqExpected){
		//     readReturnChan = c.readReturnChan
		// }
		if c.messageToPush != nil && c.messageToPush.seqNum == c.seqExpected {
			select {
			case <-c.mainCloseChan:
				return

			//write channels
			case payload := <-c.writeChan:
				checksum := makeCheckSum(c.connID, c.curSeqNum, len(payload), payload)
				original := NewData(c.connID, c.curSeqNum, len(payload), payload, checksum)
				msg, err := marshal(original)
				elem = &windowElem{
					seqNum: seqNum,
					ackChan: make(chan *Message),
					msg: msg, 
				}
				c.addToWindowChan <- elem
				// c.sendMessage(originalMsg)
				c.curSeqNum += 1

			// below two cases are for partA
			// to avoid bug, the two cases are not copied in the else clause
			case elem := <- c.addToWindowChan:
				seqNum = elem.seqNum
				// the below condition is ** key ** 
				if (seqNum < c.windowStart + DefaultWindowSize && c.window[seqNum - windowStart] == nil){
					// can be put into the window
					c.window[seqNum - windowStart] = elem
					******* go resendRoutine // NOTE: the first time sending is also done in resendRoutine
				} else {
					append(c.buffer, elem)
				}

			case index := <- c.resendSuccessChan:
				c.window[index] = nil
				window := c.window
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
					c.windowStart += offset
					c.window = new_window
					c.buffer = new_buffer
				}

			case seqNum := <-c.writeAckChan:
				ack := NewAck(c.connID, seqNum)
				// fmt.Println("client: ready to send ack")
				c.sendMessage(ack)

			case <-c.writeConnChan:
				conn := NewConnect()
				c.sendMessage(conn)

			case <-c.connIDRequestChan:
				c.connIDReturnChan <- c.connID

			//Reading channels, same with server implementation
			case message := <-c.appendChan: // append out of order message
				if !c.received(message.SeqNum) {
					c.pendingMessages = append(c.pendingMessages, message)
				}
			case message := <-c.stagePushChan: //prepare for a push to readReturn
				if message.SeqNum == c.seqExpected {
					wrapMessage := &readReturn{
						connID:  message.ConnID,
						seqNum:  message.SeqNum,
						payload: message.Payload,
						err:     nil,
					}
					c.messageToPush = wrapMessage
				}
			case c.readReturnChan <- c.messageToPush:
				//if entered here, means we just pushed the message with seqNum
				//client.seqExpected to the main readReturnChan, thus need to update
				//and check whether we have pendingMessages that can be
				c.seqExpected += 1
				//go through pending messages and check if already received the next
				//message in order, check againt client.seqExpected
				for i := 0; i < len(c.pendingMessages); i++ {
					message := c.pendingMessages[i]
					if  message.SeqNum == c.seqExpected {
						//make sure sending messages out in order
						wrapMessage := &readReturn{
							connID:  message.ConnID,
							seqNum:  message.SeqNum,
							payload: message.Payload,
							err:     nil,
						}
						c.messageToPush = wrapMessage
						//cut this message off pendingMessages
						c.pendingMessages = append(c.pendingMessages[:i], c.pendingMessages[i+1:]...)
						break //make sure only push one message to the read()
					}
				}
			}
		} else {
			select {
			case <-c.mainCloseChan:
				return
			//write channels
			case payload := <-c.writeChan:
				checksum := makeCheckSum(c.connID, c.curSeqNum, len(payload), payload)
				originalMsg := NewData(c.connID, c.curSeqNum, len(payload), payload, checksum)
				c.sendMessage(originalMsg)
				c.curSeqNum += 1

			case seqNum := <-c.writeAckChan:
				ack := NewAck(c.connID, seqNum)
				// fmt.Println("client: ready to send ack")
				c.sendMessage(ack)

			case <-c.writeConnChan:
				conn := NewConnect()
				c.sendMessage(conn)

			case <-c.connIDRequestChan:
				c.connIDReturnChan <- c.connID

			//Reading channels, same with server implementation
			case message := <-c.appendChan: // append out of order message
				if !c.received(message.SeqNum) {
					c.pendingMessages = append(c.pendingMessages, message)
				}
			case message := <-c.stagePushChan: //prepare for a push to readReturn
				if message.SeqNum == c.seqExpected {
					wrapMessage := &readReturn{
						connID:  message.ConnID,
						seqNum:  message.SeqNum,
						payload: message.Payload,
						err:     nil,
					}
					c.messageToPush = wrapMessage
				}
			}
		}
	}
}

func (c *client) readRoutine() {
	for {
		select {
		case <-c.readCloseChan:
			return
		default:
			b := make([]byte, 2000)
			n, err := c.clientConn.Read(b)

			//fmt.Println(b)
			//_ = m
			if err == nil { //deal with error later
				var message Message
				// fmt.Println("client: before unmarshal")
				//fmt.Println(b)
				unmarshal(b[:n], &message) //unMarshall returns *Message
				// fmt.Println("client: after unmarshal, integrity checks now")
				if integrityCheck(&message) { //check integrity here with checksum and size
					// fmt.Println("client: passs integrity check")
					// fmt.Println("client: get message type is "+strconv.Itoa(int(message.Type))+" and connID: "+strconv.Itoa(int(message.ConnID)))
					if message.Type == MsgData {
						// fmt.Println("client: it's data message!")
						seq := message.SeqNum
						if seq > c.seqExpected { //out of order, pending
							c.appendChan <- &message
						}
						if seq == c.seqExpected {
							//let clientMain try pushing message to s.readReturnChan
							c.stagePushChan <- &message
						}
						// fmt.Println("client: pushing ack to chan")
						c.writeAckChan <- seq //signal to send Ack back
					} else if message.Type == MsgAck {
						// fmt.Println("client: it's ack message!")
						if message.SeqNum == 0 {
							// fmt.Println("client: set up NewClient")
							c.connIDChan <- message.ConnID //set up NewClient
						}
					} else {
						fmt.Println("client: this message is wrong type")
					}
					//if its ACK, do sth later for epoch
				}
			} else { //deal with error
				fmt.Println("client: got error")
				return //connection lost?
			}
		}

	}
}
