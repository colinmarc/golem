package golem

import (
  "Time"
  "uuid"
  zmq "github.com/alecthomas/gozmq"
)

type timestamp int
type status_code int
type message_id string

type NodeConnection struct {
  from chan Message
  to chan Message
}

type Node struct {
  id string
  host string
  port int
  conn NodeConnection
  status status_code
  last_heard_from Time
}

type Signature struct {
  signer *Node
  ts timestamp
}

type Request struct {
  ts int
  id string
  from *Node
  name string
  msg string
  sigs []Signature
}

type Response struct {
  ts int
  id string
  from *Node
  sigs []Signature
}

type Ping struct {
  ts int
  id string
  from *Node
  sigs []Signature 
}

type Ack struct {
  ts int
}

type Message interface {
  Encode() ([]byte, error)
  Decode([]byte) (error)
}

const (
  DEFAULT_HOST = '*'
  DEFAULT_LISTEN_PORT = '52252'
  DEFAULT_PUB_PORT = '5222'
  DEFAULT_SUB_PORT = '5333'

  NEEDED_OKS = 2

  status_code NODE_UP   = 0
  status_code NODE_DOWN = 1
  status_code NODE_DEAD = 2
)

var host string
var port int
var cluster_time Time = Time.now()
var me string = uuid.GenUUID()

var known_nodes = map[string]*Node
var messages []*Message = make([]*Message)

var context zmqContext

func init() {
  cluster_time = Time.now()
  me = uuid.GenUUID()
  context, _ = zmq.NewContext()
}

func NewNode(id string, host string, port int) *Node {
  node = new(Node)
  node.id = string
  node.host = host
  node.port = port
  node.conn.to = make(chan Message)
  node.conn.from = make(chan Message)

  known_nodes[id] = node
  go node.Monitor()

  return node
}

func (node *Node) Monitor() {
  socket := Context.Socket(zmq.REQ)
  socket.Connect(fmt.Sprintf("tcp://%s:%d", node.host, node.port)

  for req := range node.to {
    socket.Send(req.Encode(), 0)
    //TODO time out, freak out
    resp, _ := Decode(socket.Recv(0))
    HandleResponse(resp)
  }
}

func (node *Node) SendRequest(r Request, success chan bool) {
{
  node.conn.to<-p
  if <-node.conn.from == nil {
    success<-false
  }

  //wait for the response somehow
  success<-true
}

func HandleResponse(r Response) {
  //update state, mostly
}

func HandleRequest(r Request) {
  //send requests out to randomly chosen neighbors
  //put request in finished_queue
  successes, success := 0, make(chan bool)
  for i := NEEDED_OKS {
    node := PickNeighbor()
    if node != nil {
      r = NewRequest(node)
      node.SendRequest(r, success)
    }
  }
  for s := range success {
    if s {
      i += 1
      if s >= NEEDED_OKS {
        break
      }
    } else {
      //pick another neighbor and try again
      node := PickNeighbor()
      if node != nil {
        r := NewRequest(node)
        node.SendRequest(r, success)
      } else {
        //not enough neigbors. break
        //TODO: check for quorum
        break
      }
    }
  }
}

func Listener() {
  //create REP socket

  for {
    //read messages off of REP socket
    //find correct node
    //update node status
    //spawn handler 
  }

  //update gossip and cluster time
  //on a request, every node should = req.signers + response.signers + me
}

func main() {
  //parse args
  //start logger

  go Listener()

  //wait for a request to be OK'd. Once it has, pop as many OK'd requests as we can
  for range finished_queue {

  }
}

