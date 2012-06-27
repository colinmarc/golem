package golem

import (
  "Time"
  "uuid"
  zmq "github.com/alecthomas/gozmq"
)

type timestamp int
type status_code int
type message_id string

type Node struct {
  id string
  host string
  port int
  to chan Outgoing
  open_requests map[string](*chan bool)
  status status_code
  last_heard_from Time
}

type Signature struct {
  signer *Node
  ts timestamp
}

type OpenRequest struct {
  id string
  name string
  msg string
  done bool
  sigs []Signature
}

//Incoming
//TODO rename to question and answer? or something
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

type Incoming interface {
  Decode([]byte) (error)
}

type Outgoing interface {
  Encode() ([]byte, error)
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

type Golem struct {
  host string
  port int
  cluster_time Time
  me string

  wake_up chan bool
  known_nodes map[string]*Node
  open_request_queue OpenRequestQueue

  zcontext zmq.zmqContext
}

func Instance() {
  g = Golem.new()
  g.cluster_time = Time.now()
  g.me = uuid.GenUUID()

  g.wake_up = make(chan int)
  g.known_nodes = make(map[string]*Node)
  g.request_queue = RequestQueue.new()

  g.zcontext, _ = zmq.NewContext()
}

func NewNode(id string, host string, port int) *Node {
  node = new(Node)
  node.id = string
  node.host = host
  node.port = port
  node.to = make(chan Outgoing)
  node.open_requests = make(map[string](*chan bool))

  //TODO move these out
  known_nodes[id] = node
  go node.Monitor()

  return node
}

func (node *Node) SendRequest(r Request, success *chan bool) {
  node.to<-r

  //would some other multiplexing model suit better?
  response := make(chan bool)
  node.open_requests[r.id] = response
  //wait for said response
  <-response

  success<-true
}

func (g *Golem) MonitorNode(node *Node) {
  socket := gzcontext.Socket(zmq.REQ)
  socket.Connect(fmt.Sprintf("tcp://%s:%d", node.host, node.port))
  socket.setSockOptString(zmq.IDENTITY, me)

  for req := range node.to {
    socket.Send(req.Encode(), 0)

    //TODO time out, freak out

    resp, _ := socket.Recv(0)
    ack = Ack.new()
    _ = ack.Decode(resp)
    //TODO error handling
    //update timestamp state
  }
}

func (g *Golem) DisenfranchiseNode(node *Node) {
    //stuff to do when we freak out:
    //set state to NODE_DOWN
    //close any open requests
    // node down event? (for locks, to release open ones)
}

func (g *Golem) AskRandomNode(request r, success *chan bool) {
  //TODO pick neighbor
  node := nil

  if node == nil {
    return false
  }

  r = NewRequest(r, node)
  go node.SendRequest(r, success)
  return true
}

func (g *Golem) HandleRequest(r Request) {
  //update timestamp state, gossip

  or := OpenRequest.new()
  or.sigs = make([]Signature)
  copy(r.sigs, or.sigs)
  action_queue.Push(or)

  oks, success := 0, make(chan bool)
  for i := NEEDED_OKS {
    if !AskRandomNeighbor(r, success) {
      break
    }
  }

  for s := range success {
    if s {
      oks += 1
      if oks >= NEEDED_OKS {
        break
      }
    } else {
      //pick another neighbor and try again
      if !g.AskRandomNeighbor(r, success) {
        //not enough neigbors. break
        break
      }
    }
  }

  r.done = true
  wake_up<-true
}

func (g *Golem) HandleResponse(r Response) {
  //update timestamp state, gossip
  //copy sigs on to open_request
  //update last_heard_from on nodes (from sigs)

  if success, present := r.from.open_requests[r.id]; present {
    delete(r.from.open_requests, r.id)
    success<-true
  } else {
    //freak out, or something, I donno
  }
}

func (g *Golem) Listen() {
  socket = g.zcontext.Socket(zmq.REP)
  socket.Connect(fmt.Sprintf("tcp://%s:%d", host, port))
  socket.setSockOptString(zmq.IDENTITY, me)

  for {
    //TODO error handling
    msg, _ := socket.Recv(0)
    socket.Send(newAck(), 0)

    switch msg_type = DetermineMessageType(msg); msg_type {
    case "request":
      req := Request.Decode(msg)
      //update node status and time

      HandleRequest(req)
    case "ping":
      //update node status and time
    case "bye"
      //what is a bye?
      //todo
    }

  }
}

func Log() {

}

func (g *Golem) start() {
  //start logger

  go g.Log()
  go g.Listen()

  //wait for a request to be OK'd. Once it has, pop as many OK'd requests as we can
  //but only execute them if we have a quorum. otherwise, re-open them
  for range wake_up {
    for {
      if !g.request_queue.peek().done {
        break
      }

      req := g.request_queue.pop()

      //check for quorum
      if len(req.sigs)*2 <= len(g.known_hosts) {
        req.Reopen()
        continue
      }

      req.action.Execute()
    }
  }
}

//other todos
//logging/verbosity
//encode and decode
//action interface
// -serialize, deserialize, execute
