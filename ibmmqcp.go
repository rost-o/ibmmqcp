//
// IBM MQ Connection Pool
//
// Rostislav Olitto (c) 2019
// This code is licensed under MIT license
//
package ibmmqcp

import (
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
)

// Error / Text definition
var (
	ErrInvalidServerAddress  = errors.New("ibmmq cp: server address not in the correct format")
	ErrQueueNotFound         = errors.New("ibmmq cp: queue not found in the pool")
	ErrPoolClosed            = errors.New("ibmmq cp: pool closed")
	ErrInvalidMessageID      = errors.New("ibmmq cp: invalid message id")
	ErrInvalidCorrelationID  = errors.New("ibmmq cp: invalid correlation id")
	WrnUnableCloseQueue      = "ibmmq cp: close queue error %s\n"
	WrnUnableCloseConnection = "ibmmq cp: close connection error %s\n"
)

// Constants
const (
	MessageIdSize     = 24
	CorrelationIdSize = 24
)

type MessageProcessingCallback func(*Connection, *MessageQueue, *ibmmq.MQMD, *ibmmq.MQGMO, []byte, *ibmmq.MQCBC, *ibmmq.MQReturn)

// Configuration
type PoolConfig struct {
	ServerURI    string // Expected format: ibmmq://user:password@hostname:1414/QM1/SYSTEM.BKR.CONFIG
	ConnMax      int
	ConnIdleMax  int
	WaitInterval int
}

func (config *PoolConfig) NewPool() *Pool {
	pool := &Pool{
		config:      *config,
		connList:    make([]*Connection, 0, config.ConnMax),
		connReqList: &PendingQueue{},
		closed:      false,
	}

	return pool
}

func (config *PoolConfig) ParseServerURI() (string, string, string, string, string, error) {
	var (
		connURI  *url.URL
		connName string
		qmName   string
		chanName string
		authUser string
		authPass string
		tmp      []string
		e        error
	)

	if connURI, e = url.Parse(config.ServerURI); e != nil {
		return "", "", "", "", "", e
	}

	tmp = strings.Split(connURI.Host, ":")

	switch len(tmp) {
	case 1:
		connName = fmt.Sprintf("%s", tmp[0])
	case 2:
		connName = fmt.Sprintf("%s(%s)", tmp[0], tmp[1])
	default:
		return "", "", "", "", "", ErrInvalidServerAddress
	}

	tmp = strings.Split(strings.Trim(connURI.Path, "/"), "/")

	switch len(tmp) {
	case 1:
		qmName = "QM1"
		chanName = tmp[0]
	case 2:
		qmName = tmp[0]
		chanName = tmp[1]
	default:
		qmName = "QM1"
		chanName = "SYSTEM.DEF.SVRCONN"
	}

	authUser = connURI.User.Username()
	authPass, _ = connURI.User.Password()

	return connName, qmName, chanName, authUser, authPass, nil
}

// Connection
type Connection struct {
	qManager    *ibmmq.MQQueueManager
	ctrlOptions *ibmmq.MQCTLO
	qList       []*MessageQueue
	busy        bool
	syncLock    sync.Mutex
}

func (conn *Connection) OpenQueue(qName string, writeMode bool) (*MessageQueue, error) {
	conn.syncLock.Lock()
	defer conn.syncLock.Unlock()

	qObjectDesc := ibmmq.NewMQOD()
	qObjectDesc.ObjectType = ibmmq.MQOT_Q
	qObjectDesc.ObjectName = qName

	qOpenOptions := ibmmq.MQOO_FAIL_IF_QUIESCING

	if writeMode {
		qOpenOptions |= ibmmq.MQOO_OUTPUT
	} else {
		qOpenOptions |= ibmmq.MQOO_INPUT_SHARED
	}

	qObject, e := conn.qManager.Open(qObjectDesc, qOpenOptions) //MQOO_INPUT_AS_Q_DEF

	if e != nil {
		return nil, e
	}

	queue := &MessageQueue{
		qConn:   conn,
		qObject: &qObject,
		qName:   qName,
	}

	conn.qList = append(conn.qList, queue)
	return queue, nil
}

func (conn *Connection) CloseQueue(queue *MessageQueue) error {
	conn.syncLock.Lock()
	defer conn.syncLock.Unlock()

	queueIndex := -1

	for i := 0; i < len(conn.qList); i++ {
		if queue == conn.qList[i] {
			queueIndex = i
			break
		}
	}

	if queueIndex > -1 {
		copy(conn.qList[queueIndex:], conn.qList[queueIndex+1:])

		conn.qList[len(conn.qList)-1] = nil
		conn.qList = conn.qList[:len(conn.qList)-1]
	} else {
		return ErrQueueNotFound
	}

	return queue.close()
}

func (conn *Connection) StartConsume() error {
	conn.syncLock.Lock()
	defer conn.syncLock.Unlock()

	if conn.ctrlOptions != nil {
		return nil
	}

	conn.ctrlOptions = ibmmq.NewMQCTLO()

	if e := conn.qManager.Ctl(ibmmq.MQOP_START, conn.ctrlOptions); e != nil {
		conn.ctrlOptions = nil
		return e
	}

	return nil
}

func (conn *Connection) StopConsume() error {
	conn.syncLock.Lock()
	defer conn.syncLock.Unlock()

	if conn.ctrlOptions != nil {
		if e := conn.qManager.Ctl(ibmmq.MQOP_STOP, conn.ctrlOptions); e != nil {
			return e
		}

		conn.ctrlOptions = nil
	}

	return nil
}

func (conn *Connection) Commit() error {
	conn.syncLock.Lock()
	defer conn.syncLock.Unlock()

	return conn.qManager.Cmit()
}

func (conn *Connection) Rollback() error {
	conn.syncLock.Lock()
	defer conn.syncLock.Unlock()

	return conn.qManager.Back()
}

func (conn *Connection) PrepareMessageID(id string, size int) (string, []byte) {
	var (
		resID    string
		resIDHex []byte
		e        error
	)

	if size < 1 {
		size = MessageIdSize
	}

	if len(id) > 0 {
		resID = strings.ReplaceAll(id, "-", "")

		if len(resID) > 1 {
			if resIDHex, e = hex.DecodeString(id); e != nil {
				resIDHex = []byte(id)

				if len(resIDHex) < size {
					tmp := make([]byte, size)

					copy(tmp, resIDHex)
					resIDHex = tmp
				}

				resID = hex.EncodeToString(resIDHex)
			}

			if len(resIDHex) == size {
				return resID, resIDHex
			}
		}
	}

	resIDHex = make([]byte, size)
	rand.Read(resIDHex)

	resID = hex.EncodeToString(resIDHex)

	return resID, resIDHex
}

func (conn *Connection) getBusyStatus() bool {
	conn.syncLock.Lock()
	defer conn.syncLock.Unlock()

	return conn.busy
}

func (conn *Connection) setBusyStatus(status bool) {
	conn.syncLock.Lock()
	defer conn.syncLock.Unlock()

	conn.busy = status
}

func (conn *Connection) close() error {
	conn.syncLock.Lock()
	defer conn.syncLock.Unlock()

	for i, queue := range conn.qList {
		if e := queue.close(); e != nil {
			log.Printf(WrnUnableCloseQueue, e)
		}

		conn.qList[i] = nil
	}

	if conn.qManager != nil {
		if e := conn.qManager.Disc(); e != nil {
			log.Printf(WrnUnableCloseConnection, e)
		}
	}

	conn.qManager = nil
	conn.qList = nil

	return nil
}

// Message Queue
type MessageQueue struct {
	qConn    *Connection
	qObject  *ibmmq.MQObject
	qName    string
	syncLock sync.Mutex
}

func (queue *MessageQueue) PutMessage(messageID, correlationID, data []byte) error {
	queue.syncLock.Lock()
	defer queue.syncLock.Unlock()

	msgDesc := ibmmq.NewMQMD()
	msgDesc.Format = ibmmq.MQFMT_NONE // ibmmq.MQFMT_STRING

	if l := len(messageID); l > 0 {
		if l != MessageIdSize {
			return ErrInvalidMessageID
		}

		msgDesc.MsgId = messageID
	}

	if l := len(correlationID); l > 0 {
		if l != CorrelationIdSize {
			return ErrInvalidCorrelationID
		}

		msgDesc.CorrelId = correlationID
	}

	pmo := ibmmq.NewMQPMO()
	pmo.Options = ibmmq.MQPMO_NO_SYNCPOINT

	return queue.qObject.Put(msgDesc, pmo, data)
}

func (queue *MessageQueue) RegisterCallback(cb MessageProcessingCallback, msgWaitInterval int) error {
	queue.syncLock.Lock()
	defer queue.syncLock.Unlock()

	msgDesc := ibmmq.NewMQMD()

	getOptions := ibmmq.NewMQGMO()
	getOptions.Options = ibmmq.MQGMO_SYNCPOINT | ibmmq.MQGMO_WAIT
	getOptions.WaitInterval = int32(msgWaitInterval)

	cbDesc := ibmmq.NewMQCBD()
	cbDesc.CallbackFunction = func(_ *ibmmq.MQQueueManager, _ *ibmmq.MQObject, msgDesc *ibmmq.MQMD, getOptions *ibmmq.MQGMO, msgBody []byte, cbDesc *ibmmq.MQCBC, result *ibmmq.MQReturn) {
		cb(queue.qConn, queue, msgDesc, getOptions, msgBody, cbDesc, result)
	}

	if e := queue.qObject.CB(ibmmq.MQOP_REGISTER, cbDesc, msgDesc, getOptions); e != nil {
		return e
	}

	return nil
}

func (queue *MessageQueue) UnregisterCallback() error {
	queue.syncLock.Lock()
	defer queue.syncLock.Unlock()

	return queue.qObject.CB(ibmmq.MQOP_DEREGISTER, nil, nil, nil)
}

func (queue *MessageQueue) close() error {
	queue.syncLock.Lock()
	defer queue.syncLock.Unlock()

	if e := queue.qObject.Close(0); e != nil {
		log.Printf(WrnUnableCloseQueue, e)
	}

	queue.qConn = nil
	queue.qObject = nil
	queue.qName = ""

	return nil
}

// List of requests for Connection allocation
type PendingQueue struct {
	connNotifyList []chan *Connection
	syncLock       sync.Mutex
}

func (pendingQueue *PendingQueue) Len() int {
	pendingQueue.syncLock.Lock()
	defer pendingQueue.syncLock.Unlock()

	return len(pendingQueue.connNotifyList)
}

func (pendingQueue *PendingQueue) Put(ch chan *Connection) {
	pendingQueue.syncLock.Lock()
	defer pendingQueue.syncLock.Unlock()

	pendingQueue.connNotifyList = append(pendingQueue.connNotifyList, ch)
}

func (pendingQueue *PendingQueue) NotifyOne(conn *Connection) bool {
	pendingQueue.syncLock.Lock()
	defer pendingQueue.syncLock.Unlock()

	if len(pendingQueue.connNotifyList) > 0 {
		var ch chan *Connection

		ch, pendingQueue.connNotifyList = pendingQueue.connNotifyList[0], pendingQueue.connNotifyList[1:]
		ch <- conn

		close(ch)
		return true
	}

	return false
}

func (pendingQueue *PendingQueue) NotifyAll() {
	pendingQueue.syncLock.Lock()
	defer pendingQueue.syncLock.Unlock()

	for i := 0; i < len(pendingQueue.connNotifyList); i++ {
		close(pendingQueue.connNotifyList[i])
	}

	pendingQueue.connNotifyList = nil
}

// Connection Pool
type Pool struct {
	config        PoolConfig
	connList      []*Connection
	connReqList   *PendingQueue
	connBusyCount int32
	closed        bool
	syncLock      sync.Mutex
}

type PoolState struct {
	ConnIdle      int
	ConnBusy      int
	ConnRequested int
}

func (pool *Pool) GetConnection() (*Connection, error) {
	for {
		pool.syncLock.Lock()

		if pool.closed {
			pool.syncLock.Unlock()

			return nil, ErrPoolClosed
		}

		// #1 Reuse free connections
		for _, conn := range pool.connList {
			if !conn.getBusyStatus() {
				conn.setBusyStatus(true)

				pool.incrConnBusy()
				pool.syncLock.Unlock()

				return conn, nil
			}
		}

		if len(pool.connList) >= pool.config.ConnMax {
			pool.syncLock.Unlock()

			// #2 Waiting for available connection
			chanNotify := make(chan *Connection)

			pool.connReqList.Put(chanNotify)

			if ch := <-chanNotify; ch != nil {
				pool.incrConnBusy()
				return ch, nil
			}

			continue
		}

		break
	}

	// #3 Trying to open new connection
	connName, qmName, chanName, authUser, authPass, e := pool.config.ParseServerURI()

	if e != nil {
		pool.syncLock.Unlock()
		pool.Close()

		return nil, e
	}

	chanDefinition := ibmmq.NewMQCD()
	chanDefinition.ChannelName = chanName
	chanDefinition.ConnectionName = connName

	connOptions := ibmmq.NewMQCNO()
	connOptions.ClientConn = chanDefinition
	connOptions.Options = ibmmq.MQCNO_CLIENT_BINDING

	if len(authUser) > 0 {
		secParms := ibmmq.NewMQCSP()
		secParms.AuthenticationType = ibmmq.MQCSP_AUTH_USER_ID_AND_PWD
		secParms.UserId = authUser
		secParms.Password = authPass

		connOptions.SecurityParms = secParms
	}

	qManager, e := ibmmq.Connx(qmName, connOptions)

	if e != nil {
		pool.syncLock.Unlock()
		//pool.Close()

		return nil, e
	}

	conn := &Connection{
		qManager: &qManager,
		qList:    []*MessageQueue{},
		busy:     true,
	}

	pool.connList = append(pool.connList, conn)
	pool.syncLock.Unlock()

	return conn, nil

}

func (pool *Pool) PutConnection(conn *Connection) {
	pool.decrConnBusy()

	if pool.connReqList.NotifyOne(conn) {
		return
	}

	pool.syncLock.Lock()
	defer pool.syncLock.Unlock()

	available := 0

	for _, ci := range pool.connList {
		if !ci.getBusyStatus() {
			available++
		}
	}

	if available >= pool.config.ConnIdleMax {
		_ = pool.removeConnection(conn)
		return
	}

	conn.setBusyStatus(false)
}

func (pool *Pool) Close() {
	pool.syncLock.Lock()
	defer pool.syncLock.Unlock()

	if pool.closed {
		return
	}

	pool.closed = true

	for i, conn := range pool.connList {
		_ = conn.close()
		pool.connList[i] = nil
	}

	pool.connList = nil
}

func (pool *Pool) State() *PoolState {
	pool.syncLock.Lock()
	defer pool.syncLock.Unlock()

	idle, busy := len(pool.connList), pool.getConnBusyCount()
	idle -= busy

	return &PoolState{
		ConnIdle:      idle,
		ConnBusy:      busy,
		ConnRequested: pool.connReqList.Len(),
	}
}

func (pool *Pool) removeConnection(conn *Connection) error {
	connIndex := -1

	for i := 0; i < len(pool.connList); i++ {
		if conn == pool.connList[i] {
			connIndex = i
			break
		}
	}

	if connIndex > -1 {
		copy(pool.connList[connIndex:], pool.connList[connIndex+1:])

		pool.connList[len(pool.connList)-1] = nil
		pool.connList = pool.connList[:len(pool.connList)-1]
	}

	if e := conn.close(); e != nil {
		return e
	}

	return nil
}

func (pool *Pool) getConnBusyCount() int {
	return int(atomic.LoadInt32(&pool.connBusyCount))
}

func (pool *Pool) incrConnBusy() {
	atomic.AddInt32(&pool.connBusyCount, int32(1))
}

func (pool *Pool) decrConnBusy() {
	atomic.AddInt32(&pool.connBusyCount, int32(-1))
}
