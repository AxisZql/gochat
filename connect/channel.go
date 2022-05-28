package connect

/**
 * Created by lock
 * Date: 2019-08-09
 * Time: 15:18
 */

import (
	"github.com/gorilla/websocket"
	"gochat/proto"
	"net"
)

// Channel in fact, Channel it's a user Connect session
type Channel struct {
	Room      *Room
	Next      *Channel // 双向链表保存用户会话实例
	Prev      *Channel
	broadcast chan *proto.Msg // TODO:这里设置为chan，应该是为了实时触发消息广播的操作
	userId    int
	conn      *websocket.Conn
	connTcp   *net.TCPConn
}

func NewChannel(size int) (c *Channel) {
	c = new(Channel)
	c.broadcast = make(chan *proto.Msg, size)
	c.Next = nil
	c.Prev = nil
	return
}

func (ch *Channel) Push(msg *proto.Msg) (err error) {
	// 往广播通道里写入消息
	select {
	// TODO:如果消息体为nil，会触发panic吗？err的值是怎么初始化的？
	case ch.broadcast <- msg:
	default:
	}
	return
}
