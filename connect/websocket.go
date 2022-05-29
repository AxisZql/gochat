package connect

/**
 * Created by lock
 * Date: 2019-08-09
 * Time: 15:19
 */

import (
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
	"gochat/config"
	"net/http"
)

func (c *Connect) InitWebsocket() error {
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		// TODO:通过调用顺序保证DefaultServer此时已经被被初始化 ask question：这样做是否妥当？
		c.serveWs(DefaultServer, w, r)
	})
	// 项目配置把websocket服务的RESTful API暴露到700端口中
	err := http.ListenAndServe(config.Conf.Connect.ConnectWebsocket.Bind, nil)
	return err
}

func (c *Connect) serveWs(server *Server, w http.ResponseWriter, r *http.Request) {
	// 进行协议升级，将http协议升级为websocket协议
	var upGrader = websocket.Upgrader{
		ReadBufferSize:  server.Options.ReadBufferSize,  // 项目配置的读缓冲区大小为1024
		WriteBufferSize: server.Options.WriteBufferSize, // 项目配置的写缓冲区大小为1024
	}
	//cross origin domain support，配置跨域支持
	upGrader.CheckOrigin = func(r *http.Request) bool { return true }

	conn, err := upGrader.Upgrade(w, r, nil)

	if err != nil {
		logrus.Errorf("serverWs err:%s", err.Error())
		return
	}
	var ch *Channel
	//default broadcast size eq 512
	// 默认广播数据大小限制为512比特
	ch = NewChannel(server.Options.BroadcastSize)
	ch.conn = conn
	//send data to websocket conn
	go server.writePump(ch, c)
	//get data from websocket conn
	go server.readPump(ch, c)
}
