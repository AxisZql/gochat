package config

/**
 * Created by lock
 * Date: 2019-08-09
 * Time: 15:24
 */

import (
	"github.com/spf13/viper"
	"os"
	"runtime"
	"strings"
	"sync"
)

var (
	//realPath string
	once sync.Once
	Conf *Config
)

const (
	SuccessReplyCode      = 0
	FailReplyCode         = 1
	SuccessReplyMsg       = "success"
	QueueName             = "gochat_queue"
	RedisBaseValidTime    = 86400 // 设置redis对应key的基本有效时间
	RedisPrefix           = "gochat_"
	RedisRoomPrefix       = "gochat_room_"
	RedisRoomOnlinePrefix = "gochat_room_online_count_"
	MsgVersion            = 1
	OpSingleSend          = 2 // single user
	OpRoomSend            = 3 // send to room
	OpRoomCountSend       = 4 // get online user count
	OpRoomInfoSend        = 5 // send info to room
	OpBuildTcpConn        = 6 // build tcp conn
)

type Config struct {
	Common  Common
	Connect ConnectConfig
	Logic   LogicConfig
	Task    TaskConfig
	Api     ApiConfig
	Site    SiteConfig
}

func init() {
	Init()
}

func getCurrentDir() string {
	_, fileName, _, _ := runtime.Caller(1)
	aPath := strings.Split(fileName, "/")
	dir := strings.Join(aPath[0:len(aPath)-1], "/")
	return dir
}

func Init() {
	once.Do(func() {
		env := GetMode()
		//realPath, _ := filepath.Abs("./")
		realPath := getCurrentDir()
		configFilePath := realPath + "/" + env + "/"
		viper.SetConfigType("toml")
		viper.SetConfigName("/connect")
		viper.AddConfigPath(configFilePath)
		err := viper.ReadInConfig()
		if err != nil {
			panic(err)
		}
		viper.SetConfigName("/common")
		err = viper.MergeInConfig()
		if err != nil {
			panic(err)
		}
		viper.SetConfigName("/task")
		err = viper.MergeInConfig()
		if err != nil {
			panic(err)
		}
		viper.SetConfigName("/logic")
		err = viper.MergeInConfig()
		if err != nil {
			panic(err)
		}
		viper.SetConfigName("/api")
		err = viper.MergeInConfig()
		if err != nil {
			panic(err)
		}
		viper.SetConfigName("/site")
		err = viper.MergeInConfig()
		if err != nil {
			panic(err)
		}
		Conf = new(Config)
		_ = viper.Unmarshal(&Conf.Common)
		_ = viper.Unmarshal(&Conf.Connect)
		_ = viper.Unmarshal(&Conf.Task)
		_ = viper.Unmarshal(&Conf.Logic)
		_ = viper.Unmarshal(&Conf.Api)
		_ = viper.Unmarshal(&Conf.Site)
	})
}

func GetMode() string {
	// 在Dockerfile里面设置环境变量RUN_MODE默认为dev
	env := os.Getenv("RUN_MODE")
	if env == "" {
		env = "dev"
	}
	return env
}

func GetGinRunMode() string {
	env := GetMode()
	//gin have debug,test,release mode
	if env == "dev" {
		return "debug"
	}
	if env == "test" {
		return "debug"
	}
	if env == "prod" {
		return "release"
	}
	return "release"
}

type CommonEtcd struct {
	Host              string `mapstructure:"host"`
	BasePath          string `mapstructure:"basePath"`
	ServerPathLogic   string `mapstructure:"serverPathLogic"`
	ServerPathConnect string `mapstructure:"serverPathConnect"`
	UserName          string `mapstructure:"userName"`
	Password          string `mapstructure:"password"`
	ConnectionTimeout int    `mapstructure:"connectionTimeout"`
}

type CommonRedis struct {
	RedisAddress  string `mapstructure:"redisAddress"`
	RedisPassword string `mapstructure:"redisPassword"`
	Db            int    `mapstructure:"db"`
}

type Common struct {
	CommonEtcd  CommonEtcd  `mapstructure:"common-etcd"`
	CommonRedis CommonRedis `mapstructure:"common-redis"`
}

type ConnectBase struct {
	CertPath string `mapstructure:"certPath"`
	KeyPath  string `mapstructure:"keyPath"`
}

type ConnectRpcAddressWebsockets struct {
	Address string `mapstructure:"address"`
}

type ConnectRpcAddressTcp struct {
	Address string `mapstructure:"address"`
}

type ConnectBucket struct {
	CpuNum        int    `mapstructure:"cpuNum"`
	Channel       int    `mapstructure:"channel"`
	Room          int    `mapstructure:"room"`
	SrvProto      int    `mapstructure:"svrProto"`
	RoutineAmount uint64 `mapstructure:"routineAmount"`
	RoutineSize   int    `mapstructure:"routineSize"`
}

type ConnectWebsocket struct {
	ServerId string `mapstructure:"serverId"`
	Bind     string `mapstructure:"bind"`
}

type ConnectTcp struct {
	ServerId      string `mapstructure:"serverId"`
	Bind          string `mapstructure:"bind"`
	SendBuf       int    `mapstructure:"sendbuf"`
	ReceiveBuf    int    `mapstructure:"receivebuf"`
	KeepAlive     bool   `mapstructure:"keepalive"`
	Reader        int    `mapstructure:"reader"`
	ReadBuf       int    `mapstructure:"readBuf"`
	ReadBufSize   int    `mapstructure:"readBufSize"`
	Writer        int    `mapstructure:"writer"`
	WriterBuf     int    `mapstructure:"writerBuf"`
	WriterBufSize int    `mapstructure:"writeBufSize"`
}

type ConnectConfig struct {
	ConnectBase                 ConnectBase                 `mapstructure:"connect-base"`
	ConnectRpcAddressWebSockets ConnectRpcAddressWebsockets `mapstructure:"connect-rpcAddress-websockets"`
	ConnectRpcAddressTcp        ConnectRpcAddressTcp        `mapstructure:"connect-rpcAddress-tcp"`
	ConnectBucket               ConnectBucket               `mapstructure:"connect-bucket"`
	ConnectWebsocket            ConnectWebsocket            `mapstructure:"connect-websocket"`
	ConnectTcp                  ConnectTcp                  `mapstructure:"connect-tcp"`
}

type LogicBase struct {
	ServerId   string `mapstructure:"serverId"`
	CpuNum     int    `mapstructure:"cpuNum"`
	RpcAddress string `mapstructure:"rpcAddress"`
	CertPath   string `mapstructure:"certPath"`
	KeyPath    string `mapstructure:"keyPath"`
}

type LogicConfig struct {
	LogicBase LogicBase `mapstructure:"logic-base"`
}

type TaskBase struct {
	CpuNum        int    `mapstructure:"cpuNum"`
	RedisAddr     string `mapstructure:"redisAddr"`
	RedisPassword string `mapstructure:"redisPassword"`
	RpcAddress    string `mapstructure:"rpcAddress"`
	PushChan      int    `mapstructure:"pushChan"`
	PushChanSize  int    `mapstructure:"pushChanSize"`
}

type TaskConfig struct {
	TaskBase TaskBase `mapstructure:"task-base"`
}

type ApiBase struct {
	ListenPort int `mapstructure:"listenPort"`
}

type ApiConfig struct {
	ApiBase ApiBase `mapstructure:"api-base"`
}

type SiteBase struct {
	ListenPort int `mapstructure:"listenPort"`
}

type SiteConfig struct {
	SiteBase SiteBase `mapstructure:"site-base"`
}
