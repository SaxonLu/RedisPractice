package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/viper"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// --------------

var (
	serverName = "chatroomCreate-script"
	db         *sqlx.DB
	redisAddr  = "127.0.0.1:7000"
	redisPwd   = ""
	redisKey   = "chatRoom"
)

func main() {
	fmt.Println("script init ...")
	initConfig()
	Init(viper.GetString("mysql.url") + viper.GetString("mysql.db_account") + viper.GetString("mysql.url_suffix"))
	RedisInit(redisAddr, redisPwd)
	err := initMgoConn(viper.GetString("mongo.url"), viper.GetString("mongo.db_msg"))
	if err != nil {
		log.Fatal("Run", err, "mongoDB conn fail")
		return
	}

	fmt.Println("On")

	testDataIN()

	for i := 1; i <= 10; i++ {
		go RedisPOP(i)
	}

	time.Sleep(1000000 * time.Second)

}

func testDataIN() {

	fmt.Println("測試塞資料")

	ra := RedisData{
		ChatroomID: "er1",
		UserID:     "zz1",
	}
	rb := RedisData{ChatroomID: "0008916500038b61",
		UserID: "00089165",
	}
	rc := RedisData{ChatroomID: "53b37978548e1649",
		UserID: "53b37978",
	}

	bra, _ := json.Marshal(ra)
	brb, _ := json.Marshal(rb)
	brc, _ := json.Marshal(rc)

	err := client.LPush(redisKey, bra).Err()
	if err != nil {
		fmt.Println(err)
	}
	err = client.LPush(redisKey, brb).Err()
	if err != nil {
		fmt.Println(err)
	}
	err = client.LPush(redisKey, brc).Err()
	if err != nil {
		fmt.Println(err)
	}
}

func RedisPOP(i int) {

	for {
		fmt.Println("Task :", i)
		data, err := client.RPop(redisKey).Result()
		if err != nil {
			log.Println("RPop ----Redis取出失敗")
			time.Sleep(1 * time.Second)
			continue
		}

		fmt.Println("Task :", i, data)
		warnUser, err := chatroomchick(data)
		if err != nil {
			fmt.Println(err)
		}
		if warnUser != "" {
			// 把統計完 有問題的項目 塞回去
			err = client.LPush(redisKey, warnUser).Err()
			if err != nil {
				fmt.Println(err)
			}
		}
		time.Sleep(1 * time.Second)
		// fmt.Println("目前第 ", rnd, " 次")
	}
}

func chatroomchick(data string) (string, error) {

	rData := RedisData{}
	err := json.Unmarshal([]byte(data), &rData)
	if err != nil {
		log.Println("Unmarshal ----Byte To Struct 轉型失敗")
		return data, err
	}

	if len(rData.ChatroomID) != 16 {
		log.Println("Param ----ChatroomID 參數錯誤")
		return data, err
	}
	if len(rData.UserID) != 8 {
		log.Println("Param ----UserID 參數錯誤")
		return data, err
	}

	ok, err := checkRooms(rData.ChatroomID, rData.UserID)
	if err != nil {
		log.Println("checkRooms ----MongoDB Error")
		return data, err
	}
	if !ok {
		_, d, err := checkFriendship(rData.ChatroomID, rData.UserID)
		if err != nil {
			return data, err
		}
		err = createChatroom(d)
		if err != nil {
			log.Println("createChatroom ----MongoDB Error")
			return data, err
		}
	}
	return "", nil
}

//----------------------

const _cRoom = "rooms"
const _friendType = "f"
const _isFriend = 3

type RedisData struct {
	UserID     string `json:"uid"`
	ChatroomID string `json:"cid"`
}

type Room struct {
	ChatroomID string    `bson:"_id" mapstructure:"_id"`
	Type       string    `bson:"t" mapstructure:"t"`
	Msgs       int64     `bson:"msgs" mapstructure:"msgs"`
	CreatedAt  time.Time `bson:"ts" mapstructure:"ts"`
	UT         time.Time `bson:"ut" mapstructure:"ut"`
}

type Friendship struct {
	UID       string    `db:"uid"`
	FID       string    `db:"fid"`
	Relation  int       `db:"relation"`
	UpdatedAt time.Time `db:"updated_at"`
}

func checkRooms(cid, uid string) (ok bool, err error) {

	d := []Room{}
	conn := _session.Copy()
	defer conn.Close()
	query := bson.M{"_id": cid}

	c := conn.DB(_mgMSGdb).C(_cRoom)
	err = c.Find(query).All(&d)

	fmt.Println(d)

	if len(d) > 0 {
		ok = true
		return
	}

	return
}

func checkFriendship(cid, uid string) (ok bool, data Room, err error) {

	if len(cid) != 16 {
		return
	}
	var fid string
	if cid[:8] == uid {
		fid = cid[8:]
	}
	if cid[8:] == uid {
		fid = cid[:8]
	}

	query := "SELECT uid,fid,relation,updated_at  FROM account_rep.friendship WHERE uid = ? and fid =?;"
	fs := Friendship{}
	err = db.Get(&fs, query, uid, fid)
	if err != nil {
		log.Println("db.Get ----MYSQL Error")
		return
	}
	if fs.Relation != _isFriend {
		return
	}

	ok = true

	data = Room{
		ChatroomID: cid,
		Type:       _friendType,
		Msgs:       0,
		CreatedAt:  fs.UpdatedAt,
		UT:         fs.UpdatedAt,
	}

	return
}

func createChatroom(in Room) (err error) {
	conn := _session.Copy()
	defer conn.Close()

	c := conn.DB(_mgMSGdb).C(_cRoom)

	err = c.Insert(&in)
	if err != nil {
		return
	}
	log.Println("Chatroom 建立完成 ChatroomID為:")

	return
}

// ----------------------------------------------------------------
// init
func initConfig() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal("config init error", err)
	}
}

// mongo base
var _session *mgo.Session
var _mgMSGdb string

func initMgoConn(mongoURL, db string) (err error) {
	conn, err := mgo.Dial(mongoURL)
	if err != nil {
		return err
	}
	conn.DB(db)
	conn.SetMode(mgo.Monotonic, true)
	conn.SetPoolLimit(100)

	_mgMSGdb = db
	_session = conn
	return nil
}

func Init(url string) error {
	var err error
	db, err = sqlx.Connect("mysql", url)
	if err != nil {
		return err
	}

	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(50)
	db.SetConnMaxLifetime(1 * time.Hour)
	err = db.Ping()
	if err != nil {
		return err
	}

	return nil
}

var client *redis.Client

func RedisInit(addrs string, pwd string) error {
	client = redis.NewClient(&redis.Options{
		Addr:     addrs,
		Password: pwd, // no password set
		DB:       0,   // use default DB
	})

	_, err := client.Ping().Result()
	if err != nil {
		return err
	}
	fmt.Println("---redis ok---")
	return nil
}

// ----------------------------------------------------------------
