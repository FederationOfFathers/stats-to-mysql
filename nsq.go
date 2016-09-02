package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/apokalyptik/cfg"
	nsq "github.com/nsqio/go-nsq"
)

var (
	nsqTopic   = "fof-stats"
	nsqChannel = "stats-to-mysql"
	nsqAddress = "127.0.0.1:4150"
)

type statMessage struct {
	Platform string    `json:"platform"`
	Member   string    `json:"member"`
	Product  string    `json:"product"`
	Stat     string    `json:"stat"`
	Sub1     string    `json:"sub1"`
	Sub2     string    `json:"sub2"`
	Sub3     string    `json:"sub3"`
	Info     string    `json:"info"`
	When     time.Time `json:"When"`
	Value    int       `json:"value"`
	Method   string    `json:"method"`
}

func init() {
	nsqc := cfg.New("nsq")
	nsqc.StringVar(&nsqTopic, "topic", nsqTopic, "NSQD Topic (env: NSQ_TOPIC)")
	nsqc.StringVar(&nsqChannel, "chan", nsqChannel, "NSQD Channel (env: NSQ_CHAN)")
	nsqc.StringVar(&nsqAddress, "addr", nsqAddress, "NSQD Address (env: NSQ_ADDR)")
}

func initNSQ() {
	consumer, err := nsq.NewConsumer(nsqTopic, nsqChannel, nsq.NewConfig())
	if err != nil {
		log.Fatal(err)
	}
	consumer.AddConcurrentHandlers(nsq.HandlerFunc(statHandler), 50)
	if err := consumer.ConnectToNSQD(nsqAddress); err != nil {
		log.Fatal(err)
	}
}

func statHandler(m *nsq.Message) error {
	var stat statMessage
	if err := json.Unmarshal(m.Body, &stat); err != nil {
		log.Printf("Error unmarshalling stat message: %s -- %s", err.Error(), string(m.Body))
	}
	memberID := userToID.find(stat.Member)
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Error beginning transaction: %s", err.Error())
	}
	_, err = tx.Stmt(stats).Exec(stat.Platform, stat.Product, stat.Stat, stat.Sub1, stat.Sub2, stat.Sub3, stat.Info)
	switch stat.Method {
	case "", "inc":
		if err == nil {
			_, err = tx.Stmt(incrDaily).Exec(
				stat.When.Format(dateTimeFormat),
				memberID,
				stat.Value,
				stat.Platform,
				stat.Product,
				stat.Stat,
				stat.Sub1,
				stat.Sub2,
				stat.Sub3,
				stat.Value)
		}
		if err == nil {
			_, err = tx.Stmt(incrHourly).Exec(
				stat.When.Format(dateTimeFormat),
				memberID,
				stat.Value,
				stat.Platform,
				stat.Product,
				stat.Stat,
				stat.Sub1,
				stat.Sub2,
				stat.Sub3,
				stat.Value)
		}
		break
	case "set":
		if err == nil {
			_, err = tx.Stmt(setDaily).Exec(
				stat.When.Format(dateTimeFormat),
				memberID,
				stat.Value,
				stat.Platform,
				stat.Product,
				stat.Stat,
				stat.Sub1,
				stat.Sub2,
				stat.Sub3,
				stat.Value)
		}
		if err == nil {
			_, err = tx.Stmt(setHourly).Exec(
				stat.When.Format(dateTimeFormat),
				memberID,
				stat.Value,
				stat.Platform,
				stat.Product,
				stat.Stat,
				stat.Sub1,
				stat.Sub2,
				stat.Sub3,
				stat.Value)
		}
		break
	}
	if err == nil {
		_, err = tx.Stmt(latest).Exec(
			memberID,
			stat.When.Format(dateTimeFormat),
			stat.When.Format(dateTimeFormat),
			stat.Platform,
			stat.Product,
			stat.Stat,
			stat.Sub1,
			stat.Sub2,
			stat.Sub3)
	}
	if err != nil {
		log.Printf("Error Inserting or updating stats: %s", err.Error())
		tx.Rollback()
		return nil
	}
	tx.Commit()
	log.Println(stat)
	return nil
}
