package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/apokalyptik/cfg"
	"github.com/nsqio/go-nsq"

	_ "github.com/go-sql-driver/mysql"
)

const (
	dateTimeFormat = "2006-01-02 15:04:05"
)

var (
	databaseUser     = "my_db_user"
	databasePassword = "my_db_pass"
	databaseHost     = "the_db_host"
	databasePort     = "3306"
	databaseName     = "my_db_name"
	nsqTopic         = "fof-stats"
	nsqChannel       = "stats-to-mysql"
	nsqAddress       = "127.0.0.1:4150"
	db               *sql.DB
	userToID         = userLookup{}
	userAdd          *sql.Stmt
	userGet          *sql.Stmt
)

type userLookup map[string]string

func (u userLookup) find(userID string) string {
	memberID, ok := u[userID]
	if !ok {
		_, err := userAdd.Exec(userID)
		if err != nil {
			log.Println("Error adding user", userID)
		}
		rows, err := userGet.Query(userID)
		if err != nil {
			log.Fatal("Error looking up user", userID)
		} else {
			for rows.Next() {
				err := rows.Scan(&memberID)
				if err != nil {
					log.Println("Error scanning user lookup result into memberID", err)
				} else {
					userToID[userID] = memberID
					log.Println("found ID for", userID, "=", memberID)
				}
			}
		}
		rows.Close()
	}
	return memberID
}

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
	dbc := cfg.New("db")
	dbc.StringVar(&databaseUser, "user", databaseUser, "MySQL Username (env: DB_USER)")
	dbc.StringVar(&databasePassword, "pass", databasePassword, "MySQL Password (env: DB_PASS)")
	dbc.StringVar(&databaseHost, "host", databaseHost, "MySQL TCP Hostname (env: DB_HOST)")
	dbc.StringVar(&databasePort, "port", databasePort, "MySQL TCP Port (env: DB_PORT)")
	dbc.StringVar(&databaseName, "name", databaseName, "MySQL Database Name (env: DB_NAME)")

	nsqc := cfg.New("nsq")
	nsqc.StringVar(&nsqTopic, "topic", nsqTopic, "NSQD Topic (env: NSQ_TOPIC)")
	nsqc.StringVar(&nsqChannel, "chan", nsqChannel, "NSQD Channel (env: NSQ_CHAN)")
	nsqc.StringVar(&nsqAddress, "addr", nsqAddress, "NSQD Address (env: NSQ_ADDR)")

	cfg.Parse()
}

func mustPrepare(name string, query string) *sql.Stmt {
	s, err := db.Prepare(query)
	if err != nil {
		log.Fatalf("Error creating %s: %s", name, err.Error())
	}
	return s
}

func mustConnect() *sql.DB {
	db, err := sql.Open(
		"mysql",
		fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", databaseUser, databasePassword, databaseHost, databasePort, databaseName))
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func cleanupHourly() {
	t := time.Tick(time.Minute)
	for {
		select {
		case <-t:
			res, err := db.Exec("DELETE FROM `stats_hourly` WHERE `when` < DATE_SUB(NOW(), INTERVAL 720 HOUR) LIMIT 250")
			if err != nil {
				log.Fatal("Error cleaning up stats_hourly")
			}
			n, _ := res.RowsAffected()
			log.Println("cleaning records from `stats_hourly`", n)
		}
	}
}

func main() {
	db = mustConnect()
	defer db.Close()
	userAdd = mustPrepare(
		"add user stmt",
		"INSERT IGNORE INTO `members` (`slack`) VALUES(?)")
	userGet = mustPrepare(
		"get user stmt",
		"SELECT `ID` FROM `members` WHERE `slack` = ? LIMIT 1")
	stats := mustPrepare(
		"stats insert stmt",
		"INSERT IGNORE INTO `stats` (platform,product,stat,sub1,sub2,sub3,info) VALUES(?,?,?,?,?,?,?)")
	latest := mustPrepare(
		"stats_latest update statement",
		"INSERT INTO `stats_latest` (`member_id`,`stat_id`,`daily`,`hourly`)"+
			"  SELECT ?,ID,DATE_FORMAT(?,'%Y-%m-%d %H:00:00'),DATE_FORMAT(?,'%Y-%m-%d %H:00:00')"+
			"    FROM `stats`"+
			"    WHERE platform=?"+
			"      AND product=?"+
			"      AND stat=?"+
			"      AND sub1=?"+
			"      AND sub2=?"+
			"      AND sub3=?"+
			"	  ON DUPLICATE KEY UPDATE `daily`=VALUES(`daily`),`hourly`=VALUES(`hourly`)")
	setDaily := mustPrepare(
		"daily insert stmt",
		"INSERT INTO `stats_daily` (`when`,`stat_id`,`member_id`,`value`)"+
			"  SELECT ?,ID,?,?"+
			"    FROM `stats`"+
			"    WHERE platform=?"+
			"      AND product=?"+
			"      AND stat=?"+
			"      AND sub1=?"+
			"      AND sub2=?"+
			"      AND sub3=?"+
			"  ON DUPLICATE KEY UPDATE `value`=?")
	incrDaily := mustPrepare(
		"daily insert stmt",
		"INSERT INTO `stats_daily` (`when`,`stat_id`,`member_id`,`value`)"+
			"  SELECT ?,ID,?,?"+
			"    FROM `stats`"+
			"    WHERE platform=?"+
			"      AND product=?"+
			"      AND stat=?"+
			"      AND sub1=?"+
			"      AND sub2=?"+
			"      AND sub3=?"+
			"  ON DUPLICATE KEY UPDATE `value`=`value`+?")
	setHourly := mustPrepare(
		"hourly insert stmt",
		"INSERT INTO `stats_hourly` (`when`,`stat_id`,`member_id`,`value`)"+
			"  SELECT DATE_FORMAT(?,'%Y-%m-%d %H:00:00'),ID,?,?"+
			"    FROM `stats`"+
			"    WHERE platform=?"+
			"      AND product=?"+
			"      AND stat=?"+
			"      AND sub1=?"+
			"      AND sub2=?"+
			"      AND sub3=?"+
			"  ON DUPLICATE KEY UPDATE `value`=?")
	incrHourly := mustPrepare(
		"hourly insert stmt",
		"INSERT INTO `stats_hourly` (`when`,`stat_id`,`member_id`,`value`)"+
			"  SELECT DATE_FORMAT(?,'%Y-%m-%d %H:00:00'),ID,?,?"+
			"    FROM `stats`"+
			"    WHERE platform=?"+
			"      AND product=?"+
			"      AND stat=?"+
			"      AND sub1=?"+
			"      AND sub2=?"+
			"      AND sub3=?"+
			"  ON DUPLICATE KEY UPDATE `value`=`value`+?")

	go cleanupHourly()
	consumer, err := nsq.NewConsumer(nsqTopic, nsqChannel, nsq.NewConfig())
	if err != nil {
		log.Fatal(err)
	}

	consumer.AddConcurrentHandlers(nsq.HandlerFunc(func(m *nsq.Message) error {
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
	}),
		10)
	if err := consumer.ConnectToNSQD(nsqAddress); err != nil {
		log.Fatal(err)
	}
	mindHTTP()
}
