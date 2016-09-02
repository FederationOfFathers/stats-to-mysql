package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/apokalyptik/cfg"
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
	db               *sql.DB
	userAdd          *sql.Stmt
	userGet          *sql.Stmt
	stats            *sql.Stmt
	latest           *sql.Stmt
	setDaily         *sql.Stmt
	incrDaily        *sql.Stmt
	setHourly        *sql.Stmt
	incrHourly       *sql.Stmt
	lastestUserStats *sql.Stmt
)

func init() {
	dbc := cfg.New("db")
	dbc.StringVar(&databaseUser, "user", databaseUser, "MySQL Username (env: DB_USER)")
	dbc.StringVar(&databasePassword, "pass", databasePassword, "MySQL Password (env: DB_PASS)")
	dbc.StringVar(&databaseHost, "host", databaseHost, "MySQL TCP Hostname (env: DB_HOST)")
	dbc.StringVar(&databasePort, "port", databasePort, "MySQL TCP Port (env: DB_PORT)")
	dbc.StringVar(&databaseName, "name", databaseName, "MySQL Database Name (env: DB_NAME)")
}

func initMySQL() {
	db = mustConnect()

	lastestUserStats = mustPrepare(
		"members latest daily stats query",
		"  SELECT"+
			"    h.member_id,"+
			"    h.value,"+
			"    s.*,"+
			"    `when`"+
			"  FROM stats_daily h"+
			"  INNER JOIN stats_latest l"+
			"  INNER JOIN stats s"+
			"  ON(h.member_id=l.member_id and h.stat_id=l.stat_id and daily=`when` AND h.stat_id = s.ID)"+
			"  WHERE l.member_id = ?")

	userAdd = mustPrepare(
		"add user stmt",
		"INSERT IGNORE INTO `members` (`slack`) VALUES(?)")

	userGet = mustPrepare(
		"get user stmt",
		"SELECT `ID` FROM `members` WHERE `slack` = ? LIMIT 1")

	stats = mustPrepare(
		"stats insert stmt",
		"INSERT IGNORE INTO `stats` (platform,product,stat,sub1,sub2,sub3,info) VALUES(?,?,?,?,?,?,?)")

	latest = mustPrepare(
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

	setDaily = mustPrepare(
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

	incrDaily = mustPrepare(
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

	setHourly = mustPrepare(
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

	incrHourly = mustPrepare(
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
