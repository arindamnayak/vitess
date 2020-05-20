package vstream1

import (
	"os"
	"testing"

	"vitess.io/vitess/go/vt/dbconfigs"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/binlog"
	_ "vitess.io/vitess/go/vt/proto/binlogdata"
)

var (
	binLogPosPrefixSlave = "c52353fe-9ac4-11ea-9857-40234316aeb5"
	// use the last GTID to which recovery data is there
	// and after that we started applying the binlogs

	tmToRecoverSlave uint32 = 1589998179
)

func TestSlaveReplication(t *testing.T) {

	startPos, err := mysql.DecodePosition("MySQL56/" + binLogPosPrefixSlave + startBinlogPos)
	require.NoError(t, err)
	cp := &mysql.ConnParams{
		Host:  "127.0.0.1",
		Port:  11000,
		Uname: "ripple",
	}
	connStr := dbconfigs.New(cp)
	conn, err := binlog.NewSlaveConnection(connStr)
	events, err := conn.StartBinlogDumpFromPosition(ctx, startPos)
	for {
		//var event mysql.BinlogEvent
		//var ok bool

		select {
		case event, ok := <-events:
			if !ok {
				// events channel has been closed, which means the connection died.
				println("Error with binlogs")
			} else {
				if event.Timestamp() > tmToRecoverSlave && event.IsGTID() {
					println("Reached end of parsing")
					binlogFormat, _ := event.Format()
					gtid, _, _ := event.GTID(binlogFormat)
					println("Gtid : " + gtid.String())
					os.Exit(0)
				}
			}
		}
	}
}
