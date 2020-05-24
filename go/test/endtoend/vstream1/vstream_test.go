package vstream1

import (
	"context"
	"os"
	"testing"

	"vitess.io/vitess/go/vt/dbconfigs"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/mysql"
	_ "vitess.io/vitess/go/vt/proto/binlogdata"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

var (
	ctx = context.Background()
	// format and pos are updated by parseEvent.
	format mysql.BinlogFormat
	pos    mysql.Position

	binLogPosPrefix = "59a81ad5-9daf-11ea-87e8-40234316aeb5"
	// use the last GTID to which recovery data is there
	// and after that we started applying the binlogs
	startBinlogPos = ":1-8"

	tmToRecover int64 = 1590319642
)

func TestVstreamReplication(t *testing.T) {

	pos, err := mysql.DecodePosition("MySQL56/" + binLogPosPrefix + startBinlogPos)
	require.NoError(t, err)
	connParams := &mysql.ConnParams{
		Host:  "127.0.0.1",
		Port:  11000,
		Uname: "ripple",
	}

	dbCfgs := &dbconfigs.DBConfigs{
		Host: connParams.Host,
		Port: connParams.Port,
	}
	dbCfgs.SetDbParams(*connParams)
	vsClient := vreplication.NewReplicaConnector(connParams)

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}

	_ = vsClient.VStream(ctx, mysql.EncodePosition(pos), filter, func(events []*binlogdatapb.VEvent) error {
		for _, event := range events {
			if event.Gtid != "" && event.Timestamp > tmToRecover {
				println("Reached end of parsing")
				println(event.Gtid)
				os.Exit(0)
			}
		}
		return nil
	})
}
