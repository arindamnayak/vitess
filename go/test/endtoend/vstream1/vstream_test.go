package vstream1

import (
	"context"
	"os"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/dbconfigs"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sync2"

	"vitess.io/vitess/go/mysql"
	_ "vitess.io/vitess/go/vt/proto/binlogdata"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	planbuilder "vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer"
)

var PacketSize int
var HeartbeatTime = 900 * time.Millisecond
var vschemaUpdateCount sync2.AtomicInt64

var (
	ctx    = context.Background()
	cancel func()

	cp             *mysql.ConnParams
	se             *schema.Engine
	startPos       string
	filter         *binlogdatapb.Filter
	send           func([]*binlogdatapb.VEvent) error
	plans          map[uint64]*streamerPlan
	journalTableID uint64

	// format and pos are updated by parseEvent.
	format mysql.BinlogFormat
	pos    mysql.Position

	binLogPosPrefix = "100973f1-99f5-11ea-b72c-40234316aeb5"
	// use the last GTID to which recovery data is there
	// and after that we started applying the binlogs
	startBinlogPos = ":1-8"

	tmToRecover int64 = 1589908892
)

type streamerPlan struct {
	*planbuilder.Plan
	TableMap *mysql.TableMap
}

func TestVstreamReplication(t *testing.T) {

	pos, err := mysql.DecodePosition("MySQL56/" + binLogPosPrefix + startBinlogPos)

	require.NoError(t, err)
	dbCfgs := &dbconfigs.DBConfigs{
		Host: "127.0.0.1",
		Port: 11000,
		Dba: dbconfigs.UserConfig{
			User: "ripple",
		},
	}
	dbCfgs.SetDbParams(mysql.ConnParams{
		Host:  "127.0.0.1",
		Port:  11000,
		Uname: "ripple",
	})
	extConnector := vreplication.NewMysqlConnector(map[string]*dbconfigs.DBConfigs{"test": dbCfgs})
	//
	vsClient, err := extConnector.Get("test")
	err = vsClient.Open(ctx)
	require.NoError(t, err)
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
