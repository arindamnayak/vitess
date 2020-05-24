package vstream1

import (
	"fmt"
	"os"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/vt/dbconfigs"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/binlog"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

var (
	binLogPosPrefixSlave = "fd231330-9b8d-11ea-8d19-40234316aeb5"
	// use the last GTID to which recovery data is there
	// and after that we started applying the binlogs

	tmToRecoverSlave uint32 = 1590084523
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
				print("timestamp :")
				println(event.Timestamp())

				if event.Timestamp() > tmToRecoverSlave && event.IsGTID() {
					vevents, err := parseEvent(event)
					if err != nil {
						continue
					}
					fmt.Printf("%+v", vevents)
					println("Reached end of parsing")
					os.Exit(0)
				}
			}
		}
	}
}
func parseEvent(ev mysql.BinlogEvent) ([]*binlogdatapb.VEvent, error) {
	if !ev.IsValid() {
		return nil, fmt.Errorf("can't parse binlog event: invalid data: %#v", ev)
	}

	// We need to keep checking for FORMAT_DESCRIPTION_EVENT even after we've
	// seen one, because another one might come along (e.g. on log rotate due to
	// binlog settings change) that changes the format.
	if ev.IsFormatDescription() {
		var err error
		format, err = ev.Format()
		if err != nil {
			return nil, fmt.Errorf("can't parse FORMAT_DESCRIPTION_EVENT: %v, event data: %#v", err, ev)
		}
		return nil, nil
	}

	// We can't parse anything until we get a FORMAT_DESCRIPTION_EVENT that
	// tells us the size of the event header.
	if format.IsZero() {
		// The only thing that should come before the FORMAT_DESCRIPTION_EVENT
		// is a fake ROTATE_EVENT, which the master sends to tell us the name
		// of the current log file.
		if ev.IsRotate() {
			return nil, nil
		}
		return nil, fmt.Errorf("got a real event before FORMAT_DESCRIPTION_EVENT: %#v", ev)
	}

	// Strip the checksum, if any. We don't actually verify the checksum, so discard it.
	ev, _, err := ev.StripChecksum(format)
	if err != nil {
		return nil, fmt.Errorf("can't strip checksum from binlog event: %v, event data: %#v", err, ev)
	}

	// Get the DbName for vstreamer

	var vevents []*binlogdatapb.VEvent
	switch {
	case ev.IsGTID():
		gtid, hasBegin, err := ev.GTID(format)
		if err != nil {
			return nil, fmt.Errorf("can't get GTID from binlog event: %v, event data: %#v", err, ev)
		}
		if hasBegin {
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_BEGIN,
			})
		}
		pos = mysql.AppendGTID(pos, gtid)
	case ev.IsXID():
		vevents = append(vevents, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_GTID,
			Gtid: mysql.EncodePosition(pos),
		}, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_COMMIT,
		})
	case ev.IsQuery():
		q, err := ev.Query(format)
		if err != nil {
			return nil, fmt.Errorf("can't get query from binlog event: %v, event data: %#v", err, ev)
		}
		// Insert/Delete/Update are supported only to be used in the context of external mysql streams where source databases
		// could be using SBR. Vitess itself will never run into cases where it needs to consume non rbr statements.
		switch cat := sqlparser.Preview(q.SQL); cat {
		case sqlparser.StmtInsert:

			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_INSERT,
				Dml:  q.SQL,
			})

		case sqlparser.StmtUpdate:

			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_UPDATE,
				Dml:  q.SQL,
			})

		case sqlparser.StmtDelete:

			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_DELETE,
				Dml:  q.SQL,
			})

		case sqlparser.StmtReplace:

			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_REPLACE,
				Dml:  q.SQL,
			})

		case sqlparser.StmtBegin:
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_BEGIN,
			})
		case sqlparser.StmtCommit:
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_COMMIT,
			})
		case sqlparser.StmtDDL:

			// If the DDL need not be sent, send a dummy OTHER event.
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_GTID,
				Gtid: mysql.EncodePosition(pos),
			}, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_OTHER,
			})

		case sqlparser.StmtOther, sqlparser.StmtPriv:
			// These are either:
			// 1) DBA statements like REPAIR that can be ignored.
			// 2) Privilege-altering statements like GRANT/REVOKE
			//    that we want to keep out of the stream for now.
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_GTID,
				Gtid: mysql.EncodePosition(pos),
			}, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_OTHER,
			})
		default:
			return nil, fmt.Errorf("unexpected statement type %s in row-based replication: %q", cat, q.SQL)
		}
	}
	for _, vevent := range vevents {
		vevent.Timestamp = int64(ev.Timestamp())
		vevent.CurrentTime = time.Now().UnixNano()
	}
	return vevents, nil
}
