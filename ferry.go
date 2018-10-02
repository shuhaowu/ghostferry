package ghostferry

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	siddontanglog "github.com/siddontang/go-log/log"
	siddontangmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

var (
	VersionString string = "?.?.?+??????????????+???????"
	WebUiBasedir  string = ""
)

const (
	StateStarting          = "starting"
	StateCopying           = "copying"
	StateWaitingForCutover = "wait-for-cutover"
	StateCutover           = "cutover"
	StateDone              = "done"
)

func quoteField(field string) string {
	return fmt.Sprintf("`%s`", field)
}

func MaskedDSN(c *mysql.Config) string {
	oldPass := c.Passwd
	c.Passwd = "<masked>"
	defer func() {
		c.Passwd = oldPass
	}()

	return c.FormatDSN()
}

type Ferry struct {
	*Config

	SourceDB *sql.DB
	TargetDB *sql.DB

	BinlogStreamer *BinlogStreamer
	BinlogWriter   *BinlogWriter

	DataIterator *DataIterator
	BatchWriter  *BatchWriter

	StateTracker *StateTracker

	ErrorHandler ErrorHandler
	Throttler    Throttler

	Tables TableSchemaCache

	StartTime    time.Time
	DoneTime     time.Time
	OverallState string

	WaitUntilReplicaIsCaughtUpToMaster *WaitUntilReplicaIsCaughtUpToMaster

	logger *logrus.Entry

	rowCopyCompleteCh chan struct{}
}

func (f *Ferry) NewDataIterator() *DataIterator {
	dataIterator := &DataIterator{
		DB:          f.SourceDB,
		Concurrency: f.Config.DataIterationConcurrency,

		ErrorHandler: f.ErrorHandler,
		CursorConfig: &CursorConfig{
			DB:        f.SourceDB,
			Throttler: f.Throttler,

			BatchSize:   f.Config.DataIterationBatchSize,
			ReadRetries: f.Config.DBReadRetries,
		},
		StateTracker: f.StateTracker,
	}

	if f.CopyFilter != nil {
		dataIterator.CursorConfig.BuildSelect = f.CopyFilter.BuildSelect
	}

	return dataIterator
}

func (f *Ferry) NewDataIteratorWithoutStateTracker() *DataIterator {
	dataIterator := f.NewDataIterator()
	dataIterator.StateTracker = nil
	return dataIterator
}

func (f *Ferry) NewBinlogStreamer() *BinlogStreamer {
	return &BinlogStreamer{
		DB:           f.SourceDB,
		DBConfig:     f.Source,
		MyServerId:   f.Config.MyServerId,
		ErrorHandler: f.ErrorHandler,
		Filter:       f.CopyFilter,
		TableSchema:  f.Tables,
	}
}

// Even tho this function is identical to NewBinlogStreamer, it is here
// for consistency so it will lead to less confusion.
func (f *Ferry) NewBinlogStreamerWithoutStateTracker() *BinlogStreamer {
	return f.NewBinlogStreamer()
}

func (f *Ferry) NewBinlogWriter() *BinlogWriter {
	return &BinlogWriter{
		DB:               f.TargetDB,
		DatabaseRewrites: f.Config.DatabaseRewrites,
		TableRewrites:    f.Config.TableRewrites,
		Throttler:        f.Throttler,

		BatchSize:    f.Config.BinlogEventBatchSize,
		WriteRetries: f.Config.DBWriteRetries,

		ErrorHandler: f.ErrorHandler,
		StateTracker: f.StateTracker,
	}
}

func (f *Ferry) NewBinlogWriterWithoutStateTracker() *BinlogWriter {
	binlogWriter := f.NewBinlogWriter()
	binlogWriter.StateTracker = nil
	return binlogWriter
}

func (f *Ferry) NewBatchWriter() *BatchWriter {
	batchWriter := &BatchWriter{
		DB:           f.TargetDB,
		StateTracker: f.StateTracker,

		DatabaseRewrites: f.Config.DatabaseRewrites,
		TableRewrites:    f.Config.TableRewrites,

		WriteRetries: f.Config.DBWriteRetries,
	}

	batchWriter.Initialize()
	return batchWriter
}

func (f *Ferry) NewBatchWriterWithoutStateTracker() *BatchWriter {
	batchWriter := f.NewBatchWriter()
	batchWriter.StateTracker = nil
	return batchWriter
}

// Initialize all the components of Ghostferry and connect to the Database
func (f *Ferry) Initialize() (err error) {
	f.StartTime = time.Now().Truncate(time.Second)
	f.OverallState = StateStarting

	f.logger = logrus.WithField("tag", "ferry")
	f.rowCopyCompleteCh = make(chan struct{})

	f.logger.Infof("hello world from %s", VersionString)

	// Suppress siddontang/go-mysql logging as we already log the equivalents.
	// It also by defaults logs to stdout, which is different from Ghostferry
	// logging, which all goes to stderr. stdout in Ghostferry is reserved for
	// dumping states due to an abort.
	siddontanglog.SetDefaultLogger(siddontanglog.NewDefault(&siddontanglog.NullHandler{}))

	// Connect to the source and target databases and check the validity
	// of the connections
	f.SourceDB, err = f.Source.SqlDB(f.logger.WithField("dbname", "source"))
	if err != nil {
		f.logger.WithError(err).Error("failed to connect to source database")
		return err
	}

	err = f.checkConnection("source", f.SourceDB)
	if err != nil {
		f.logger.WithError(err).Error("source connection checking failed")
		return err
	}

	err = f.checkConnectionForBinlogFormat(f.SourceDB)
	if err != nil {
		f.logger.WithError(err).Error("binlog format for source db is not compatible")
		return err
	}

	f.TargetDB, err = f.Target.SqlDB(f.logger.WithField("dbname", "target"))
	if err != nil {
		f.logger.WithError(err).Error("failed to connect to target database")
		return err
	}

	err = f.checkConnection("target", f.TargetDB)
	if err != nil {
		f.logger.WithError(err).Error("target connection checking failed")
		return err
	}

	isReplica, err := CheckDbIsAReplica(f.TargetDB)
	if err != nil {
		f.logger.WithError(err).Error("cannot check if target db is writable")
		return err
	}
	if isReplica {
		return fmt.Errorf("@@read_only must be OFF on target db")
	}

	// Check if we're running from a replica or not and sanity check
	// the configurations given to Ghostferry as well as the configurations
	// of the MySQL databases.
	if f.WaitUntilReplicaIsCaughtUpToMaster != nil {
		f.WaitUntilReplicaIsCaughtUpToMaster.ReplicaDB = f.SourceDB

		if f.WaitUntilReplicaIsCaughtUpToMaster.MasterDB == nil {
			err = errors.New("must specify a MasterDB")
			f.logger.WithError(err).Error("must specify a MasterDB")
			return err
		}

		err = f.checkConnection("source_master", f.WaitUntilReplicaIsCaughtUpToMaster.MasterDB)
		if err != nil {
			f.logger.WithError(err).Error("source master connection checking failed")
			return err
		}

		var zeroPosition siddontangmysql.Position
		// Ensures the query to check for position is executable.
		_, err = f.WaitUntilReplicaIsCaughtUpToMaster.IsCaughtUp(zeroPosition, 1)
		if err != nil {
			f.logger.WithError(err).Error("cannot check replicated master position on the source database")
			return err
		}

		isReplica, err := CheckDbIsAReplica(f.WaitUntilReplicaIsCaughtUpToMaster.MasterDB)
		if err != nil {
			f.logger.WithError(err).Error("cannot check if master is a read replica")
			return err
		}

		if isReplica {
			err = errors.New("source master is a read replica, not a master writer")
			f.logger.WithError(err).Error("source master is a read replica")
			return err
		}
	} else {
		isReplica, err := CheckDbIsAReplica(f.SourceDB)
		if err != nil {
			f.logger.WithError(err).Error("cannot check if source is a replica")
			return err
		}

		if isReplica {
			err = errors.New("source is a read replica. running Ghostferry with a source replica is unsafe unless WaitUntilReplicaIsCaughtUpToMaster is used")
			f.logger.WithError(err).Error("source is a read replica")
			return err
		}
	}

	// Initializing the necessary components of Ghostferry.
	if f.ErrorHandler == nil {
		f.ErrorHandler = &PanicErrorHandler{
			Ferry: f,
		}
	}

	if f.Throttler == nil {
		f.Throttler = &PauserThrottler{}
	}

	if f.StateToResumeFrom == nil {
		f.StateTracker = NewStateTracker(f.DataIterationConcurrency * 10)
	} else {
		f.StateTracker = NewStateTrackerFromSerializedState(f.DataIterationConcurrency*10, f.StateToResumeFrom)
	}

	// Loads the schema of the tables that are applicable.
	// We need to do this at the beginning of the run as this is required
	// in order to determine the PrimaryKey of each table as well as finding
	// which value in the binlog event correspond to which field in the
	// table.
	if f.StateToResumeFrom != nil {
		f.Tables = f.StateToResumeFrom.LastKnownTableSchemaCache
	} else {
		metrics.Measure("LoadTables", nil, 1.0, func() {
			err = f.RebuildTableSchemaCache()
		})
		if err != nil {
			return err
		}
	}

	f.BinlogStreamer = f.NewBinlogStreamer()
	f.BinlogWriter = f.NewBinlogWriter()
	f.DataIterator = f.NewDataIterator()
	f.BatchWriter = f.NewBatchWriter()

	f.logger.Info("ferry initialized")
	return nil
}

func (f *Ferry) RebuildTableSchemaCache() error {
	var err error
	f.Tables, err = LoadTables(f.SourceDB, f.TableFilter)
	return err
}

// Determine the binlog coordinates, table mapping for the pending
// Ghostferry run.
func (f *Ferry) Start() error {
	// Event listeners for the BinlogStreamer and DataIterator are called
	// in the order they are registered.
	// The builtin event listeners are to write the events to the target
	// database.
	// Registering the builtin event listeners in Start allows the consumer
	// of the library to register event listeners that gets called before
	// and after the data gets written to the target database.
	f.BinlogStreamer.AddEventListener(f.BinlogWriter.BufferBinlogEvents)
	f.DataIterator.AddBatchListener(f.BatchWriter.WriteRowBatch)

	// The starting binlog coordinates must be determined first. If it is
	// determined after the DataIterator starts, the DataIterator might
	// miss some records that are inserted between the time the
	// DataIterator determines the range of IDs to copy and the time that
	// the starting binlog coordinates are determined.
	var err error
	if f.StateToResumeFrom != nil {
		err = f.BinlogStreamer.ConnectBinlogStreamerToMysqlFrom(f.StateToResumeFrom.LastWrittenBinlogPosition)
	} else {
		err = f.BinlogStreamer.ConnectBinlogStreamerToMysql()
	}
	if err != nil {
		return err
	}

	return nil
}

// Spawns the background tasks that actually perform the run.
// Wait for the background tasks to finish.
func (f *Ferry) Run() {
	f.logger.Info("starting ferry run")
	f.OverallState = StateCopying

	ctx, shutdown := context.WithCancel(context.Background())

	handleError := func(name string, err error) {
		if err != nil && err != context.Canceled {
			f.ErrorHandler.Fatal(name, err)
		}
	}

	supportingServicesWg := &sync.WaitGroup{}
	supportingServicesWg.Add(1)

	go func() {
		defer supportingServicesWg.Done()
		handleError("throttler", f.Throttler.Run(ctx))
	}()

	if f.DumpStateToStdoutOnSignal {
		supportingServicesWg.Add(1)

		go func() {
			defer supportingServicesWg.Done()

			c := make(chan os.Signal, 1)
			signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

			select {
			case <-ctx.Done():
				f.logger.Debug("shutting down signal monitoring goroutine")
				return
			case s := <-c:
				f.ErrorHandler.Fatal("user", fmt.Errorf("signal received: %v", s.String()))
			}
		}()

	}

	binlogWg := &sync.WaitGroup{}
	binlogWg.Add(2)

	go func() {
		defer binlogWg.Done()
		f.BinlogWriter.Run()
	}()

	go func() {
		defer binlogWg.Done()

		f.BinlogStreamer.Run()
		f.BinlogWriter.Stop()
	}()

	dataIteratorWg := &sync.WaitGroup{}
	dataIteratorWg.Add(1)

	go func() {
		defer dataIteratorWg.Done()
		f.DataIterator.Run(f.Tables.AsSlice())
	}()

	dataIteratorWg.Wait()

	f.logger.Info("data copy is complete, waiting for cutover")
	f.OverallState = StateWaitingForCutover
	f.waitUntilAutomaticCutoverIsTrue()

	f.logger.Info("entering cutover phase, notifying caller that row copy is complete")
	f.OverallState = StateCutover
	f.notifyRowCopyComplete()

	binlogWg.Wait()

	f.logger.Info("ghostferry run is complete, shutting down auxiliary services")
	f.OverallState = StateDone
	f.DoneTime = time.Now()

	shutdown()
	supportingServicesWg.Wait()
}

func (f *Ferry) RunStandaloneDataCopy(tables []*schema.Table) error {
	if len(tables) == 0 {
		return nil
	}

	dataIterator := f.NewDataIteratorWithoutStateTracker()

	// BUG: if the PanicErrorHandler fires while running the standalone copy, we
	// will get an error dump even though we should not get one, which could be
	// misleading.

	dataIterator.AddBatchListener(f.BatchWriter.WriteRowBatch)
	f.logger.WithField("tables", tables).Info("starting delta table copy in cutover")

	dataIterator.Run(tables)

	return nil
}

// Call this method and perform the cutover after this method returns.
func (f *Ferry) WaitUntilRowCopyIsComplete() {
	<-f.rowCopyCompleteCh
}

func (f *Ferry) WaitUntilBinlogStreamerCatchesUp() {
	for !f.BinlogStreamer.IsAlmostCaughtUp() {
		time.Sleep(500 * time.Millisecond)
	}
}

// After you stop writing to the source and made sure that all inflight
// transactions to the source are completed, call this method to ensure
// that the binlog streaming has caught up and stop the binlog streaming.
//
// This method will actually not shutdown the BinlogStreamer immediately.
// You will know that the BinlogStreamer finished when .Run() returns.
func (f *Ferry) FlushBinlogAndStopStreaming() {
	if f.WaitUntilReplicaIsCaughtUpToMaster != nil {
		isReplica, err := CheckDbIsAReplica(f.WaitUntilReplicaIsCaughtUpToMaster.MasterDB)
		if err != nil {
			f.ErrorHandler.Fatal("wait_replica", err)
		}

		if isReplica {
			err = errors.New("source master is no longer a master writer")
			msg := "aborting the ferry since the source master is no longer the master writer " +
				"this means that the perceived master can be lagging compared to the actual master " +
				"and lead to missed writes. aborting ferry"

			f.logger.Error(msg)
			f.ErrorHandler.Fatal("wait_replica", err)
		}

		err = f.WaitUntilReplicaIsCaughtUpToMaster.Wait()
		if err != nil {
			f.ErrorHandler.Fatal("wait_replica", err)
		}
	}

	f.BinlogStreamer.FlushAndStop()
}

func (f *Ferry) SerializeStateToJSON() (string, error) {
	serializedState := f.StateTracker.Serialize(f.Tables)
	stateBytes, err := json.MarshalIndent(serializedState, "", "  ")
	return string(stateBytes), err
}

func (f *Ferry) waitUntilAutomaticCutoverIsTrue() {
	for !f.AutomaticCutover {
		time.Sleep(1 * time.Second)
		f.logger.Debug("waiting for AutomaticCutover to become true before signaling for row copy complete")
	}
}

func (f *Ferry) notifyRowCopyComplete() {
	f.rowCopyCompleteCh <- struct{}{}
}

func (f *Ferry) checkConnection(dbname string, db *sql.DB) error {
	row := db.QueryRow("SHOW STATUS LIKE 'Ssl_cipher'")
	var name, cipher string
	err := row.Scan(&name, &cipher)
	if err != nil {
		return err
	}

	hasSSL := cipher != ""

	f.logger.WithFields(logrus.Fields{
		"hasSSL":     hasSSL,
		"ssl_cipher": cipher,
		"dbname":     dbname,
	}).Infof("connected to %s", dbname)

	return nil
}

func (f *Ferry) checkConnectionForBinlogFormat(db *sql.DB) error {
	var name, value string

	row := db.QueryRow("SHOW VARIABLES LIKE 'binlog_format'")
	err := row.Scan(&name, &value)
	if err != nil {
		return err
	}

	if strings.ToUpper(value) != "ROW" {
		return fmt.Errorf("binlog_format must be ROW, not %s", value)
	}

	row = db.QueryRow("SHOW VARIABLES LIKE 'binlog_row_image'")
	err = row.Scan(&name, &value)
	if err != nil {
		return err
	}

	if strings.ToUpper(value) != "FULL" {
		return fmt.Errorf("binlog_row_image must be FULL, not %s", value)
	}

	return nil
}
