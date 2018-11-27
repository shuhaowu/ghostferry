package ghostferry

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Masterminds/squirrel"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

type ReconcilationDMLEvent struct {
	*DMLEventBase
	newValues RowData
	pk        uint64
}

func (e *ReconcilationDMLEvent) OldValues() RowData {
	return nil
}

func (e *ReconcilationDMLEvent) NewValues() RowData {
	return e.newValues
}

func (e *ReconcilationDMLEvent) PK() (uint64, error) {
	return e.pk, nil
}

func (e *ReconcilationDMLEvent) AsSQLString(target *schema.Table) (string, error) {
	var query string
	if e.newValues != nil {
		columns, err := loadColumnsForTable(&e.table, e.newValues)
		if err != nil {
			return "", err
		}

		query = "REPLACE INTO " +
			QuotedTableNameFromString(target.Schema, target.Name) +
			" (" + strings.Join(columns, ",") + ")" +
			" VALUES (" + buildStringListForValues(e.newValues) + ")"
	} else {
		pkColumnName := e.TableSchema().GetPKColumn(0).Name
		if pkColumnName == "" {
			return "", fmt.Errorf("cannot get PK column for table %s", e.Table())
		}

		pkColumnName = quoteField(pkColumnName)
		query = "DELETE FROM " + QuotedTableNameFromString(target.Schema, target.Name) +
			" WHERE " + buildStringMapForWhere([]string{pkColumnName}, []interface{}{e.pk})
	}

	return query, nil
}

func NewReconciliationDMLEvent(table *schema.Table, pk uint64, row RowData) DMLEvent {
	return &ReconcilationDMLEvent{
		DMLEventBase: &DMLEventBase{table: *table},
		pk:           pk,
		newValues:    row,
	}
}

type UniqueRowMap map[TableIdentifier]map[uint64]struct{}

func (m UniqueRowMap) AddRow(table *schema.Table, pk uint64) bool {
	tableId := NewTableIdentifierFromSchemaTable(table)
	if _, exists := m[tableId]; !exists {
		m[tableId] = make(map[uint64]struct{})
	}

	if _, exists := m[tableId][pk]; !exists {
		m[tableId][pk] = struct{}{}
		return true
	}

	return false
}

// The Reconciler is a required algorithm for catching up the binlog events
// missed while Ghostferry is down for the database table schema has changed
// on either the source or the target.
//
// It does not need to WaitUntilReplicaIsCaughtUpToMaster as long as the source
// and target has equalized even if the source data is delayed with respect to
// its own master shard.
type Reconciler struct {
	StartFromBinlogPosition mysql.Position
	TargetBinlogPosition    mysql.Position
	BatchSize               int

	SourceDB         *sql.DB
	BinlogStreamer   *BinlogStreamer
	BinlogWriter     *BinlogWriter
	Throttler        Throttler
	ErrorHandler     ErrorHandler
	TableSchemaCache TableSchemaCache

	modifiedRows UniqueRowMap
	logger       *logrus.Entry
}

func (r *Reconciler) Run() error {
	r.logger = logrus.WithField("tag", "reconciler")
	r.modifiedRows = make(UniqueRowMap)

	// Sanity check options
	if r.StartFromBinlogPosition == (mysql.Position{}) {
		return errors.New("must specify StartFromBinlogPosition")
	}

	if r.TargetBinlogPosition == (mysql.Position{}) {
		return errors.New("must specify TargetBinlogPosition")
	}

	ctx, shutdown := context.WithCancel(context.Background())
	throttlerWg := &sync.WaitGroup{}
	throttlerWg.Add(1)
	go func() {
		defer throttlerWg.Done()
		err := r.Throttler.Run(ctx)
		if err != nil {
			r.ErrorHandler.Fatal("throttler", err)
		}
	}()
	defer func() {
		shutdown()
		throttlerWg.Wait()
	}()

	r.logger.WithFields(logrus.Fields{
		"start":  r.StartFromBinlogPosition,
		"target": r.TargetBinlogPosition,
	}).Info("running reconciler")

	// Setting up binlog streamer
	r.BinlogStreamer.AddEventListener(r.AddRowsToStore)
	_, err := r.BinlogStreamer.ConnectBinlogStreamerToMysqlFrom(r.StartFromBinlogPosition)
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.BinlogStreamer.Run()
	}()

	// Instruct binlog streamer to catch up to a target position and quit.
	r.BinlogStreamer.FlushToTargetBinlogPositionAndStop(r.TargetBinlogPosition)
	wg.Wait()

	r.logger.WithFields(logrus.Fields{
		"streamed_pos": r.BinlogStreamer.GetLastStreamedBinlogPosition(),
		"target_pos":   r.TargetBinlogPosition,
	}).Info("binlog streaming completed")

	// Sanity check BinlogStreamer has streamed everything
	if r.BinlogStreamer.GetLastStreamedBinlogPosition().Compare(r.TargetBinlogPosition) < 0 {
		return fmt.Errorf("last streamed position %v is less than target position %v?", r.BinlogStreamer.GetLastStreamedBinlogPosition(), r.TargetBinlogPosition)
	}

	// Process modifiedRows
	err = r.replaceModifiedRows()
	if err != nil {
		return err
	}

	r.logger.Info("reconciler run finished")
	return nil
}

func (r *Reconciler) AddRowsToStore(events []DMLEvent) error {
	for _, ev := range events {
		pk, err := ev.PK()
		if err != nil {
			return err
		}

		r.modifiedRows.AddRow(ev.TableSchema(), pk)
	}

	return nil
}

// Could be parallelized here with WorkerPool.
func (r *Reconciler) replaceModifiedRows() error {
	batch := make([]DMLEvent, 0, r.BatchSize)

	count := 0
	for tableId, pkSet := range r.modifiedRows {
		table := r.TableSchemaCache.Get(tableId.SchemaName, tableId.TableName)

		for pk, _ := range pkSet {
			count++
			r.logger.WithField("d", len(batch)).Debug("it")
			if len(batch) == r.BatchSize {
				err := r.replaceBatch(batch)
				if err != nil {
					return err
				}

				batch = make([]DMLEvent, 0, r.BatchSize)
			}

			row, err := r.fetchRowData(table, pk)
			if err != nil {
				r.logger.WithError(err).Error("failed to fetch row data")
				return err
			}

			batch = append(batch, NewReconciliationDMLEvent(table, pk, row))
		}
	}

	if len(batch) > 0 {
		err := r.replaceBatch(batch)
		if err != nil {
			return err
		}
	}

	r.logger.WithField("count", count).Info("processed modified rows")

	return nil
}

func (r *Reconciler) replaceBatch(batch []DMLEvent) error {
	// TODO: make writeEvents a public consumable method
	err := r.BinlogWriter.writeEvents(batch)
	if err != nil {
		r.logger.WithError(err).Error("cannot write events")
	}

	return err
}

func (r *Reconciler) fetchRowData(table *schema.Table, pk uint64) (RowData, error) {
	quotedPK := quoteField(table.GetPKColumn(0).Name)

	query, args, err := squirrel.
		Select("*").
		From(QuotedTableName(table)).
		Where(squirrel.Eq{quotedPK: pk}).ToSql()

	if err != nil {
		return nil, err
	}

	// TODO: make this cached for faster reconciliation
	stmt, err := r.SourceDB.Prepare(query)
	if err != nil {
		return nil, err
	}

	defer stmt.Close()

	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rowData RowData = nil
	count := 0

	for rows.Next() {
		rowData, err = ScanGenericRow(rows, len(table.Columns))
		if err != nil {
			return nil, err
		}

		count++
	}

	if count > 1 {
		return nil, fmt.Errorf("multiple rows detected when only one or zero is expected for %s %v", table.String(), pk)
	}

	return rowData, rows.Err()
}
