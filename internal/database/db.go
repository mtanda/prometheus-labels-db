package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	_ "embed"

	lru "github.com/hashicorp/golang-lru/v2"
	_ "github.com/mattn/go-sqlite3"

	_ "github.com/mtanda/prometheus-labels-db/internal/database/regexp"
)

const (
	DbPathPattern     = "labels%s.db"
	PartitionInterval = 3 * 4 * 7 * 24 * time.Hour
	InitCacheSize     = 1000
	WalAutoCheckpoint = 100
	IdleTimeout       = 1 * time.Hour
)

type DBCache struct {
	db       *sql.DB
	lastUsed time.Time
}

type LabelDB struct {
	dir         string
	dbCache     map[string]DBCache
	initialized *lru.Cache[string, struct{}]
}

//go:embed sql/table.sql
var createTableStmt string

func Open(dir string) (*LabelDB, error) {
	cache, err := lru.New[string, struct{}](InitCacheSize)
	if err != nil {
		return nil, err
	}
	return &LabelDB{
		dir:         dir,
		dbCache:     make(map[string]DBCache),
		initialized: cache,
	}, nil
}

func (ldb *LabelDB) getDB(t time.Time) (*sql.DB, error) {
	suffix := getTableSuffix(t)

	dbPath := fmt.Sprintf(DbPathPattern, suffix)
	if dbCache, ok := ldb.dbCache[dbPath]; ok {
		dbCache.lastUsed = time.Now().UTC()
		return dbCache.db, nil
	}

	// TODO: support mode=ro for query command
	db, err := sql.Open("sqlite3", "file:"+ldb.dir+"/"+dbPath+"?_journal_mode=WAL&_sync=NORMAL&_busy_timeout=10000")
	if err != nil {
		return nil, err
	}
	setAutoCheckpoint(db, WalAutoCheckpoint)
	ldb.dbCache[dbPath] = DBCache{
		db:       db,
		lastUsed: time.Now().UTC(),
	}

	return db, nil
}

func (ldb *LabelDB) Close() error {
	var allErr error
	for dbPath, dbCache := range ldb.dbCache {
		if err := dbCache.db.Close(); err != nil {
			// ignore error
			slog.Error("failed to close db", "err", err, "dbPath", dbPath)
			allErr = errors.Join(allErr, err)
		}
	}
	return allErr
}

func (ldb *LabelDB) CleanupUnusedDB(ctx context.Context) error {
	for dbPath, dbCache := range ldb.dbCache {
		if dbCache.lastUsed.Add(IdleTimeout).After(time.Now().UTC()) {
			// still used
			continue
		}

		if err := dbCache.db.Close(); err != nil {
			// ignore error
			slog.Error("failed to close db", "err", err, "dbPath", dbPath)
			continue
		}
		delete(ldb.dbCache, dbPath)
		slog.Info("close unused db", "dbPath", dbPath)
	}
	return nil
}

type timeRange struct {
	From time.Time
	To   time.Time
}

func getPartition(t time.Time) timeRange {
	from := t.Truncate(PartitionInterval)
	to := from.Add(PartitionInterval).Add(-1 * time.Second)
	return timeRange{
		From: from,
		To:   to,
	}
}

func getTableSuffix(t time.Time) string {
	p := getPartition(t)
	return "_" + p.From.Format("20060102") + "_" + p.To.Format("20060102")
}

func getLifetimeTableSuffix(t time.Time, namespace string) string {
	namespace = strings.ReplaceAll(namespace, "/", "_")
	return getTableSuffix(t) + "_" + namespace
}

func getLifetimeRanges(from time.Time, to time.Time) []timeRange {
	var partitions []timeRange
	for t := from; t.Before(to); t = t.Add(PartitionInterval) {
		partitions = append(partitions, getPartition(t))
	}
	partitions[0].From = from
	partitions[len(partitions)-1].To = to
	return partitions
}
