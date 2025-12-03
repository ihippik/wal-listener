//go:build integration

package listener

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ihippik/wal-listener/v2/config"
	"github.com/ihippik/wal-listener/v2/publisher"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testSlotName        = "test_wal_listener_slot"
	testPublicationName = "wal-listener"
)

// testConfig holds database connection settings for integration tests
type testConfig struct {
	Host     string
	Port     uint16
	Database string
	User     string
	Password string
}

func getTestConfig() testConfig {
	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("POSTGRES_PORT")
	if port == "" {
		port = "5432"
	}
	var portNum uint16 = 5432
	fmt.Sscanf(port, "%d", &portNum)

	database := os.Getenv("POSTGRES_DB")
	if database == "" {
		database = "postgres"
	}
	user := os.Getenv("POSTGRES_USER")
	if user == "" {
		user = "postgres"
	}
	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		password = "postgres"
	}
	return testConfig{
		Host:     host,
		Port:     portNum,
		Database: database,
		User:     user,
		Password: password,
	}
}

// getDownstreamTestConfig returns config for the downstream PostgreSQL instance
// used for testing foreign origin filtering with logical replication chains
func getDownstreamTestConfig() testConfig {
	host := os.Getenv("POSTGRES_DOWNSTREAM_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("POSTGRES_DOWNSTREAM_PORT")
	if port == "" {
		port = "5433"
	}
	var portNum uint16 = 5433
	fmt.Sscanf(port, "%d", &portNum)

	database := os.Getenv("POSTGRES_DB")
	if database == "" {
		database = "postgres"
	}
	user := os.Getenv("POSTGRES_USER")
	if user == "" {
		user = "postgres"
	}
	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		password = "postgres"
	}
	return testConfig{
		Host:     host,
		Port:     portNum,
		Database: database,
		User:     user,
		Password: password,
	}
}

// testPublisher collects published events for verification
type testPublisher struct {
	mu     sync.Mutex
	events []*publisher.Event
}

func newTestPublisher() *testPublisher {
	return &testPublisher{
		events: make([]*publisher.Event, 0),
	}
}

func (p *testPublisher) Publish(ctx context.Context, topic string, event *publisher.Event) publisher.PublishResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Make a copy to avoid data races
	eventCopy := *event
	p.events = append(p.events, &eventCopy)
	return publisher.NewPublishResult(nil)
}

func (p *testPublisher) Flush(topic string) {}

func (p *testPublisher) GetEvents() []*publisher.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]*publisher.Event, len(p.events))
	copy(result, p.events)
	return result
}

func (p *testPublisher) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = make([]*publisher.Event, 0)
}

func (p *testPublisher) WaitForEvents(count int, timeout time.Duration) []*publisher.Event {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		events := p.GetEvents()
		if len(events) >= count {
			return events
		}
		time.Sleep(50 * time.Millisecond)
	}
	return p.GetEvents()
}

// testMonitor is a no-op monitor for testing
type testMonitor struct{}

func (m *testMonitor) IncPublishedEvents(subject, table string) {}
func (m *testMonitor) IncFilterSkippedEvents(table string)      {}
func (m *testMonitor) IncProblematicEvents(kind string)         {}

// testHelper provides helper methods for integration tests
type testHelper struct {
	t        *testing.T
	cfg      testConfig
	conn     *pgx.Conn
	replConn *pgconn.PgConn
	logger   *slog.Logger
}

func newTestHelper(t *testing.T) *testHelper {
	cfg := getTestConfig()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("DEBUG") != "" {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return &testHelper{
		t:      t,
		cfg:    cfg,
		logger: logger,
	}
}

func (h *testHelper) connect(ctx context.Context) {
	connStr := (&url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(h.cfg.User, h.cfg.Password),
		Host:     fmt.Sprintf("%s:%d", h.cfg.Host, h.cfg.Port),
		Path:     fmt.Sprintf("/%s", h.cfg.Database),
		RawQuery: "sslmode=disable",
	}).String()

	var err error
	h.conn, err = pgx.Connect(ctx, connStr)
	require.NoError(h.t, err, "failed to connect to database")

	replConnStr := (&url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(h.cfg.User, h.cfg.Password),
		Host:     fmt.Sprintf("%s:%d", h.cfg.Host, h.cfg.Port),
		Path:     fmt.Sprintf("/%s", h.cfg.Database),
		RawQuery: "sslmode=disable&replication=database",
	}).String()

	replPgConnConf, err := pgconn.ParseConfig(replConnStr)
	require.NoError(h.t, err, "failed to parse replication connection string")

	h.replConn, err = pgconn.ConnectConfig(ctx, replPgConnConf)
	require.NoError(h.t, err, "failed to connect replication connection")
}

func (h *testHelper) close(ctx context.Context) {
	if h.conn != nil {
		h.conn.Close(ctx)
	}
	if h.replConn != nil {
		h.replConn.Close(ctx)
	}
}

// cleanup drops all test resources in the correct order:
// 1. Drop replication slot first (to release any active connections)
// 2. Drop publication
func (h *testHelper) cleanup(ctx context.Context, slotName string) {
	// Drop replication slot first
	_, _ = h.conn.Exec(ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s') FROM pg_replication_slots WHERE slot_name = '%s'", slotName, slotName))

	// Drop publication
	_, _ = h.conn.Exec(ctx, fmt.Sprintf(`DROP PUBLICATION IF EXISTS "%s"`, testPublicationName))
}

// cleanupTable drops a test table
func (h *testHelper) cleanupTable(ctx context.Context, tableName string) {
	_, _ = h.conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tableName))
}

// createTestTable creates a test table with various column types
func (h *testHelper) createTestTable(ctx context.Context, tableName string) {
	_, err := h.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255),
			email TEXT,
			age INTEGER,
			active BOOLEAN,
			balance BIGINT,
			metadata JSONB,
			created_at TIMESTAMP DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW(),
			uuid_col UUID DEFAULT gen_random_uuid()
		)
	`, tableName))
	require.NoError(h.t, err, "failed to create test table")

	// Set REPLICA IDENTITY to FULL so we get old values on UPDATE/DELETE
	_, err = h.conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tableName))
	require.NoError(h.t, err, "failed to set replica identity")
}

// listenerOptions configures optional settings for creating a listener
type listenerOptions struct {
	maxTransactionSize       int
	skipTransactionBuffering bool
	dropForeignOrigin        bool
}

func (h *testHelper) createListener(ctx context.Context, slotName string, pub *testPublisher) *Listener {
	return h.createListenerWithOptions(ctx, slotName, pub, listenerOptions{})
}

func (h *testHelper) createListenerWithOptions(ctx context.Context, slotName string, pub *testPublisher, opts listenerOptions) *Listener {
	cfg := &config.Config{
		Listener: &config.ListenerCfg{
			SlotName:                 slotName,
			RefreshConnection:        5 * time.Second,
			HeartbeatInterval:        5 * time.Second,
			MaxTransactionSize:       opts.maxTransactionSize,
			SkipTransactionBuffering: opts.skipTransactionBuffering,
			DropForeignOrigin:        opts.dropForeignOrigin,
		},
		Database: &config.DatabaseCfg{
			Host:     h.cfg.Host,
			Port:     h.cfg.Port,
			Name:     h.cfg.Database,
			User:     h.cfg.User,
			Password: h.cfg.Password,
		},
		Publisher: &config.PublisherCfg{
			Type:  config.PublisherTypeStdout,
			Topic: "test",
		},
	}

	repo := NewRepository(h.conn)
	repl := NewReplicationWrapper(h.replConn, h.logger)
	parser := NewBinaryParser(h.logger, binary.BigEndian)
	monitor := &testMonitor{}

	return NewWalListener(cfg, h.logger, repo, repl, pub, parser, monitor)
}

// TestIntegration_BasicInsert tests that INSERT operations are captured correctly
func TestIntegration_BasicInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	helper := newTestHelper(t)
	helper.connect(ctx)
	defer helper.close(ctx)

	slotName := testSlotName + "_insert"
	tableName := "test_insert_" + fmt.Sprintf("%d", time.Now().UnixNano())

	// Cleanup before and after test
	helper.cleanup(ctx, slotName)
	helper.cleanupTable(ctx, tableName)
	defer func() {
		helper.cleanup(ctx, slotName)
		helper.cleanupTable(ctx, tableName)
	}()

	// Create test table
	helper.createTestTable(ctx, tableName)

	// Create publisher to collect events
	pub := newTestPublisher()

	// Create and start listener
	listener := helper.createListener(ctx, slotName, pub)

	// Start listener in background
	listenerCtx, listenerCancel := context.WithCancel(ctx)
	listenerDone := make(chan error, 1)
	go func() {
		listenerDone <- listener.Process(listenerCtx)
	}()

	// Wait for listener to be ready
	time.Sleep(2 * time.Second)

	// Perform INSERT operation using a separate connection
	insertConn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		helper.cfg.User, helper.cfg.Password, helper.cfg.Host, helper.cfg.Port, helper.cfg.Database))
	require.NoError(t, err)
	defer insertConn.Close(ctx)

	_, err = insertConn.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (name, email, age, active, balance, metadata) 
		VALUES ($1, $2, $3, $4, $5, $6)
	`, tableName), "John Doe", "john@example.com", 30, true, int64(1000), `{"key": "value"}`)
	require.NoError(t, err)

	// Wait for events
	events := pub.WaitForEvents(1, 10*time.Second)

	// Stop listener
	listenerCancel()
	<-listenerDone

	// Verify events
	require.GreaterOrEqual(t, len(events), 1, "expected at least 1 event")

	// Find the INSERT event for our table
	var insertEvent *publisher.Event
	for _, e := range events {
		if e.Table == tableName && e.Action == "INSERT" {
			insertEvent = e
			break
		}
	}
	require.NotNil(t, insertEvent, "INSERT event not found")

	assert.Equal(t, "public", insertEvent.Schema)
	assert.Equal(t, tableName, insertEvent.Table)
	assert.Equal(t, "INSERT", insertEvent.Action)
	assert.Equal(t, "John Doe", insertEvent.Data["name"])
	assert.Equal(t, "john@example.com", insertEvent.Data["email"])
	assert.Equal(t, 30, insertEvent.Data["age"])
	assert.Equal(t, true, insertEvent.Data["active"])
	assert.Equal(t, int64(1000), insertEvent.Data["balance"])
	assert.Equal(t, map[string]any{"key": "value"}, insertEvent.Data["metadata"])
}

// TestIntegration_Update tests that UPDATE operations are captured correctly
func TestIntegration_Update(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	helper := newTestHelper(t)
	helper.connect(ctx)
	defer helper.close(ctx)

	slotName := testSlotName + "_update"
	tableName := "test_update_" + fmt.Sprintf("%d", time.Now().UnixNano())

	helper.cleanup(ctx, slotName)
	helper.cleanupTable(ctx, tableName)
	defer func() {
		helper.cleanup(ctx, slotName)
		helper.cleanupTable(ctx, tableName)
	}()

	helper.createTestTable(ctx, tableName)

	// Insert initial data before starting listener
	dataConn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		helper.cfg.User, helper.cfg.Password, helper.cfg.Host, helper.cfg.Port, helper.cfg.Database))
	require.NoError(t, err)
	defer dataConn.Close(ctx)

	_, err = dataConn.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, name, email, age) VALUES (1, 'Jane Doe', 'jane@example.com', 25)
	`, tableName))
	require.NoError(t, err)

	pub := newTestPublisher()
	listener := helper.createListener(ctx, slotName, pub)

	listenerCtx, listenerCancel := context.WithCancel(ctx)
	listenerDone := make(chan error, 1)
	go func() {
		listenerDone <- listener.Process(listenerCtx)
	}()

	time.Sleep(2 * time.Second)

	// Perform UPDATE
	_, err = dataConn.Exec(ctx, fmt.Sprintf(`UPDATE %s SET name = 'Jane Smith', age = 26 WHERE id = 1`, tableName))
	require.NoError(t, err)

	events := pub.WaitForEvents(1, 10*time.Second)

	listenerCancel()
	<-listenerDone

	require.GreaterOrEqual(t, len(events), 1, "expected at least 1 event")

	var updateEvent *publisher.Event
	for _, e := range events {
		if e.Table == tableName && e.Action == "UPDATE" {
			updateEvent = e
			break
		}
	}
	require.NotNil(t, updateEvent, "UPDATE event not found")

	assert.Equal(t, "public", updateEvent.Schema)
	assert.Equal(t, tableName, updateEvent.Table)
	assert.Equal(t, "UPDATE", updateEvent.Action)
	// New values
	assert.Equal(t, "Jane Smith", updateEvent.Data["name"])
	assert.Equal(t, 26, updateEvent.Data["age"])
	// Old values (since REPLICA IDENTITY FULL)
	assert.Equal(t, "Jane Doe", updateEvent.DataOld["name"])
	assert.Equal(t, 25, updateEvent.DataOld["age"])
}

// TestIntegration_Delete tests that DELETE operations are captured correctly
func TestIntegration_Delete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	helper := newTestHelper(t)
	helper.connect(ctx)
	defer helper.close(ctx)

	slotName := testSlotName + "_delete"
	tableName := "test_delete_" + fmt.Sprintf("%d", time.Now().UnixNano())

	helper.cleanup(ctx, slotName)
	helper.cleanupTable(ctx, tableName)
	defer func() {
		helper.cleanup(ctx, slotName)
		helper.cleanupTable(ctx, tableName)
	}()

	helper.createTestTable(ctx, tableName)

	dataConn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		helper.cfg.User, helper.cfg.Password, helper.cfg.Host, helper.cfg.Port, helper.cfg.Database))
	require.NoError(t, err)
	defer dataConn.Close(ctx)

	_, err = dataConn.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, name, email) VALUES (1, 'To Delete', 'delete@example.com')
	`, tableName))
	require.NoError(t, err)

	pub := newTestPublisher()
	listener := helper.createListener(ctx, slotName, pub)

	listenerCtx, listenerCancel := context.WithCancel(ctx)
	listenerDone := make(chan error, 1)
	go func() {
		listenerDone <- listener.Process(listenerCtx)
	}()

	time.Sleep(2 * time.Second)

	// Perform DELETE
	_, err = dataConn.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id = 1`, tableName))
	require.NoError(t, err)

	events := pub.WaitForEvents(1, 10*time.Second)

	listenerCancel()
	<-listenerDone

	require.GreaterOrEqual(t, len(events), 1, "expected at least 1 event")

	var deleteEvent *publisher.Event
	for _, e := range events {
		if e.Table == tableName && e.Action == "DELETE" {
			deleteEvent = e
			break
		}
	}
	require.NotNil(t, deleteEvent, "DELETE event not found")

	assert.Equal(t, "public", deleteEvent.Schema)
	assert.Equal(t, tableName, deleteEvent.Table)
	assert.Equal(t, "DELETE", deleteEvent.Action)
	// Old values should be captured
	assert.Equal(t, "To Delete", deleteEvent.DataOld["name"])
	assert.Equal(t, "delete@example.com", deleteEvent.DataOld["email"])
}

// TestIntegration_MultipleOperations tests multiple operations in sequence
func TestIntegration_MultipleOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	helper := newTestHelper(t)
	helper.connect(ctx)
	defer helper.close(ctx)

	slotName := testSlotName + "_multi"
	tableName := "test_multi_" + fmt.Sprintf("%d", time.Now().UnixNano())

	helper.cleanup(ctx, slotName)
	helper.cleanupTable(ctx, tableName)
	defer func() {
		helper.cleanup(ctx, slotName)
		helper.cleanupTable(ctx, tableName)
	}()

	helper.createTestTable(ctx, tableName)

	pub := newTestPublisher()
	listener := helper.createListener(ctx, slotName, pub)

	listenerCtx, listenerCancel := context.WithCancel(ctx)
	listenerDone := make(chan error, 1)
	go func() {
		listenerDone <- listener.Process(listenerCtx)
	}()

	time.Sleep(2 * time.Second)

	dataConn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		helper.cfg.User, helper.cfg.Password, helper.cfg.Host, helper.cfg.Port, helper.cfg.Database))
	require.NoError(t, err)
	defer dataConn.Close(ctx)

	// INSERT
	_, err = dataConn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, name) VALUES (100, 'First')`, tableName))
	require.NoError(t, err)

	// UPDATE
	_, err = dataConn.Exec(ctx, fmt.Sprintf(`UPDATE %s SET name = 'Updated' WHERE id = 100`, tableName))
	require.NoError(t, err)

	// INSERT another
	_, err = dataConn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, name) VALUES (101, 'Second')`, tableName))
	require.NoError(t, err)

	// DELETE
	_, err = dataConn.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id = 100`, tableName))
	require.NoError(t, err)

	events := pub.WaitForEvents(4, 15*time.Second)

	listenerCancel()
	<-listenerDone

	require.GreaterOrEqual(t, len(events), 4, "expected at least 4 events")

	// Count event types for our table
	counts := map[string]int{}
	for _, e := range events {
		if e.Table == tableName {
			counts[e.Action]++
		}
	}

	assert.Equal(t, 2, counts["INSERT"], "expected 2 INSERT events")
	assert.Equal(t, 1, counts["UPDATE"], "expected 1 UPDATE event")
	assert.Equal(t, 1, counts["DELETE"], "expected 1 DELETE event")
}

// TestIntegration_UUIDColumn tests UUID column handling
func TestIntegration_UUIDColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	helper := newTestHelper(t)
	helper.connect(ctx)
	defer helper.close(ctx)

	slotName := testSlotName + "_uuid"
	tableName := "test_uuid_" + fmt.Sprintf("%d", time.Now().UnixNano())

	helper.cleanup(ctx, slotName)
	helper.cleanupTable(ctx, tableName)
	defer func() {
		helper.cleanup(ctx, slotName)
		helper.cleanupTable(ctx, tableName)
	}()

	// Create table with UUID column
	_, err := helper.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			name TEXT
		)
	`, tableName))
	require.NoError(t, err)

	_, err = helper.conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tableName))
	require.NoError(t, err)

	pub := newTestPublisher()
	listener := helper.createListener(ctx, slotName, pub)

	listenerCtx, listenerCancel := context.WithCancel(ctx)
	listenerDone := make(chan error, 1)
	go func() {
		listenerDone <- listener.Process(listenerCtx)
	}()

	time.Sleep(2 * time.Second)

	dataConn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		helper.cfg.User, helper.cfg.Password, helper.cfg.Host, helper.cfg.Port, helper.cfg.Database))
	require.NoError(t, err)
	defer dataConn.Close(ctx)

	testUUID := uuid.New()
	_, err = dataConn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, name) VALUES ($1, 'UUID Test')`, tableName), testUUID)
	require.NoError(t, err)

	events := pub.WaitForEvents(1, 10*time.Second)

	listenerCancel()
	<-listenerDone

	require.GreaterOrEqual(t, len(events), 1, "expected at least 1 event")

	var insertEvent *publisher.Event
	for _, e := range events {
		if e.Table == tableName && e.Action == "INSERT" {
			insertEvent = e
			break
		}
	}
	require.NotNil(t, insertEvent, "INSERT event not found")

	// Verify UUID is correctly parsed
	idValue := insertEvent.Data["id"]
	parsedUUID, ok := idValue.(uuid.UUID)
	require.True(t, ok, "expected UUID type, got %T", idValue)
	assert.Equal(t, testUUID, parsedUUID)
}

// TestIntegration_TransactionBatch tests that multiple operations in a single transaction are captured
func TestIntegration_TransactionBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	helper := newTestHelper(t)
	helper.connect(ctx)
	defer helper.close(ctx)

	slotName := testSlotName + "_batch"
	tableName := "test_batch_" + fmt.Sprintf("%d", time.Now().UnixNano())

	helper.cleanup(ctx, slotName)
	helper.cleanupTable(ctx, tableName)
	defer func() {
		helper.cleanup(ctx, slotName)
		helper.cleanupTable(ctx, tableName)
	}()

	helper.createTestTable(ctx, tableName)

	pub := newTestPublisher()
	listener := helper.createListener(ctx, slotName, pub)

	listenerCtx, listenerCancel := context.WithCancel(ctx)
	listenerDone := make(chan error, 1)
	go func() {
		listenerDone <- listener.Process(listenerCtx)
	}()

	time.Sleep(2 * time.Second)

	dataConn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		helper.cfg.User, helper.cfg.Password, helper.cfg.Host, helper.cfg.Port, helper.cfg.Database))
	require.NoError(t, err)
	defer dataConn.Close(ctx)

	// Execute multiple operations in a single transaction
	tx, err := dataConn.Begin(ctx)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, name) VALUES (200, 'Batch 1')`, tableName))
	require.NoError(t, err)

	_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, name) VALUES (201, 'Batch 2')`, tableName))
	require.NoError(t, err)

	_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, name) VALUES (202, 'Batch 3')`, tableName))
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	events := pub.WaitForEvents(3, 10*time.Second)

	listenerCancel()
	<-listenerDone

	// All 3 inserts should be captured
	insertCount := 0
	for _, e := range events {
		if e.Table == tableName && e.Action == "INSERT" {
			insertCount++
		}
	}

	assert.Equal(t, 3, insertCount, "expected 3 INSERT events from transaction batch")
}

// TestIntegration_JSONBColumn tests JSONB column handling with various JSON structures
func TestIntegration_JSONBColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	helper := newTestHelper(t)
	helper.connect(ctx)
	defer helper.close(ctx)

	slotName := testSlotName + "_jsonb"
	tableName := "test_jsonb_" + fmt.Sprintf("%d", time.Now().UnixNano())

	helper.cleanup(ctx, slotName)
	helper.cleanupTable(ctx, tableName)
	defer func() {
		helper.cleanup(ctx, slotName)
		helper.cleanupTable(ctx, tableName)
	}()

	_, err := helper.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			data JSONB
		)
	`, tableName))
	require.NoError(t, err)

	_, err = helper.conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tableName))
	require.NoError(t, err)

	pub := newTestPublisher()
	listener := helper.createListener(ctx, slotName, pub)

	listenerCtx, listenerCancel := context.WithCancel(ctx)
	listenerDone := make(chan error, 1)
	go func() {
		listenerDone <- listener.Process(listenerCtx)
	}()

	time.Sleep(2 * time.Second)

	dataConn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		helper.cfg.User, helper.cfg.Password, helper.cfg.Host, helper.cfg.Port, helper.cfg.Database))
	require.NoError(t, err)
	defer dataConn.Close(ctx)

	// Test complex nested JSON
	_, err = dataConn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (data) VALUES ($1)`, tableName),
		`{"name": "test", "nested": {"key": "value"}, "array": [1, 2, 3]}`)
	require.NoError(t, err)

	// Test JSON array
	_, err = dataConn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (data) VALUES ($1)`, tableName),
		`["a", "b", "c"]`)
	require.NoError(t, err)

	events := pub.WaitForEvents(2, 10*time.Second)

	listenerCancel()
	<-listenerDone

	require.GreaterOrEqual(t, len(events), 2, "expected at least 2 events")

	// Find our events
	var objectEvent, arrayEvent *publisher.Event
	for _, e := range events {
		if e.Table == tableName && e.Action == "INSERT" {
			if data, ok := e.Data["data"].(map[string]any); ok {
				if _, hasName := data["name"]; hasName {
					objectEvent = e
				}
			}
			if _, ok := e.Data["data"].([]any); ok {
				arrayEvent = e
			}
		}
	}

	require.NotNil(t, objectEvent, "object JSONB event not found")
	require.NotNil(t, arrayEvent, "array JSONB event not found")

	// Verify object structure
	objData := objectEvent.Data["data"].(map[string]any)
	assert.Equal(t, "test", objData["name"])
	nested := objData["nested"].(map[string]any)
	assert.Equal(t, "value", nested["key"])

	// Verify array structure
	arrData := arrayEvent.Data["data"].([]any)
	assert.Equal(t, []any{"a", "b", "c"}, arrData)
}

// test that our test helper for cleanup works correctly
func TestMetaIntegration_SlotCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	helper := newTestHelper(t)
	helper.connect(ctx)
	defer helper.close(ctx)

	slotName := testSlotName + "_cleanup_test"

	// Ensure clean state
	helper.cleanup(ctx, slotName)

	// Create slot manually
	_, err := helper.conn.Exec(ctx, fmt.Sprintf(
		"SELECT pg_create_logical_replication_slot('%s', 'pgoutput')", slotName))
	require.NoError(t, err)

	// Create publication manually
	_, err = helper.conn.Exec(ctx, fmt.Sprintf(`CREATE PUBLICATION "%s" FOR ALL TABLES`, testPublicationName))
	// Ignore error if already exists
	_ = err

	// Verify slot exists
	var slotExists bool
	err = helper.conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slotName).Scan(&slotExists)
	require.NoError(t, err)
	require.True(t, slotExists, "slot should exist before cleanup")

	// Run cleanup
	helper.cleanup(ctx, slotName)

	// Verify slot is gone
	err = helper.conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slotName).Scan(&slotExists)
	require.NoError(t, err)
	assert.False(t, slotExists, "slot should not exist after cleanup")

	// Verify publication is gone
	var pubExists bool
	err = helper.conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)", testPublicationName).Scan(&pubExists)
	require.NoError(t, err)
	assert.False(t, pubExists, "publication should not exist after cleanup")
}

// TestIntegration_MaxTransactionSize tests that transactions larger than maxTransactionSize
// are truncated and only the first N actions are published
func TestIntegration_MaxTransactionSize(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	helper := newTestHelper(t)
	helper.connect(ctx)
	defer helper.close(ctx)

	slotName := testSlotName + "_maxsize"
	tableName := "test_maxsize_" + fmt.Sprintf("%d", time.Now().UnixNano())

	helper.cleanup(ctx, slotName)
	helper.cleanupTable(ctx, tableName)
	defer func() {
		helper.cleanup(ctx, slotName)
		helper.cleanupTable(ctx, tableName)
	}()

	helper.createTestTable(ctx, tableName)

	pub := newTestPublisher()

	// Create listener with maxTransactionSize = 5, meaning only 5 actions per transaction
	const maxTxSize = 5
	listener := helper.createListenerWithOptions(ctx, slotName, pub, listenerOptions{
		maxTransactionSize: maxTxSize,
	})

	listenerCtx, listenerCancel := context.WithCancel(ctx)
	listenerDone := make(chan error, 1)
	go func() {
		listenerDone <- listener.Process(listenerCtx)
	}()

	time.Sleep(2 * time.Second)

	dataConn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		helper.cfg.User, helper.cfg.Password, helper.cfg.Host, helper.cfg.Port, helper.cfg.Database))
	require.NoError(t, err)
	defer dataConn.Close(ctx)

	// Insert 20 rows in a single transaction - this exceeds maxTransactionSize of 5
	const totalInserts = 20
	tx, err := dataConn.Begin(ctx)
	require.NoError(t, err)

	for i := 0; i < totalInserts; i++ {
		_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name) VALUES ($1)`, tableName), fmt.Sprintf("row_%d", i))
		require.NoError(t, err)
	}

	err = tx.Commit(ctx)
	require.NoError(t, err)

	// Wait for events - we should only receive maxTxSize events, not all 20
	events := pub.WaitForEvents(maxTxSize, 15*time.Second)

	// Give a bit more time to ensure no extra events arrive
	time.Sleep(2 * time.Second)
	events = pub.GetEvents()

	listenerCancel()
	<-listenerDone

	// Count INSERT events for our table
	insertCount := 0
	for _, e := range events {
		if e.Table == tableName && e.Action == "INSERT" {
			insertCount++
		}
	}

	// We should have exactly maxTxSize events, not all 20
	assert.Equal(t, maxTxSize, insertCount,
		"expected exactly %d INSERT events (maxTransactionSize), but got %d", maxTxSize, insertCount)

	// Verify we got the first N rows (they should be row_0 through row_4)
	receivedNames := make([]string, 0)
	for _, e := range events {
		if e.Table == tableName && e.Action == "INSERT" {
			if name, ok := e.Data["name"].(string); ok {
				receivedNames = append(receivedNames, name)
			}
		}
	}

	// The first maxTxSize rows should be received
	for i := 0; i < maxTxSize; i++ {
		expectedName := fmt.Sprintf("row_%d", i)
		assert.Contains(t, receivedNames, expectedName,
			"expected to receive %s in the first %d events", expectedName, maxTxSize)
	}
}

// TestIntegration_MaxTransactionSizeWithSkipBuffering tests maxTransactionSize works correctly
// when skipTransactionBuffering is enabled (events are published immediately)
func TestIntegration_MaxTransactionSizeWithSkipBuffering(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	helper := newTestHelper(t)
	helper.connect(ctx)
	defer helper.close(ctx)

	slotName := testSlotName + "_maxsize_skip"
	tableName := "test_maxsize_skip_" + fmt.Sprintf("%d", time.Now().UnixNano())

	helper.cleanup(ctx, slotName)
	helper.cleanupTable(ctx, tableName)
	defer func() {
		helper.cleanup(ctx, slotName)
		helper.cleanupTable(ctx, tableName)
	}()

	helper.createTestTable(ctx, tableName)

	pub := newTestPublisher()

	// Create listener with maxTransactionSize = 3 AND skipTransactionBuffering = true
	const maxTxSize = 3
	listener := helper.createListenerWithOptions(ctx, slotName, pub, listenerOptions{
		maxTransactionSize:       maxTxSize,
		skipTransactionBuffering: true,
	})

	listenerCtx, listenerCancel := context.WithCancel(ctx)
	listenerDone := make(chan error, 1)
	go func() {
		listenerDone <- listener.Process(listenerCtx)
	}()

	time.Sleep(2 * time.Second)

	dataConn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		helper.cfg.User, helper.cfg.Password, helper.cfg.Host, helper.cfg.Port, helper.cfg.Database))
	require.NoError(t, err)
	defer dataConn.Close(ctx)

	// Insert 10 rows in a single transaction
	const totalInserts = 10
	tx, err := dataConn.Begin(ctx)
	require.NoError(t, err)

	for i := 0; i < totalInserts; i++ {
		_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name) VALUES ($1)`, tableName), fmt.Sprintf("skip_row_%d", i))
		require.NoError(t, err)
	}

	err = tx.Commit(ctx)
	require.NoError(t, err)

	// Wait for events
	events := pub.WaitForEvents(maxTxSize, 15*time.Second)

	// Give time to ensure no extra events arrive
	time.Sleep(2 * time.Second)
	events = pub.GetEvents()

	listenerCancel()
	<-listenerDone

	// Count INSERT events for our table
	insertCount := 0
	for _, e := range events {
		if e.Table == tableName && e.Action == "INSERT" {
			insertCount++
		}
	}

	// Even with skipTransactionBuffering, we should only get maxTxSize events
	assert.Equal(t, maxTxSize, insertCount,
		"expected exactly %d INSERT events with skipTransactionBuffering, but got %d", maxTxSize, insertCount)
}

// TestIntegration_LargeTransactionNoLimit tests that without maxTransactionSize,
// all events in a large transaction are published
func TestIntegration_LargeTransactionNoLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	helper := newTestHelper(t)
	helper.connect(ctx)
	defer helper.close(ctx)

	slotName := testSlotName + "_nolimit"
	tableName := "test_nolimit_" + fmt.Sprintf("%d", time.Now().UnixNano())

	helper.cleanup(ctx, slotName)
	helper.cleanupTable(ctx, tableName)
	defer func() {
		helper.cleanup(ctx, slotName)
		helper.cleanupTable(ctx, tableName)
	}()

	helper.createTestTable(ctx, tableName)

	pub := newTestPublisher()

	// Create listener WITHOUT maxTransactionSize (default 0 = no limit)
	listener := helper.createListener(ctx, slotName, pub)

	listenerCtx, listenerCancel := context.WithCancel(ctx)
	listenerDone := make(chan error, 1)
	go func() {
		listenerDone <- listener.Process(listenerCtx)
	}()

	time.Sleep(2 * time.Second)

	dataConn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		helper.cfg.User, helper.cfg.Password, helper.cfg.Host, helper.cfg.Port, helper.cfg.Database))
	require.NoError(t, err)
	defer dataConn.Close(ctx)

	// Insert 15 rows in a single transaction
	const totalInserts = 15
	tx, err := dataConn.Begin(ctx)
	require.NoError(t, err)

	for i := 0; i < totalInserts; i++ {
		_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (name) VALUES ($1)`, tableName), fmt.Sprintf("nolimit_row_%d", i))
		require.NoError(t, err)
	}

	err = tx.Commit(ctx)
	require.NoError(t, err)

	// Wait for ALL events since there's no limit
	events := pub.WaitForEvents(totalInserts, 15*time.Second)

	listenerCancel()
	<-listenerDone

	// Count INSERT events for our table
	insertCount := 0
	for _, e := range events {
		if e.Table == tableName && e.Action == "INSERT" {
			insertCount++
		}
	}

	// Without maxTransactionSize, we should get ALL events
	assert.Equal(t, totalInserts, insertCount,
		"expected all %d INSERT events without maxTransactionSize, but got %d", totalInserts, insertCount)
}

// foreignOriginTestHelper manages two PostgreSQL instances for testing foreign origin filtering
type foreignOriginTestHelper struct {
	t              *testing.T
	primaryCfg     testConfig
	downstreamCfg  testConfig
	primaryConn    *pgx.Conn
	downstreamConn *pgx.Conn
	replConn       *pgconn.PgConn
	logger         *slog.Logger
}

func newForeignOriginTestHelper(t *testing.T) *foreignOriginTestHelper {
	primaryCfg := getTestConfig()
	downstreamCfg := getDownstreamTestConfig()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("DEBUG") != "" {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return &foreignOriginTestHelper{
		t:             t,
		primaryCfg:    primaryCfg,
		downstreamCfg: downstreamCfg,
		logger:        logger,
	}
}

func (h *foreignOriginTestHelper) connect(ctx context.Context) {
	// Connect to primary
	primaryConnStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		h.primaryCfg.User, h.primaryCfg.Password, h.primaryCfg.Host, h.primaryCfg.Port, h.primaryCfg.Database)
	var err error
	h.primaryConn, err = pgx.Connect(ctx, primaryConnStr)
	require.NoError(h.t, err, "failed to connect to primary database")

	// Connect to downstream (regular connection)
	downstreamConnStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		h.downstreamCfg.User, h.downstreamCfg.Password, h.downstreamCfg.Host, h.downstreamCfg.Port, h.downstreamCfg.Database)
	h.downstreamConn, err = pgx.Connect(ctx, downstreamConnStr)
	require.NoError(h.t, err, "failed to connect to downstream database")

	// Replication connection to downstream
	replConnStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable&replication=database",
		h.downstreamCfg.User, h.downstreamCfg.Password, h.downstreamCfg.Host, h.downstreamCfg.Port, h.downstreamCfg.Database)
	replPgConnConf, err := pgconn.ParseConfig(replConnStr)
	require.NoError(h.t, err, "failed to parse replication connection string")
	h.replConn, err = pgconn.ConnectConfig(ctx, replPgConnConf)
	require.NoError(h.t, err, "failed to connect replication connection to downstream")
}

func (h *foreignOriginTestHelper) close(ctx context.Context) {
	if h.primaryConn != nil {
		h.primaryConn.Close(ctx)
	}
	if h.downstreamConn != nil {
		h.downstreamConn.Close(ctx)
	}
	if h.replConn != nil {
		h.replConn.Close(ctx)
	}
}

func (h *foreignOriginTestHelper) cleanup(ctx context.Context, slotName, subscriptionName, publicationName, tableName string) {
	// Clean up downstream: subscription first, then slot
	if subscriptionName != "" {
		_, _ = h.downstreamConn.Exec(ctx, fmt.Sprintf("ALTER SUBSCRIPTION %s DISABLE", subscriptionName))
		_, _ = h.downstreamConn.Exec(ctx, fmt.Sprintf("ALTER SUBSCRIPTION %s SET (slot_name = NONE)", subscriptionName))
		_, _ = h.downstreamConn.Exec(ctx, fmt.Sprintf("DROP SUBSCRIPTION IF EXISTS %s", subscriptionName))
	}

	// Drop wal-listener slot on downstream
	if slotName != "" {
		_, _ = h.downstreamConn.Exec(ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s') FROM pg_replication_slots WHERE slot_name = '%s'", slotName, slotName))
	}

	// Drop publication on downstream
	_, _ = h.downstreamConn.Exec(ctx, fmt.Sprintf(`DROP PUBLICATION IF EXISTS "%s"`, testPublicationName))

	// Clean up primary: publication and slot
	if publicationName != "" {
		_, _ = h.primaryConn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", publicationName))
	}

	// Drop table on both
	if tableName != "" {
		_, _ = h.primaryConn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tableName))
		_, _ = h.downstreamConn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tableName))
	}
}

func (h *foreignOriginTestHelper) setupReplication(ctx context.Context, tableName, publicationName, subscriptionName string) {
	// Create table on primary
	_, err := h.primaryConn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255),
			source VARCHAR(50)
		)
	`, tableName))
	require.NoError(h.t, err, "failed to create table on primary")

	// Set REPLICA IDENTITY FULL on primary
	_, err = h.primaryConn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tableName))
	require.NoError(h.t, err, "failed to set replica identity on primary")

	// Create publication on primary
	_, err = h.primaryConn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", publicationName, tableName))
	require.NoError(h.t, err, "failed to create publication on primary")

	// Create the same table on downstream
	_, err = h.downstreamConn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255),
			source VARCHAR(50)
		)
	`, tableName))
	require.NoError(h.t, err, "failed to create table on downstream")

	// Set REPLICA IDENTITY FULL on downstream
	_, err = h.downstreamConn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tableName))
	require.NoError(h.t, err, "failed to set replica identity on downstream")

	// Create subscription on downstream to primary
	// The subscription runs inside the downstream container, so it needs the internal hostname
	// to reach the primary container (e.g., docker container name or docker-compose service name)
	primaryHostInternal := os.Getenv("POSTGRES_PRIMARY_HOST_INTERNAL")
	if primaryHostInternal == "" {
		primaryHostInternal = h.primaryCfg.Host // fallback for local testing without docker
	}
	primaryPortInternal := os.Getenv("POSTGRES_PRIMARY_PORT_INTERNAL")
	if primaryPortInternal == "" {
		primaryPortInternal = fmt.Sprintf("%d", h.primaryCfg.Port)
	}

	subscriptionConnStr := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s",
		primaryHostInternal, primaryPortInternal, h.primaryCfg.Database, h.primaryCfg.User, h.primaryCfg.Password)

	_, err = h.downstreamConn.Exec(ctx, fmt.Sprintf(
		"CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION %s",
		subscriptionName, subscriptionConnStr, publicationName))
	require.NoError(h.t, err, "failed to create subscription on downstream")

	// Wait for subscription to be ready
	time.Sleep(2 * time.Second)
}

func (h *foreignOriginTestHelper) createListener(ctx context.Context, slotName string, pub *testPublisher, dropForeignOrigin bool) *Listener {
	cfg := &config.Config{
		Listener: &config.ListenerCfg{
			SlotName:          slotName,
			RefreshConnection: 5 * time.Second,
			HeartbeatInterval: 5 * time.Second,
			DropForeignOrigin: dropForeignOrigin,
		},
		Database: &config.DatabaseCfg{
			Host:     h.downstreamCfg.Host,
			Port:     h.downstreamCfg.Port,
			Name:     h.downstreamCfg.Database,
			User:     h.downstreamCfg.User,
			Password: h.downstreamCfg.Password,
		},
		Publisher: &config.PublisherCfg{
			Type:  config.PublisherTypeStdout,
			Topic: "test",
		},
	}

	repo := NewRepository(h.downstreamConn)
	repl := NewReplicationWrapper(h.replConn, h.logger)
	parser := NewBinaryParser(h.logger, binary.BigEndian)
	monitor := &testMonitor{}

	return NewWalListener(cfg, h.logger, repo, repl, pub, parser, monitor)
}

// TestIntegration_DropForeignOrigin tests that transactions from upstream databases
// are dropped when dropForeignOrigin is enabled
func TestIntegration_DropForeignOrigin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	helper := newForeignOriginTestHelper(t)
	helper.connect(ctx)
	defer helper.close(ctx)

	slotName := testSlotName + "_foreignorigin"
	tableName := "test_foreign_" + fmt.Sprintf("%d", time.Now().UnixNano())
	publicationName := "test_pub_" + fmt.Sprintf("%d", time.Now().UnixNano())
	subscriptionName := "test_sub_" + fmt.Sprintf("%d", time.Now().UnixNano())

	// Cleanup before and after
	helper.cleanup(ctx, slotName, subscriptionName, publicationName, tableName)
	defer helper.cleanup(ctx, slotName, subscriptionName, publicationName, tableName)

	// Setup logical replication chain: primary -> downstream
	helper.setupReplication(ctx, tableName, publicationName, subscriptionName)

	pub := newTestPublisher()

	// Create listener on downstream with dropForeignOrigin = true
	listener := helper.createListener(ctx, slotName, pub, true)

	listenerCtx, listenerCancel := context.WithCancel(ctx)
	listenerDone := make(chan error, 1)
	go func() {
		listenerDone <- listener.Process(listenerCtx)
	}()

	time.Sleep(3 * time.Second)

	// Insert data on PRIMARY - this will be replicated to downstream with foreign origin
	// Use explicit ID to avoid conflicts with downstream inserts
	_, err := helper.primaryConn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (id, name, source) VALUES (1000, 'from_primary', 'primary')`, tableName))
	require.NoError(t, err)

	// Wait for replication to downstream
	time.Sleep(3 * time.Second)

	// Insert data directly on DOWNSTREAM - this has no foreign origin
	// Use explicit ID in a different range to avoid conflicts with replicated data
	downstreamDataConn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		helper.downstreamCfg.User, helper.downstreamCfg.Password, helper.downstreamCfg.Host,
		helper.downstreamCfg.Port, helper.downstreamCfg.Database))
	require.NoError(t, err)
	defer downstreamDataConn.Close(ctx)

	_, err = downstreamDataConn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (id, name, source) VALUES (2000, 'from_downstream', 'downstream')`, tableName))
	require.NoError(t, err)

	// Wait for the local event to be processed
	// The foreign origin event should be dropped, so we expect only 1 event from downstream
	// Give enough time for the transaction to be fully committed and replicated
	time.Sleep(8 * time.Second)
	events := pub.GetEvents()

	listenerCancel()
	<-listenerDone

	// Count events by source
	var fromPrimary, fromDownstream int
	for _, e := range events {
		if e.Table == tableName && e.Action == "INSERT" {
			if source, ok := e.Data["source"].(string); ok {
				if source == "primary" {
					fromPrimary++
				} else if source == "downstream" {
					fromDownstream++
				}
			}
		}
	}

	// With dropForeignOrigin=true, we should only see downstream events (no foreign origin)
	assert.Equal(t, 0, fromPrimary, "expected 0 events from primary (foreign origin should be dropped)")
	assert.Equal(t, 1, fromDownstream, "expected 1 event from downstream (local origin)")
}

// TestIntegration_KeepForeignOrigin tests that transactions from upstream databases
// are kept when dropForeignOrigin is disabled (default behavior)
func TestIntegration_KeepForeignOrigin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	helper := newForeignOriginTestHelper(t)
	helper.connect(ctx)
	defer helper.close(ctx)

	slotName := testSlotName + "_keeporigin"
	tableName := "test_keeporigin_" + fmt.Sprintf("%d", time.Now().UnixNano())
	publicationName := "test_pub_keep_" + fmt.Sprintf("%d", time.Now().UnixNano())
	subscriptionName := "test_sub_keep_" + fmt.Sprintf("%d", time.Now().UnixNano())

	// Cleanup before and after
	helper.cleanup(ctx, slotName, subscriptionName, publicationName, tableName)
	defer helper.cleanup(ctx, slotName, subscriptionName, publicationName, tableName)

	// Setup logical replication chain: primary -> downstream
	helper.setupReplication(ctx, tableName, publicationName, subscriptionName)

	pub := newTestPublisher()

	// Create listener on downstream with dropForeignOrigin = false (keep all)
	listener := helper.createListener(ctx, slotName, pub, false)

	listenerCtx, listenerCancel := context.WithCancel(ctx)
	listenerDone := make(chan error, 1)
	go func() {
		listenerDone <- listener.Process(listenerCtx)
	}()

	time.Sleep(3 * time.Second)

	// Insert data on PRIMARY - this will be replicated to downstream with foreign origin
	// Use explicit ID to avoid conflicts with downstream inserts
	_, err := helper.primaryConn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (id, name, source) VALUES (1000, 'from_primary', 'primary')`, tableName))
	require.NoError(t, err)

	// Wait for replication to downstream
	time.Sleep(3 * time.Second)

	// Insert data directly on DOWNSTREAM - this has no foreign origin
	// Use explicit ID in a different range to avoid conflicts with replicated data
	downstreamDataConn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		helper.downstreamCfg.User, helper.downstreamCfg.Password, helper.downstreamCfg.Host,
		helper.downstreamCfg.Port, helper.downstreamCfg.Database))
	require.NoError(t, err)
	defer downstreamDataConn.Close(ctx)

	_, err = downstreamDataConn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (id, name, source) VALUES (2000, 'from_downstream', 'downstream')`, tableName))
	require.NoError(t, err)

	// Wait for events - expect both events
	events := pub.WaitForEvents(2, 15*time.Second)

	listenerCancel()
	<-listenerDone

	// Count events by source
	var fromPrimary, fromDownstream int
	for _, e := range events {
		if e.Table == tableName && e.Action == "INSERT" {
			if source, ok := e.Data["source"].(string); ok {
				if source == "primary" {
					fromPrimary++
				} else if source == "downstream" {
					fromDownstream++
				}
			}
		}
	}

	// With dropForeignOrigin=false, we should see both events
	assert.Equal(t, 1, fromPrimary, "expected 1 event from primary (foreign origin kept)")
	assert.Equal(t, 1, fromDownstream, "expected 1 event from downstream (local origin)")
}
