package client

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Mock driver.Conn implementation
type MockConn struct {
	mock.Mock
	driver.Conn
}

func (m *MockConn) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	ret := m.Called(ctx, query, args)
	return ret.Get(0).(driver.Rows), ret.Error(1)
}

func (m *MockConn) Ping(ctx context.Context) error {
	return m.Called(ctx).Error(0)
}

func (m *MockConn) Close() error {
	return m.Called().Error(0)
}

// Mock driver.Rows implementation
type MockRows struct {
	mock.Mock
	driver.Rows
}

func (m *MockRows) Next() bool {
	return m.Called().Bool(0)
}

func (m *MockRows) Scan(dest ...interface{}) error {
	return m.Called(dest).Error(0)
}

func (m *MockRows) Close() error {
	return m.Called().Error(0)
}

func (m *MockRows) Columns() []string {
	return m.Called().Get(0).([]string)
}

func (m *MockRows) ColumnTypes() []driver.ColumnType {
	return m.Called().Get(0).([]driver.ColumnType)
}

// TestNewClient is ignored for now
func TestNewClient(t *testing.T) {
	t.Skip("Skipping TestNewClient for now")
	opts := &Options{
		Addresses:        []string{"localhost:9000"},
		Database:         "default",
		Username:         "default",
		Password:         "",
		MaxOpenConns:     10,
		MaxIdleConns:     5,
		DialTimeout:      5 * time.Second,
		MaxExecutionTime: 60 * time.Second,
	}
	client, err := NewClient(opts)
	assert.NoError(t, err)
	assert.NotNil(t, client)
}

func TestClient_Query(t *testing.T) {
	mockConn := new(MockConn)
	mockRows := new(MockRows)
	client := &Client{conn: mockConn}

	ctx := context.Background()
	query := "SELECT * FROM test"
	args := []interface{}{}

	mockConn.On("Query", ctx, query, args).Return(mockRows, nil)
	mockRows.On("Next").Return(false)
	mockRows.On("Close").Return(nil)

	rows, err := client.Query(ctx, query, args...)
	assert.NoError(t, err)
	assert.NotNil(t, rows)
	assert.False(t, rows.Next())
	assert.NoError(t, rows.Close())

	mockConn.AssertExpectations(t)
	mockRows.AssertExpectations(t)
}

func TestClient_Close(t *testing.T) {
	mockConn := new(MockConn)
	client := &Client{conn: mockConn}

	mockConn.On("Close").Return(nil)

	err := client.Close()
	assert.NoError(t, err)

	mockConn.AssertExpectations(t)
}

func TestMetricsClient_QueryGauge(t *testing.T) {
	mockConn := new(MockConn)
	mockRows := new(MockRows)
	client := &MetricsClient{Client: &Client{conn: mockConn}}

	ctx := context.Background()
	query := "SELECT * FROM gauge"
	args := []interface{}{}

	mockConn.On("Query", ctx, query, args).Return(mockRows, nil)
	mockRows.On("Next").Return(false)
	mockRows.On("Close").Return(nil)

	rows, err := client.QueryGauge(ctx, query, args...)
	assert.NoError(t, err)
	assert.NotNil(t, rows)
	assert.False(t, rows.Next())
	assert.NoError(t, rows.Close())

	mockConn.AssertExpectations(t)
	mockRows.AssertExpectations(t)
}

func TestMetricsClient_QueryHistogram(t *testing.T) {
	mockConn := new(MockConn)
	mockRows := new(MockRows)
	client := &MetricsClient{Client: &Client{conn: mockConn}}

	ctx := context.Background()
	query := "SELECT * FROM histogram"
	args := []interface{}{}

	mockConn.On("Query", ctx, query, args).Return(mockRows, nil)
	mockRows.On("Next").Return(false)
	mockRows.On("Close").Return(nil)

	rows, err := client.QueryHistogram(ctx, query, args...)
	assert.NoError(t, err)
	assert.NotNil(t, rows)
	assert.False(t, rows.Next())
	assert.NoError(t, rows.Close())

	mockConn.AssertExpectations(t)
	mockRows.AssertExpectations(t)
}

func TestMetricsClient_QuerySum(t *testing.T) {
	mockConn := new(MockConn)
	mockRows := new(MockRows)
	client := &MetricsClient{Client: &Client{conn: mockConn}}

	ctx := context.Background()
	query := "SELECT * FROM sum"
	args := []interface{}{}

	mockConn.On("Query", ctx, query, args).Return(mockRows, nil)
	mockRows.On("Next").Return(false)
	mockRows.On("Close").Return(nil)

	rows, err := client.QuerySum(ctx, query, args...)
	assert.NoError(t, err)
	assert.NotNil(t, rows)
	assert.False(t, rows.Next())
	assert.NoError(t, rows.Close())

	mockConn.AssertExpectations(t)
	mockRows.AssertExpectations(t)
}

func TestMetricsClient_QuerySummary(t *testing.T) {
	mockConn := new(MockConn)
	mockRows := new(MockRows)
	client := &MetricsClient{Client: &Client{conn: mockConn}}

	ctx := context.Background()
	query := "SELECT * FROM summary"
	args := []interface{}{}

	mockConn.On("Query", ctx, query, args).Return(mockRows, nil)
	mockRows.On("Next").Return(false)
	mockRows.On("Close").Return(nil)

	rows, err := client.QuerySummary(ctx, query, args...)
	assert.NoError(t, err)
	assert.NotNil(t, rows)
	assert.False(t, rows.Next())
	assert.NoError(t, rows.Close())

	mockConn.AssertExpectations(t)
	mockRows.AssertExpectations(t)
}
