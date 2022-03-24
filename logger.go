package zfs

import (
	"testing"
)

// Logger is an interface for logging
type Logger interface {
	WithField(k string, v interface{}) Logger
	WithFields(data map[string]interface{}) Logger
	WithError(err error) Logger
	Info(msg string)
	Infof(format string, args ...interface{})
	Error(msg string)
	Errorf(format string, args ...interface{})
}

// NoopLogger implements the Logger by doing nothing
type NoopLogger struct{}

func (n NoopLogger) WithField(k string, v interface{}) Logger {
	return n
}

func (n NoopLogger) WithFields(data map[string]interface{}) Logger {
	return n
}

func (n NoopLogger) WithError(err error) Logger {
	return n
}
func (n NoopLogger) Info(msg string)                           {}
func (n NoopLogger) Infof(format string, args ...interface{})  {}
func (n NoopLogger) Error(msg string)                          {}
func (n NoopLogger) Errorf(format string, args ...interface{}) {}

// TestLogger is a logger for testing
type TestLogger struct {
	t      *testing.T
	fields map[string]interface{}
}

func NewTestLogger(t *testing.T) Logger {
	return &TestLogger{
		t:      t,
		fields: make(map[string]interface{}),
	}
}

func (t *TestLogger) cloneFields() map[string]interface{} {
	fields := make(map[string]interface{}, len(t.fields))
	for k, v := range t.fields {
		fields[k] = v
	}
	return fields
}

func (t *TestLogger) WithField(k string, v interface{}) Logger {
	fields := t.cloneFields()
	fields[k] = v
	return &TestLogger{
		t:      t.t,
		fields: fields,
	}
}

func (t *TestLogger) WithFields(data map[string]interface{}) Logger {
	fields := t.cloneFields()
	for k, v := range data {
		fields[k] = v
	}
	return &TestLogger{
		t:      t.t,
		fields: fields,
	}
}

func (t *TestLogger) WithError(err error) Logger {
	fields := t.cloneFields()
	fields["error"] = err
	return &TestLogger{
		t:      t.t,
		fields: fields,
	}
}

func (t *TestLogger) Info(msg string) {
	t.Infof(msg)
}

func (t *TestLogger) Infof(format string, args ...interface{}) {
	t.t.Logf("[INF] "+format+" [%#v]", append(args, t.fields)...)
}

func (t *TestLogger) Error(msg string) {
	t.Errorf(msg)
}

func (t *TestLogger) Errorf(format string, args ...interface{}) {
	t.t.Logf("[ERR] "+format+" [%#v]", append(args, t.fields)...)
}
