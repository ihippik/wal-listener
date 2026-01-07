package config

import (
	"testing"

	scfg "github.com/ihippik/config"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	type fields struct {
		Logger    *scfg.Logger
		Listener  *ListenerCfg
		Database  *DatabaseCfg
		Publisher *PublisherCfg
	}

	tests := []struct {
		name    string
		fields  fields
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "success",
			fields: fields{
				Logger: &scfg.Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:        "kafka",
					Address:     "addr",
					Topic:       "stream",
					TopicPrefix: "prefix",
				},
			},
			wantErr: require.NoError,
		},
		{
			name: "bad listener cfg",
			fields: fields{
				Logger: &scfg.Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "",
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:        "kafka",
					Address:     "addr",
					Topic:       "stream",
					TopicPrefix: "prefix",
				},
			},
			wantErr: require.Error,
		},
		{
			name: "bad db cfg",
			fields: fields{
				Logger: &scfg.Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Type:        "kafka",
					Address:     "addr",
					Topic:       "stream",
					TopicPrefix: "prefix",
				},
			},
			wantErr: require.Error,
		},
		{
			name: "empty publisher kind",
			fields: fields{
				Logger: &scfg.Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: &DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Publisher: &PublisherCfg{
					Topic:       "stream",
					TopicPrefix: "prefix",
				},
			},
			wantErr: require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Config{
				Logger:    tt.fields.Logger,
				Listener:  tt.fields.Listener,
				Database:  tt.fields.Database,
				Publisher: tt.fields.Publisher,
			}

			err := c.Validate()
			tt.wantErr(t, err)
		})
	}
}

func TestDatabaseCfg_TLS(t *testing.T) {
	tests := []struct {
		name     string
		database *DatabaseCfg
		wantErr  require.ErrorAssertionFunc
	}{
		{
			name: "TLS disabled - valid config",
			database: &DatabaseCfg{
				Host:      "host",
				Port:      5432,
				Name:      "db",
				User:      "usr",
				Password:  "pass",
				EnableTLS: false,
			},
			wantErr: require.NoError,
		},
		{
			name: "TLS enabled - valid config with require mode",
			database: &DatabaseCfg{
				Host:      "host",
				Port:      5432,
				Name:      "db",
				User:      "usr",
				Password:  "pass",
				EnableTLS: true,
				SSLMode:   "require",
			},
			wantErr: require.NoError,
		},
		{
			name: "TLS enabled - valid config with verify-ca mode",
			database: &DatabaseCfg{
				Host:      "host",
				Port:      5432,
				Name:      "db",
				User:      "usr",
				Password:  "pass",
				EnableTLS: true,
				SSLMode:   "verify-ca",
			},
			wantErr: require.NoError,
		},
		{
			name: "TLS enabled - valid config with verify-full mode",
			database: &DatabaseCfg{
				Host:      "host",
				Port:      5432,
				Name:      "db",
				User:      "usr",
				Password:  "pass",
				EnableTLS: true,
				SSLMode:   "verify-full",
			},
			wantErr: require.NoError,
		},
		{
			name: "TLS enabled - valid config with empty SSL mode (defaults to require)",
			database: &DatabaseCfg{
				Host:      "host",
				Port:      5432,
				Name:      "db",
				User:      "usr",
				Password:  "pass",
				EnableTLS: true,
				SSLMode:   "",
			},
			wantErr: require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Config{
				Logger: &scfg.Logger{
					Level: "info",
				},
				Listener: &ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: tt.database,
				Publisher: &PublisherCfg{
					Type:        "kafka",
					Address:     "addr",
					Topic:       "stream",
					TopicPrefix: "prefix",
				},
			}

			err := c.Validate()
			tt.wantErr(t, err)
		})
	}
}
