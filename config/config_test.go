package config

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_Validate(t *testing.T) {
	type fields struct {
		Listener ListenerCfg
		Database DatabaseCfg
		Nats     NatsCfg
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr error
	}{
		{
			name: "success",
			fields: fields{
				Listener: ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Nats: NatsCfg{
					Address:     "addr",
					ClusterID:   "cluster",
					ClientID:    "client",
					TopicPrefix: "prefix",
				},
			},
			wantErr: nil,
		},
		{
			name: "bad listener cfg",
			fields: fields{
				Listener: ListenerCfg{
					HeartbeatInterval: 10,
				},
				Database: DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Nats: NatsCfg{
					Address:     "addr",
					ClusterID:   "cluster",
					ClientID:    "client",
					TopicPrefix: "prefix",
				},
			},
			wantErr: errors.New("Listener.SlotName: non zero value required;Listener.AckTimeout: non zero value required;Listener.RefreshConnection: non zero value required"),
		},
		{
			name: "bad db cfg",
			fields: fields{
				Listener: ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: DatabaseCfg{
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Nats: NatsCfg{
					Address:     "addr",
					ClusterID:   "cluster",
					ClientID:    "client",
					TopicPrefix: "prefix",
				},
			},
			wantErr: errors.New("Database.Host: non zero value required;Database.Port: non zero value required"),
		},
		{
			name: "empty nats addr cfg",
			fields: fields{
				Listener: ListenerCfg{
					SlotName:          "slot",
					AckTimeout:        10,
					RefreshConnection: 10,
					HeartbeatInterval: 10,
				},
				Database: DatabaseCfg{
					Host:     "host",
					Port:     10,
					Name:     "db",
					User:     "usr",
					Password: "pass",
				},
				Nats: NatsCfg{
					ClusterID:   "cluster",
					ClientID:    "client",
					TopicPrefix: "prefix",
				},
			},
			wantErr: errors.New("Nats.Address: non zero value required"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Config{
				Listener: tt.fields.Listener,
				Database: tt.fields.Database,
				Nats:     tt.fields.Nats,
			}
			err := c.Validate()
			if err == nil {
				assert.Nil(t, tt.wantErr)
			} else {
				assert.EqualError(t, tt.wantErr, err.Error())
			}
		})
	}
}
