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
					StreamName:  "stream",
					TopicPrefix: "prefix",
				},
			},
			wantErr: nil,
		},
		{
			name: "bad listener cfg",
			fields: fields{
				Listener: ListenerCfg{
					SlotName:          "",
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
					StreamName:  "stream",
					TopicPrefix: "prefix",
				},
			},
			wantErr: errors.New("Listener.RefreshConnection: non zero value required;Listener.SlotName: non zero value required"),
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
					StreamName:  "stream",
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
					StreamName:  "stream",
					TopicPrefix: "prefix",
				},
			},
			wantErr: errors.New("Nats.Address: non zero value required"),
		},
		{
			name: "empty nats stream cfg",
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
					TopicPrefix: "prefix",
				},
			},
			wantErr: errors.New("Nats.StreamName: non zero value required"),
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
			if err != nil && assert.Error(t, tt.wantErr, err.Error()) {
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				assert.Nil(t, tt.wantErr)
			}
		})
	}
}
