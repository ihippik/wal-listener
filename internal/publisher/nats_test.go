package publisher

import (
	"github.com/ihippik/wal-listener/v2/apis"
	"testing"
)

func TestEvent_GetSubjectName(t *testing.T) {
	type fields struct {
		Schema string
		Table  string
		Action string
		Data   map[string]any
	}

	type args struct {
		cfg *apis.Config
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "success",
			fields: fields{
				Schema: "public",
				Table:  "users",
				Action: "insert",
				Data:   nil,
			},
			args: args{
				cfg: &apis.Config{
					Listener: &apis.ListenerCfg{
						TopicsMap: nil,
					},
					Publisher: &apis.PublisherCfg{TopicPrefix: "prefix_", Topic: "STREAM"},
				},
			},
			want: "STREAM.prefix_public_users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &apis.Event{
				Schema: tt.fields.Schema,
				Table:  tt.fields.Table,
				Action: tt.fields.Action,
				Data:   tt.fields.Data,
			}

			if got := e.SubjectName(tt.args.cfg); got != tt.want {
				t.Errorf("SubjectName() = %v, want %v", got, tt.want)
			}
		})
	}
}
