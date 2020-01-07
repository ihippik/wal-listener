package listener

import "testing"

func TestEvent_GetSubjectName(t *testing.T) {
	type fields struct {
		TableName string
		Action    string
		Data      map[string]interface{}
	}
	type args struct {
		prefix string
		dbName string
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
				TableName: "users",
				Action:    "insert",
				Data:      nil,
			},
			args: args{
				prefix: "prefix_",
				dbName: "db_scum",
			},
			want: "prefix_db_scum_users",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Event{
				TableName: tt.fields.TableName,
				Action:    tt.fields.Action,
				Data:      tt.fields.Data,
			}
			if got := e.GetSubjectName(tt.args.prefix, tt.args.dbName); got != tt.want {
				t.Errorf("GetSubjectName() = %v, want %v", got, tt.want)
			}
		})
	}
}
