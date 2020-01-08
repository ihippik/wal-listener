package listener

import "testing"

func TestEvent_GetSubjectName(t *testing.T) {
	type fields struct {
		Schema string
		Table  string
		Action string
		Data   map[string]interface{}
	}
	type args struct {
		prefix string
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
				prefix: "prefix_",
			},
			want: "prefix_public_users",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Event{
				Schema: tt.fields.Schema,
				Table:  tt.fields.Table,
				Action: tt.fields.Action,
				Data:   tt.fields.Data,
			}
			if got := e.GetSubjectName(tt.args.prefix); got != tt.want {
				t.Errorf("GetSubjectName() = %v, want %v", got, tt.want)
			}
		})
	}
}
