package listener

import "testing"

func TestEvent_GetSubjectName(t *testing.T) {
	type fields struct {
		Scheme string
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
				Scheme: "public",
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
				Scheme: tt.fields.Scheme,
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
