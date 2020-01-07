package listener

import (
	"errors"
	"reflect"
	"testing"
)

func Test_serviceErr_Error(t *testing.T) {
	type fields struct {
		Caller string
		Err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "success",
			fields: fields{
				Caller: "hello()",
				Err:    errors.New("invalid username"),
			},
			want: "hello(): invalid username",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &serviceErr{
				Caller: tt.fields.Caller,
				Err:    tt.fields.Err,
			}
			if got := e.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newListenerError(t *testing.T) {
	type args struct {
		caller string
		err    error
	}
	tests := []struct {
		name string
		args args
		want *serviceErr
	}{
		{
			name: "success",
			args: args{
				caller: "hello()",
				err:    errors.New("invalid username"),
			},
			want: &serviceErr{
				Caller: "hello()",
				Err:    errors.New("invalid username"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newListenerError(tt.args.caller, tt.args.err); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newListenerError() = %v, want %v", got, tt.want)
			}
		})
	}
}
