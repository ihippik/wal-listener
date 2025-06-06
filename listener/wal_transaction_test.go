package listener

import (
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ihippik/wal-listener/v2/config"

	"github.com/magiconair/properties/assert"
)

func TestWalTransaction_CreateActionData(t *testing.T) {
	type fields struct {
		LSN           int64
		BeginTime     *time.Time
		CommitTime    *time.Time
		RelationStore map[int32]RelationData
		Actions       []ActionData
	}
	type args struct {
		relationID int32
		oldRows    []TupleData
		newRows    []TupleData
		kind       ActionKind
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	now := time.Now()

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantA   ActionData
		wantErr bool
	}{
		{
			name: "success",
			fields: fields{
				LSN:        10,
				BeginTime:  &now,
				CommitTime: &now,
				RelationStore: map[int32]RelationData{
					10: {
						Schema: "public",
						Table:  "users",
						Columns: []Column{
							{
								log:       logger,
								name:      "id",
								value:     5,
								valueType: Int4OID,
								isKey:     true,
							},
						},
					},
				},
				Actions: nil,
			},
			args: args{
				relationID: 10,
				oldRows: []TupleData{
					{
						Value: []byte{56, 48},
					},
				},
				newRows: []TupleData{
					{
						Value: []byte{49, 49},
					},
				},
				kind: ActionKindUpdate,
			},
			wantA: ActionData{
				Schema: "public",
				Table:  "users",
				Kind:   ActionKindUpdate,
				OldColumns: []Column{
					{
						log:       logger,
						name:      "id",
						value:     80,
						valueType: Int4OID,
						isKey:     true,
					},
				},
				NewColumns: []Column{
					{
						log:       logger,
						name:      "id",
						value:     11,
						valueType: Int4OID,
						isKey:     true,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "relation not exists",
			fields: fields{
				LSN:        10,
				BeginTime:  &now,
				CommitTime: &now,
				RelationStore: map[int32]RelationData{
					11: {
						Schema: "public",
						Table:  "users",
						Columns: []Column{
							{
								log:       logger,
								name:      "id",
								value:     5,
								valueType: Int4OID,
								isKey:     true,
							},
						},
					},
				},
				Actions: nil,
			},
			args: args{
				relationID: 10,
				oldRows:    nil,
				newRows:    nil,
				kind:       ActionKindUpdate,
			},
			wantA:   ActionData{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := WalTransaction{
				log:                logger,
				LSN:                tt.fields.LSN,
				BeginTime:          tt.fields.BeginTime,
				emittedActionCount: 0,
				maxTransactionSize: 0,
				CommitTime:         tt.fields.CommitTime,
				RelationStore:      tt.fields.RelationStore,
				Actions:            tt.fields.Actions,
			}

			gotA, err := w.CreateActionData(tt.args.relationID, tt.args.oldRows, tt.args.newRows, tt.args.kind)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateActionData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(gotA, tt.wantA) {
				t.Errorf("CreateActionData() gotA = %v, want %v", gotA, tt.wantA)
			}
		})
	}
}

func TestColumn_AssertValue(t *testing.T) {
	type fields struct {
		name      string
		valueType int
		isKey     bool
	}
	type args struct {
		src []byte
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Column
	}{
		{
			name: "bool",
			fields: fields{
				name:      "isBool",
				valueType: BoolOID,
				isKey:     false,
			},
			args: args{
				src: []byte{116},
			},
			want: &Column{
				log:       logger,
				name:      "isBool",
				value:     true,
				valueType: 16,
				isKey:     false,
			},
		},
		{
			name: "int",
			fields: fields{
				name:      "name",
				valueType: Int2OID,
				isKey:     false,
			},
			args: args{
				src: []byte("555"),
			},
			want: &Column{
				log:       logger,
				name:      "name",
				value:     555,
				valueType: 21,
				isKey:     false,
			},
		},
		{
			name: "int8",
			fields: fields{
				name:      "name",
				valueType: Int8OID,
				isKey:     false,
			},
			args: args{
				src: []byte("555"),
			},
			want: &Column{
				log:       logger,
				name:      "name",
				value:     int64(555),
				valueType: 20,
				isKey:     false,
			},
		},
		{
			name: "text",
			fields: fields{
				name:      "name",
				valueType: TextOID,
				isKey:     false,
			},
			args: args{
				src: []byte{104, 101, 108, 108, 111},
			},
			want: &Column{
				log:       logger,
				name:      "name",
				value:     "hello",
				valueType: 25,
				isKey:     false,
			},
		},
		{
			name: "timestamp",
			fields: fields{
				name:      "created",
				valueType: TimestampOID,
				isKey:     false,
			},
			args: args{
				src: []byte("2022-08-27 17:44:01"),
			},
			want: &Column{
				log:       logger,
				name:      "created",
				value:     time.Date(2022, 8, 27, 17, 44, 1, 0, time.UTC),
				valueType: 1114,
				isKey:     false,
			},
		},
		{
			name: "timestamp with tz",
			fields: fields{
				name:      "created",
				valueType: TimestamptzOID,
				isKey:     false,
			},
			args: args{
				src: []byte("2022-08-27 17:44:58.083316+00"),
			},
			want: &Column{
				log:       logger,
				name:      "created",
				value:     time.Date(2022, 8, 27, 17, 44, 58, 83316000, time.UTC),
				valueType: 1184,
				isKey:     false,
			},
		},
		{
			name: "uuid",
			fields: fields{
				name:      "uuid",
				valueType: UUIDOID,
				isKey:     false,
			},
			args: args{
				src: []byte("600f37ed-1d88-4262-8be4-c3360e833f50"),
			},
			want: &Column{
				log:       logger,
				name:      "uuid",
				value:     uuid.MustParse("600f37ed-1d88-4262-8be4-c3360e833f50"),
				valueType: 2950,
				isKey:     false,
			},
		},
		{
			name: "jsonb",
			fields: fields{
				name:      "jsonb",
				valueType: JSONBOID,
				isKey:     false,
			},
			args: args{
				src: []byte(`{"name":"jsonb"}`),
			},
			want: &Column{
				log:       logger,
				name:      "jsonb",
				value:     map[string]any{"name": "jsonb"},
				valueType: 3802,
				isKey:     false,
			},
		},
		{
			name: "jsonb array string",
			fields: fields{
				name:      "jsonb",
				valueType: JSONBOID,
				isKey:     false,
			},
			args: args{
				src: []byte(`["tag1", "tag2"]`),
			},
			want: &Column{
				log:       logger,
				name:      "jsonb",
				value:     []any{"tag1", "tag2"},
				valueType: 3802,
				isKey:     false,
			},
		},
		{
			name: "jsonb nil",
			fields: fields{
				name:      "jsonb",
				valueType: JSONBOID,
				isKey:     false,
			},
			args: args{
				src: nil,
			},
			want: &Column{
				log:       logger,
				name:      "jsonb",
				value:     nil,
				valueType: 3802,
				isKey:     false,
			},
		},
		{
			name: "jsonb array empty",
			fields: fields{
				name:      "jsonb",
				valueType: JSONBOID,
				isKey:     false,
			},
			args: args{
				src: []byte(`[]`),
			},
			want: &Column{
				log:       logger,
				name:      "jsonb",
				value:     []any{},
				valueType: 3802,
				isKey:     false,
			},
		},
		{
			name: "date",
			fields: fields{
				name:      "date",
				valueType: DateOID,
				isKey:     false,
			},
			args: args{
				src: []byte(`1980-03-19`),
			},
			want: &Column{
				log:       logger,
				name:      "date",
				value:     "1980-03-19",
				valueType: 1082,
				isKey:     false,
			},
		},
		{
			name: "unknown",
			fields: fields{
				name:      "created",
				valueType: 1,
				isKey:     false,
			},
			args: args{
				src: []byte{50, 48, 50, 48, 45, 49, 48, 45, 49, 50},
			},
			want: &Column{
				log:       logger,
				name:      "created",
				value:     "2020-10-12",
				valueType: 1,
				isKey:     false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Column{
				log:       logger,
				name:      tt.fields.name,
				valueType: tt.fields.valueType,
				isKey:     tt.fields.isKey,
			}

			c.AssertValue(tt.args.src)

			assert.Equal(t, c, tt.want)
		})
	}
}

func TestWalTransaction_OriginTracking(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	tests := []struct {
		name              string
		dropForeignOrigin bool
		origin            string
		wantShouldDrop    bool
	}{
		{
			name:              "drop disabled - no origin",
			dropForeignOrigin: false,
			origin:            "",
			wantShouldDrop:    false,
		},
		{
			name:              "drop disabled - with origin",
			dropForeignOrigin: false,
			origin:            "origin_1",
			wantShouldDrop:    false,
		},
		{
			name:              "drop enabled - no origin",
			dropForeignOrigin: true,
			origin:            "",
			wantShouldDrop:    false,
		},
		{
			name:              "drop enabled - with origin",
			dropForeignOrigin: true,
			origin:            "origin_1",
			wantShouldDrop:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := NewWalTransaction(logger, nil, nil, nil, config.ExcludeStruct{}, map[string]string{}, tt.dropForeignOrigin, 0)

			if tt.origin != "" {
				tx.SetOrigin(tt.origin, tt.dropForeignOrigin)
			}

			if got := tx.ShouldDropMessage(); got != tt.wantShouldDrop {
				t.Errorf("ShouldDropMessage() = %v, want %v", got, tt.wantShouldDrop)
			}

			if tx.origin != tt.origin {
				t.Errorf("Origin = %v, want %v", tx.origin, tt.origin)
			}
		})
	}
}

func TestWalTransaction_Clear(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	now := time.Now()

	tx := NewWalTransaction(logger, nil, nil, nil, config.ExcludeStruct{}, map[string]string{}, true, 0)

	// Set up transaction state
	tx.LSN = 123
	tx.BeginTime = &now
	tx.CommitTime = &now
	tx.Actions = []ActionData{{Schema: "test", Table: "table", Kind: ActionKindInsert}}
	tx.SetOrigin("origin_1", true)

	// Verify state is set
	assert.Equal(t, int64(123), tx.LSN)
	assert.Equal(t, &now, tx.BeginTime)
	assert.Equal(t, &now, tx.CommitTime)
	assert.Equal(t, 1, len(tx.Actions))
	assert.Equal(t, "origin_1", tx.origin)
	assert.Equal(t, true, tx.ShouldDropMessage())

	// Clear transaction
	tx.Clear()

	// Verify everything is cleared including origin
	assert.Equal(t, int64(123), tx.LSN) // LSN should remain
	assert.Equal(t, (*time.Time)(nil), tx.BeginTime)
	assert.Equal(t, (*time.Time)(nil), tx.CommitTime)
	assert.Equal(t, ([]ActionData)(nil), tx.Actions)
	assert.Equal(t, "", tx.origin)
	assert.Equal(t, false, tx.ShouldDropMessage())
}

func TestWalTransaction_ClearActions(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	now := time.Now()

	tx := NewWalTransaction(logger, nil, nil, nil, config.ExcludeStruct{}, map[string]string{}, true, 0)

	// Set up transaction state
	tx.LSN = 123
	tx.BeginTime = &now
	tx.CommitTime = &now
	tx.Actions = []ActionData{{Schema: "test", Table: "table", Kind: ActionKindInsert}}
	tx.SetOrigin("origin_1", true)

	// Verify state is set
	assert.Equal(t, int64(123), tx.LSN)
	assert.Equal(t, &now, tx.BeginTime)
	assert.Equal(t, &now, tx.CommitTime)
	assert.Equal(t, 1, len(tx.Actions))
	assert.Equal(t, "origin_1", tx.origin)
	assert.Equal(t, true, tx.ShouldDropMessage())

	// Clear only actions
	tx.ClearActions()

	// Verify only actions are cleared, origin and other state preserved
	assert.Equal(t, int64(123), tx.LSN)
	assert.Equal(t, &now, tx.BeginTime)
	assert.Equal(t, &now, tx.CommitTime)
	assert.Equal(t, ([]ActionData)(nil), tx.Actions)
	assert.Equal(t, "origin_1", tx.origin)        // Origin should be preserved
	assert.Equal(t, true, tx.ShouldDropMessage()) // Should still drop
}

func TestWalTransaction_SetOrigin(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	tests := []struct {
		name              string
		initialOrigin     string
		newOrigin         string
		dropForeignOrigin bool
		wantOrigin        string
		wantShouldDrop    bool
	}{
		{
			name:              "set origin with drop enabled",
			initialOrigin:     "",
			newOrigin:         "origin_1",
			dropForeignOrigin: true,
			wantOrigin:        "origin_1",
			wantShouldDrop:    true,
		},
		{
			name:              "set origin with drop disabled",
			initialOrigin:     "",
			newOrigin:         "origin_1",
			dropForeignOrigin: false,
			wantOrigin:        "origin_1",
			wantShouldDrop:    false,
		},
		{
			name:              "override existing origin",
			initialOrigin:     "origin_1",
			newOrigin:         "origin_2",
			dropForeignOrigin: true,
			wantOrigin:        "origin_2",
			wantShouldDrop:    true,
		},
		{
			name:              "clear origin",
			initialOrigin:     "origin_1",
			newOrigin:         "",
			dropForeignOrigin: true,
			wantOrigin:        "",
			wantShouldDrop:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := NewWalTransaction(logger, nil, nil, nil, config.ExcludeStruct{}, map[string]string{}, tt.dropForeignOrigin, 0)

			// Set initial origin if provided
			if tt.initialOrigin != "" {
				tx.SetOrigin(tt.initialOrigin, tt.dropForeignOrigin)
			}

			// Set new origin
			tx.SetOrigin(tt.newOrigin, tt.dropForeignOrigin)

			if tx.origin != tt.wantOrigin {
				t.Errorf("Origin = %v, want %v", tx.origin, tt.wantOrigin)
			}

			if got := tx.ShouldDropMessage(); got != tt.wantShouldDrop {
				t.Errorf("ShouldDropMessage() = %v, want %v", got, tt.wantShouldDrop)
			}
		})
	}
}

func TestWalTransaction_ActionCountLimiting(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	tests := []struct {
		name            string
		maxSize         int
		actionCount     int
		wantActionCount int
		wantShouldLimit bool
	}{
		{
			name:            "no limit set",
			maxSize:         0,
			actionCount:     100,
			wantActionCount: 100,
			wantShouldLimit: false,
		},
		{
			name:            "under limit",
			maxSize:         10,
			actionCount:     5,
			wantActionCount: 5,
			wantShouldLimit: false,
		},
		{
			name:            "at limit",
			maxSize:         10,
			actionCount:     10,
			wantActionCount: 10,
			wantShouldLimit: true,
		},
		{
			name:            "over limit",
			maxSize:         10,
			actionCount:     15,
			wantActionCount: 10,
			wantShouldLimit: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := NewWalTransaction(logger, nil, nil, nil, config.ExcludeStruct{}, map[string]string{}, false, tt.maxSize)

			for i := 0; i < tt.actionCount; i++ {
				if tt.maxSize > 0 && tx.emittedActionCount >= tt.maxSize {
					break
				}
				tx.emittedActionCount++
			}

			assert.Equal(t, tt.wantActionCount, tx.emittedActionCount)

			shouldLimit := tt.maxSize > 0 && tx.emittedActionCount >= tt.maxSize
			assert.Equal(t, tt.wantShouldLimit, shouldLimit)
		})
	}
}

func TestWalTransaction_ClearResetsActionCount(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	tx := NewWalTransaction(logger, nil, nil, nil, config.ExcludeStruct{}, map[string]string{}, false, 10)
	tx.emittedActionCount = 5
	tx.droppedActionCount = 2
	tx.Actions = []ActionData{{Schema: "test", Table: "table", Kind: ActionKindInsert}}

	tx.Clear()

	assert.Equal(t, 0, tx.emittedActionCount)
	assert.Equal(t, 0, tx.droppedActionCount)
	assert.Equal(t, ([]ActionData)(nil), tx.Actions)
}

func TestWalTransaction_ClearActionsResetsActionCount(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	tx := NewWalTransaction(logger, nil, nil, nil, config.ExcludeStruct{}, map[string]string{}, false, 10)
	tx.emittedActionCount = 5
	tx.droppedActionCount = 3
	tx.Actions = []ActionData{{Schema: "test", Table: "table", Kind: ActionKindInsert}}

	tx.ClearActions()

	assert.Equal(t, 0, tx.emittedActionCount)
	assert.Equal(t, 0, tx.droppedActionCount)
	assert.Equal(t, ([]ActionData)(nil), tx.Actions)
}
