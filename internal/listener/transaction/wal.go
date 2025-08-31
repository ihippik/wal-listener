package transaction

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/ihippik/wal-listener/v2/internal/config"
	"github.com/ihippik/wal-listener/v2/internal/publisher"
	"github.com/ihippik/wal-listener/v2/internal/transformer"
)

type monitor interface {
	IncFilterSkippedEvents(table string)
}

type transformerEngine interface {
	Transform(script string, data, oldData map[string]any, action string) (map[string]any, error)
}

// WAL transaction specified WAL message.
type WAL struct {
	log           *slog.Logger
	monitor       monitor
	LSN           int64
	BeginTime     *time.Time
	CommitTime    *time.Time
	RelationStore map[int32]RelationData
	Actions       []ActionData
	pool          *sync.Pool
	transformers  map[config.TransformationEngineType]transformerEngine
}

var errRelationNotFound = errors.New("relation not found")

// NewWAL create and initialize new WAL transaction.
func NewWAL(log *slog.Logger, pool *sync.Pool, monitor monitor) *WAL {
	const aproxData = 300

	return &WAL{
		pool:          pool,
		log:           log,
		monitor:       monitor,
		RelationStore: make(map[int32]RelationData),
		Actions:       make([]ActionData, 0, aproxData),
		transformers: map[config.TransformationEngineType]transformerEngine{
			config.TransformationEngineTypeJS: transformer.NewJSPool(),
		},
	}
}

// Clear transaction data.
func (w *WAL) Clear() {
	w.CommitTime = nil
	w.BeginTime = nil
	w.Actions = nil
}

func (w *WAL) RetrieveEvent(event *publisher.Event) {
	w.pool.Put(event)
}

func (w *WAL) getPoolEvent() *publisher.Event {
	return w.pool.Get().(*publisher.Event)
}

// CreateActionData create action from WAL message data.
func (w *WAL) CreateActionData(
	relationID int32,
	oldRows []TupleData,
	newRows []TupleData,
	kind ActionKind,
) (a ActionData, err error) {
	rel, ok := w.RelationStore[relationID]
	if !ok {
		return a, errRelationNotFound
	}

	a = ActionData{
		Schema: rel.Schema,
		Table:  rel.Table,
		Kind:   kind,
	}

	oldColumns := make([]Column, 0, len(oldRows))

	for num, row := range oldRows {
		column := InitColumn(
			w.log,
			rel.Columns[num].name,
			nil,
			rel.Columns[num].valueType,
			rel.Columns[num].isKey,
		)

		column.AssertValue(row.Value)
		oldColumns = append(oldColumns, column)
	}

	a.OldColumns = oldColumns

	newColumns := make([]Column, 0, len(newRows))

	for num, row := range newRows {
		column := InitColumn(
			w.log,
			rel.Columns[num].name,
			nil,
			rel.Columns[num].valueType,
			rel.Columns[num].isKey,
		)
		column.AssertValue(row.Value)
		newColumns = append(newColumns, column)
	}

	a.NewColumns = newColumns

	return a, nil
}

// CreateEventsWithFilter filter WAL message by table,
// action and create events for each value.
func (w *WAL) CreateEventsWithFilter(
	ctx context.Context,
	tableMap map[string][]string,
	transformationMap map[string]config.Transformation,
) <-chan *publisher.Event {
	output := make(chan *publisher.Event)

	go func(ctx context.Context) {
		for _, item := range w.Actions {
			if err := ctx.Err(); err != nil {
				w.log.Debug("create events with filter: context canceled")
				break
			}

			dataOld := make(map[string]any, len(item.OldColumns))

			for _, val := range item.OldColumns {
				dataOld[val.name] = val.value
			}

			data := make(map[string]any, len(item.NewColumns))

			for _, val := range item.NewColumns {
				data[val.name] = val.value
			}

			var foundTransformation *config.Transformation
			for tableName, transformation := range transformationMap {
				if item.Table == tableName {
					foundTransformation = &transformation
				}
			}

			if foundTransformation != nil {
				transformer, ok := w.transformers[foundTransformation.Type]
				if ok {
					transformedData, err := transformer.Transform(
						foundTransformation.Script,
						data,
						dataOld,
						item.Kind.string(),
					)
					if err != nil {
						w.log.Error(
							"wal-message error during data transformation",
							slog.String("schema", item.Schema),
							slog.String("table", item.Table),
							slog.String("action", string(item.Kind)),
							slog.String("error", err.Error()),
						)
						continue
					}

					data = transformedData
				} else {
					w.log.Debug(
						"transformer could not found for wal-message",
						slog.String("schema", item.Schema),
						slog.String("table", item.Table),
						slog.String("action", string(item.Kind)),
						slog.String("transformer", string(foundTransformation.Type)),
					)
				}
			}

			event := w.getPoolEvent()

			event.ID = uuid.New()
			event.Schema = item.Schema
			event.Table = item.Table
			event.Action = item.Kind.string()
			event.Data = data
			event.DataOld = dataOld
			event.EventTime = *w.CommitTime

			actions, validTable := tableMap[item.Table]

			validAction := inArray(actions, item.Kind.string())
			if validTable && validAction {
				output <- event
				continue
			}

			w.monitor.IncFilterSkippedEvents(item.Table)

			w.log.Debug(
				"wal-message was skipped by filter",
				slog.String("schema", item.Schema),
				slog.String("table", item.Table),
				slog.String("action", string(item.Kind)),
			)
		}

		close(output)
	}(ctx)

	return output
}

// inArray checks whether the value is in an array.
func inArray(arr []string, value string) bool {
	for _, v := range arr {
		if strings.EqualFold(v, value) {
			return true
		}
	}

	return false
}
