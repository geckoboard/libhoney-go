package transmission

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

func TestEventMsgpackIsEquivalentToStructEncoding(t *testing.T) {
	events := map[string]*Event{
		"span":     exampleSpan(),
		"nil data": {},
		"defaults": {SampleRate: 1, Data: map[string]any{"a": 1}},
		"all fields": {
			SampleRate: 5,
			Timestamp:  time.Date(2026, 1, 2, 3, 4, 5, 6, time.FixedZone("x", 3600)),
			Data: map[string]any{
				"nested": map[string]any{"b": []any{1, "two", nil}},
				"tagged": struct {
					Foo int64 `json:"f"`
				}{1},
				"bytes":    []byte("raw"),
				"duration": time.Second,
			},
		},
	}

	for name, ev := range events {
		t.Run(name, func(t *testing.T) {
			want, err := referenceEventMsgpack(ev)
			testOK(t, err)
			got, err := ev.MarshalMsgpack()
			testOK(t, err)
			testEquals(t, decodeMsgpack(t, got), decodeMsgpack(t, want))
		})
	}
}

func TestEventMsgpackIsDeterministic(t *testing.T) {
	ev := exampleSpan()
	ev.Data["nested"] = map[string]any{"a": 1, "b": 2, "c": 3, "d": 4}
	first, err := ev.MarshalMsgpack()
	testOK(t, err)
	for range 20 {
		again, err := ev.MarshalMsgpack()
		testOK(t, err)
		if !bytes.Equal(again, first) {
			t.Fatal("expected the same event to always encode to the same bytes")
		}
	}
}

func TestEncodeBatchMsgpDropsRejectedEvents(t *testing.T) {
	b := &batchAgg{responses: make(chan Response, 2), metrics: &nullMetrics{}}
	events := []*Event{
		{Data: map[string]any{"n": 1}},
		{Data: map[string]any{"bad": make(chan int)}, Metadata: "bad"},
		{Data: map[string]any{"n": 2}},
		{Data: map[string]any{"big": strings.Repeat("x", apiEventSizeMax)}, Metadata: "big"},
		{Data: map[string]any{"n": 3}},
	}

	var buf bytes.Buffer
	enc, n := b.encodeBatchMsgp(events, &buf)
	testEquals(t, n, 3)

	var decoded []map[string]any
	testOK(t, msgpack.Unmarshal(enc, &decoded))
	testEquals(t, len(decoded), 3)
	for i, ev := range decoded {
		testEquals(t, ev["data"], map[string]any{"n": int8(i + 1)})
	}
	if events[1] != nil || events[3] != nil {
		t.Error("expected rejected events to be nilled out")
	}
	testEquals(t, (<-b.responses).Metadata, "bad")
	testEquals(t, (<-b.responses).Metadata, "big")
}

func BenchmarkEncodeBatchMsgp(b *testing.B) {
	events := make([]*Event, 100)
	for i := range events {
		events[i] = exampleSpan()
	}
	b.Run("reference", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			// msgpack.Marshal on each event, as the batch encoder used to
			var buf bytes.Buffer
			for _, ev := range events {
				enc, _ := msgpack.Marshal(referenceMsgpackEvent{ev})
				buf.Write(enc)
			}
		}
	})
	b.Run("encodeBatchMsgp", func(b *testing.B) {
		b.ReportAllocs()
		agg := &batchAgg{}
		var buf bytes.Buffer
		for b.Loop() {
			buf.Reset()
			agg.encodeBatchMsgp(events, &buf)
		}
	})
}

func decodeMsgpack(t *testing.T, b []byte) any {
	t.Helper()
	var v any
	testOK(t, msgpack.Unmarshal(b, &v))
	return v
}

// exampleSpan resembles a tracing span for a database query, which is
// dominated by strings, numbers and long SQL text.
func exampleSpan() *Event {
	return &Event{
		SampleRate: 1,
		Timestamp:  time.Date(2026, 10, 9, 13, 56, 45, 769844785, time.UTC),
		Data: map[string]any{
			"name":               "QueryContext",
			"service.name":       "example",
			"trace.trace_id":     "0005238bd84585506c156404969fa9fb",
			"trace.span_id":      "2b09026a93e8c11f",
			"trace.parent_id":    "1db584c991d61a2e",
			"duration_ms":        1.03,
			"meta.type":          "db",
			"meta.beeline_ver":   "1.18.0",
			"meta.local_host":    "platform-production-i-094d46cd82d846adf",
			"aws.ecs.task.arn":   "arn:aws:ecs:us-east-1:123456789012:task/platform-production/45860589b5f94d90b92192cca002c130",
			"aws.instance_id":    "i-094d46cd82d846adf",
			"aws.region":         "us-east-1",
			"db.call":            "QueryContext",
			"db.conns_idle":      48,
			"db.conns_in_use":    2,
			"db.open_conns":      50,
			"db.wait_count":      int64(1341607),
			"db.wait_duration":   time.Duration(60221627891380),
			"db.query":           `SELECT "id", "title", "legacy_id", "organization_id", "sharing_hash", "timezone", "grid_layout", "created_at", "updated_at", "ip_restrictions", "folder_id", "owner_id" FROM "dashboards" WHERE (id = $1) LIMIT 1;`,
			"db.query_args":      []any{"dash_01K899G7MNWN5GTY8D9Y8EXBX7"},
			"process.gc.cycles":  uint32(41275),
			"process.mem_in_use": uint64(111999968),
			"process.uptime_s":   9.86,
		},
	}
}

// referenceEventMsgpack is the struct-based implementation that
// encodeMsgpack replaced, kept as the definition of the expected output.
func referenceEventMsgpack(e *Event) ([]byte, error) {
	tPointer := &(e.Timestamp)
	if e.Timestamp.IsZero() {
		tPointer = nil
	}
	sampleRate := e.SampleRate
	if sampleRate == 1 {
		sampleRate = 0
	}
	var buf bytes.Buffer
	encoder := msgpack.NewEncoder(&buf)
	encoder.SetCustomStructTag("json")
	err := encoder.Encode(struct {
		Data       map[string]any `msgpack:"data"`
		SampleRate uint           `msgpack:"samplerate,omitempty"`
		Timestamp  *time.Time     `msgpack:"time,omitempty"`
	}{e.Data, sampleRate, tPointer})
	return buf.Bytes(), err
}

type referenceMsgpackEvent struct{ *Event }

func (e referenceMsgpackEvent) MarshalMsgpack() ([]byte, error) {
	return referenceEventMsgpack(e.Event)
}
