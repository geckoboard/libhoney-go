package transmission

import (
	"bytes"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

type Event struct {
	// APIKey, if set, overrides whatever is found in Config
	APIKey string
	// Dataset, if set, overrides whatever is found in Config
	Dataset string
	// SampleRate, if set, overrides whatever is found in Config
	SampleRate uint
	// APIHost, if set, overrides whatever is found in Config
	APIHost string
	// Timestamp, if set, specifies the time for this event. If unset, defaults
	// to Now()
	Timestamp time.Time
	// Metadata is a field for you to add in data that will be handed back to you
	// on the Response object read off the Responses channel. It is not sent to
	// Honeycomb with the event.
	Metadata interface{}

	// Data contains the content of the event (all the fields and their values)
	Data map[string]interface{}
}

// Marshaling an Event for batching up to the Honeycomb servers. Omits fields
// that aren't specific to this particular event, and allows for behavior like
// omitempty'ing a zero'ed out time.Time.
func (e *Event) MarshalJSON() ([]byte, error) {
	var keys []string
	return e.appendJSON(nil, &keys)
}

func (e *Event) MarshalMsgpack() ([]byte, error) {
	var buf bytes.Buffer
	var keys []string
	if err := e.encodeMsgpack(newMsgpackEncoder(&buf), &keys); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// encodeMsgpack writes the event's wire msgpack to enc, so that a whole batch
// can share one encoder, buffer and slice for sorting keys.
func (e *Event) encodeMsgpack(enc *msgpack.Encoder, keys *[]string) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("msgpack panic: %v, trying to encode: %#v", p, e)
		}
	}()

	// don't include sample rate if it's 1; this is the default
	hasSampleRate := e.SampleRate > 1
	hasTime := !e.Timestamp.IsZero()
	fields := 1
	if hasSampleRate {
		fields++
	}
	if hasTime {
		fields++
	}
	if err := enc.EncodeMapLen(fields); err != nil {
		return err
	}
	if err := enc.EncodeString("data"); err != nil {
		return err
	}
	if err := encodeSortedMap(enc, e.Data, keys); err != nil {
		return err
	}
	if hasSampleRate {
		if err := enc.EncodeString("samplerate"); err != nil {
			return err
		}
		if err := enc.EncodeUint(uint64(e.SampleRate)); err != nil {
			return err
		}
	}
	if hasTime {
		if err := enc.EncodeString("time"); err != nil {
			return err
		}
		if err := enc.EncodeTime(e.Timestamp); err != nil {
			return err
		}
	}
	return nil
}

// newMsgpackEncoder sorts map keys so that similar events encode alike, which
// lets them compress well together.
func newMsgpackEncoder(w io.Writer) *msgpack.Encoder {
	enc := msgpack.NewEncoder(w)
	enc.SetCustomStructTag("json")
	enc.SetSortMapKeys(true)
	return enc
}

// encodeSortedMap writes m with its keys in sorted order, so that similar
// events encode alike and compress well together.
func encodeSortedMap(enc *msgpack.Encoder, m map[string]interface{}, keys *[]string) error {
	if m == nil {
		return enc.EncodeNil()
	}
	*keys = (*keys)[:0]
	for k := range m {
		*keys = append(*keys, k)
	}
	slices.Sort(*keys)
	if err := enc.EncodeMapLen(len(*keys)); err != nil {
		return err
	}
	for _, k := range *keys {
		if err := enc.EncodeString(k); err != nil {
			return err
		}
		if err := enc.Encode(m[k]); err != nil {
			return err
		}
	}
	return nil
}
