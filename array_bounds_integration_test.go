//go:build integration

package ads

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// Needs on the PLC (GVL on TC3, global list on TC2), static values:
//
//	aNegBound : ARRAY[-2..2] OF DINT       := -200, -100, 0, 100, 200
//	aMultiDim : ARRAY[0..1, 0..2] OF INT   := 11, 12, 13, 21, 22, 23
//	aNegMulti : ARRAY[-1..0, 1..2] OF INT  := -11, -12, 1, 2
func boundPrefix() string {
	if os.Getenv("ADS_TARGET_PORT") == "801" {
		return "."
	}
	return "GVL."
}

var boundWant = map[string]map[string]float64{
	"aNegBound": {"[-2]": -200, "[-1]": -100, "[0]": 0, "[1]": 100, "[2]": 200},
	"aMultiDim": {"[0,0]": 11, "[0,1]": 12, "[0,2]": 13, "[1,0]": 21, "[1,1]": 22, "[1,2]": 23},
	"aNegMulti": {"[-1,1]": -11, "[-1,2]": -12, "[0,1]": 1, "[0,2]": 2},
}

// flattenJSON maps each leaf of a JSON value to its index path, quotes dropped.
func flattenJSON(s string) (map[string]float64, error) {
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil, err
	}
	out := map[string]float64{}
	var walk func(path string, v any)
	walk = func(path string, v any) {
		switch x := v.(type) {
		case map[string]any:
			for k, c := range x {
				walk(path+strings.Trim(k, `"`), c)
			}
		case float64:
			out[path] = x
		default:
			out[path+"<?>"] = 0
		}
	}
	walk("", v)
	return out, nil
}

// Whole arrays need the datatype table: without LoadSymbols they do not decode.
func TestHardwareArrayBounds_NeedsLoadSymbols(t *testing.T) {
	sess := setupConnection(t)
	defer sess.Close()
	pre := boundPrefix()
	for n := range boundWant {
		if v, err := sess.ReadFromSymbol(context.Background(), pre+n); err == nil {
			t.Errorf("%s decoded without LoadSymbols as %q; drop this test once that works", n, v)
		}
	}
}

// Negative lower bounds and multi-dimensional arrays decode to the exact
// values after LoadSymbols, whole and per element.
func TestHardwareArrayBounds_Values(t *testing.T) {
	ctx := context.Background()
	pre := boundPrefix()
	for _, mode := range []string{"loadsymbols"} {
		t.Run(mode, func(t *testing.T) {
			sess := setupConnection(t)
			defer sess.Close()
			if err := sess.LoadSymbols(ctx); err != nil {
				t.Fatalf("LoadSymbols: %v", err)
			}
			names := make([]string, 0, len(boundWant))
			for n := range boundWant {
				names = append(names, pre+n)
			}
			got, err := sess.ReadMultipleSymbols(ctx, names)
			requireBatchOK(t, "read whole arrays", err)
			for n, want := range boundWant {
				flat, err := flattenJSON(got[pre+n])
				if err != nil {
					t.Errorf("%s: %q is not JSON: %v", n, got[pre+n], err)
					continue
				}
				if fmt.Sprint(flat) != fmt.Sprint(want) {
					t.Errorf("%s: read %v, want %v", n, flat, want)
				}
			}

			elems := map[string]string{
				pre + "aNegBound[-2]":   "-200",
				pre + "aNegBound[2]":    "200",
				pre + "aMultiDim[1,2]":  "23",
				pre + "aMultiDim[0,1]":  "12",
				pre + "aNegMulti[-1,2]": "-12",
				pre + "aNegMulti[0,1]":  "1",
			}
			en := make([]string, 0, len(elems))
			for n := range elems {
				en = append(en, n)
			}
			ev, err := sess.ReadMultipleSymbols(ctx, en)
			requireBatchOK(t, "read elements by PLC name", err)
			for n, want := range elems {
				if ev[n] != want {
					t.Errorf("%s: read %q, want %q", n, ev[n], want)
				}
			}

			// Every element name the session lists must resolve on the PLC.
			var listed []string
			sess.cache.lock.Lock()
			for n := range boundWant {
				if s := sess.cache.symbols[symbolKey(pre+n)]; s != nil {
					for _, c := range s.Children {
						listed = append(listed, c.FullName)
					}
				}
			}
			sess.cache.lock.Unlock()
			lv, err := sess.ReadMultipleSymbols(ctx, listed)
			requireBatchOK(t, "read listed element names", err)
			if len(lv) != 15 {
				t.Errorf("read %d listed elements, want 15: %v", len(lv), lv)
			}
		})
	}
}

// A subscription to a negative-bound 2-D array delivers the exact values
// once the datatype table is loaded.
func TestHardwareArrayBounds_Subscribe(t *testing.T) {
	name := boundPrefix() + "aNegMulti"
	sess := setupConnection(t)
	defer sess.Close()
	if err := sess.LoadSymbols(context.Background()); err != nil {
		t.Fatalf("LoadSymbols: %v", err)
	}
	ch := make(chan *Update, 4)
	h, err := sess.AddSymbolNotification(context.Background(), name, 100*time.Millisecond, 100*time.Millisecond, TransModeServerCycle, ch)
	if err != nil {
		t.Fatalf("subscribe %s: %v", name, err)
	}
	defer func() { _ = sess.DeleteDeviceNotification(context.Background(), h) }()
	select {
	case u := <-ch:
		flat, err := flattenJSON(u.Value)
		if err != nil || fmt.Sprint(flat) != fmt.Sprint(boundWant["aNegMulti"]) {
			t.Errorf("first sample %q (%v), want %v", u.Value, err, boundWant["aNegMulti"])
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("no sample for %s within 10s", name)
	}
}
