package main

import (
	"encoding/json"
	"log"
	"slices"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex
	type entry struct {
		priority int
		value    int
	}
	kv := make(map[int]entry)
	ts := 0

	idx := slices.Index(n.NodeIDs(), n.ID())
	priorityFromTS := func(ts int) int {
		return ts<<8 | idx
	}
	tsFromPriority := func(priority int) int {
		return priority >> 8
	}

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body struct {
			RawMsgs []json.RawMessage `json:"txn"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var txn [][]any
		fwdTxn := make(map[int]int)
		mu.Lock()
		priority := priorityFromTS(ts)
		for _, rawMsg := range body.RawMsgs {
			var op string
			var key, value int
			tup := []any{&op, &key, &value}
			if err := json.Unmarshal(rawMsg, &tup); err != nil {
				return err
			}

			if op == "r" {
				value = kv[key].value
			} else {
				kv[key] = entry{priority, value}
				fwdTxn[key] = value
			}
			txn = append(txn, []any{op, key, value})
		}
		ts++
		mu.Unlock()

		var g sync.WaitGroup
		for _, dest := range n.NodeIDs() {
			if dest == n.ID() {
				continue
			}

			g.Go(func() {
				for n.Send(dest, map[string]any{
					"type":     "fwd",
					"priority": priority,
					"txn":      fwdTxn,
				}) != nil {
				}
			})
		}

		return n.Reply(msg, map[string]any{
			"type": "txn_ok",
			"txn":  txn,
		})
	})

	n.Handle("fwd", func(msg maelstrom.Message) error {
		var body struct {
			Priority int         `json:"priority"`
			Txn      map[int]int `json:"txn"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		for key, value := range body.Txn {
			if body.Priority > kv[key].priority {
				kv[key] = entry{body.Priority, value}
			}
		}
		ts = max(ts, tsFromPriority(body.Priority)+1)
		mu.Unlock()

		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
