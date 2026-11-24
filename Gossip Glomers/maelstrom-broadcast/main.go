package main

import (
	"encoding/json"
	"log"
	"maps"
	"slices"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var mu sync.RWMutex
	var topo []string
	data := make(map[any]struct{})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		_, ok := data[body["message"]]
		if !ok {
			data[body["message"]] = struct{}{}
		}
		mu.Unlock()

		if !ok {
			var wg sync.WaitGroup
			for _, dest := range topo {
				wg.Go(func() {
					for n.Send(dest, msg.Body) != nil {
					}
				})
			}
			wg.Wait()
		}

		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		res := make(map[string]any)
		res["type"] = "read_ok"
		mu.RLock()
		res["messages"] = slices.Collect(maps.Keys(data))
		mu.RUnlock()
		return n.Reply(msg, res)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body struct {
			Topo map[string][]string `json:"topology"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		topo = body.Topo[n.ID()]
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
