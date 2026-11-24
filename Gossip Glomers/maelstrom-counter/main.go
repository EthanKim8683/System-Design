package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const key = "value"

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	ctx := context.Background()

	n.Handle("add", func(msg maelstrom.Message) (err error) {
		var body struct {
			Delta int `json:"delta"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for {
			value, err := kv.ReadInt(ctx, key)
			var rpcErr *maelstrom.RPCError
			if !errors.As(err, &rpcErr) {
				return err
			}
			if rpcErr.Code != maelstrom.KeyDoesNotExist {
				return err
			}

			err = kv.CompareAndSwap(ctx, key, value, value+body.Delta, true)
			if err == nil {
				break
			}

			if !errors.As(err, &rpcErr) {
				return err
			}
			if rpcErr.Code != maelstrom.PreconditionFailed {
				return err
			}
		}

		return n.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		value, err := kv.ReadInt(ctx, key)
		var rpcErr *maelstrom.RPCError
		if !errors.As(err, &rpcErr) {
			return err
		}
		if rpcErr.Code != maelstrom.KeyDoesNotExist {
			return err
		}

		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": value,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
