package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const pollCount = 5
const logOffsetFmt = "log-offset-%s"
const logMsgFmt = "log-msg-%s-%d"
const offsetFmt = "offset-%s"

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)

	ctx := context.Background()

	n.Handle("send", func(msg maelstrom.Message) error {
		var body struct {
			Key string `json:"key"`
			Msg int    `json:"msg"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		logOffsetKey := fmt.Sprintf(logOffsetFmt, body.Key)
		var offset int
		for {
			var err error
			offset, err = kv.ReadInt(ctx, logOffsetKey)
			if err != nil {
				var rpcErr *maelstrom.RPCError
				if !errors.As(err, &rpcErr) {
					return err
				}
				if rpcErr.Code != maelstrom.KeyDoesNotExist {
					return err
				}
			}

			if err := kv.CompareAndSwap(ctx, logOffsetKey, offset, offset+1, true); err != nil {
				var rpcErr *maelstrom.RPCError
				if !errors.As(err, &rpcErr) {
					return err
				}
				if rpcErr.Code == maelstrom.PreconditionFailed {
					continue
				}
				return err
			}
			break
		}

		logMsgKey := fmt.Sprintf(logMsgFmt, body.Key, offset)
		if err := kv.Write(ctx, logMsgKey, body.Msg); err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body struct {
			Offsets map[string]int `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		logMsgs := make(map[string][][]int)
		for key, offset := range body.Offsets {
			for i := offset; i < offset+pollCount; i++ {
				logMsgKey := fmt.Sprintf(logMsgFmt, key, i)
				logMsg, err := kv.ReadInt(ctx, logMsgKey)
				if err != nil {
					var rpcErr *maelstrom.RPCError
					if !errors.As(err, &rpcErr) {
						return err
					}
					if rpcErr.Code == maelstrom.KeyDoesNotExist {
						continue
					}
					return err
				}
				logMsgs[key] = append(logMsgs[key], []int{i, logMsg})
			}
		}

		return n.Reply(msg, map[string]any{
			"type": "poll_ok",
			"msgs": logMsgs,
		})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body struct {
			Offsets map[string]int `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for key, offset := range body.Offsets {
			offsetKey := fmt.Sprintf(offsetFmt, key)
			if err := kv.Write(ctx, offsetKey, offset); err != nil {
				return err
			}
		}

		return n.Reply(msg, map[string]any{
			"type": "commit_offsets_ok",
		})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body struct {
			Keys []string `json:"keys"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := make(map[string]int)
		for _, key := range body.Keys {
			offsetKey := fmt.Sprintf(offsetFmt, key)
			offset, err := kv.ReadInt(ctx, offsetKey)
			if err != nil {
				var rpcErr *maelstrom.RPCError
				if !errors.As(err, &rpcErr) {
					return err
				}
				if rpcErr.Code == maelstrom.KeyDoesNotExist {
					continue
				}
				return err
			}
			offsets[key] = offset
		}

		return n.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": offsets,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
