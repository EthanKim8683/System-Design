package main

import (
	"log"
	"uuid"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{
			"type": "generate_ok",
			"id":   uuid.New(),
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
