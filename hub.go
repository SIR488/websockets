package main

import (
	"context"
	"sync"
)

type Hub struct {
	register   chan *Client
	unregister chan *Client
	clients    map[*Client]bool
	broadcast  chan []byte
	ctx        context.Context

	wg sync.WaitGroup
}

func (h *Hub) hubWork() {
	for {
		select {
		case c := <-h.register:
			h.clients[c] = true

		case c := <-h.unregister:
			delete(h.clients, c)

		case data := <-h.broadcast:
			for c := range h.clients {
				if !c.Send(data) {
					c.cancel()
					delete(h.clients, c)
				}
			}

		case <-h.ctx.Done():
			return
		}
	}
}
