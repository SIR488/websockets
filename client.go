package main

import (
	"context"
	"time"

	"github.com/coder/websocket"
)

const (
	writeTimeout = 5 * time.Second
	pingPeriod   = 30 * time.Second
	pingTimeout  = 5 * time.Second
)

type Client struct {
	conn   *websocket.Conn
	send   chan []byte
	hub    *Hub
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *Client) Send(payload []byte) bool {
	select {
	case c.send <- payload:
		return true
	default:
		return false
	}
}

func (c *Client) writePump() {
	defer c.hub.wg.Done()

	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	defer c.cancel()
	defer c.conn.CloseNow()

	for {
		select {
		case <-c.ctx.Done():
			c.conn.Close(websocket.StatusNormalClosure, "server shutdown")
			return
		case msg, ok := <-c.send:
			if !ok {
				return
			}
			wctx, cancel := context.WithTimeout(c.ctx, writeTimeout)
			err := c.conn.Write(wctx, websocket.MessageText, msg)
			cancel()
			if err != nil {
				return
			}
		case <-ticker.C:
			pctx, cancel := context.WithTimeout(c.ctx, pingTimeout)
			err := c.conn.Ping(pctx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func (c *Client) readPump() {
	defer c.hub.wg.Done()
	defer c.cancel()
	defer c.conn.CloseNow()

	for {
		_, data, err := c.conn.Read(c.ctx)
		if err != nil {
			return
		}

		select {
		case c.hub.broadcast <- data:
		case <-c.ctx.Done():
			return
		}
	}
}
