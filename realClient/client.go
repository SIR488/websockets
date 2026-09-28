package main

import (
	"bufio"
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

type Client struct {
	conn   *websocket.Conn
	write  chan []byte
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, _, err := websocket.Dial(ctx, "ws://localhost:8080/ws", nil)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	client := &Client{
		conn:  conn,
		write: make(chan []byte, 256),
		ctx:   ctx,
	}

	client.wg.Add(3)
	go client.reader()
	go client.stdinReader()
	go client.writer()

	<-ctx.Done()
	client.wg.Wait()

	conn.Close(websocket.StatusNormalClosure, "Client shutting down")
	log.Println("Client exited properly.")
}

func (c *Client) reader() {
	defer c.wg.Done()
	defer c.cancel()

	for {
		_, data, err := c.conn.Read(c.ctx)
		if err != nil {
			if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				log.Println("Connection closed normally by server")
			} else {
				log.Printf("Read error: %v", err)
			}
			return
		}

		log.Printf("📩 Received: %s", string(data))
	}
}

func (c *Client) stdinReader() {
	defer c.wg.Done()

	scanner := bufio.NewScanner(os.Stdin)
	log.Println("Type a message and press Enter (Ctrl+C to exit):")

	for scanner.Scan() {
		message := scanner.Bytes()

		select {
		case c.write <- message:
		case <-c.ctx.Done():
			return
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading from stdin: %v", err)
	}

	c.cancel()
}

func (c *Client) writer() {
	defer c.wg.Done()
	defer c.cancel()

	for {
		select {
		case msg := <-c.write:
			wctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
			err := c.conn.Write(wctx, websocket.MessageText, msg)
			cancel()

			if err != nil {
				log.Printf("Write error: %v", err)
				return
			}

		case <-c.ctx.Done():
			return
		}
	}
}
