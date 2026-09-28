package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	hub := &Hub{
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		ctx:        ctx,
	}

	go hub.hubWork()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		handler(hub, w, r)
	})

	srv := &http.Server{Addr: ":8080", Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	log.Println("Server started on :8080")
	err := srv.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		log.Printf("Server error: %v", err)
	}

	hub.wg.Wait()
}

func handler(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		log.Println("Failed to accept connection:", err)
		return
	}

	ctx, cancel := context.WithCancel(hub.ctx)
	client := &Client{
		hub:    hub,
		conn:   conn,
		send:   make(chan []byte, 256),
		ctx:    ctx,
		cancel: cancel,
	}

	select {
	case hub.register <- client:
	case <-hub.ctx.Done():
		conn.CloseNow()
		return
	}

	hub.wg.Add(2)
	go client.writePump()
	go client.readPump()
}
