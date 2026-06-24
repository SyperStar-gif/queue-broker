package main

import (
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

type waiter struct {
	ch   chan string
	done chan struct{}
}

type queue struct {
	msgs []string
	wait []*waiter
}

type broker struct {
	mu sync.Mutex
	qs map[string]*queue
}

func (b *broker) q(name string) *queue {
	if b.qs == nil {
		b.qs = map[string]*queue{}
	}
	if q := b.qs[name]; q != nil {
		return q
	}
	q := &queue{}
	b.qs[name] = q
	return q
}

func (b *broker) put(name, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.q(name)
	for len(q.wait) > 0 {
		w := q.wait[0]
		q.wait = q.wait[1:]
		select {
		case w.ch <- msg:
			return
		case <-w.done:
			continue
		}
	}
	q.msgs = append(q.msgs, msg)
}

func (b *broker) get(name string, timeout time.Duration) (string, bool) {
	b.mu.Lock()
	q := b.q(name)
	if len(q.msgs) > 0 {
		msg := q.msgs[0]
		q.msgs = q.msgs[1:]
		b.mu.Unlock()
		return msg, true
	}
	if timeout == 0 {
		b.mu.Unlock()
		return "", false
	}
	w := &waiter{ch: make(chan string, 1), done: make(chan struct{})}
	q.wait = append(q.wait, w)
	b.mu.Unlock()

	select {
	case msg := <-w.ch:
		return msg, true
	case <-time.After(timeout):
		b.mu.Lock()
		for i, x := range q.wait {
			if x == w {
				q.wait = append(q.wait[:i], q.wait[i+1:]...)
				close(w.done)
				break
			}
		}
		select {
		case msg := <-w.ch:
			b.mu.Unlock()
			return msg, true
		default:
			b.mu.Unlock()
			return "", false
		}
	}
}

func main() {
	if len(os.Args) < 2 {
		os.Exit(1)
	}
	b := &broker{}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Path[1:]
		if name == "" {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodPut:
			q, err := url.ParseQuery(r.URL.RawQuery)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			vals, ok := q["v"]
			if !ok {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			b.put(name, vals[0])
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			var timeout time.Duration
			if s := r.URL.Query().Get("timeout"); s != "" {
				n, err := strconv.Atoi(s)
				if err != nil || n < 0 {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				timeout = time.Duration(n) * time.Second
			}
			if msg, ok := b.get(name, timeout); ok {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(msg))
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		default:
			http.NotFound(w, r)
		}
	})
	http.ListenAndServe(":"+os.Args[1], nil)
}
