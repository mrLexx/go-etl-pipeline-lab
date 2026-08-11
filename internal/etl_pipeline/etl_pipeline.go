// etl_pipeline.go
//
//
//   genWeb   ─ normalize ─┐
//                         ├─ merge (fan-in) ─ Event ── batch(size + timeout) ─ consume
//   genApp   ─ normalize ─┘

package etlpipeline

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// --- Структуры источников: разная форма, близкий смысл ---

type WebEvent struct {
	SessionID string
	URL       string
	TS        int64 // unix seconds
}

type AppEvent struct {
	DeviceID  string
	Screen    string
	EventTime time.Time
}

// --- Общий нормализованный вид ---

type Event struct {
	Source string // web | app

	UserID string // sessions | device
	Action string // url | screen

	At time.Time
}

// --- Генераторы ---

func genWeb(ctx context.Context, every time.Duration) <-chan WebEvent {
	out := make(chan WebEvent)
	go func() {
		defer close(out)
		t := time.NewTicker(every)

		defer t.Stop()
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				i++
				ev := WebEvent{
					SessionID: "web-" + strconv.Itoa(i),
					URL:       "/p/" + strconv.Itoa(rand.Intn(5)),
					TS:        time.Now().Unix(),
				}
				// Отправку тоже прикрываем ctx, иначе на отмене
				// горутина зависнет на out <- ev, если читателя уже нет.
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

func genApp(ctx context.Context, every time.Duration) <-chan AppEvent {
	out := make(chan AppEvent)

	go func() {
		defer close(out)

		t := time.NewTicker(every)
		defer t.Stop()
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				i++
				ev := AppEvent{
					DeviceID:  "dev-" + strconv.Itoa(i),
					Screen:    "screen_" + strconv.Itoa(rand.Intn(5)),
					EventTime: time.Now(),
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out
}

func consume(in <-chan []Event) {
	n := 0
	for b := range in {
		n++
		web, app := 0, 0
		for _, e := range b {
			switch e.Source {
			case "web":
				web++
			case "app":
				app++
			}
		}
		log.Printf("batch #%d: %d событий (web=%d app=%d)", n, len(b), web, app)
	}
	log.Printf("готово, всего батчей: %d", n)
}

func normalizeWeb(ctx context.Context, in <-chan WebEvent) <-chan Event {
	out := make(chan Event)

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
			case v, ok := <-in:
				if !ok {
					return
				}
				e := Event{
					Source: "web",
					UserID: v.SessionID,
					Action: v.URL,
					// At:     time.UnixMicro(v.TS),
					At: time.Unix(v.TS, 0),
				}
				select {
				case <-ctx.Done():
					return
				case out <- e:
				}
			}
		}
	}()

	return out
}

func normalizeApp(ctx context.Context, in <-chan AppEvent) <-chan Event {
	out := make(chan Event)

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
			case v, ok := <-in:
				if !ok {
					return
				}
				e := Event{
					Source: "app",
					UserID: v.DeviceID,
					Action: v.Screen,
					At:     v.EventTime,
				}

				select {
				case <-ctx.Done():
					return
				case out <- e:
				}

			}
		}
	}()

	return out
}

func redirect(ctx context.Context, in <-chan Event, out chan<- Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case v, ok := <-in:
			if !ok {
				return
			}
			select {
			case <-ctx.Done():
				return
			case out <- v:
			}
		}
	}
}

func merge(ctx context.Context, inputChannels ...<-chan Event) <-chan Event {
	var wg sync.WaitGroup

	out := make(chan Event)

	wg.Add(len(inputChannels))
	for _, ch := range inputChannels {
		go func() {
			defer wg.Done()
			redirect(ctx, ch, out)
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func mergePr(ctx context.Context, c1, c2 <-chan Event) <-chan Event {
	out := make(chan Event)

	go func() {
		defer close(out)

		select {

		case <-ctx.Done():
		case <-ctx.Done():
			return
		case v, ok := <-c1:
			if !ok {
				return
			}
			select {
			case <-ctx.Done():
				return
			case out <- v:
			}
		case v, ok := <-c2:
			if !ok {
				return
			}
			select {
			case <-ctx.Done():
				return
			case out <- v:
			}

		}
	}()

	return out
}

func batch(in <-chan Event, s int, t time.Duration) <-chan []Event {
	panic("реализовать")
}

func RunPipeline(
	ctx context.Context,
	web <-chan WebEvent,
	app <-chan AppEvent,

	bSize int,
	bTimeout time.Duration,
) <-chan []Event {
	panic("реализовать")
}
