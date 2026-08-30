// etl_pipeline.go
//
//
//   genWeb   ─ normalize ─┐
//                         ├─ merge (fan-in) ─ Event ── batch(size + timeout) ─ consume
//   genApp   ─ normalize ─┘

package etlpipeline

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"reflect"
	"strconv"
	"sync"
	"time"
)

// --- Структуры источников: разная форма, близкий смысл ---

// WebEvent структура источника web событий
type WebEvent struct {
	SessionID string
	URL       string
	TS        int64 // unix seconds
}

// AppEvent структура источника app событий
type AppEvent struct {
	DeviceID  string
	Screen    string
	EventTime time.Time
}

// --- Общий нормализованный вид ---

// Event нормализированная структура
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
					//nolint:gosec // для учебного проекта достаточно
					URL: "/p/" + strconv.Itoa(rand.IntN(5)),
					TS:  time.Now().Unix(),
				}
				// Отправку тоже прикрываем ctx, иначе на отмене
				// горутина зависнет на out <- ev, если читателя уже нет.
				select {
				case <-ctx.Done():
					return
				case out <- ev:
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
					DeviceID: "dev-" + strconv.Itoa(i),
					//nolint:gosec // для учебного проекта достаточно
					Screen:    "screen_" + strconv.Itoa(rand.IntN(5)),
					EventTime: time.Now(),
				}
				select {
				case <-ctx.Done():
					return
				case out <- ev:
				}
			}
		}
	}()

	return out
}

//nolint:unused // было в шаблоне
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
		slog.Info(fmt.Sprintf("batch #%d: %d событий (web=%d app=%d)", n, len(b), web, app))
	}
	slog.Info(fmt.Sprintf("готово, всего батчей: %d", n))
}

func normalizeWeb(ctx context.Context, in <-chan WebEvent) <-chan Event {
	out := make(chan Event)

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-in:
				if !ok {
					return
				}
				e := Event{
					Source: "web",
					UserID: v.SessionID,
					Action: v.URL,
					At:     time.Unix(v.TS, 0),
				}
				if !sendEvent(ctx, out, e) {
					return
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
				return
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

				if !sendEvent(ctx, out, e) {
					return
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
		go func(in <-chan Event) {
			defer wg.Done()
			redirect(ctx, in, out)
		}(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// waitForAny ожидаем с блокировкой события из любого канала.
// Сделано через reflect.SelectCase, так как у нас динамическое количество каналов
func waitForAny(ctx context.Context, inputChannels ...<-chan Event) (event Event, ok bool) {
	var cases []reflect.SelectCase

	cases = append(cases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	})
	idx := []int{-1}

	for i, ch := range inputChannels {
		if ch == nil {
			continue
		}

		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		})
		idx = append(idx, i)
	}

	chosen, val, ok := reflect.Select(cases)
	if chosen == 0 { // сработал ctx.Done()
		return Event{}, false
	}

	if !ok { // канал закрыт
		inputChannels[idx[chosen]] = nil
		return Event{}, false
	}

	return val.Interface().(Event), true
}

func hasActiveChannel(inputChannels ...<-chan Event) bool {
	for _, channel := range inputChannels {
		if channel != nil {
			return true
		}
	}

	return false
}

//nolint:gocognit // сложность оставлена для наглядности алгоритма
func mergePriority(ctx context.Context, inputChannels ...<-chan Event) <-chan Event {
	out := make(chan Event)

	go func() {
		defer close(out)

		for hasActiveChannel(inputChannels...) {
			// обработка без блокировки (через default) по приоритету, приоритет - по очереди
			progress := false

			for i, ch := range inputChannels {
				if ch == nil {
					continue
				}

				select {
				case <-ctx.Done():
					return
				case v, ok := <-ch:
					if !ok {
						inputChannels[i] = nil
						continue
					}
					if !sendEvent(ctx, out, v) {
						return
					}
					progress = true
				default:
				}

				if progress {
					break
				}
			}

			if progress {
				continue
			}

			// если к этому моменту все каналы nil - смысла работать нет
			if !hasActiveChannel(inputChannels...) {
				break
			}

			val, ok := waitForAny(ctx, inputChannels...)

			if !ok {
				continue
			}

			if !sendEvent(ctx, out, val) {
				return
			}
		}
	}()

	return out
}

func sendEvent(ctx context.Context, out chan<- Event, event Event) bool {
	select {
	case <-ctx.Done():
		return false
	case out <- event:
		return true
	}
}

//nolint:gocognit // сложность оставлена для наглядности алгоритма
func batch(in <-chan Event, s int, t time.Duration) <-chan []Event {
	out := make(chan []Event)

	go func() {
		defer close(out)

		b := make([]Event, 0, s)
		ticker := time.NewTicker(t)
		defer ticker.Stop()

		for {
			select {
			case e, ok := <-in:
				if !ok {
					if len(b) > 0 {
						out <- b
					}
					return
				}

				b = append(b, e)

				if len(b) == s {
					out <- b
					b = make([]Event, 0, s)
					ticker.Reset(t)
				}

			case <-ticker.C:
				if len(b) > 0 {
					out <- b
					b = make([]Event, 0, s)
				}
				ticker.Reset(t)
			}
		}
	}()

	return out
}

// RunPipeline запускает конвейер обработки событий из web- и app-источников.
// Нормализует события к общему типу Event, объединяет их в один поток
// и группирует в батчи заданного размера или по истечении таймаута.
func RunPipeline(
	ctx context.Context,
	web <-chan WebEvent,
	app <-chan AppEvent,

	bSize int,
	bTimeout time.Duration,
) <-chan []Event {
	mergedEvents := merge(
		ctx,
		normalizeApp(ctx, app),
		normalizeWeb(ctx, web),
	)

	return batch(mergedEvents, bSize, bTimeout)
}
