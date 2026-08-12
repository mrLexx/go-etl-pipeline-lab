package etlpipeline

import (
	"context"
	"sort"
	"strconv"
	"testing"
	"time"
)

// collectBatches дренит канал батчей до закрытия и возвращает все батчи.
// Защищён общим дедлайном, чтобы тест не висел вечно при ошибке в pipeline.
func collectBatches(t *testing.T, ch <-chan []Event, deadline time.Duration) [][]Event {
	t.Helper()
	var batches [][]Event
	timeout := time.After(deadline)
	for {
		select {
		case b, ok := <-ch:
			if !ok {
				return batches
			}
			batches = append(batches, b)
		case <-timeout:
			t.Fatalf("канал батчей не закрылся за %s (собрано %d батчей)", deadline, len(batches))
			return batches
		}
	}
}

func countEvents(batches [][]Event) int {
	n := 0
	for _, b := range batches {
		n += len(b)
	}
	return n
}

// --- Нормализаторы ---

func TestNormalizeWeb(t *testing.T) {
	in := make(chan WebEvent, 1)
	in <- WebEvent{SessionID: "s1", URL: "/checkout", TS: 1700000000}
	close(in)

	out := normalizeWeb(context.Background(), in)

	got, ok := <-out
	if !ok {
		t.Fatal("ожидали событие, канал закрыт")
	}
	want := Event{Source: "web", UserID: "s1", Action: "/checkout", At: time.Unix(1700000000, 0)}
	if got != want {
		t.Errorf("normalizeWeb = %+v, want %+v", got, want)
	}
	if _, ok := <-out; ok {
		t.Error("ожидали, что выходной канал закроется после вычитки входа")
	}
}

func TestNormalizeApp(t *testing.T) {
	now := time.Unix(1700000123, 0)
	in := make(chan AppEvent, 1)
	in <- AppEvent{DeviceID: "d1", Screen: "home", EventTime: now}
	close(in)

	out := normalizeApp(context.Background(), in)

	got := <-out
	want := Event{Source: "app", UserID: "d1", Action: "home", At: now}
	if got != want {
		t.Errorf("normalizeApp = %+v, want %+v", got, want)
	}
}

// --- merge (fan-in) ---

func TestMerge_AllEventsArriveAndChannelCloses(t *testing.T) {
	c1 := make(chan Event)
	c2 := make(chan Event)
	c3 := make(chan Event)
	out := merge(context.Background(), c1, c2, c3)

	go func() {
		for i := 0; i < 5; i++ {
			c1 <- Event{Source: "a"}
		}
		close(c1)
	}()
	go func() {
		for i := 0; i < 3; i++ {
			c2 <- Event{Source: "b"}
		}
		close(c2)
	}()
	go func() {
		for i := 0; i < 7; i++ {
			c3 <- Event{Source: "c"}
		}
		close(c3)
	}()

	a, b, c := 0, 0, 0
	for ev := range out { // range завершится только если out закрылся
		switch ev.Source {
		case "a":
			a++
		case "b":
			b++
		case "c":
			c++
		}
	}
	if a != 5 || b != 3 || c != 7 {
		t.Errorf("получено a=%d b=%d c=%d, want a=5 b=3 c=7", a, b, c)
	}
}

func TestMerge_CancelStopsAndCloses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c1 := make(chan Event) // никто не пишет и не закрывает
	out := merge(ctx, c1, nil)

	cancel()

	select {
	case _, ok := <-out:
		if ok {
			t.Error("после отмены не ждали значений")
		}
	case <-time.After(time.Second):
		t.Fatal("merge не закрыл выход после отмены ctx")
	}
}

func collectEvents(t *testing.T, out <-chan Event, count int, deadline time.Duration) []Event {
	t.Helper()

	events := make([]Event, 0, count)
	timeout := time.After(deadline)

	for i := 0; i < count; i++ {
		select {
		case event, ok := <-out:
			if !ok {
				t.Fatalf("out закрылся раньше, чем прочитано %d событий (получено %d)", count, i)
			}
			events = append(events, event)
		case <-timeout:
			t.Fatalf("таймаут: получено только %d из %d событий", i, count)
		}
	}

	return events
}

func TestMergePriority_PrefersCh1WhenBothReady(t *testing.T) {
	ctx := t.Context()

	const (
		eventsPerC1 = 4
		eventsPerC2 = 7
	)

	c1 := make(chan Event, eventsPerC1)
	c2 := make(chan Event, eventsPerC2)

	for i := range eventsPerC1 {
		c1 <- Event{Source: "c1", UserID: strconv.Itoa(i)}
	}
	close(c1)
	for i := range eventsPerC2 {
		c2 <- Event{Source: "c2", UserID: strconv.Itoa(i)}
	}
	close(c2)

	out := mergePriority(ctx, c1, c2)

	got := collectEvents(t, out, eventsPerC1+eventsPerC2, 2*time.Second)

	for i := range eventsPerC1 {
		if got[i].Source != "c1" || got[i].UserID != strconv.Itoa(i) {
			t.Errorf("событие %d: ожидалось {c1 %d}, получено %+v", i, i, got[i])
		}
	}

	for i := range eventsPerC2 {
		idx := eventsPerC1 + i
		if got[idx].Source != "c2" || got[idx].UserID != strconv.Itoa(i) {
			t.Errorf("событие %d: ожидалось {c2 %d}, получено %+v", idx, i, got[idx])
		}
	}
}

func TestMergePriority_FallsBackToCh2WhenCh1Empty(t *testing.T) {
	ctx := t.Context()

	c1 := make(chan Event)
	c2 := make(chan Event)
	defer func() {
		close(c1)
		close(c2)
	}()

	out := mergePriority(ctx, c1, c2)

	want := Event{Source: "c2", UserID: strconv.Itoa(42)}

	done := make(chan struct{})
	go func() {
		defer close(done)
		c2 <- want
	}()

	select {
	case got := <-out:
		if got != want {
			t.Errorf("ожидалось %+v, получено %+v", want, got)
		}
	case <-time.After(time.Second):
		t.Fatal("таймаут: событие из c2 не было получено — возможен deadlock")
	}

	<-done
}

func TestMergePriority_ClosesOutWhenBothInputsClosed(t *testing.T) {
	ctx := t.Context()

	c1 := make(chan Event)
	c2 := make(chan Event)
	close(c1)
	close(c2)

	out := mergePriority(ctx, c1, c2)

	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("ожидалось закрытие out, но получено событие")
		}
	case <-time.After(time.Second):
		t.Fatal("таймаут: out не закрылся после закрытия обоих входных каналов")
	}
}

// --- batch ---

func TestBatch_FlushBySize(t *testing.T) {
	in := make(chan Event)
	// Таймаут заведомо большой — флашить может только размер.
	out := batch(in, 3, time.Hour)

	go func() {
		for i := 0; i < 7; i++ {
			in <- Event{UserID: strconv.Itoa(i)}
		}
		close(in)
	}()

	batches := collectBatches(t, out, 2*time.Second)

	wantSizes := []int{3, 3, 1} // 7 событий: два полных батча + остаток при закрытии
	if len(batches) != len(wantSizes) {
		t.Fatalf("получено %d батчей, want %d (%v)", len(batches), len(wantSizes), wantSizes)
	}
	for i, b := range batches {
		if len(b) != wantSizes[i] {
			t.Errorf("батч #%d размер %d, want %d", i, len(b), wantSizes[i])
		}
	}
	if got := countEvents(batches); got != 7 {
		t.Errorf("всего событий %d, want 7", got)
	}
}

func TestBatch_FlushByTimeout(t *testing.T) {
	in := make(chan Event)
	// Размер заведомо большой — флашить может только таймаут.
	out := batch(in, 100, 40*time.Millisecond)

	go func() {
		in <- Event{UserID: "a"}
		in <- Event{UserID: "b"}
		// Не закрываем сразу: ждём, пока сработает таймаут.
		time.Sleep(120 * time.Millisecond)
		close(in)
	}()

	batches := collectBatches(t, out, 2*time.Second)

	if len(batches) == 0 {
		t.Fatal("ожидали хотя бы один батч по таймауту")
	}
	if len(batches[0]) != 2 {
		t.Errorf("первый батч размер %d, want 2", len(batches[0]))
	}
	if got := countEvents(batches); got != 2 {
		t.Errorf("всего событий %d, want 2", got)
	}
}

func TestBatch_FlushesRemainderOnClose(t *testing.T) {
	in := make(chan Event)
	out := batch(in, 10, time.Hour) // ни размер, ни таймаут не сработают

	go func() {
		for i := 0; i < 4; i++ {
			in <- Event{UserID: strconv.Itoa(i)}
		}
		close(in)
	}()

	batches := collectBatches(t, out, 2*time.Second)

	if len(batches) != 1 || len(batches[0]) != 4 {
		t.Fatalf("ожидали один батч из 4 событий, got %v", batches)
	}
}

// --- Сборка целиком ---

func TestRunPipeline_EndToEnd_NoEventLost(t *testing.T) {
	web := make(chan WebEvent)
	app := make(chan AppEvent)
	out := RunPipeline(context.Background(), web, app, 4, 30*time.Millisecond)

	const nWeb, nApp = 6, 4
	go func() {
		for i := 0; i < nWeb; i++ {
			web <- WebEvent{SessionID: "w" + strconv.Itoa(i), TS: 1700000000}
		}
		close(web)
	}()
	go func() {
		for i := 0; i < nApp; i++ {
			app <- AppEvent{DeviceID: "d" + strconv.Itoa(i), EventTime: time.Unix(1700000000, 0)}
		}
		close(app)
	}()

	batches := collectBatches(t, out, 3*time.Second)

	if got := countEvents(batches); got != nWeb+nApp {
		t.Errorf("всего событий %d, want %d", got, nWeb+nApp)
	}

	// Проверяем, что ничего не потерялось и не задвоилось: набор UserID совпадает.
	var gotIDs []string
	web2, app2 := 0, 0
	for _, b := range batches {
		for _, e := range b {
			gotIDs = append(gotIDs, e.UserID)
			switch e.Source {
			case "web":
				web2++
			case "app":
				app2++
			}
		}
	}
	if web2 != nWeb || app2 != nApp {
		t.Errorf("по источникам web=%d app=%d, want web=%d app=%d", web2, app2, nWeb, nApp)
	}

	wantIDs := []string{"d0", "d1", "d2", "d3", "w0", "w1", "w2", "w3", "w4", "w5"}
	sort.Strings(gotIDs)
	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("UserID count %d, want %d", len(gotIDs), len(wantIDs))
	}
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] {
			t.Errorf("UserID[%d]=%q, want %q", i, gotIDs[i], wantIDs[i])
		}
	}
}

// Graceful shutdown: после отмены ctx pipeline (с настоящими генераторами)
// должен корректно закрыться, а не зависнуть.
func TestRunPipeline_CancelShutsDownCleanly(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	web := genWeb(ctx, 5*time.Millisecond)
	app := genApp(ctx, 7*time.Millisecond)
	out := RunPipeline(ctx, web, app, 100, time.Second)

	time.Sleep(40 * time.Millisecond) // дать поработать
	cancel()

	done := make(chan struct{})
	go func() {
		for range out { // дренируем до закрытия
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline не завершился после отмены ctx (возможна утечка горутины)")
	}
}
