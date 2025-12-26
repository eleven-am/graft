package connectors

import "context"

// WatcherGroup starts and stops background watchers.
type WatcherGroup struct {
	startFns []func()
	stopFns  []func()
}

func NewWatcherGroup() *WatcherGroup {
	return &WatcherGroup{}
}

func (g *WatcherGroup) Add(start func(), stop func()) {
	g.startFns = append(g.startFns, start)
	g.stopFns = append(g.stopFns, stop)
}

func (g *WatcherGroup) Start(ctx context.Context) {
	for _, fn := range g.startFns {

		go fn()
	}
}

func (g *WatcherGroup) Stop() {
	for _, fn := range g.stopFns {
		if fn != nil {
			fn()
		}
	}
}
