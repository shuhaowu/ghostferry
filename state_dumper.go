package ghostferry

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/sirupsen/logrus"
)

type SerializableStage interface {
	SerializeStateToJSON() (string, error)
}

type StateDumper struct {
	*sync.Mutex
	runningStage SerializableStage
	logger       *logrus.Entry
}

func NewStateDumper(runningStage SerializableStage) *StateDumper {
	return &StateDumper{
		Mutex:        &sync.Mutex{},
		runningStage: runningStage,
		logger:       logrus.WithField("tag", "state_dumper"),
	}
}

func (s *StateDumper) HandleOsSignals(ctx context.Context) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		s.logger.Debug("shutting down signal monitoring goroutine")
		return
	case sig := <-c:
		s.logger.Debug("signals received, dumping state...")
		s.DumpState()
		panic(fmt.Sprintf("signal received: %v", sig.String()))
	}
}

func (s *StateDumper) DumpState() {
	s.Lock()
	defer s.Unlock()

	stateJSON, err := s.runningStage.SerializeStateToJSON()
	if err != nil {
		s.logger.WithError(err).Error("failed to dump state to JSON...")
	} else {
		fmt.Fprintln(os.Stdout, stateJSON)
	}
}

func (s *StateDumper) SwapRunningStage(newStage SerializableStage) {
	s.Lock()
	defer s.Unlock()

	s.runningStage = newStage
}
