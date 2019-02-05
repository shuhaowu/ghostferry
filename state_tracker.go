package ghostferry

import (
	"container/ring"
	"math"
	"sync"
	"time"

	"github.com/siddontang/go-mysql/mysql"
)

// StateTracker design
// ===================
//
// General Overview
// ----------------
//
// The StateTracker keeps track of the progress of Ghostferry as it runs. The
// StateTracker is initialized and managed by the Ferry. Each Ghostferry
// components that requires state to be serialized during an interruption
// will get passed a copy of an applicable StateTracker. During the run, these
// components will post their last successful position to the StateTracker in
// an atomic manner (guaranteed by the StateTracker API).
//
// The StateTracker can be serialized using the Serialize method. This returns
// a serialization friendly struct (SerializableState) that can be easily
// stored. The same struct can be passed into a Ferry instance to instruct the
// Ferry to resume from that state.
//
// Hierarchy
// ---------
//
// In a Ghostferry run, there are two "stages" of operation: the copy stage and
// the verify stage. Both stages must emit their states to the StateTracker in
// order for them to be interruptible and resumable. These two stages are very
// similar: they both iterate over the data and tail the binlog. However, there
// are some minor differences. Example: the verifier stage needs to keep track
// of the reverify store while the copy stage doesn't.
//
// In order to not repeat code, "base structs" are created for the state
// tracking and serializable state: BinlogAndIterationStateTracker and
// BinlogAndIterationSerializableState. These structs have most of the
// code/definitions required for the state tracker of both stages to function.
// CopyStateTracker and VerifierStateTracker contain a minor amount of
// customized code due to the small differences in requirements.
//
// These two stages of state tracker (and serializable states) are owned by a
// "global" StateTracker. This struct keeps two member variables pointing to
// the state tracker of each stage and that's it. The Ferry then passes out
// the references to the applicable stage of state tracker to components like
// the BatchWriter, IterativeVerifier, etc.
//
// To summarize, what we have is (arrows point from member variables to owner
// structs):
//
//      BinlogAndIterationStateTracker
//         |                     |
//         v                     v
//  CopyStateTracker    VerifierStateTracker
//         |                     |
//         +----------+----------+
//                    |
//                    v
//               StateTracker
//                    |
//                    v
//                  Ferry
//
// The same relationship exists for the SerializableState.
//

type BinlogAndIterationSerializableState struct {
	LastSuccessfulPrimaryKeys map[string]uint64
	CompletedTables           map[string]bool
	LastWrittenBinlogPosition mysql.Position
}

type CopySerializableState struct {
	*BinlogAndIterationSerializableState
}

type VerifierSerializableState struct {
	*BinlogAndIterationSerializableState

	// This is not efficient because we have to build this map of a different
	// type from the original ReverifyStore struct.
	//
	// TODO: address this inefficiency later.
	ReverifyStore map[string][]uint64
}

const (
	StageCopy   = "COPY"
	StageVerify = "VERIFY"
)

// This is the struct that is dumped by Ghostferry when it is interrupted. It
// is the same struct that is given to Ghostferry when it is resumed.
type SerializableState struct {
	GhostferryVersion         string
	LastKnownTableSchemaCache TableSchemaCache

	CurrentStage  string
	CopyStage     *CopySerializableState
	VerifierStage *VerifierSerializableState
}

// The binlog writer and the verify binlog positions are different because the
// binlog writer is buffered in a background go routine. Its position can race
// with respect to the verifier binlog position. The minimum position between
// the two are always safe to resume from.
func (s *SerializableState) MinBinlogPosition() mysql.Position {
	if s.VerifierStage == nil {
		return s.CopyStage.LastWrittenBinlogPosition
	}

	c := s.CopyStage.LastWrittenBinlogPosition.Compare(s.VerifierStage.LastWrittenBinlogPosition)

	if c >= 0 {
		return s.CopyStage.LastWrittenBinlogPosition
	} else {
		return s.VerifierStage.LastWrittenBinlogPosition
	}
}

// For tracking the speed of the copy
type PKPositionLog struct {
	Position uint64
	At       time.Time
}

func newSpeedLogRing(speedLogCount int) *ring.Ring {
	if speedLogCount <= 0 {
		return nil
	}

	speedLog := ring.New(speedLogCount)
	speedLog.Value = PKPositionLog{
		Position: 0,
		At:       time.Now(),
	}

	return speedLog
}

type BinlogAndIterationStateTracker struct {
	lastSuccessfulPrimaryKeys map[string]uint64
	completedTables           map[string]bool
	lastWrittenBinlogPosition mysql.Position

	binlogMutex *sync.RWMutex
	tableMutex  *sync.RWMutex

	copySpeedLog *ring.Ring
}

func NewBinlogAndIterationStateTracker(speedLogCount int) *BinlogAndIterationStateTracker {
	return &BinlogAndIterationStateTracker{
		lastSuccessfulPrimaryKeys: make(map[string]uint64),
		completedTables:           make(map[string]bool),
		lastWrittenBinlogPosition: mysql.Position{},
		binlogMutex:               &sync.RWMutex{},
		tableMutex:                &sync.RWMutex{},
		copySpeedLog:              newSpeedLogRing(speedLogCount),
	}
}

func (s *BinlogAndIterationStateTracker) UpdateLastSuccessfulPK(table string, pk uint64) {
	s.tableMutex.Lock()
	defer s.tableMutex.Unlock()

	deltaPK := pk - s.lastSuccessfulPrimaryKeys[table]
	s.lastSuccessfulPrimaryKeys[table] = pk

	s.updateSpeedLog(deltaPK)
}

func (s *BinlogAndIterationStateTracker) LastSuccessfulPK(table string) uint64 {
	s.tableMutex.RLock()
	defer s.tableMutex.RUnlock()

	_, found := s.completedTables[table]
	if found {
		return math.MaxUint64
	}

	pk, found := s.lastSuccessfulPrimaryKeys[table]
	if !found {
		return 0
	}

	return pk
}

func (s *BinlogAndIterationStateTracker) MarkTableAsCompleted(table string) {
	s.tableMutex.Lock()
	defer s.tableMutex.Unlock()

	s.completedTables[table] = true
}

func (s *BinlogAndIterationStateTracker) IsTableComplete(table string) bool {
	s.tableMutex.Lock()
	defer s.tableMutex.Unlock()

	return s.completedTables[table]
}

func (s *BinlogAndIterationStateTracker) UpdateLastWrittenBinlogPosition(pos mysql.Position) {
	s.binlogMutex.Lock()
	defer s.binlogMutex.Unlock()

	s.lastWrittenBinlogPosition = pos
}

func (s *BinlogAndIterationStateTracker) Serialize() *BinlogAndIterationSerializableState {
	s.tableMutex.RLock()
	s.binlogMutex.RLock()
	defer func() {
		s.tableMutex.RUnlock()
		s.binlogMutex.RUnlock()
	}()

	state := &BinlogAndIterationSerializableState{
		LastWrittenBinlogPosition: s.lastWrittenBinlogPosition,
		LastSuccessfulPrimaryKeys: make(map[string]uint64),
		CompletedTables:           make(map[string]bool),
	}

	for k, v := range s.lastSuccessfulPrimaryKeys {
		state.LastSuccessfulPrimaryKeys[k] = v
	}

	for k, v := range s.completedTables {
		state.CompletedTables[k] = v
	}

	return state
}

// This is reasonably accurate if the rows copied are distributed uniformly
// between pk = 0 -> max(pk). It would not be accurate if the distribution is
// concentrated in a particular region.
func (s *BinlogAndIterationStateTracker) EstimatedPKsPerSecond() float64 {
	if s.copySpeedLog == nil {
		return 0.0
	}

	s.tableMutex.RLock()
	defer s.tableMutex.RUnlock()

	if s.copySpeedLog.Value.(PKPositionLog).Position == 0 {
		return 0.0
	}

	earliest := s.copySpeedLog
	for earliest.Prev() != nil && earliest.Prev() != s.copySpeedLog && earliest.Prev().Value.(PKPositionLog).Position != 0 {
		earliest = earliest.Prev()
	}

	currentValue := s.copySpeedLog.Value.(PKPositionLog)
	earliestValue := earliest.Value.(PKPositionLog)
	deltaPK := currentValue.Position - earliestValue.Position
	deltaT := currentValue.At.Sub(earliestValue.At).Seconds()

	return float64(deltaPK) / deltaT
}

func (s *BinlogAndIterationStateTracker) updateSpeedLog(deltaPK uint64) {
	if s.copySpeedLog == nil {
		return
	}

	currentTotalPK := s.copySpeedLog.Value.(PKPositionLog).Position
	s.copySpeedLog = s.copySpeedLog.Next()
	s.copySpeedLog.Value = PKPositionLog{
		Position: currentTotalPK + deltaPK,
		At:       time.Now(),
	}
}

type CopyStateTracker struct {
	*BinlogAndIterationStateTracker
}

func (s *CopyStateTracker) Serialize() *CopySerializableState {
	return &CopySerializableState{
		BinlogAndIterationSerializableState: s.BinlogAndIterationStateTracker.Serialize(),
	}
}

func NewCopyStateTracker(speedLogCount int) *CopyStateTracker {
	return &CopyStateTracker{NewBinlogAndIterationStateTracker(speedLogCount)}
}

type VerifierStateTracker struct {
	*BinlogAndIterationStateTracker
	// TODO: this struct needs to keep track of the reverify store and dump it
	//       with Serialize.
}

func (s *VerifierStateTracker) Serialize() *VerifierSerializableState {
	// TODO: this method needs to dump the reverify store.
	return &VerifierSerializableState{
		BinlogAndIterationSerializableState: s.BinlogAndIterationStateTracker.Serialize(),
	}
}

type StateTracker struct {
	CopyStage *CopyStateTracker
	// TODO: implement this
	// VerifierStage *VerifierStateTracker
}

// speedLogCount should be a number that is an order of magnitude or so larger
// than the number of table iterators. This is to ensure the ring buffer used
// to calculate the speed is not filled with only data from the last iteration
// of the cursor and thus would be wildly inaccurate.
func NewStateTracker(speedLogCount int) *StateTracker {
	return &StateTracker{
		CopyStage: NewCopyStateTracker(speedLogCount),
	}
}

// serializedState is a state the tracker should start from, as opposed to
// starting from the beginning.
func NewStateTrackerFromSerializedState(speedLogCount int, serializedState *SerializableState) *StateTracker {
	s := NewStateTracker(speedLogCount)
	s.CopyStage.lastSuccessfulPrimaryKeys = serializedState.CopyStage.LastSuccessfulPrimaryKeys
	s.CopyStage.completedTables = serializedState.CopyStage.CompletedTables
	s.CopyStage.lastWrittenBinlogPosition = serializedState.CopyStage.LastWrittenBinlogPosition
	return s
}

func (s *StateTracker) Serialize(lastKnownTableSchemaCache TableSchemaCache) *SerializableState {
	return &SerializableState{
		GhostferryVersion:         VersionString,
		LastKnownTableSchemaCache: lastKnownTableSchemaCache,

		// TODO: implement verifier stage
		CurrentStage: StageCopy,
		CopyStage:    s.CopyStage.Serialize(),
	}
}
