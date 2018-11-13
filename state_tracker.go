package ghostferry

import (
	"container/ring"
	"math"
	"sync"
	"time"

	"github.com/siddontang/go-mysql/mysql"
)

type CopySerializableState struct {
	LastSuccessfulPrimaryKeys map[string]uint64
	CompletedTables           map[string]bool
	LastWrittenBinlogPosition mysql.Position
}

type VerifierSerializableState struct {
	LastSuccessfulPrimaryKeys map[string]uint64
	CompletedTables           map[string]bool
	LastWrittenBinlogPosition mysql.Position

	// This is not efficient because we have to build this distinct map from the
	// original ReverifyStore struct.
	//
	// TODO: address this inefficiency later
	ReverifyStore      map[string][]uint64
	ReverifyStoreCount uint64
}

type SerializableState struct {
	GhostferryVersion         string
	LastKnownTableSchemaCache TableSchemaCache

	CopyStage     *CopySerializableState
	VerifierStage *VerifierSerializableState
}

func (s *SerializableState) HasVerifierStage() bool {
	return s.VerifierStage != nil
}

func (s *SerializableState) MinBinlogPosition() mysql.Position {
	if !s.HasVerifierStage() {
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

type CopyStateTracker struct {
	lastSuccessfulPrimaryKeys map[string]uint64
	completedTables           map[string]bool
	lastWrittenBinlogPosition mysql.Position

	binlogMutex *sync.RWMutex
	tableMutex  *sync.RWMutex

	copySpeedLog *ring.Ring
}

type VerifierStateTracker struct {
	// The VerifierStateTracker is mostly the same, except we retain a reference
	// to the ReverifyStore
	*CopyStateTracker

	ReverifyStore *ReverifyStore
}

type StateTracker struct {
	CopyStage     *CopyStateTracker
	VerifierStage *VerifierStateTracker

	TableSchemaCache TableSchemaCache
}

func (s *StateTracker) Serialize() *SerializableState {
	state := &SerializableState{
		GhostferryVersion:         VersionString,
		LastKnownTableSchemaCache: s.TableSchemaCache,
		CopyStage:                 s.CopyStage.Serialize(),
	}

	if s.VerifierStage != nil {
		state.VerifierStage = s.VerifierStage.Serialize()
	}

	return state
}

// speedLogCount should be a number that is an order of magnitude or so larger
// than the number of table iterators. This is to ensure the ring buffer used
// to calculate the speed is not filled with only data from the last iteration
// of the cursor and thus would be wildly inaccurate.
func NewCopyStateTracker(speedLogCount int) *CopyStateTracker {
	var speedLog *ring.Ring = nil

	if speedLogCount > 0 {
		speedLog = ring.New(speedLogCount)
		speedLog.Value = PKPositionLog{
			Position: 0,
			At:       time.Now(),
		}
	}

	return &CopyStateTracker{
		lastSuccessfulPrimaryKeys: make(map[string]uint64),
		completedTables:           make(map[string]bool),
		lastWrittenBinlogPosition: mysql.Position{},
		binlogMutex:               &sync.RWMutex{},
		tableMutex:                &sync.RWMutex{},
		copySpeedLog:              speedLog,
	}
}

// serializedState is a state the tracker should start from, as opposed to
// starting from the beginning.
func NewCopyStateTrackerFromSerializedState(speedLogCount int, serializedState *CopySerializableState) *CopyStateTracker {
	s := NewCopyStateTracker(speedLogCount)
	s.lastSuccessfulPrimaryKeys = serializedState.LastSuccessfulPrimaryKeys
	s.completedTables = serializedState.CompletedTables
	s.lastWrittenBinlogPosition = serializedState.LastWrittenBinlogPosition
	return s
}

func (s *CopyStateTracker) UpdateLastSuccessfulPK(table string, pk uint64) {
	s.tableMutex.Lock()
	defer s.tableMutex.Unlock()

	deltaPK := pk - s.lastSuccessfulPrimaryKeys[table]
	s.lastSuccessfulPrimaryKeys[table] = pk

	s.updateSpeedLog(deltaPK)
}

func (s *CopyStateTracker) LastSuccessfulPK(table string) uint64 {
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

func (s *CopyStateTracker) MarkTableAsCompleted(table string) {
	s.tableMutex.Lock()
	defer s.tableMutex.Unlock()

	s.completedTables[table] = true
}

func (s *CopyStateTracker) IsTableComplete(table string) bool {
	s.tableMutex.Lock()
	defer s.tableMutex.Unlock()

	return s.completedTables[table]
}

func (s *CopyStateTracker) UpdateLastWrittenBinlogPosition(pos mysql.Position) {
	s.binlogMutex.Lock()
	defer s.binlogMutex.Unlock()

	s.lastWrittenBinlogPosition = pos
}

func (s *CopyStateTracker) LastWrittenBinlogPosition() mysql.Position {
	s.binlogMutex.Lock()
	defer s.binlogMutex.Unlock()

	return s.lastWrittenBinlogPosition
}

func (s *CopyStateTracker) Serialize() *CopySerializableState {
	s.tableMutex.RLock()
	s.binlogMutex.RLock()
	defer func() {
		s.tableMutex.RUnlock()
		s.binlogMutex.RUnlock()
	}()

	state := &CopySerializableState{
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
func (s *CopyStateTracker) EstimatedPKCopiedPerSecond() float64 {
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

func (s *CopyStateTracker) updateSpeedLog(deltaPK uint64) {
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

func NewVerifierStateTracker(reverifyStore *ReverifyStore) *VerifierStateTracker {
	return &VerifierStateTracker{
		CopyStateTracker: NewCopyStateTracker(0),

		ReverifyStore: reverifyStore,
	}
}

func NewVerifierStateTrackerFromSerializedState(reverifyStore *ReverifyStore, serializedState *VerifierSerializableState) *VerifierStateTracker {
	s := NewVerifierStateTracker(reverifyStore)
	s.lastSuccessfulPrimaryKeys = serializedState.LastSuccessfulPrimaryKeys
	s.completedTables = serializedState.CompletedTables
	s.lastWrittenBinlogPosition = serializedState.LastWrittenBinlogPosition
	return s
}

func (s *VerifierStateTracker) Serialize() *VerifierSerializableState {
	baseState := s.CopyStateTracker.Serialize()

	state := &VerifierSerializableState{
		LastWrittenBinlogPosition: baseState.LastWrittenBinlogPosition,
		LastSuccessfulPrimaryKeys: baseState.LastSuccessfulPrimaryKeys,
		CompletedTables:           baseState.CompletedTables,
	}

	state.ReverifyStore, state.ReverifyStoreCount = s.ReverifyStore.Serialize()

	return state
}
