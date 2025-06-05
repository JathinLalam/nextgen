package campaign

import (
	"container/heap"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	ErrCampaignExpired = errors.New("campaign has ended")
	ErrBudgetExhausted = errors.New("campaign budget exhausted")
	ErrInvalidCampaign = errors.New("invalid campaign parameters")
)

// Enhanced Campaign structure with additional business fields
type Campaign struct {
	ID                 string               `json:"id"`                 // Unique identifier
	Name               string               `json:"name"`               // Human-readable name
	Budget             float64              `json:"budget"`             // Total budget
	DailyBudget        float64              `json:"dailyBudget"`        // Daily spending limit
	TargetReach        int                  `json:"targetReach"`        // Desired impressions
	TargetConversions  int                  `json:"targetConversions"`  // Desired conversions
	Platforms          []PlatformPreference `json:"platforms"`          // Platform-specific settings
	StartTime          time.Time            `json:"startTime"`          // When campaign goes live
	EndTime            time.Time            `json:"endTime"`            // When campaign ends
	Priority           int                  `json:"priority"`           // 1-100 scale
	Status             CampaignStatus       `json:"status"`             // Active/Paused/Ended
	CurrentSpend       float64              `json:"currentSpend"`       // Amount spent so far
	CurrentConversions int                  `json:"currentConversions"` // Conversions achieved
	CreatedAt          time.Time            `json:"createdAt"`          // When created
	UpdatedAt          time.Time            `json:"updatedAt"`          // Last update
	Targeting          TargetingParams      `json:"targeting"`          // Audience targeting
	CreativeIDs        []string             `json:"creativeIds"`        // Associated creatives
}

type CampaignStatus string

const (
	StatusDraft  CampaignStatus = "draft"
	StatusActive CampaignStatus = "active"
	StatusPaused CampaignStatus = "paused"
	StatusEnded  CampaignStatus = "ended"
)

type PlatformPreference struct {
	PlatformName string            `json:"platformName"` // e.g., "google", "meta"
	Weight       float64           `json:"weight"`       // 0-1, relative importance
	MaxBid       float64           `json:"maxBid"`       // Maximum allowed bid
	MinBid       float64           `json:"minBid"`       // Minimum allowed bid
	Targeting    PlatformTargeting `json:"targeting"`    // Platform-specific targeting
}

type PlatformTargeting struct {
	Placements   []string `json:"placements"`   // Where ads can appear
	Devices      []string `json:"devices"`      // Mobile/desktop/tablet
	Optimization string   `json:"optimization"` // "conversions", "clicks", etc.
}

type TargetingParams struct {
	Geo          GeoTargeting `json:"geo"`
	Demographics Demographics `json:"demographics"`
	Interests    []string     `json:"interests"`
	Behaviors    []string     `json:"behaviors"`
}

type GeoTargeting struct {
	Countries []string         `json:"countries"`
	Regions   []string         `json:"regions"`
	Cities    []string         `json:"cities"`
	Radius    *RadiusTargeting `json:"radius,omitempty"`
}

type RadiusTargeting struct {
	Lat      float64 `json:"lat"`
	Lng      float64 `json:"lng"`
	Distance int     `json:"distance"` // in meters
}

type Demographics struct {
	AgeMin    int      `json:"ageMin"`
	AgeMax    int      `json:"ageMax"`
	Genders   []string `json:"genders"`
	Languages []string `json:"languages"`
}

// CampaignQueue implements a thread-safe priority queue
type CampaignQueue struct {
	heap   campaignHeap
	mutex  sync.RWMutex
	lookup map[string]int // Tracks positions for O(1) access
}

// NewCampaignQueue creates a new initialized queue
func NewCampaignQueue() *CampaignQueue {
	cq := &CampaignQueue{
		lookup: make(map[string]int),
	}
	heap.Init(&cq.heap)
	return cq
}

func (cq *CampaignQueue) Push(c Campaign) error {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()

	if _, exists := cq.lookup[c.ID]; exists {
		return errors.New("campaign already in queue")
	}

	heap.Push(&cq.heap, c)
	cq.lookup[c.ID] = len(cq.heap) - 1
	return nil
}

func (cq *CampaignQueue) Pop() (Campaign, error) {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()

	if len(cq.heap) == 0 {
		return Campaign{}, errors.New("queue is empty")
	}

	c := heap.Pop(&cq.heap).(Campaign)
	delete(cq.lookup, c.ID)
	return c, nil
}

func (cq *CampaignQueue) Peek() (Campaign, error) {
	cq.mutex.RLock()
	defer cq.mutex.RUnlock()

	if len(cq.heap) == 0 {
		return Campaign{}, errors.New("queue is empty")
	}

	return cq.heap[0], nil
}

func (cq *CampaignQueue) UpdatePriority(id string, newPriority int) error {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()

	index, exists := cq.lookup[id]
	if !exists {
		return errors.New("campaign not found")
	}

	cq.heap[index].Priority = newPriority
	heap.Fix(&cq.heap, index)
	return nil
}

func (cq *CampaignQueue) Remove(id string) error {
	cq.mutex.Lock()
	defer cq.mutex.Unlock()

	index, exists := cq.lookup[id]
	if !exists {
		return errors.New("campaign not found")
	}

	heap.Remove(&cq.heap, index)
	delete(cq.lookup, id)

	// Update lookup for elements that moved
	for i := index; i < len(cq.heap); i++ {
		cq.lookup[cq.heap[i].ID] = i
	}

	return nil
}

func (cq *CampaignQueue) Len() int {
	cq.mutex.RLock()
	defer cq.mutex.RUnlock()
	return len(cq.heap)
}

// campaignHeap implements heap.Interface with additional methods
type campaignHeap []Campaign

func (h campaignHeap) Len() int { return len(h) }
func (h campaignHeap) Less(i, j int) bool {
	// Higher priority comes first
	if h[i].Priority != h[j].Priority {
		return h[i].Priority > h[j].Priority
	}
	// For equal priority, older campaigns come first
	return h[i].CreatedAt.Before(h[j].CreatedAt)
}
func (h campaignHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *campaignHeap) Push(x interface{}) {
	*h = append(*h, x.(Campaign))
}

func (h *campaignHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// CampaignManager provides higher-level campaign operations
type CampaignManager struct {
	activeQueue *CampaignQueue
	archive     map[string]Campaign
	mu          sync.RWMutex
}

func NewCampaignManager() *CampaignManager {
	return &CampaignManager{
		activeQueue: NewCampaignQueue(),
		archive:     make(map[string]Campaign),
	}
}

func (cm *CampaignManager) AddCampaign(c Campaign) error {
	if err := validateCampaign(c); err != nil {
		return err
	}

	if c.ID == "" {
		c.ID = uuid.New().String()
	}

	c.CreatedAt = time.Now()
	c.UpdatedAt = time.Now()
	c.Status = StatusActive

	cm.mu.Lock()
	defer cm.mu.Unlock()

	return cm.activeQueue.Push(c)
}

func validateCampaign(c Campaign) error {
	if c.Budget <= 0 || c.DailyBudget <= 0 {
		return ErrInvalidCampaign
	}
	if c.StartTime.IsZero() || c.EndTime.IsZero() {
		return ErrInvalidCampaign
	}
	if len(c.Platforms) == 0 {
		return ErrInvalidCampaign
	}
	if c.EndTime.Before(c.StartTime) {
		return ErrInvalidCampaign
	}
	return nil
}

func (cm *CampaignManager) GetNextCampaign() (Campaign, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for {
		c, err := cm.activeQueue.Peek()
		if err != nil {
			return Campaign{}, err
		}

		// Check if campaign is still active
		if time.Now().After(c.EndTime) {
			// Move to archive
			popped, _ := cm.activeQueue.Pop()
			popped.Status = StatusEnded
			cm.archive[popped.ID] = popped
			continue
		}

		if c.Status != StatusActive {
			_, _ = cm.activeQueue.Pop()
			continue
		}

		return c, nil
	}
}

func (cm *CampaignManager) RecordSpend(campaignID string, amount float64) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	index, exists := cm.activeQueue.lookup[campaignID]
	if !exists {
		return errors.New("campaign not found")
	}

	cm.activeQueue.heap[index].CurrentSpend += amount
	cm.activeQueue.heap[index].UpdatedAt = time.Now()

	// Check if budget exhausted
	if cm.activeQueue.heap[index].CurrentSpend >= cm.activeQueue.heap[index].Budget {
		cm.activeQueue.heap[index].Status = StatusEnded
	}

	return nil
}

func (cm *CampaignManager) PauseCampaign(campaignID string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	index, exists := cm.activeQueue.lookup[campaignID]
	if !exists {
		return errors.New("campaign not found")
	}

	cm.activeQueue.heap[index].Status = StatusPaused
	cm.activeQueue.heap[index].UpdatedAt = time.Now()
	return nil
}

func (cm *CampaignManager) ResumeCampaign(campaignID string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	index, exists := cm.activeQueue.lookup[campaignID]
	if !exists {
		return errors.New("campaign not found")
	}

	cm.activeQueue.heap[index].Status = StatusActive
	cm.activeQueue.heap[index].UpdatedAt = time.Now()
	return nil
}

// Marshal/Unmarshal functions for persistence
func (c *Campaign) ToJSON() ([]byte, error) {
	return json.Marshal(c)
}

func CampaignFromJSON(data []byte) (Campaign, error) {
	var c Campaign
	err := json.Unmarshal(data, &c)
	return c, err
}
