package analytics

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
	"gonum.org/v1/gonum/stat"

	"next-gen/campaign"
)

var (
	ErrInsufficientData  = errors.New("insufficient historical data for prediction")
	ErrInvalidParameters = errors.New("invalid campaign parameters")
	ErrPredictionTimeout = errors.New("prediction timed out")
	DefaultPredictionTTL = 5 * time.Minute
	DefaultHistoryWindow = 30 * time.Minute
	MinDataPoints        = 10
)

// Enhanced BidPredictor with better concurrency and prediction capabilities
type BidPredictor struct {
	model         PredictiveModel
	dataPipeline  DataStream
	cache         *cache.Cache
	mu            sync.RWMutex
	modelVersion  string
	predictionTTL time.Duration
	historyWindow time.Duration
	metrics       *PredictionMetrics
}

type PredictionMetrics struct {
	TotalRequests    int64
	CacheHits        int64
	PredictionTimeNS int64
	Errors           map[string]int64
}

// Enhanced MarketData with additional fields
type MarketData struct {
	Platform     string    `json:"platform"`
	Timestamp    time.Time `json:"timestamp"`
	CPC          float64   `json:"cpc"`
	CVR          float64   `json:"cvr"`
	Impressions  int       `json:"impressions"`
	ClickThrough float64   `json:"click_through"`
	Competition  float64   `json:"competition"` // Competition level (0-1)
	DayOfWeek    int       `json:"day_of_week"` // 0-6 (Sunday-Saturday)
	HourOfDay    int       `json:"hour_of_day"` // 0-23
}

// Campaign represents the advertising campaign with enhanced fields
type Campaign struct {
	ID                 string                        `json:"id"`
	Budget             float64                       `json:"budget"`
	DailyBudget        float64                       `json:"daily_budget"`
	TargetReach        int                           `json:"target_reach"`
	TargetConversions  int                           `json:"target_conversions"`
	Platforms          []campaign.PlatformPreference `json:"platforms"`
	StartTime          time.Time                     `json:"start_time"`
	EndTime            time.Time                     `json:"end_time"`
	Priority           int                           `json:"priority"`
	TargetingRules     TargetingRules                `json:"targeting_rules"`
	CurrentSpend       float64                       `json:"current_spend"`
	CurrentConversions int                           `json:"current_conversions"`
}

type TargetingRules struct {
	Demographics Demographics `json:"demographics"`
	Geos         []string     `json:"geos"`
	Interests    []string     `json:"interests"`
	Devices      []string     `json:"devices"`
}

type Demographics struct {
	AgeMin    int      `json:"age_min"`
	AgeMax    int      `json:"age_max"`
	Genders   []string `json:"genders"`
	Languages []string `json:"languages"`
}

// NewBidPredictor creates a new BidPredictor instance
func NewBidPredictor(model PredictiveModel, pipeline DataStream) *BidPredictor {
	return &BidPredictor{
		model:         model,
		dataPipeline:  pipeline,
		cache:         cache.New(DefaultPredictionTTL, 10*time.Minute),
		predictionTTL: DefaultPredictionTTL,
		historyWindow: DefaultHistoryWindow,
		metrics: &PredictionMetrics{
			Errors: make(map[string]int64),
		},
	}
}

// GetOptimalBid returns the optimal bid amount with enhanced error handling and metrics
func (bp *BidPredictor) GetOptimalBid(ctx context.Context, campaign Campaign, platform string) (float64, error) {
	startTime := time.Now()
	bp.metrics.TotalRequests++

	// Validate input
	if campaign.ID == "" || platform == "" {
		bp.recordError("invalid_input")
		return 0, ErrInvalidParameters
	}

	// Check cache with context support
	if bid, err := bp.getFromCache(ctx, campaign.ID, platform); err == nil {
		bp.metrics.CacheHits++
		return bid, nil
	}

	// Get historical data with timeout
	history, err := bp.getHistoryWithTimeout(ctx, platform)
	if err != nil {
		bp.recordError(err.Error())
		return 0, err
	}

	// Check if we have enough data points
	if len(history) < MinDataPoints {
		bp.recordError("insufficient_data")
		return 0, ErrInsufficientData
	}

	// Make predictions
	predictedCPC, predictedCVR, err := bp.makePredictions(ctx, platform, history)
	if err != nil {
		return 0, err
	}

	// Calculate optimal bid with budget awareness
	bid := bp.calculateOptimalBid(campaign, platform, predictedCPC, predictedCVR)

	// Cache the result
	bp.cacheResult(campaign.ID, platform, bid)

	// Record prediction time
	bp.metrics.PredictionTimeNS += time.Since(startTime).Nanoseconds()

	return bid, nil
}

// getFromCache safely retrieves from cache with context support
func (bp *BidPredictor) getFromCache(ctx context.Context, campaignID, platform string) (float64, error) {
	select {
	case <-ctx.Done():
		return 0, ErrPredictionTimeout
	default:
		key := cacheKey(campaignID, platform)
		if bid, found := bp.cache.Get(key); found {
			return bid.(float64), nil
		}
		return 0, errors.New("not in cache")
	}
}

// getHistoryWithTimeout gets history with context timeout handling
func (bp *BidPredictor) getHistoryWithTimeout(ctx context.Context, platform string) ([]MarketData, error) {
	historyChan := make(chan []MarketData, 1)
	errChan := make(chan error, 1)

	go func() {
		history, err := bp.dataPipeline.GetHistory(platform, bp.historyWindow)
		if err != nil {
			errChan <- err
			return
		}
		historyChan <- history
	}()

	select {
	case <-ctx.Done():
		return nil, ErrPredictionTimeout
	case err := <-errChan:
		return nil, err
	case history := <-historyChan:
		return history, nil
	}
}

// makePredictions handles the prediction logic with error handling
func (bp *BidPredictor) makePredictions(ctx context.Context, platform string, history []MarketData) (float64, float64, error) {
	// Check context first
	select {
	case <-ctx.Done():
		return 0, 0, ErrPredictionTimeout
	default:
	}

	// Get current market conditions
	currentConditions := bp.calculateCurrentConditions(history)

	// Make predictions
	predictedCPC, _ := bp.model.PredictCPC(platform, history, currentConditions)
	predictedCVR, _ := bp.model.PredictCVR(platform, history, currentConditions)

	return predictedCPC, predictedCVR, nil
}

// calculateCurrentConditions analyzes recent market data
func (bp *BidPredictor) calculateCurrentConditions(history []MarketData) MarketData {
	if len(history) == 0 {
		return MarketData{}
	}

	var (
		cpcValues       []float64
		cvrValues       []float64
		competition     []float64
		clickThrough    []float64
		recentTimestamp = history[0].Timestamp
		currentHour     = time.Now().Hour()
		currentDay      = int(time.Now().Weekday())
	)

	for _, data := range history {
		cpcValues = append(cpcValues, data.CPC)
		cvrValues = append(cvrValues, data.CVR)
		competition = append(competition, data.Competition)
		clickThrough = append(clickThrough, data.ClickThrough)
	}

	return MarketData{
		CPC:          stat.Mean(cpcValues, nil),
		CVR:          stat.Mean(cvrValues, nil),
		Competition:  stat.Mean(competition, nil),
		ClickThrough: stat.Mean(clickThrough, nil),
		HourOfDay:    currentHour,
		DayOfWeek:    currentDay,
		Timestamp:    recentTimestamp,
	}
}

// calculateOptimalBid implements multi-objective optimization
func (bp *BidPredictor) calculateOptimalBid(campaign Campaign, platform string, predictedCPC, predictedCVR float64) float64 {
	// Basic bid calculation with budget constraints
	maxBid := bp.getMaxBidForPlatform(campaign, platform)
	if maxBid <= 0 {
		return 0
	}

	// Calculate base bid based on predicted performance
	baseBid := predictedCPC * (1 + predictedCVR) // Simple heuristic

	// Apply budget constraints
	budgetRatio := campaign.RemainingBudget() // campaign.DailyBudget
	timeRatio := timeRemainingRatio(campaign)

	// Adjust bid based on budget and time remaining
	adjustedBid := baseBid * budgetRatio * timeRatio

	// Apply platform-specific constraints
	finalBid := math.Min(adjustedBid, maxBid)

	// Ensure bid is within reasonable bounds
	return math.Max(finalBid, 0.01) // Minimum bid of 0.01
}

// getMaxBidForPlatform gets the platform-specific max bid
func (bp *BidPredictor) getMaxBidForPlatform(campaign Campaign, platform string) float64 {
	for _, pref := range campaign.Platforms {
		if pref.PlatformName == platform {
			return pref.MaxBid
		}
	}
	return 0
}

// cacheResult safely stores the prediction result
func (bp *BidPredictor) cacheResult(campaignID, platform string, bid float64) {
	key := cacheKey(campaignID, platform)
	bp.cache.Set(key, bid, bp.predictionTTL)
}

func cacheKey(campaignID, platform string) string {
	return campaignID + ":" + platform
}

func (c Campaign) RemainingBudget() float64 {
	return c.DailyBudget - c.CurrentSpend
}

func timeRemainingRatio(campaign Campaign) float64 {
	totalDuration := campaign.EndTime.Sub(campaign.StartTime).Seconds()
	remaining := campaign.EndTime.Sub(time.Now()).Seconds()
	if remaining <= 0 || totalDuration <= 0 {
		return 1.0
	}
	return remaining / totalDuration
}

func (bp *BidPredictor) recordError(err string) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.metrics.Errors[err]++
}

// UpdateModel updates the prediction model safely
func (bp *BidPredictor) UpdateModel(newModel PredictiveModel, version string) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.model = newModel
	bp.modelVersion = version
	bp.cache.Flush() // Clear cache when model updates
}

// GetMetrics returns a copy of the current metrics
func (bp *BidPredictor) GetMetrics() PredictionMetrics {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	// Create a deep copy of the metrics
	errorsCopy := make(map[string]int64)
	for k, v := range bp.metrics.Errors {
		errorsCopy[k] = v
	}

	return PredictionMetrics{
		TotalRequests:    bp.metrics.TotalRequests,
		CacheHits:        bp.metrics.CacheHits,
		PredictionTimeNS: bp.metrics.PredictionTimeNS,
		Errors:           errorsCopy,
	}
}
