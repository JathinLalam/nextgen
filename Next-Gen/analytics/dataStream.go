package analytics

import (
	"context"
	"time"
)

// DataStream provides access to real-time and historical market data
type DataStream interface {
	// GetHistory returns historical market data for a platform
	GetHistory(platform string, window time.Duration) ([]MarketData, error)

	// SubscribeRealTime subscribes to real-time market updates
	SubscribeRealTime(ctx context.Context, platform string) (<-chan MarketData, error)

	// GetCurrent returns the most recent market data point
	GetCurrent(platform string) (MarketData, error)

	// GetPlatforms returns the list of available platforms
	GetPlatforms() []string

	// Store stores new market data
	Store(data MarketData) error

	// BulkStore stores multiple data points efficiently
	BulkStore(data []MarketData) error
}
