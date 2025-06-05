package analytics

// PredictiveModel defines the interface for our machine learning models
type PredictiveModel interface {
	// PredictCPC predicts the Cost Per Click for a platform
	PredictCPC(platform string, history []MarketData, current MarketData) (float64, error)

	// PredictCVR predicts the Conversion Rate for a platform
	PredictCVR(platform string, history []MarketData, current MarketData) (float64, error)

	// Train updates the model with new data
	Train(newData []MarketData) error

	// Version returns the current model version
	Version() string

	// Save persists the model to storage
	Save(path string) error

	// Load loads the model from storage
	Load(path string) error

	// FeatureImportance returns the importance of different features
	FeatureImportance() map[string]float64
}
