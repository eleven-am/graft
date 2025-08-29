package ports

import "strings"

type NodeWeightProvider interface {
	GetWeight() float64
}

var DefaultNodeWeights = []struct {
	Pattern string
	Weight  float64
}{
	{"ocr-processor", 6.0},
	{"nlp-processor", 4.0},
	{"ml-training", 8.0},
	{"ml-inference", 5.0},
	{"data-export", 4.0},
	{"transformer", 2.0},
	{"aggregator", 3.0},
	{"processor", 2.0},
	{"analyzer", 3.0},
	{"validator", 1.0},
}

func GetNodeWeight(node interface{}, nodeName string) float64 {
	if provider, ok := node.(NodeWeightProvider); ok {
		return provider.GetWeight()
	}

	nodeLower := strings.ToLower(nodeName)
	for _, entry := range DefaultNodeWeights {
		if strings.Contains(nodeLower, entry.Pattern) {
			return entry.Weight
		}
	}

	return 1.0
}
