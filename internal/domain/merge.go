package domain

import (
	json "github.com/goccy/go-json"
	"dario.cat/mergo"
)

func MergeStates(current, results json.RawMessage) (json.RawMessage, error) {
	if len(current) == 0 {
		return results, nil
	}

	if len(results) == 0 {
		return current, nil
	}

	var currentData, resultsData interface{}

	if err := json.Unmarshal(current, &currentData); err != nil {
		return nil, NewDiscoveryError("merge", "unmarshal_current", err)
	}

	if err := json.Unmarshal(results, &resultsData); err != nil {
		return nil, NewDiscoveryError("merge", "unmarshal_results", err)
	}

	switch {
	case isObject(currentData) && isObject(resultsData):
		currentMap := currentData.(map[string]interface{})
		resultsMap := resultsData.(map[string]interface{})

		if err := mergo.Merge(&currentMap, resultsMap,
			mergo.WithOverride,
			mergo.WithAppendSlice); err != nil {
			return nil, NewDiscoveryError("merge", "mergo_merge", err)
		}

		merged, err := json.Marshal(currentMap)
		if err != nil {
			return nil, NewDiscoveryError("merge", "marshal_merged", err)
		}
		return merged, nil

	case isArray(currentData) && isArray(resultsData):
		currentSlice := currentData.([]interface{})
		resultsSlice := resultsData.([]interface{})

		merged := make([]interface{}, 0, len(currentSlice)+len(resultsSlice))
		merged = append(merged, currentSlice...)
		merged = append(merged, resultsSlice...)

		mergedBytes, err := json.Marshal(merged)
		if err != nil {
			return nil, NewDiscoveryError("merge", "marshal_array", err)
		}
		return mergedBytes, nil

	default:
		return results, nil
	}
}

func isObject(v interface{}) bool {
	_, ok := v.(map[string]interface{})
	return ok
}

func isArray(v interface{}) bool {
	_, ok := v.([]interface{})
	return ok
}