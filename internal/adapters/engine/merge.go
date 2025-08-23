package engine

import (
	"encoding/json"
	"reflect"

	"dario.cat/mergo"
)

func MergeStates(current, results interface{}) (interface{}, error) {
	if current == nil {
		return results, nil
	}

	if results == nil {
		return current, nil
	}

	if isSlice(current) && isSlice(results) {
		return appendSlices(current, results), nil
	}

	if isMap(current) && isMap(results) {
		return mergeMaps(current, results)
	}

	if reflect.TypeOf(current) == reflect.TypeOf(results) {
		return mergeSameTypes(current, results)
	}

	return mergeDifferentTypes(current, results)
}

func isSlice(v interface{}) bool {
	if v == nil {
		return false
	}
	rt := reflect.TypeOf(v)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	return rt.Kind() == reflect.Slice || rt.Kind() == reflect.Array
}

func isMap(v interface{}) bool {
	if v == nil {
		return false
	}

	if _, ok := v.(map[string]interface{}); ok {
		return true
	}

	rt := reflect.TypeOf(v)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	return rt.Kind() == reflect.Map
}

func appendSlices(current, results interface{}) interface{} {
	currentVal := reflect.ValueOf(current)
	resultsVal := reflect.ValueOf(results)

	if currentVal.Kind() == reflect.Ptr {
		currentVal = currentVal.Elem()
	}
	if resultsVal.Kind() == reflect.Ptr {
		resultsVal = resultsVal.Elem()
	}

	currentLen := currentVal.Len()
	resultsLen := resultsVal.Len()

	merged := reflect.MakeSlice(currentVal.Type(), currentLen+resultsLen, currentLen+resultsLen)

	reflect.Copy(merged.Slice(0, currentLen), currentVal)

	for i := 0; i < resultsLen; i++ {
		merged.Index(currentLen + i).Set(resultsVal.Index(i))
	}

	return merged.Interface()
}

func mergeMaps(current, results interface{}) (interface{}, error) {
	currentMap := make(map[string]interface{})
	resultsMap := make(map[string]interface{})

	if err := convertToStringMap(current, &currentMap); err != nil {
		return nil, err
	}

	if err := convertToStringMap(results, &resultsMap); err != nil {
		return nil, err
	}

	if err := mergo.Merge(&currentMap, resultsMap, mergo.WithOverride, mergo.WithAppendSlice); err != nil {
		return nil, err
	}

	return currentMap, nil
}

func convertToStringMap(src interface{}, dst *map[string]interface{}) error {
	if m, ok := src.(map[string]interface{}); ok {
		*dst = m
		return nil
	}

	data, err := json.Marshal(src)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, dst)
}

func mergeSameTypes(current, results interface{}) (interface{}, error) {
	currentMap := make(map[string]interface{})
	resultsMap := make(map[string]interface{})

	if err := mergo.Map(&currentMap, current); err != nil {
		return convertAndMergeViaJSON(current, results)
	}

	if err := mergo.Map(&resultsMap, results); err != nil {
		return convertAndMergeViaJSON(current, results)
	}

	if err := mergo.Merge(&currentMap, resultsMap, mergo.WithOverride, mergo.WithAppendSlice); err != nil {
		return nil, err
	}

	return currentMap, nil
}

func mergeDifferentTypes(current, results interface{}) (interface{}, error) {
	return convertAndMergeViaJSON(current, results)
}

func convertAndMergeViaJSON(current, results interface{}) (interface{}, error) {
	currentMap := make(map[string]interface{})
	resultsMap := make(map[string]interface{})

	currentData, err := json.Marshal(current)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(currentData, &currentMap); err != nil {
		return nil, err
	}

	resultsData, err := json.Marshal(results)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(resultsData, &resultsMap); err != nil {
		return nil, err
	}

	if err := mergo.Merge(&currentMap, resultsMap, mergo.WithOverride, mergo.WithAppendSlice); err != nil {
		return nil, err
	}

	return currentMap, nil
}
