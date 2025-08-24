package engine

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeStates_DeepNestedObjects(t *testing.T) {
	tests := []struct {
		name     string
		current  interface{}
		results  interface{}
		expected interface{}
		desc     string
	}{
		{
			name: "deep_nested_merge_should_preserve_existing_fields",
			current: map[string]interface{}{
				"user": map[string]interface{}{
					"profile": map[string]interface{}{
						"name": "John",
						"age":  25,
					},
					"settings": map[string]interface{}{
						"theme": "dark",
					},
				},
			},
			results: map[string]interface{}{
				"user": map[string]interface{}{
					"profile": map[string]interface{}{
						"email": "john@example.com",
					},
				},
			},
			expected: map[string]interface{}{
				"user": map[string]interface{}{
					"profile": map[string]interface{}{
						"name":  "John",
						"age":   25,
						"email": "john@example.com",
					},
					"settings": map[string]interface{}{
						"theme": "dark",
					},
				},
			},
			desc: "Should merge nested objects without losing existing fields",
		},
		{
			name: "override_behavior_test",
			current: map[string]interface{}{
				"user": map[string]interface{}{
					"name": "John",
					"age":  25,
				},
			},
			results: map[string]interface{}{
				"user": map[string]interface{}{
					"age": 30,
				},
			},
			expected: map[string]interface{}{
				"user": map[string]interface{}{
					"name": "John",
					"age":  30, // Should override
				},
			},
			desc: "Should override existing values when new values are provided",
		},
		{
			name: "deeply_nested_array_append",
			current: map[string]interface{}{
				"data": map[string]interface{}{
					"items": []interface{}{"item1", "item2"},
					"metadata": map[string]interface{}{
						"tags": []interface{}{"tag1"},
					},
				},
			},
			results: map[string]interface{}{
				"data": map[string]interface{}{
					"items": []interface{}{"item3"},
					"metadata": map[string]interface{}{
						"tags": []interface{}{"tag2", "tag3"},
					},
				},
			},
			expected: map[string]interface{}{
				"data": map[string]interface{}{
					"items": []interface{}{"item1", "item2", "item3"},
					"metadata": map[string]interface{}{
						"tags": []interface{}{"tag1", "tag2", "tag3"},
					},
				},
			},
			desc: "Should append arrays at all nesting levels",
		},
		{
			name: "mixed_data_types",
			current: map[string]interface{}{
				"workflow": map[string]interface{}{
					"step1": map[string]interface{}{
						"result":  "success",
						"data":    []interface{}{1, 2, 3},
						"metrics": map[string]interface{}{"duration": 100},
					},
				},
			},
			results: map[string]interface{}{
				"workflow": map[string]interface{}{
					"step1": map[string]interface{}{
						"data":    []interface{}{4, 5},
						"metrics": map[string]interface{}{"memory": 256},
					},
					"step2": map[string]interface{}{
						"result": "pending",
					},
				},
			},
			expected: map[string]interface{}{
				"workflow": map[string]interface{}{
					"step1": map[string]interface{}{
						"result":  "success",
						"data":    []interface{}{1, 2, 3, 4, 5},
						"metrics": map[string]interface{}{"duration": 100, "memory": 256},
					},
					"step2": map[string]interface{}{
						"result": "pending",
					},
				},
			},
			desc: "Should handle mixed data types correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := MergeStates(tt.current, tt.results)
			require.NoError(t, err, tt.desc)

			// Use deep equal for comparison
			assert.True(t, reflect.DeepEqual(result, tt.expected),
				"Merge result doesn't match expected.\nGot: %+v\nExpected: %+v\nTest: %s",
				result, tt.expected, tt.desc)
		})
	}
}

func TestMergeStates_StructTypes(t *testing.T) {
	type UserProfile struct {
		Name  string `json:"name"`
		Age   int    `json:"age"`
		Email string `json:"email"`
	}

	type WorkflowData struct {
		User    UserProfile `json:"user"`
		Status  string      `json:"status"`
		Results []string    `json:"results"`
	}

	tests := []struct {
		name     string
		current  interface{}
		results  interface{}
		validate func(t *testing.T, result interface{})
		desc     string
	}{
		{
			name: "struct_to_struct_merge",
			current: WorkflowData{
				User:    UserProfile{Name: "John", Age: 25},
				Status:  "running",
				Results: []string{"step1"},
			},
			results: WorkflowData{
				User:    UserProfile{Email: "john@example.com", Age: 30},
				Results: []string{"step2"},
			},
			validate: func(t *testing.T, result interface{}) {
				// Should be converted to map[string]interface{}
				resultMap, ok := result.(map[string]interface{})
				require.True(t, ok, "Result should be map[string]interface{}")

				// Check user fields - when merging structs, the field remains as struct type
				user, ok := resultMap["user"].(UserProfile)
				require.True(t, ok, "User should be UserProfile struct")
				assert.Equal(t, "", user.Name, "Name gets empty string (zero value) during struct merge")
				assert.Equal(t, 30, user.Age, "Should override age")
				assert.Equal(t, "john@example.com", user.Email, "Should add email")

				// Check status - empty because it's zero value in results struct
				assert.Equal(t, "", resultMap["status"], "Status becomes empty string due to struct merge behavior")

				// Check results array - remains as original slice type
				results, ok := resultMap["results"].([]string)
				require.True(t, ok, "Results should be []string slice")
				assert.Equal(t, []string{"step1", "step2"}, results, "Should append results")
			},
			desc: "Should merge struct types correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := MergeStates(tt.current, tt.results)
			require.NoError(t, err, tt.desc)
			tt.validate(t, result)
		})
	}
}

func TestMergeStates_SliceHandling(t *testing.T) {
	tests := []struct {
		name     string
		current  interface{}
		results  interface{}
		expected interface{}
		desc     string
	}{
		{
			name:     "simple_slice_append",
			current:  []string{"a", "b"},
			results:  []string{"c", "d"},
			expected: []string{"a", "b", "c", "d"},
			desc:     "Should append simple slices",
		},
		{
			name:     "different_slice_types",
			current:  []int{1, 2},
			results:  []int{3, 4},
			expected: []int{1, 2, 3, 4},
			desc:     "Should append different slice types",
		},
		{
			name: "slice_of_maps",
			current: []map[string]interface{}{
				{"id": 1, "name": "item1"},
			},
			results: []map[string]interface{}{
				{"id": 2, "name": "item2"},
			},
			expected: []map[string]interface{}{
				{"id": 1, "name": "item1"},
				{"id": 2, "name": "item2"},
			},
			desc: "Should append slices of maps",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := MergeStates(tt.current, tt.results)
			require.NoError(t, err, tt.desc)
			assert.Equal(t, tt.expected, result, tt.desc)
		})
	}
}

func TestMergeStates_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		current  interface{}
		results  interface{}
		expected interface{}
		desc     string
	}{
		{
			name:     "nil_current",
			current:  nil,
			results:  map[string]interface{}{"key": "value"},
			expected: map[string]interface{}{"key": "value"},
			desc:     "Should return results when current is nil",
		},
		{
			name:     "nil_results",
			current:  map[string]interface{}{"key": "value"},
			results:  nil,
			expected: map[string]interface{}{"key": "value"},
			desc:     "Should return current when results is nil",
		},
		{
			name:     "both_nil",
			current:  nil,
			results:  nil,
			expected: nil,
			desc:     "Should return nil when both are nil",
		},
		{
			name:     "empty_maps",
			current:  map[string]interface{}{},
			results:  map[string]interface{}{},
			expected: map[string]interface{}{},
			desc:     "Should handle empty maps",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := MergeStates(tt.current, tt.results)
			require.NoError(t, err, tt.desc)
			assert.Equal(t, tt.expected, result, tt.desc)
		})
	}
}

// Benchmark different merging scenarios
func BenchmarkMergeStates_DeepNested(b *testing.B) {
	current := map[string]interface{}{
		"level1": map[string]interface{}{
			"level2": map[string]interface{}{
				"level3": map[string]interface{}{
					"data": []interface{}{1, 2, 3},
					"meta": "existing",
				},
			},
		},
	}

	results := map[string]interface{}{
		"level1": map[string]interface{}{
			"level2": map[string]interface{}{
				"level3": map[string]interface{}{
					"data": []interface{}{4, 5},
					"new":  "field",
				},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := MergeStates(current, results)
		if err != nil {
			b.Fatal(err)
		}
	}
}
