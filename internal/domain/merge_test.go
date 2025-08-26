package domain

import (
	"encoding/json"
	"testing"
)

func TestMergeStates_ObjectMerging(t *testing.T) {
	tests := []struct {
		name     string
		current  json.RawMessage
		results  json.RawMessage
		expected string
	}{
		{
			name:     "simple object merge",
			current:  json.RawMessage(`{"name": "John", "age": 30}`),
			results:  json.RawMessage(`{"age": 31, "city": "NYC"}`),
			expected: `{"age":31,"city":"NYC","name":"John"}`,
		},
		{
			name:     "nested object merge",
			current:  json.RawMessage(`{"user": {"name": "John", "age": 30}, "count": 5}`),
			results:  json.RawMessage(`{"user": {"age": 31, "email": "john@example.com"}, "status": "active"}`),
			expected: `{"count":5,"status":"active","user":{"age":31,"email":"john@example.com","name":"John"}}`,
		},
		{
			name:     "override existing values",
			current:  json.RawMessage(`{"name": "John", "age": 30, "city": "Boston"}`),
			results:  json.RawMessage(`{"age": 31, "city": "NYC"}`),
			expected: `{"age":31,"city":"NYC","name":"John"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged, err := MergeStates(tt.current, tt.results)
			if err != nil {
				t.Fatalf("MergeStates failed: %v", err)
			}
			
			if string(merged) != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, string(merged))
			}
		})
	}
}

func TestMergeStates_ArrayMerging(t *testing.T) {
	tests := []struct {
		name     string
		current  json.RawMessage
		results  json.RawMessage
		expected string
	}{
		{
			name:     "simple array concatenation",
			current:  json.RawMessage(`[1, 2, 3]`),
			results:  json.RawMessage(`[4, 5, 6]`),
			expected: `[1,2,3,4,5,6]`,
		},
		{
			name:     "mixed type arrays",
			current:  json.RawMessage(`["a", "b"]`),
			results:  json.RawMessage(`[1, 2]`),
			expected: `["a","b",1,2]`,
		},
		{
			name:     "empty arrays",
			current:  json.RawMessage(`[]`),
			results:  json.RawMessage(`[1, 2]`),
			expected: `[1,2]`,
		},
		{
			name:     "object arrays",
			current:  json.RawMessage(`[{"id": 1}, {"id": 2}]`),
			results:  json.RawMessage(`[{"id": 3}]`),
			expected: `[{"id":1},{"id":2},{"id":3}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged, err := MergeStates(tt.current, tt.results)
			if err != nil {
				t.Fatalf("MergeStates failed: %v", err)
			}
			
			if string(merged) != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, string(merged))
			}
		})
	}
}

func TestMergeStates_PrimitiveOverride(t *testing.T) {
	tests := []struct {
		name     string
		current  json.RawMessage
		results  json.RawMessage
		expected string
	}{
		{
			name:     "string override",
			current:  json.RawMessage(`"old_value"`),
			results:  json.RawMessage(`"new_value"`),
			expected: `"new_value"`,
		},
		{
			name:     "number override",
			current:  json.RawMessage(`42`),
			results:  json.RawMessage(`100`),
			expected: `100`,
		},
		{
			name:     "boolean override",
			current:  json.RawMessage(`true`),
			results:  json.RawMessage(`false`),
			expected: `false`,
		},
		{
			name:     "null override",
			current:  json.RawMessage(`"something"`),
			results:  json.RawMessage(`null`),
			expected: `null`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged, err := MergeStates(tt.current, tt.results)
			if err != nil {
				t.Fatalf("MergeStates failed: %v", err)
			}
			
			if string(merged) != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, string(merged))
			}
		})
	}
}

func TestMergeStates_TypeMismatch(t *testing.T) {
	tests := []struct {
		name     string
		current  json.RawMessage
		results  json.RawMessage
		expected string
	}{
		{
			name:     "object to array",
			current:  json.RawMessage(`{"key": "value"}`),
			results:  json.RawMessage(`[1, 2, 3]`),
			expected: `[1, 2, 3]`,
		},
		{
			name:     "array to object",
			current:  json.RawMessage(`[1, 2, 3]`),
			results:  json.RawMessage(`{"key": "value"}`),
			expected: `{"key": "value"}`,
		},
		{
			name:     "primitive to object",
			current:  json.RawMessage(`"string"`),
			results:  json.RawMessage(`{"key": "value"}`),
			expected: `{"key": "value"}`,
		},
		{
			name:     "object to primitive",
			current:  json.RawMessage(`{"key": "value"}`),
			results:  json.RawMessage(`"string"`),
			expected: `"string"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged, err := MergeStates(tt.current, tt.results)
			if err != nil {
				t.Fatalf("MergeStates failed: %v", err)
			}
			
			if string(merged) != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, string(merged))
			}
		})
	}
}

func TestMergeStates_EmptyInputs(t *testing.T) {
	tests := []struct {
		name     string
		current  json.RawMessage
		results  json.RawMessage
		expected string
	}{
		{
			name:     "empty current",
			current:  json.RawMessage(``),
			results:  json.RawMessage(`{"key": "value"}`),
			expected: `{"key": "value"}`,
		},
		{
			name:     "empty results",
			current:  json.RawMessage(`{"key": "value"}`),
			results:  json.RawMessage(``),
			expected: `{"key": "value"}`,
		},
		{
			name:     "both empty",
			current:  json.RawMessage(``),
			results:  json.RawMessage(``),
			expected: ``,
		},
		{
			name:     "nil current",
			current:  nil,
			results:  json.RawMessage(`{"key": "value"}`),
			expected: `{"key": "value"}`,
		},
		{
			name:     "nil results",
			current:  json.RawMessage(`{"key": "value"}`),
			results:  nil,
			expected: `{"key": "value"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged, err := MergeStates(tt.current, tt.results)
			if err != nil {
				t.Fatalf("MergeStates failed: %v", err)
			}
			
			if string(merged) != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, string(merged))
			}
		})
	}
}

func TestMergeStates_ComplexWorkflowScenarios(t *testing.T) {
	tests := []struct {
		name     string
		current  json.RawMessage
		results  json.RawMessage
		expected string
	}{
		{
			name:    "workflow state merge",
			current: json.RawMessage(`{"step": 1, "user": {"id": 123, "name": "John"}, "data": [1, 2]}`),
			results: json.RawMessage(`{"step": 2, "user": {"email": "john@example.com"}, "data": [3, 4], "status": "processing"}`),
			expected: `{"data":[1,2,3,4],"status":"processing","step":2,"user":{"email":"john@example.com","id":123,"name":"John"}}`,
		},
		{
			name:    "nested arrays in objects",
			current: json.RawMessage(`{"config": {"items": [{"id": 1}]}, "metadata": {"tags": ["a", "b"]}}`),
			results: json.RawMessage(`{"config": {"items": [{"id": 2}]}, "metadata": {"tags": ["c"], "version": "1.0"}}`),
			expected: `{"config":{"items":[{"id":1},{"id":2}]},"metadata":{"tags":["a","b","c"],"version":"1.0"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged, err := MergeStates(tt.current, tt.results)
			if err != nil {
				t.Fatalf("MergeStates failed: %v", err)
			}
			
			if string(merged) != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, string(merged))
			}
		})
	}
}

func TestMergeStates_ErrorHandling(t *testing.T) {
	tests := []struct {
		name    string
		current json.RawMessage
		results json.RawMessage
		wantErr bool
	}{
		{
			name:    "invalid current JSON",
			current: json.RawMessage(`{invalid json`),
			results: json.RawMessage(`{"key": "value"}`),
			wantErr: true,
		},
		{
			name:    "invalid results JSON",
			current: json.RawMessage(`{"key": "value"}`),
			results: json.RawMessage(`{invalid json`),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := MergeStates(tt.current, tt.results)
			if tt.wantErr && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func BenchmarkMergeStates_SimpleObject(b *testing.B) {
	current := json.RawMessage(`{"name": "John", "age": 30}`)
	results := json.RawMessage(`{"age": 31, "city": "NYC"}`)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := MergeStates(current, results)
		if err != nil {
			b.Fatalf("MergeStates failed: %v", err)
		}
	}
}

func BenchmarkMergeStates_ComplexObject(b *testing.B) {
	current := json.RawMessage(`{"step": 1, "user": {"id": 123, "name": "John", "preferences": {"theme": "dark", "lang": "en"}}, "data": [1, 2, 3], "metadata": {"created": "2023-01-01", "tags": ["workflow", "test"]}}`)
	results := json.RawMessage(`{"step": 2, "user": {"email": "john@example.com", "preferences": {"notifications": true}}, "data": [4, 5], "status": "processing", "metadata": {"updated": "2023-01-02", "tags": ["updated"]}}`)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := MergeStates(current, results)
		if err != nil {
			b.Fatalf("MergeStates failed: %v", err)
		}
	}
}

func BenchmarkMergeStates_ArrayConcatenation(b *testing.B) {
	current := json.RawMessage(`[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`)
	results := json.RawMessage(`[11, 12, 13, 14, 15, 16, 17, 18, 19, 20]`)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := MergeStates(current, results)
		if err != nil {
			b.Fatalf("MergeStates failed: %v", err)
		}
	}
}

func BenchmarkMergeStates_EmptyFastPath(b *testing.B) {
	current := json.RawMessage(``)
	results := json.RawMessage(`{"key": "value"}`)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := MergeStates(current, results)
		if err != nil {
			b.Fatalf("MergeStates failed: %v", err)
		}
	}
}