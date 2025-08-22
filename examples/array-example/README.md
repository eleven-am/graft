# Array Example

This example demonstrates how to use arrays and primitive types as both configuration and state in Graft workflows.

## Features Demonstrated

1. **Array State Processing**: Shows how to handle different types of arrays (strings, integers, mixed) as workflow state
2. **Array Configuration**: Uses arrays as node configuration parameters
3. **Primitive Processing**: Converts arrays to primitive strings
4. **Type Flexibility**: Demonstrates the interface{} flexibility allowing any data type

## Node Types

### ArrayProcessorNode
- **Input**: Arrays of strings, integers, or mixed types
- **Config**: Array of integers (first element used as delay in ms)
- **Output**: Array of processed strings
- **Purpose**: Process array elements by prefixing each with "processed-"

### StringConcatNode  
- **Input**: Array of strings
- **Config**: String (used as suffix)
- **Output**: Single concatenated string
- **Purpose**: Join array elements into a single string

## Test Cases

### Test 1: Array of Strings
- **State**: `[]string{"apple", "banana", "cherry"}`
- **Config**: `[]int{200, 300}` (200ms delay)
- **Expected**: `[]string{"processed-apple", "processed-banana", "processed-cherry"}`

### Test 2: Array of Integers
- **State**: `[]int{1, 2, 3, 4, 5}`
- **Config**: `[]int{100}` (100ms delay)  
- **Expected**: `[]string{"processed-1", "processed-2", "processed-3", "processed-4", "processed-5"}`

### Test 3: Array to String Processing  
- **State**: `[]string{"hello", "world", "from", "arrays"}`
- **Config**: `[]int{50}` (50ms delay)
- **Expected**: Processed array → concatenated string

## Running the Example

```bash
cd examples/array-example
go run main.go
```

## Key Implementation Details

### Type Flexibility
```go
func (n *ArrayProcessorNode) Execute(ctx context.Context, state interface{}, config interface{}) (interface{}, []graft.NextNode, error) {
    // Handle different array types
    switch s := state.(type) {
    case []string:
        // Process string array
    case []int:  
        // Process integer array
    case []interface{}:
        // Process mixed array
    }
    
    // Use array config
    if configArray, ok := config.([]int); ok && len(configArray) > 0 {
        delay = time.Duration(configArray[0]) * time.Millisecond
    }
}
```

### Workflow Trigger with Arrays
```go
trigger := graft.WorkflowTrigger{
    WorkflowID: "array-workflow",
    InitialState: []string{"item1", "item2", "item3"}, // Array state
    InitialNodes: []graft.NodeConfig{
        {
            Name: "array-processor", 
            Config: []int{200, 300}, // Array config
        },
    },
}
```

This demonstrates the maximum flexibility that Graft provides - users can pass any data type as state or configuration without being restricted to `map[string]interface{}`.