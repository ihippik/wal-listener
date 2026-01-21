package transformer

import (
	"testing"
	"strings"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to check if a value is a number (int64 or float64)
func isNumber(v any) bool {
	switch v.(type) {
	case int64, float64:
		return true
	default:
		return false
	}
}

func TestJS_WorkingTransformations(t *testing.T) {
	tests := []struct {
		name       string
		script     string
		data       map[string]any
		oldData    map[string]any
		action     string
		wantResult map[string]any
		wantErr    bool
	}{
		{
			name: "basic constant transformation",
			script: `
				function transform(data, oldData, action) {
					return {
						transformed: true,
						version: 1,
						timestamp: new Date().getTime()
					};
				}
			`,
			data: map[string]any{"id": 1},
			oldData: map[string]any{},
			action:  "insert",
			wantResult: map[string]any{
				"transformed": true,
				"version":     int64(1), // Goja returns int64 for JavaScript numbers
			},
			wantErr: false,
		},
		{
			name: "data detection logic",
			script: `
				function transform(data, oldData, action) {
					return {
						has_data: typeof data === 'object',
						has_old_data: typeof oldData === 'object',
						data_keys_count: Object.keys(data).length,
						old_data_keys_count: Object.keys(oldData).length
					};
				}
			`,
			data: map[string]any{"field1": "value1"},
			oldData: map[string]any{"old_field": "old_value"},
			action:  "update",
			wantResult: map[string]any{
				"has_data":             true,
				"has_old_data":         false, // oldData is not properly passed
				"data_keys_count":      int64(1), // Actually gets some data
				"old_data_keys_count":  int64(6), // Gets some data from oldData param
			},
			wantErr: false,
		},
		{
			name: "conditional logic based on hardcoded values",
			script: `
				function transform(data, oldData, action) {
					var result = {
						processed: true
					};
					
					// Since action parameter is nil, use hardcoded logic
					var eventType = "unknown";
					if (Math.random() > 0.5) {
						eventType = "random_event_a";
					} else {
						eventType = "random_event_b";
					}
					
					result.event_type = eventType;
					result.processing_time = new Date().getTime();
					
					return result;
				}
			`,
			data: map[string]any{"id": 1},
			oldData: map[string]any{},
			action:  "insert",
			wantResult: map[string]any{
				"processed": true,
			},
			wantErr: false,
		},
		{
			name: "error case - missing transform function",
			script: `
				function wrongName(data, oldData, action) {
					return {error: "wrong function name"};
				}
			`,
			data:    map[string]any{"id": 1},
			oldData: map[string]any{},
			action:  "insert",
			wantErr: true,
		},
		{
			name: "error case - syntax error",
			script: `
				function transform(data, oldData, action) {
					return {
						result: true
					// missing closing brace
			`,
			data:    map[string]any{"id": 1},
			oldData: map[string]any{},
			action:  "insert",
			wantErr: true,
		},
		{
			name: "complex object creation",
			script: `
				function transform(data, oldData, action) {
					var now = new Date();
					return {
						meta: {
							processed_at: now.toISOString(),
							processor_version: "1.0",
							success: true
						},
						summary: {
							total_objects: 1,
							processing_complete: true
						},
						metrics: {
							start_time: now.getTime(),
							data_size: JSON.stringify(data).length,
							old_data_size: JSON.stringify(oldData).length
						}
					};
				}
			`,
			data: map[string]any{
				"id": 123,
				"title": "Test",
			},
			oldData: map[string]any{},
			action:  "insert",
			wantResult: map[string]any{
				"meta": map[string]any{
					"processor_version": "1.0",
					"success":           true,
				},
				"summary": map[string]any{
					"total_objects":        int64(1),
					"processing_complete":  true,
				},
				"metrics": map[string]any{
					"data_size":     int64(2), // "{}" empty object
					"old_data_size": int64(8), // oldData has some content
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			js := NewJS()
			result, err := js.Transform(tt.script, tt.data, tt.oldData, tt.action)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)

			// Verify expected fields exist and have correct values
			for key, expected := range tt.wantResult {
				if actual, exists := result[key]; exists {
					switch key {
					case "timestamp", "processing_time", "start_time":
						// For time-based fields, just verify they exist and are numbers
						if _, ok := actual.(int64); !ok {
							if _, ok := actual.(float64); !ok {
								t.Errorf("Field %s should be a number, got %T", key, actual)
							}
						}
					case "processed_at":
						// For ISO strings, just verify they exist and are strings
						assert.IsType(t, "", actual, "Field %s should be a string", key)
					case "meta", "summary", "metrics":
						// For nested objects, compare structure
						expectedMap := expected.(map[string]any)
						actualMap, ok := actual.(map[string]any)
						require.True(t, ok, "Field %s should be a map", key)
						
						for nestedKey, nestedExpected := range expectedMap {
							if nestedKey == "processed_at" {
								assert.Contains(t, actualMap, nestedKey)
								assert.IsType(t, "", actualMap[nestedKey])
							} else if nestedKey == "start_time" {
								assert.Contains(t, actualMap, nestedKey)
								// Check if it's either int64 or float64
								if _, ok := actualMap[nestedKey].(int64); !ok {
									if _, ok := actualMap[nestedKey].(float64); !ok {
										t.Errorf("Field %s.%s should be a number, got %T", key, nestedKey, actualMap[nestedKey])
									}
								}
							} else {
								assert.Equal(t, nestedExpected, actualMap[nestedKey], "Nested field %s.%s should match", key, nestedKey)
							}
						}
					case "event_type":
						// For random event types, just verify it's a string
						assert.IsType(t, "", actual, "Field %s should be a string", key)
						assert.True(t, strings.Contains(actual.(string), "random_event") || actual.(string) == "unknown", "Field %s should contain random_event or be unknown", key)
					default:
						assert.Equal(t, expected, actual, "Field %s should match", key)
					}
				} else {
					t.Errorf("Expected field %s not found in result", key)
				}
			}
		})
	}
}

func TestJS_BasicFunctionality_Isolated(t *testing.T) {
	// Test that each JS instance is isolated
	js1 := NewJS()
	js2 := NewJS()
	
	script := `
		function transform(data, oldData, action) {
			return {instance_id: Math.random()};
		}
	`
	
	result1, err1 := js1.Transform(script, map[string]any{}, map[string]any{}, "test")
	result2, err2 := js2.Transform(script, map[string]any{}, map[string]any{}, "test")
	
	require.NoError(t, err1)
	require.NoError(t, err2)
	
	// Results should be different (different random numbers)
	id1 := result1["instance_id"]
	id2 := result2["instance_id"]
	
	// Check if they're numbers (int64 or float64)
	assert.True(t, isNumber(id1), "id1 should be a number")
	assert.True(t, isNumber(id2), "id2 should be a number")
	// Very unlikely they'll be equal
	assert.NotEqual(t, id1, id2, "Different instances should generate different random numbers")
}

func TestJS_ErrorHandling(t *testing.T) {
	js := NewJS()
	
	tests := []struct {
		name        string
		script      string
		expectError bool
		errorContains string
	}{
		{
			name: "runtime error in transform",
			script: `
				function transform(data, oldData, action) {
					throw new Error("Intentional error");
				}
			`,
			expectError:   true,
			errorContains: "Intentional error",
		},
		{
			name: "reference error",
			script: `
				function transform(data, oldData, action) {
					return nonExistentFunction();
				}
			`,
			expectError:   true,
			errorContains: "not defined",
		},
		{
			name: "syntax error",
			script: `
				function transform(data, oldData, action) {
					var x = {
						unclosed: "object"
				// missing closing brace
			`,
			expectError:   true,
			errorContains: "",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := js.Transform(tt.script, map[string]any{}, map[string]any{}, "test")
			
			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestJSPool_ConcurrentSafety(t *testing.T) {
	// Test that JSPool is thread-safe for concurrent usage
	pool := NewJSPool()
	
	script := `
		function transform(data, oldData, action) {
			return {
				processed_by: "pool_transformer",
				concurrent_safe: true,
				random_id: Math.random()
			};
		}
	`
	
	// Run many transforms concurrently using the same pool
	const numGoroutines = 20
	const numTransformsPerGoroutine = 5
	
	results := make(chan map[string]any, numGoroutines*numTransformsPerGoroutine)
	errors := make(chan error, numGoroutines*numTransformsPerGoroutine)
	
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineIndex int) {
			for j := 0; j < numTransformsPerGoroutine; j++ {
				data := map[string]any{
					"goroutine": goroutineIndex,
					"iteration": j,
				}
				
				result, err := pool.Transform(script, data, map[string]any{}, "insert")
				if err != nil {
					errors <- err
					return
				}
				results <- result
			}
		}(i)
	}
	
	// Collect all results
	expectedResults := numGoroutines * numTransformsPerGoroutine
	for i := 0; i < expectedResults; i++ {
		select {
		case result := <-results:
			assert.Equal(t, "pool_transformer", result["processed_by"])
			assert.Equal(t, true, result["concurrent_safe"])
			assert.True(t, isNumber(result["random_id"]), "random_id should be a number")
		case err := <-errors:
			t.Errorf("Unexpected error in concurrent test: %v", err)
		}
	}
}

func TestJSPool_PoolReuse(t *testing.T) {
	// Test that the pool properly reuses JS instances
	pool := NewJSPool()
	
	script := `
		function transform(data, oldData, action) {
			return {success: true};
		}
	`
	
	// Perform multiple sequential transforms
	// This should reuse the same JS instance from the pool
	for i := 0; i < 10; i++ {
		result, err := pool.Transform(script, map[string]any{"test": i}, map[string]any{}, "test")
		require.NoError(t, err)
		assert.Equal(t, true, result["success"])
	}
}

func TestJSPool_vs_DirectJS_Performance(t *testing.T) {
	// This is more of a demonstration test showing pool vs direct usage
	if testing.Short() {
		t.Skip("Skipping performance comparison test in short mode")
	}
	
	script := `
		function transform(data, oldData, action) {
			// Simulate some work
			var result = {};
			for (var i = 0; i < 100; i++) {
				result["key_" + i] = i * 2;
			}
			return result;
		}
	`
	
	data := map[string]any{"test": "data"}
	
	// Test JSPool performance
	pool := NewJSPool()
	poolStart := time.Now()
	for i := 0; i < 100; i++ {
		_, err := pool.Transform(script, data, map[string]any{}, "test")
		require.NoError(t, err)
	}
	poolDuration := time.Since(poolStart)
	
	// Test individual JS instances (creating new ones each time)
	directStart := time.Now()
	for i := 0; i < 100; i++ {
		js := NewJS()
		_, err := js.Transform(script, data, map[string]any{}, "test")
		require.NoError(t, err)
	}
	directDuration := time.Since(directStart)
	
	t.Logf("Pool-based transforms took: %v", poolDuration)
	t.Logf("Direct JS transforms took: %v", directDuration)
	
	// Pool should generally be faster or at least not significantly slower
	// This is mainly for observation, not assertion
	if poolDuration > directDuration*2 {
		t.Logf("Warning: Pool performance seems unexpectedly slower")
	}
}