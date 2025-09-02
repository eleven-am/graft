package core

import (
	"context"
	"encoding/json"
	"fmt"
)

func WrapHandler[T any](handler func(ctx context.Context, from string, params T) error) CommandHandler {
	return func(ctx context.Context, from string, params interface{}) error {
		if v, ok := params.(T); ok {
			return handler(ctx, from, v)
		}

		var typedParams T

		switch p := params.(type) {
		case json.RawMessage:
			if err := json.Unmarshal(p, &typedParams); err != nil {
				return fmt.Errorf("failed to unmarshal raw params to %T: %w", typedParams, err)
			}
			return handler(ctx, from, typedParams)
		case []byte:
			if err := json.Unmarshal(p, &typedParams); err != nil {
				return fmt.Errorf("failed to unmarshal bytes to %T: %w", typedParams, err)
			}
			return handler(ctx, from, typedParams)
		default:
			jsonData, err := json.Marshal(params)
			if err != nil {
				return fmt.Errorf("failed to marshal params: %w", err)
			}
			if err := json.Unmarshal(jsonData, &typedParams); err != nil {
				return fmt.Errorf("failed to unmarshal params to %T: %w", typedParams, err)
			}
			return handler(ctx, from, typedParams)
		}
	}
}
