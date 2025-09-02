package core

import (
	"context"
	"encoding/json"
	"fmt"
)

func WrapHandler[T any](handler func(ctx context.Context, from string, params T) error) CommandHandler {
	return func(ctx context.Context, from string, params interface{}) error {
		var typedParams T

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
