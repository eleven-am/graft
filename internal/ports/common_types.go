package ports

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/eleven-am/graft/internal/domain"
)

type TypedNodeConfig struct {
	Name   string `json:"name"`
	Config TypedConfig `json:"config"`
}

type TypedConfig struct {
	Value interface{} `json:"value"`
}

func NewTypedConfig(value interface{}) TypedConfig {
	return TypedConfig{Value: value}
}

func (tc TypedConfig) AsString() (string, error) {
	if str, ok := tc.Value.(string); ok {
		return str, nil
	}
	return "", domain.Error{
		Type:    domain.ErrorTypeValidation,
		Message: "config value is not a string",
		Details: map[string]interface{}{
			"actual_type": fmt.Sprintf("%T", tc.Value),
		},
	}
}

func (tc TypedConfig) AsInt() (int, error) {
	switch v := tc.Value.(type) {
	case int:
		return v, nil
	case float64:
		return int(v), nil
	case json.Number:
		if i64, err := v.Int64(); err == nil {
			return int(i64), nil
		}
	}
	return 0, domain.Error{
		Type:    domain.ErrorTypeValidation,
		Message: "config value is not convertible to int",
		Details: map[string]interface{}{
			"actual_type": fmt.Sprintf("%T", tc.Value),
		},
	}
}

func (tc TypedConfig) AsBool() (bool, error) {
	if b, ok := tc.Value.(bool); ok {
		return b, nil
	}
	return false, domain.Error{
		Type:    domain.ErrorTypeValidation,
		Message: "config value is not a bool",
		Details: map[string]interface{}{
			"actual_type": fmt.Sprintf("%T", tc.Value),
		},
	}
}

func (tc TypedConfig) AsMap() (map[string]interface{}, error) {
	if m, ok := tc.Value.(map[string]interface{}); ok {
		return m, nil
	}
	return nil, domain.Error{
		Type:    domain.ErrorTypeValidation,
		Message: "config value is not a map",
		Details: map[string]interface{}{
			"actual_type": fmt.Sprintf("%T", tc.Value),
		},
	}
}

func (tc TypedConfig) AsStruct(target interface{}) error {
	data, err := json.Marshal(tc.Value)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to marshal config for struct conversion",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}
	
	if err := json.Unmarshal(data, target); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to unmarshal config to struct",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}
	
	return nil
}

type GlobalState struct {
	Value interface{} `json:"value"`
}

func NewGlobalState(value interface{}) GlobalState {
	return GlobalState{Value: value}
}

func (gs GlobalState) AsMap() (map[string]interface{}, error) {
	if m, ok := gs.Value.(map[string]interface{}); ok {
		return m, nil
	}
	return nil, domain.Error{
		Type:    domain.ErrorTypeValidation,
		Message: "global state is not a map",
		Details: map[string]interface{}{
			"actual_type": fmt.Sprintf("%T", gs.Value),
		},
	}
}

func (gs GlobalState) GetField(key string) (interface{}, bool) {
	if m, err := gs.AsMap(); err == nil {
		value, exists := m[key]
		return value, exists
	}
	return nil, false
}

func (gs *GlobalState) SetField(key string, value interface{}) error {
	if m, err := gs.AsMap(); err == nil {
		m[key] = value
		gs.Value = m
		return nil
	}
	return domain.Error{
		Type:    domain.ErrorTypeValidation,
		Message: "cannot set field on non-map global state",
		Details: map[string]interface{}{
			"key":         key,
			"actual_type": fmt.Sprintf("%T", gs.Value),
		},
	}
}

func (gs GlobalState) AsStruct(target interface{}) error {
	data, err := json.Marshal(gs.Value)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to marshal global state for struct conversion",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}
	
	if err := json.Unmarshal(data, target); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to unmarshal global state to struct",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}
	
	return nil
}

type ExecutionResult struct {
	Value interface{} `json:"value"`
}

func NewExecutionResult(value interface{}) ExecutionResult {
	return ExecutionResult{Value: value}
}

func (er ExecutionResult) AsError() error {
	if err, ok := er.Value.(error); ok {
		return err
	}
	if str, ok := er.Value.(string); ok && str != "" {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: str,
			Details: map[string]interface{}{
				"source": "execution_result",
			},
		}
	}
	return nil
}

func (er ExecutionResult) AsMap() (map[string]interface{}, error) {
	if m, ok := er.Value.(map[string]interface{}); ok {
		return m, nil
	}
	return nil, domain.Error{
		Type:    domain.ErrorTypeValidation,
		Message: "execution result is not a map",
		Details: map[string]interface{}{
			"actual_type": fmt.Sprintf("%T", er.Value),
		},
	}
}

func (er ExecutionResult) AsStruct(target interface{}) error {
	data, err := json.Marshal(er.Value)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to marshal execution result for struct conversion",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}
	
	if err := json.Unmarshal(data, target); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to unmarshal execution result to struct",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}
	
	return nil
}

type CommonConfigs struct{}

type DatabaseConfig struct {
	Host     string        `json:"host"`
	Port     int           `json:"port"`
	Database string        `json:"database"`
	Username string        `json:"username"`
	Password string        `json:"password"`
	Timeout  time.Duration `json:"timeout"`
}

type HTTPConfig struct {
	URL         string            `json:"url"`
	Method      string            `json:"method"`
	Headers     map[string]string `json:"headers"`
	Timeout     time.Duration     `json:"timeout"`
	RetryCount  int               `json:"retry_count"`
	RetryDelay  time.Duration     `json:"retry_delay"`
}

type FileConfig struct {
	Path     string      `json:"path"`
	Mode     string      `json:"mode"`
	Format   string      `json:"format"`
	Encoding string      `json:"encoding"`
	Options  interface{} `json:"options"`
}

type NotificationConfig struct {
	Type      string            `json:"type"`
	Target    string            `json:"target"`
	Template  string            `json:"template"`
	Variables map[string]string `json:"variables"`
}

type ConfigValidator struct{}

type ValidationError struct {
	Field   string `json:"field"`
	Value   interface{} `json:"value"`
	Rule    string `json:"rule"`
	Message string `json:"message"`
}

func (ve ValidationError) Error() string {
	return fmt.Sprintf("validation failed for field '%s': %s (value: %v, rule: %s)", 
		ve.Field, ve.Message, ve.Value, ve.Rule)
}

func NewValidationError(field string, value interface{}, rule string, message string) ValidationError {
	return ValidationError{
		Field:   field,
		Value:   value,
		Rule:    rule,
		Message: message,
	}
}

func (cv ConfigValidator) ValidateRequired(field string, value interface{}) error {
	if value == nil {
		return NewValidationError(field, value, "required", "field is required but was nil")
	}
	
	switch v := value.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return NewValidationError(field, value, "required", "string field cannot be empty")
		}
	case int, int32, int64:
		if v == 0 {
			return NewValidationError(field, value, "required", "numeric field cannot be zero")
		}
	case time.Duration:
		if v == 0 {
			return NewValidationError(field, value, "required", "duration field cannot be zero")
		}
	}
	
	return nil
}

func (cv ConfigValidator) ValidatePositive(field string, value interface{}) error {
	switch v := value.(type) {
	case int:
		if v <= 0 {
			return NewValidationError(field, value, "positive", "must be positive")
		}
	case int32:
		if v <= 0 {
			return NewValidationError(field, value, "positive", "must be positive")
		}
	case int64:
		if v <= 0 {
			return NewValidationError(field, value, "positive", "must be positive")
		}
	case float32:
		if v <= 0 {
			return NewValidationError(field, value, "positive", "must be positive")
		}
	case float64:
		if v <= 0 {
			return NewValidationError(field, value, "positive", "must be positive")
		}
	case time.Duration:
		if v <= 0 {
			return NewValidationError(field, value, "positive", "duration must be positive")
		}
	default:
		return NewValidationError(field, value, "positive", "field type not supported for positive validation")
	}
	
	return nil
}

func (cv ConfigValidator) ValidateNonNegative(field string, value interface{}) error {
	switch v := value.(type) {
	case int:
		if v < 0 {
			return NewValidationError(field, value, "non_negative", "cannot be negative")
		}
	case int32:
		if v < 0 {
			return NewValidationError(field, value, "non_negative", "cannot be negative")
		}
	case int64:
		if v < 0 {
			return NewValidationError(field, value, "non_negative", "cannot be negative")
		}
	case float32:
		if v < 0 {
			return NewValidationError(field, value, "non_negative", "cannot be negative")
		}
	case float64:
		if v < 0 {
			return NewValidationError(field, value, "non_negative", "cannot be negative")
		}
	case time.Duration:
		if v < 0 {
			return NewValidationError(field, value, "non_negative", "duration cannot be negative")
		}
	default:
		return NewValidationError(field, value, "non_negative", "field type not supported for non-negative validation")
	}
	
	return nil
}

func (cv ConfigValidator) ValidateRange(field string, value interface{}, min, max interface{}) error {
	switch v := value.(type) {
	case int:
		minVal, minOk := min.(int)
		maxVal, maxOk := max.(int)
		if minOk && maxOk {
			if v < minVal || v > maxVal {
				return NewValidationError(field, value, "range", fmt.Sprintf("must be between %d and %d", minVal, maxVal))
			}
		}
	case float64:
		minVal, minOk := min.(float64)
		maxVal, maxOk := max.(float64)
		if minOk && maxOk {
			if v < minVal || v > maxVal {
				return NewValidationError(field, value, "range", fmt.Sprintf("must be between %.2f and %.2f", minVal, maxVal))
			}
		}
	case time.Duration:
		minVal, minOk := min.(time.Duration)
		maxVal, maxOk := max.(time.Duration)
		if minOk && maxOk {
			if v < minVal || v > maxVal {
				return NewValidationError(field, value, "range", fmt.Sprintf("must be between %v and %v", minVal, maxVal))
			}
		}
	}
	
	return nil
}

func (cv ConfigValidator) ValidateOneOf(field string, value string, allowedValues []string) error {
	for _, allowed := range allowedValues {
		if value == allowed {
			return nil
		}
	}
	
	return NewValidationError(field, value, "oneof", 
		fmt.Sprintf("must be one of: %s", strings.Join(allowedValues, ", ")))
}

func (cv ConfigValidator) ValidateURL(field string, value string) error {
	if !strings.HasPrefix(value, "http://") && !strings.HasPrefix(value, "https://") {
		return NewValidationError(field, value, "url", "must be a valid URL starting with http:// or https://")
	}
	
	if strings.TrimSpace(value) == "" {
		return NewValidationError(field, value, "url", "URL cannot be empty")
	}
	
	return nil
}

func (cv ConfigValidator) ValidateMapNotEmpty(field string, value map[string]interface{}) error {
	if value == nil {
		return NewValidationError(field, value, "map_not_empty", "map cannot be nil")
	}
	
	if len(value) == 0 {
		return NewValidationError(field, value, "map_not_empty", "map cannot be empty")
	}
	
	return nil
}

func (cv ConfigValidator) ValidateSliceNotEmpty(field string, value []interface{}) error {
	if value == nil {
		return NewValidationError(field, value, "slice_not_empty", "slice cannot be nil")
	}
	
	if len(value) == 0 {
		return NewValidationError(field, value, "slice_not_empty", "slice cannot be empty")
	}
	
	return nil
}

type MultiValidationError struct {
	Errors []ValidationError `json:"errors"`
}

func (mve MultiValidationError) Error() string {
	if len(mve.Errors) == 1 {
		return mve.Errors[0].Error()
	}
	
	var messages []string
	for _, err := range mve.Errors {
		messages = append(messages, err.Error())
	}
	
	return fmt.Sprintf("multiple validation errors: [%s]", strings.Join(messages, "; "))
}

func ValidateAll(validations ...func() error) error {
	var errors []ValidationError
	
	for _, validate := range validations {
		if err := validate(); err != nil {
			if ve, ok := err.(ValidationError); ok {
				errors = append(errors, ve)
			} else {
				errors = append(errors, ValidationError{
					Field:   "unknown",
					Value:   nil,
					Rule:    "generic",
					Message: err.Error(),
				})
			}
		}
	}
	
	if len(errors) > 0 {
		return MultiValidationError{Errors: errors}
	}
	
	return nil
}