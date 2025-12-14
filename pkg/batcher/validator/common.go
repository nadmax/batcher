package validator

import (
	"fmt"
	"regexp"
	"time"
)

type ParameterValidator interface {
	Validate(params map[string]any) error
}

type RequiredParametersValidator struct {
	RequiredParams []string
}

func NewRequiredParametersValidator(requiredParams ...string) *RequiredParametersValidator {
	return &RequiredParametersValidator{
		RequiredParams: requiredParams,
	}
}

func (v *RequiredParametersValidator) Validate(params map[string]any) error {
	for _, required := range v.RequiredParams {
		val, exists := params[required]
		if !exists {
			return fmt.Errorf("required parameter '%s' is missing", required)
		}
		if val == nil {
			return fmt.Errorf("required parameter '%s' is nil", required)
		}
	}
	return nil
}

type TypeValidator struct {
	TypeChecks map[string]string
}

func NewTypeValidator(typeChecks map[string]string) *TypeValidator {
	return &TypeValidator{
		TypeChecks: typeChecks,
	}
}

func (v *TypeValidator) Validate(params map[string]any) error {
	for paramName, expectedType := range v.TypeChecks {
		val, exists := params[paramName]
		if !exists {
			continue
		}

		if !v.checkType(val, expectedType) {
			return fmt.Errorf("parameter '%s' has incorrect type (expected: %s, got: %T)",
				paramName, expectedType, val)
		}
	}
	return nil
}

func (v *TypeValidator) checkType(val any, expectedType string) bool {
	switch expectedType {
	case "string":
		_, ok := val.(string)
		return ok
	case "int":
		_, ok := val.(int)
		return ok
	case "int64":
		_, ok := val.(int64)
		return ok
	case "float64":
		_, ok := val.(float64)
		return ok
	case "bool":
		_, ok := val.(bool)
		return ok
	case "time.Time":
		_, ok := val.(time.Time)
		return ok
	default:
		return false
	}
}

type RangeValidator struct {
	Ranges map[string]Range
}

type Range struct {
	Min float64
	Max float64
}

func NewRangeValidator(ranges map[string]Range) *RangeValidator {
	return &RangeValidator{
		Ranges: ranges,
	}
}

func (v *RangeValidator) Validate(params map[string]any) error {
	for paramName, validRange := range v.Ranges {
		val, exists := params[paramName]
		if !exists {
			continue
		}

		numVal, err := toFloat64(val)
		if err != nil {
			return fmt.Errorf("parameter '%s' is not a number: %v", paramName, err)
		}

		if numVal < validRange.Min {
			return fmt.Errorf("parameter '%s' is below minimum (value: %.2f, min: %.2f)",
				paramName, numVal, validRange.Min)
		}
		if numVal > validRange.Max {
			return fmt.Errorf("parameter '%s' is above maximum (value: %.2f, max: %.2f)",
				paramName, numVal, validRange.Max)
		}
	}
	return nil
}

func toFloat64(val any) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", val)
	}
}

type DateRangeValidator struct {
	StartDateParam string
	EndDateParam   string
	MaxRange       time.Duration
}

func NewDateRangeValidator(startDateParam, endDateParam string, maxRange time.Duration) *DateRangeValidator {
	return &DateRangeValidator{
		StartDateParam: startDateParam,
		EndDateParam:   endDateParam,
		MaxRange:       maxRange,
	}
}

func (v *DateRangeValidator) Validate(params map[string]any) error {
	startVal, startExists := params[v.StartDateParam]
	endVal, endExists := params[v.EndDateParam]

	if !startExists || !endExists {
		return fmt.Errorf("date range parameters '%s' and '%s' are required",
			v.StartDateParam, v.EndDateParam)
	}

	startDate, ok := startVal.(time.Time)
	if !ok {
		return fmt.Errorf("parameter '%s' must be a time.Time", v.StartDateParam)
	}

	endDate, ok := endVal.(time.Time)
	if !ok {
		return fmt.Errorf("parameter '%s' must be a time.Time", v.EndDateParam)
	}

	if endDate.Before(startDate) {
		return fmt.Errorf("end date '%s' must be after start date '%s'",
			v.EndDateParam, v.StartDateParam)
	}

	if v.MaxRange > 0 {
		duration := endDate.Sub(startDate)
		if duration > v.MaxRange {
			return fmt.Errorf("date range exceeds maximum allowed duration (duration: %v, max: %v)",
				duration, v.MaxRange)
		}
	}

	return nil
}

type PatternValidator struct {
	Patterns map[string]*regexp.Regexp
}

func NewPatternValidator(patterns map[string]string) (*PatternValidator, error) {
	compiled := make(map[string]*regexp.Regexp)
	for paramName, pattern := range patterns {
		regex, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid regex pattern for parameter '%s': %v", paramName, err)
		}
		compiled[paramName] = regex
	}

	return &PatternValidator{
		Patterns: compiled,
	}, nil
}

func (v *PatternValidator) Validate(params map[string]any) error {
	for paramName, pattern := range v.Patterns {
		val, exists := params[paramName]
		if !exists {
			continue
		}

		strVal, ok := val.(string)
		if !ok {
			return fmt.Errorf("parameter '%s' must be a string", paramName)
		}

		if !pattern.MatchString(strVal) {
			return fmt.Errorf("parameter '%s' does not match required pattern (value: %s, pattern: %s)",
				paramName, strVal, pattern.String())
		}
	}
	return nil
}

type EnumValidator struct {
	AllowedValues map[string][]any
}

func NewEnumValidator(allowedValues map[string][]any) *EnumValidator {
	return &EnumValidator{
		AllowedValues: allowedValues,
	}
}

func (v *EnumValidator) Validate(params map[string]any) error {
	for paramName, allowedList := range v.AllowedValues {
		val, exists := params[paramName]
		if !exists {
			continue
		}

		found := false
		for _, allowed := range allowedList {
			if val == allowed {
				found = true
				break
			}
		}

		if !found {
			return fmt.Errorf("parameter '%s' has invalid value (value: %v, allowed: %v)",
				paramName, val, allowedList)
		}
	}
	return nil
}

type CompositeValidator struct {
	Validators []ParameterValidator
}

func NewCompositeValidator(validators ...ParameterValidator) *CompositeValidator {
	return &CompositeValidator{
		Validators: validators,
	}
}

func (v *CompositeValidator) Validate(params map[string]any) error {
	for _, validator := range v.Validators {
		if err := validator.Validate(params); err != nil {
			return err
		}
	}
	return nil
}

type CustomValidator struct {
	ValidateFunc func(params map[string]any) error
}

func NewCustomValidator(validateFunc func(params map[string]any) error) *CustomValidator {
	return &CustomValidator{
		ValidateFunc: validateFunc,
	}
}

func (v *CustomValidator) Validate(params map[string]any) error {
	if v.ValidateFunc == nil {
		return nil
	}
	return v.ValidateFunc(params)
}

func ValidateNotEmpty(params map[string]any, paramName string) error {
	val, exists := params[paramName]
	if !exists {
		return fmt.Errorf("parameter '%s' is required", paramName)
	}

	strVal, ok := val.(string)
	if !ok {
		return fmt.Errorf("parameter '%s' must be a string", paramName)
	}

	if strVal == "" {
		return fmt.Errorf("parameter '%s' cannot be empty", paramName)
	}

	return nil
}

func ValidatePositive(params map[string]any, paramName string) error {
	val, exists := params[paramName]
	if !exists {
		return fmt.Errorf("parameter '%s' is required", paramName)
	}

	numVal, err := toFloat64(val)
	if err != nil {
		return fmt.Errorf("parameter '%s' must be a number: %v", paramName, err)
	}

	if numVal <= 0 {
		return fmt.Errorf("parameter '%s' must be positive (value: %.2f)", paramName, numVal)
	}

	return nil
}

func ValidateFileExists(params map[string]any, paramName string) error {
	val, exists := params[paramName]
	if !exists {
		return fmt.Errorf("parameter '%s' is required", paramName)
	}

	path, ok := val.(string)
	if !ok {
		return fmt.Errorf("parameter '%s' must be a string (file path)", paramName)
	}

	if path == "" {
		return fmt.Errorf("parameter '%s' cannot be empty", paramName)
	}

	return nil
}
