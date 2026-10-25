package engine

import (
	"context"
	"fmt"

	"github.com/fluffyTighnari/workflow-engine/pkg/workflow"
)

// ComputeExecutor adds the two numeric config values "a" and "b" and
// returns the sum as output key "result".
type ComputeExecutor struct{}

func (e *ComputeExecutor) Execute(
	ctx context.Context,
	step workflow.Step,
	input map[string]any,
) (map[string]any, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	a, err := parseNumber(step.Config["a"])
	if err != nil {
		return nil, fmt.Errorf("config a: %w", err)
	}

	b, err := parseNumber(step.Config["b"])
	if err != nil {
		return nil, fmt.Errorf("config b: %w", err)
	}

	return map[string]any{
		"result": a + b,
	}, nil
}

func parseNumber(s string) (float64, error) {
	var n float64

	_, err := fmt.Sscan(s, &n)
	if err != nil {
		return 0, fmt.Errorf("invalid number %q: %w", s, err)
	}

	return n, nil
}

// DoubleExecutor reads the numeric input "value" and returns it doubled
// as output key "result". The input must be declared explicitly via
// Step.Inputs, e.g.:
//
//	Inputs: map[string]workflow.InputRef{
//	    "value": {Source: "upstream_step_id", Key: "result"},
//	}
type DoubleExecutor struct{}

func (e *DoubleExecutor) Execute(
	ctx context.Context,
	step workflow.Step,
	input map[string]any,
) (map[string]any, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	value, ok := input["value"].(float64)
	if !ok {
		return nil, fmt.Errorf(
			"step %q: input \"value\" must be a number",
			step.ID,
		)
	}

	return map[string]any{
		"result": value * 2,
	}, nil
}
