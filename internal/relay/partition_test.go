package relay

import (
	"testing"

	"cloud.google.com/go/spanner"
)

func TestParsePayload(t *testing.T) {
	const traceparent = "00-91c68607826dc74e90698094221e9074-206532280e758499-01"

	tests := []struct {
		name        string
		payload     spanner.NullJSON
		wantErr     bool
		wantTrace   string
		wantDataStr string
	}{
		{
			name: "change stream string value",
			payload: spanner.NullJSON{
				Value: `{"data":{"video_id":"v1"},"traceparent":"` + traceparent + `"}`,
				Valid: true,
			},
			wantTrace:   traceparent,
			wantDataStr: `{"video_id":"v1"}`,
		},
		{
			name: "direct read object value",
			payload: spanner.NullJSON{
				Value: map[string]any{
					"traceparent": traceparent,
					"data":        map[string]any{"video_id": "v1"},
				},
				Valid: true,
			},
			wantTrace:   traceparent,
			wantDataStr: `{"video_id":"v1"}`,
		},
		{
			name:    "invalid null payload",
			payload: spanner.NullJSON{Valid: false},
			wantErr: true,
		},
		{
			name:    "malformed json string",
			payload: spanner.NullJSON{Value: `{"data":`, Valid: true},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, data, err := parsePayload(tt.payload)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parsePayload() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePayload() unexpected error: %v", err)
			}
			if env.Traceparent != tt.wantTrace {
				t.Errorf("traceparent = %q, want %q", env.Traceparent, tt.wantTrace)
			}
			if string(data) != tt.wantDataStr {
				t.Errorf("data = %q, want %q", string(data), tt.wantDataStr)
			}
		})
	}
}
