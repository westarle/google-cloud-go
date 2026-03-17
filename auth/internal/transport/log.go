// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transport

import (
	"context"
	"log/slog"

	"github.com/googleapis/gax-go/v2/apierror"
	"github.com/googleapis/gax-go/v2/callctx"
)

// LogActionableError appends shared attributes (like resource_name, domain, error metadata, error.type) and logs the message at Debug level.
func LogActionableError(ctx context.Context, logger *slog.Logger, errorType string, msg string, attrs []slog.Attr, apiErr *apierror.APIError) {
	if resName, ok := callctx.TelemetryFromContext(ctx, "resource_name"); ok {
		attrs = append(attrs, slog.String("gcp.resource.destination.id", resName))
	}

	if apiErr != nil {
		if domain := apiErr.Domain(); domain != "" {
			attrs = append(attrs, slog.String("gcp.errors.domain", domain))
		}
		for k, v := range apiErr.Metadata() {
			attrs = append(attrs, slog.String("gcp.errors.metadata."+k, v))
		}
	}

	attrs = append(attrs, slog.String("error.type", errorType))

	logger.LogAttrs(ctx, slog.LevelDebug, msg, attrs...)
}
