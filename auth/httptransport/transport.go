// Copyright 2023 Google LLC
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

package httptransport

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/auth/internal"
	"cloud.google.com/go/auth/internal/transport"
	"cloud.google.com/go/auth/internal/transport/cert"
	"cloud.google.com/go/auth/internal/transport/headers"
	"github.com/googleapis/gax-go/v2"
	"github.com/googleapis/gax-go/v2/callctx"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/net/http2"
	"google.golang.org/api/googleapi"
)

const (
	quotaProjectHeaderKey = "X-goog-user-project"
)

func newTransport(base http.RoundTripper, opts *Options) (http.RoundTripper, error) {
	var headers = opts.Headers
	ht := &headerTransport{
		base:    base,
		headers: headers,
	}
	var trans http.RoundTripper = ht
	trans = addOpenTelemetryTransport(trans, opts)
	switch {
	case opts.DisableAuthentication:
		// Do nothing.
	case opts.APIKey != "":
		qp := internal.GetQuotaProject(nil, opts.Headers.Get(quotaProjectHeaderKey))
		if qp != "" {
			if headers == nil {
				headers = make(map[string][]string, 1)
			}
			headers.Set(quotaProjectHeaderKey, qp)
		}
		trans = &apiKeyTransport{
			Transport: trans,
			Key:       opts.APIKey,
		}
	default:
		var creds *auth.Credentials
		if opts.Credentials != nil {
			creds = opts.Credentials
		} else {
			var err error
			creds, err = credentials.DetectDefault(opts.resolveDetectOptions())
			if err != nil {
				return nil, err
			}
		}
		qp, err := creds.QuotaProjectID(context.Background())
		if err != nil {
			return nil, err
		}
		if qp != "" {
			if headers == nil {
				headers = make(map[string][]string, 1)
			}
			// Don't overwrite user specified quota
			if v := headers.Get(quotaProjectHeaderKey); v == "" {
				headers.Set(quotaProjectHeaderKey, qp)
			}
		}
		var skipUD bool
		if iOpts := opts.InternalOptions; iOpts != nil {
			skipUD = iOpts.SkipUniverseDomainValidation
		}
		creds.TokenProvider = auth.NewCachedTokenProvider(creds.TokenProvider, nil)
		trans = &authTransport{
			base:                         trans,
			creds:                        creds,
			clientUniverseDomain:         opts.UniverseDomain,
			skipUniverseDomainValidation: skipUD,
		}
	}
	return trans, nil
}

// defaultBaseTransport returns the base HTTP transport.
// On App Engine, this is urlfetch.Transport.
// Otherwise, use a default transport, taking most defaults from
// http.DefaultTransport.
// If TLSCertificate is available, set TLSClientConfig as well.
func defaultBaseTransport(clientCertSource cert.Provider, dialTLSContext func(context.Context, string, string) (net.Conn, error)) http.RoundTripper {
	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		defaultTransport = transport.BaseTransport()
	}
	trans := defaultTransport.Clone()
	trans.MaxIdleConnsPerHost = 100

	if clientCertSource != nil {
		trans.TLSClientConfig = &tls.Config{
			GetClientCertificate: clientCertSource,
		}
	}
	if dialTLSContext != nil {
		// If DialTLSContext is set, TLSClientConfig wil be ignored
		trans.DialTLSContext = dialTLSContext
	}

	// Configures the ReadIdleTimeout HTTP/2 option for the
	// transport. This allows broken idle connections to be pruned more quickly,
	// preventing the client from attempting to re-use connections that will no
	// longer work.
	http2Trans, err := http2.ConfigureTransports(trans)
	if err == nil {
		http2Trans.ReadIdleTimeout = time.Second * 31
	}

	return trans
}

type apiKeyTransport struct {
	// Key is the API Key to set on requests.
	Key string
	// Transport is the underlying HTTP transport.
	// If nil, http.DefaultTransport is used.
	Transport http.RoundTripper
}

func (t *apiKeyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	newReq := *req
	args := newReq.URL.Query()
	args.Set("key", t.Key)
	newReq.URL.RawQuery = args.Encode()
	return t.Transport.RoundTrip(&newReq)
}

type headerTransport struct {
	headers http.Header
	base    http.RoundTripper
}

func (t *headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt := t.base
	newReq := *req
	newReq.Header = make(http.Header)
	for k, vv := range req.Header {
		newReq.Header[k] = vv
	}

	for k, v := range t.headers {
		newReq.Header[k] = v
	}

	return rt.RoundTrip(&newReq)
}

func addOpenTelemetryTransport(trans http.RoundTripper, opts *Options) http.RoundTripper {
	if opts.DisableTelemetry {
		return trans
	}

	var traceAttrs []attribute.KeyValue
	var scopedLogger *slog.Logger

	if gax.IsFeatureEnabled("LOGGING") && opts.Logger != nil {
		scopedLogger = opts.Logger
	}

	if opts.InternalOptions != nil {
		attrs := transport.StaticTelemetryAttributes(opts.InternalOptions.TelemetryAttributes)
		if gax.IsFeatureEnabled("TRACING") {
			traceAttrs = attrs
		}
		if scopedLogger != nil {
			var logAttrs []any
			for _, attr := range attrs {
				logAttrs = append(logAttrs, slog.String(string(attr.Key), attr.Value.AsString()))
			}
			scopedLogger = scopedLogger.With(logAttrs...)
		}
	}

	if gax.IsFeatureEnabled("METRICS") || gax.IsFeatureEnabled("TRACING") || gax.IsFeatureEnabled("LOGGING") {
		trans = &otelAttributeTransport{
			base:   trans,
			logger: scopedLogger,
		}
	}

	if !gax.IsFeatureEnabled("TRACING") && !gax.IsFeatureEnabled("LOGGING") {
		return otelhttp.NewTransport(trans)
	}

	var otelOpts []otelhttp.Option
	if len(traceAttrs) > 0 {
		otelOpts = append(otelOpts, otelhttp.WithSpanOptions(trace.WithAttributes(traceAttrs...)))
	}
	return otelhttp.NewTransport(trans, otelOpts...)
}

// otelAttributeTransport is a wrapper around an http.RoundTripper that adds
// custom Google Cloud-specific attributes to OpenTelemetry spans.
type otelAttributeTransport struct {
	base   http.RoundTripper
	logger *slog.Logger
}

// RoundTrip intercepts the HTTP request and response to enrich the active
// OpenTelemetry span with static and dynamic attributes, as well as detailed
// error information.
func (t *otelAttributeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var span trace.Span
	if gax.IsFeatureEnabled("TRACING") {
		if s := trace.SpanFromContext(req.Context()); s != nil && s.IsRecording() {
			span = s
		}
	}

	if span != nil {
		var attrs []attribute.KeyValue
		attrs = append(attrs, attribute.String("rpc.system.name", "http"))
		if resName, ok := callctx.TelemetryFromContext(req.Context(), "resource_name"); ok {
			attrs = append(attrs, attribute.String("gcp.resource.destination.id", resName))
		}
		if resendCountStr, ok := callctx.TelemetryFromContext(req.Context(), "resend_count"); ok {
			if count, err := strconv.Atoi(resendCountStr); err == nil {
				attrs = append(attrs, attribute.Int("http.request.resend_count", count))
			}
		}
		if urlTemplate, ok := callctx.TelemetryFromContext(req.Context(), "url_template"); ok {
			attrs = append(attrs, attribute.String("url.template", urlTemplate))
			span.SetName(fmt.Sprintf("%s %s", req.Method, urlTemplate))
		}
		span.SetAttributes(attrs...)
	}

	var data *gax.TransportTelemetryData
	if gax.IsFeatureEnabled("METRICS") {
		data = gax.ExtractTransportTelemetry(req.Context())
		if data != nil && req.URL != nil {
			host := req.URL.Hostname()
			if host != "" {
				data.SetServerAddress(host)
			}
			portStr := req.URL.Port()
			if portStr == "" {
				if req.URL.Scheme == "https" {
					portStr = "443"
				} else if req.URL.Scheme == "http" {
					portStr = "80"
				}
			}
			if port, pErr := strconv.Atoi(portStr); pErr == nil {
				data.SetServerPort(port)
			}
		}
	}

	resp, err := t.base.RoundTrip(req)

	var logger *slog.Logger
	if gax.IsFeatureEnabled("LOGGING") {
		if l := t.logger; l != nil && l.Enabled(req.Context(), slog.LevelDebug) {
			logger = l
		}
	}

	if span == nil && logger == nil {
		return resp, err
	}

	if err != nil {
		t.logAndSpanError(req, resp, err, err, span, logger)
	} else if resp.StatusCode >= 400 {
		if resp.Body != nil && resp.Body != http.NoBody && (resp.ContentLength < 0 || resp.ContentLength <= maxErrorReadBytes) {
			resp.Body = &errorTrackingBody{
				ReadCloser: resp.Body,
				req:        req,
				resp:       resp,
				span:       span,
				logger:     logger,
				t:          t,
			}
		} else {
			t.logAndSpanError(req, resp, &googleapi.Error{
				Code:    resp.StatusCode,
				Message: resp.Status,
			}, nil, span, logger)
		}
	} else {
		if span != nil {
			span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
		}
	}

	return resp, err
}

func (t *otelAttributeTransport) onError(req *http.Request, resp *http.Response, err error, span trace.Span, logger *slog.Logger) {
	var httpStatusCode int
	if resp != nil {
		httpStatusCode = resp.StatusCode
	}

	var errToParse error
	if err != nil {
		errToParse = err
	} else if respErr := readResponseError(resp); respErr != nil {
		errToParse = respErr
	}

	info := gax.ExtractTelemetryErrorInfo(req.Context(), errToParse)

	if err == nil && resp != nil && resp.StatusCode >= 400 {
		if info.ErrorType == "*googleapi.Error" {
			info.ErrorType = strconv.Itoa(resp.StatusCode)
		}
	}

	if logger != nil {
		logAttrs := []slog.Attr{
			slog.String("rpc.system.name", "http"),
		}
		if httpStatusCode > 0 {
			logAttrs = append(logAttrs, slog.Int64("http.response.status_code", int64(httpStatusCode)))
		}

		ctx := req.Context()
		if resendCountStr, ok := callctx.TelemetryFromContext(ctx, "resend_count"); ok {
			if count, e := strconv.Atoi(resendCountStr); e == nil {
				logAttrs = append(logAttrs, slog.Int64("http.request.resend_count", int64(count)))
			}
		}
		if urlTemplate, ok := callctx.TelemetryFromContext(ctx, "url_template"); ok {
			logAttrs = append(logAttrs, slog.String("url.template", urlTemplate))
		}
		logAttrs = append(logAttrs, slog.String("http.request.method", req.Method))

		msg := info.StatusMessage
		if msg == "" {
			msg = "API call failed"
		}

		if rpcMethod, ok := callctx.TelemetryFromContext(ctx, "rpc_method"); ok {
			logAttrs = append(logAttrs, slog.String("rpc.method", rpcMethod))
		}

		if resName, ok := callctx.TelemetryFromContext(ctx, "resource_name"); ok {
			logAttrs = append(logAttrs, slog.String("gcp.resource.destination.id", resName))
		}

		if info.Domain != "" {
			logAttrs = append(logAttrs, slog.String("gcp.errors.domain", info.Domain))
		}
		for k, v := range info.Metadata {
			logAttrs = append(logAttrs, slog.String("gcp.errors.metadata."+k, v))
		}

		logAttrs = append(logAttrs, slog.String("error.type", info.ErrorType))
		if info.StatusCode != "" {
			logAttrs = append(logAttrs, slog.String("rpc.response.status_code", info.StatusCode))
		}

		logger.LogAttrs(ctx, slog.LevelDebug, msg, logAttrs...)
	}

	if span != nil {
		if err != nil {
			span.SetAttributes(
				attribute.String("error.type", info.ErrorType),
				attribute.String("status.message", info.StatusMessage),
				attribute.String("exception.type", fmt.Sprintf("%T", err)),
			)
		} else {
			span.SetAttributes(
				attribute.Int("http.response.status_code", httpStatusCode),
				attribute.String("error.type", info.ErrorType),
				attribute.String("status.message", info.StatusMessage),
			)
		}
	}
}

type bodyReader struct {
	io.Reader
	orig io.ReadCloser
}

func (b *bodyReader) Close() error {
	return b.orig.Close()
}

type errReader struct {
	err error
}

func (e *errReader) Read(p []byte) (int, error) {
	return 0, e.err
}

// cloneResponse reads the response body into memory and returns a shallow clone of the response
// with an unexhausted stream, while transparently restoring the original response stream.
//
// This is necessary because we need to parse the error details for transport layer telemetry
// but the downstream generated client also expects to read the response. Since error
// responses are typically small and infrequent, the overhead of double-parsing is acceptable.
// To prevent memory spikes and out-of-memory errors from unexpectedly large error responses,
// we limit the bytes buffered in memory to 1MB. The original response will still be able to
// read the full stream.
func cloneResponse(resp *http.Response) (*http.Response, error) {
	if resp == nil || resp.Body == nil || resp.Body == http.NoBody {
		return resp, nil
	}

	const maxErrorReadBytes = int64(1 << 20) // 1MB

	var buf bytes.Buffer
	if resp.ContentLength > 0 && resp.ContentLength <= maxErrorReadBytes {
		buf.Grow(int(resp.ContentLength))
	}

	_, err := buf.ReadFrom(io.LimitReader(resp.Body, maxErrorReadBytes))
	var reader io.Reader = bytes.NewReader(buf.Bytes())
	if err != nil {
		reader = io.MultiReader(reader, &errReader{err: err})
	} else {
		// Concatenate any unread portion of the stream back onto the end
		reader = io.MultiReader(reader, resp.Body)
	}
	resp.Body = &bodyReader{Reader: reader, orig: resp.Body}

	clone := *resp
	clone.Body = io.NopCloser(bytes.NewReader(buf.Bytes()))
	return &clone, err
}

// readResponseError attempts to extract an actionable error payload from the HTTP response.
// It clones the response stream up to a safe limit, parses the JSON error, and ensures
// a baseline fallback message is always populated to guarantee high-quality telemetry.
func readResponseError(resp *http.Response) error {
	if resp == nil || resp.StatusCode < 400 {
		return nil
	}

	errToParse := &googleapi.Error{
		Code:    resp.StatusCode,
		Message: resp.Status,
	}

	if resp.Body != nil {
		clone, ioErr := cloneResponse(resp)
		if ioErr == nil {
			if errResp := googleapi.CheckResponse(clone); errResp != nil {
				if gErr, ok := errResp.(*googleapi.Error); ok {
					if gErr.Message == "" {
						// Keep the default Status string if CheckResponse parsed no message.
						gErr.Message = resp.Status
					}
					errToParse = gErr
				} else {
					// Fallback in case googleapi returns a different error implementation in the future
					errToParse = &googleapi.Error{
						Code:    resp.StatusCode,
						Message: errResp.Error(),
					}
				}
			}
		}
	}

	return errToParse
}

type authTransport struct {
	creds                        *auth.Credentials
	base                         http.RoundTripper
	clientUniverseDomain         string
	skipUniverseDomainValidation bool
}

// getClientUniverseDomain returns the default service domain for a given Cloud
// universe, with the following precedence:
//
// 1. A non-empty option.WithUniverseDomain or similar client option.
// 2. A non-empty environment variable GOOGLE_CLOUD_UNIVERSE_DOMAIN.
// 3. The default value "googleapis.com".
//
// This is the universe domain configured for the client, which will be compared
// to the universe domain that is separately configured for the credentials.
func (t *authTransport) getClientUniverseDomain() string {
	if t.clientUniverseDomain != "" {
		return t.clientUniverseDomain
	}
	if envUD := os.Getenv(internal.UniverseDomainEnvVar); envUD != "" {
		return envUD
	}
	return internal.DefaultUniverseDomain
}

// RoundTrip authorizes and authenticates the request with an
// access token from Transport's Source. Per the RoundTripper contract we must
// not modify the initial request, so we clone it, and we must close the body
// on any errors that happens during our token logic.
func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	reqBodyClosed := false
	if req.Body != nil {
		defer func() {
			if !reqBodyClosed {
				req.Body.Close()
			}
		}()
	}
	token, err := t.creds.Token(req.Context())
	if err != nil {
		return nil, err
	}
	if !t.skipUniverseDomainValidation && token.MetadataString("auth.google.tokenSource") != "compute-metadata" {
		credentialsUniverseDomain, err := t.creds.UniverseDomain(req.Context())
		if err != nil {
			return nil, err
		}
		if err := transport.ValidateUniverseDomain(t.getClientUniverseDomain(), credentialsUniverseDomain); err != nil {
			return nil, err
		}
	}
	req2 := req.Clone(req.Context())
	headers.SetAuthHeader(token, req2)
	reqBodyClosed = true
	return t.base.RoundTrip(req2)
}
