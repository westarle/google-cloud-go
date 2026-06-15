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

package internal

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"

	"errors"
	"net/http"
	"reflect"
	"testing"

	"cloud.google.com/go/compute/metadata"
)

func TestComputeUniverseDomainProvider(t *testing.T) {
	fatalErr := errors.New("fatal error")
	notDefinedError := metadata.NotDefinedError("universe/universe_domain")
	testCases := []struct {
		name    string
		getFunc func(context.Context, *metadata.Client) (string, error)
		want    string
		wantErr error
	}{
		{
			name: "test error",
			getFunc: func(context.Context, *metadata.Client) (string, error) {
				return "", fatalErr
			},
			want:    "",
			wantErr: fatalErr,
		},
		{
			name: "test error 404",
			getFunc: func(context.Context, *metadata.Client) (string, error) {
				return "", notDefinedError
			},
			want:    DefaultUniverseDomain,
			wantErr: nil,
		},
		{
			name: "test valid",
			getFunc: func(context.Context, *metadata.Client) (string, error) {
				return "example.com", nil
			},
			want:    "example.com",
			wantErr: nil,
		},
	}

	oldGet := httpGetMetadataUniverseDomain
	defer func() {
		httpGetMetadataUniverseDomain = oldGet
	}()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			httpGetMetadataUniverseDomain = tc.getFunc
			c := ComputeUniverseDomainProvider{}
			got, err := c.GetProperty(context.Background())
			if err != tc.wantErr {
				t.Errorf("got error %v; want error %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("got %v; want %v", got, tc.want)
			}
		})
	}
}

type fakeClonableTransport struct {
	clone *http.Transport
}

func (t *fakeClonableTransport) Clone() *http.Transport {
	return t.clone
}

func (t *fakeClonableTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

type fakeTransport struct{}

func (t *fakeTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func TestDefaultClient(t *testing.T) {
	transportBeforeTest := http.DefaultTransport
	defer func() { http.DefaultTransport = transportBeforeTest }()

	got := DefaultClient()
	if got.Transport == http.DefaultTransport {
		t.Errorf("DefaultClient() = %v, expected a clone of http.DefaultTransport", got)
	}

	cloneTransport := &http.Transport{}
	http.DefaultTransport = &fakeClonableTransport{clone: cloneTransport}
	got = DefaultClient()
	if got.Transport != cloneTransport {
		t.Errorf("DefaultClient() = %v, want %v", got, cloneTransport)
	}

	fakeTransport := &fakeTransport{}
	http.DefaultTransport = fakeTransport
	got = DefaultClient()
	if got.Transport != fakeTransport {
		t.Errorf("DefaultClient() = %v, want %v", got, fakeTransport)
	}
}

func TestNewRegionalAccessBoundaryData(t *testing.T) {
	tests := []struct {
		name             string
		locations        []string
		encodedLocations string
		wantLocations    []string
		wantEncoded      string
	}{
		{
			name:             "Standard data",
			locations:        []string{"us-central1", "europe-west1"},
			encodedLocations: "0xABC123",
			wantLocations:    []string{"us-central1", "europe-west1"},
			wantEncoded:      "0xABC123",
		},
		{
			name:             "Empty locations, with encoded locations",
			locations:        []string{},
			encodedLocations: "0xDEF456",
			wantLocations:    []string{},
			wantEncoded:      "0xDEF456",
		},
		{
			name:             "Nil locations, with encoded locations",
			locations:        nil,
			encodedLocations: "0xGHI789",
			wantLocations:    []string{}, // Expect empty slice, not nil
			wantEncoded:      "0xGHI789",
		},
		{
			name:             "Empty string encoded locations",
			locations:        []string{},
			encodedLocations: "",
			wantLocations:    []string{},
			wantEncoded:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := NewRegionalAccessBoundaryData(tt.locations, tt.encodedLocations)

			if got := data.EncodedLocations; got != tt.wantEncoded {
				t.Errorf("NewRegionalAccessBoundaryData().EncodedLocations = %q, want %q", got, tt.wantEncoded)
			}

			gotLocations := data.Locations
			if !reflect.DeepEqual(gotLocations, tt.wantLocations) {
				t.Errorf("NewRegionalAccessBoundaryData().Locations = %v, want %v", gotLocations, tt.wantLocations)
			}
		})
	}
}

func TestRegionalAccessBoundaryHeader(t *testing.T) {
	tests := []struct {
		name        string
		tbd         RegionalAccessBoundaryData
		wantValue   string
		wantPresent bool
	}{
		{
			name:        "empty data",
			tbd:         RegionalAccessBoundaryData{},
			wantValue:   "",
			wantPresent: false,
		},
		{
			name:        "regular data",
			tbd:         *NewRegionalAccessBoundaryData(nil, "some-encoded-locations"),
			wantValue:   "some-encoded-locations",
			wantPresent: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotValue, gotPresent := tt.tbd.RegionalAccessBoundaryHeader()
			if gotValue != tt.wantValue {
				t.Errorf("RegionalAccessBoundaryHeader() gotValue = %v, want %v", gotValue, tt.wantValue)
			}
			if gotPresent != tt.wantPresent {
				t.Errorf("RegionalAccessBoundaryHeader() gotPresent = %v, want %v", gotPresent, tt.wantPresent)
			}
		})
	}
}



func TestParseKey(t *testing.T) {
	// Generate RSA key for PKCS1 and PKCS8
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate RSA key: %v", err)
	}

	// PKCS1
	pkcs1Bytes := x509.MarshalPKCS1PrivateKey(rsaKey)
	pkcs1Pem := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: pkcs1Bytes})

	// PKCS8
	pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(rsaKey)
	if err != nil {
		t.Fatalf("failed to marshal PKCS8: %v", err)
	}
	pkcs8Pem := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8Bytes})

	// EC
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate EC key: %v", err)
	}
	ecBytes, err := x509.MarshalECPrivateKey(ecKey)
	if err != nil {
		t.Fatalf("failed to marshal EC: %v", err)
	}
	ecPem := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: ecBytes})

	// Invalid
	invalidKey := []byte("invalid-key-data")

	tests := []struct {
		name    string
		key     []byte
		wantErr bool
	}{
		{"PKCS1", pkcs1Pem, false},
		{"PKCS8", pkcs8Pem, false},
		{"EC", ecPem, false},
		{"Raw PKCS1 (No PEM)", pkcs1Bytes, false},
		{"Raw PKCS8 (No PEM)", pkcs8Bytes, false},
		{"Raw EC (No PEM)", ecBytes, false},
		{"Invalid Key", invalidKey, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseKey(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseKey() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
