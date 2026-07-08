/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package dynauth bootstraps the webhook server's TLS material without an
// external CA dependency. A single operator pod self-signs a CA, mints a leaf
// cert for the webhook Service, persists both as Secrets in the operator
// namespace, materialises the leaf to a mounted directory the webhook server
// reads via certwatcher, and rotates both certificates ahead of expiry.
//
// This file holds only the pure cert-material primitives (key + template +
// signing). It has no Kubernetes dependencies so it can be exhaustively
// unit-tested without envtest.
package dynauth

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"
)

const (
	// rsaKeyBits is the RSA key size for both CA and leaf private keys. 2048
	// is the floor for K8s 1.30+ admission webhooks (apiserver enforces
	// minimum modulus sizes); going to 4096 doubles signing latency for no
	// security gain over the certificate's expected lifetime.
	rsaKeyBits = 2048

	// caCommonName is stamped on every CA we mint. The CN is informational —
	// SAN-based validation does the real work — but a stable CN makes
	// `openssl x509 -text` output legible during incident triage.
	caCommonName = "velkir-ca"
)

// CertMaterial is the PEM-encoded output of a generation operation: ready to
// drop straight into a Secret's `tls.crt` / `tls.key`. No callers ever see the
// in-memory `*x509.Certificate` / `*rsa.PrivateKey` — keeping the surface PEM
// avoids accidental key material leaks via logging or event payloads.
type CertMaterial struct {
	CertPEM []byte
	KeyPEM  []byte
}

// GenerateCA mints a fresh self-signed CA valid for `lifetime`. The result is
// PEM-encoded and ready to persist to the CA Secret's `tls.crt` / `tls.key`.
//
// `now` is injected so tests can pin time and assert on NotBefore/NotAfter
// without flaking on clock drift.
func GenerateCA(now time.Time, lifetime time.Duration) (CertMaterial, error) {
	key, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	if err != nil {
		return CertMaterial{}, fmt.Errorf("generate CA key: %w", err)
	}

	serial, err := randomSerial()
	if err != nil {
		return CertMaterial{}, err
	}

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: caCommonName},
		NotBefore:             now.Add(-1 * time.Minute),
		NotAfter:              now.Add(lifetime),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            0,
		MaxPathLenZero:        true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return CertMaterial{}, fmt.Errorf("self-sign CA: %w", err)
	}

	keyPEM, err := marshalRSAKeyPEM(key)
	if err != nil {
		return CertMaterial{}, fmt.Errorf("encode CA key: %w", err)
	}
	return CertMaterial{
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  keyPEM,
	}, nil
}

// GenerateLeaf mints a leaf certificate signed by `ca` with the given DNS SANs.
// The leaf is what the webhook server presents to the apiserver during
// admission TLS handshakes; the SAN list must contain every hostname the
// apiserver will dial (typically `<svc>.<ns>.svc` and the FQDN form).
func GenerateLeaf(now time.Time, lifetime time.Duration, dnsNames []string, ca CertMaterial) (CertMaterial, error) {
	if len(dnsNames) == 0 {
		return CertMaterial{}, fmt.Errorf("leaf cert needs at least one DNS SAN")
	}

	caCert, caKey, err := parseCertAndKey(ca)
	if err != nil {
		return CertMaterial{}, fmt.Errorf("parse CA for leaf signing: %w", err)
	}

	leafKey, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	if err != nil {
		return CertMaterial{}, fmt.Errorf("generate leaf key: %w", err)
	}

	serial, err := randomSerial()
	if err != nil {
		return CertMaterial{}, err
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: dnsNames[0]},
		NotBefore:    now.Add(-1 * time.Minute),
		NotAfter:     now.Add(lifetime),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     dnsNames,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		return CertMaterial{}, fmt.Errorf("sign leaf with CA: %w", err)
	}

	keyPEM, err := marshalRSAKeyPEM(leafKey)
	if err != nil {
		return CertMaterial{}, fmt.Errorf("encode leaf key: %w", err)
	}
	return CertMaterial{
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  keyPEM,
	}, nil
}

// ParseCert decodes the first CERTIFICATE block from PEM. Returned for
// callers that need NotBefore / NotAfter to drive rotation decisions; the
// rotation predicate in rotation.go is the only intended consumer outside
// tests.
func ParseCert(pemBytes []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("decode CERTIFICATE PEM block")
	}
	return x509.ParseCertificate(block.Bytes)
}

func parseCertAndKey(m CertMaterial) (*x509.Certificate, *rsa.PrivateKey, error) {
	cert, err := ParseCert(m.CertPEM)
	if err != nil {
		return nil, nil, err
	}
	key, err := parseRSAKeyPEM(m.KeyPEM)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

// PEM block types for persisted private keys. Freshly-minted keys are
// written as PKCS#8 (pemBlockPKCS8); the legacy PKCS#1 block is still
// accepted on read so Secrets written by an operator version predating
// the PKCS#8 switch keep parsing across an upgrade without a forced
// reissue.
const (
	pemBlockPKCS8 = "PRIVATE KEY"
	pemBlockPKCS1 = "RSA PRIVATE KEY"
)

// marshalRSAKeyPEM encodes key as a PKCS#8 PEM block. PKCS#8 is the
// modern, algorithm-agnostic key container; PKCS#1 is the RSA-only
// legacy format the operator persisted before this change.
func marshalRSAKeyPEM(key *rsa.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("marshal PKCS#8 private key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: pemBlockPKCS8, Bytes: der}), nil
}

// parseRSAKeyPEM decodes an RSA private key from PEM, accepting both the
// current PKCS#8 encoding and the legacy PKCS#1 encoding. A PKCS#8 block
// carrying a non-RSA key (the Secret payload is untyped bytes, so a
// future EC key could land here) is rejected explicitly rather than
// panicking on the type assertion.
func parseRSAKeyPEM(pemBytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("decode private key PEM block")
	}
	switch block.Type {
	case pemBlockPKCS8:
		k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse PKCS#8 private key: %w", err)
		}
		rsaKey, ok := k.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("PKCS#8 key is %T, want *rsa.PrivateKey", k)
		}
		return rsaKey, nil
	case pemBlockPKCS1:
		k, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse PKCS#1 private key: %w", err)
		}
		return k, nil
	default:
		return nil, fmt.Errorf("unexpected private key PEM block type %q", block.Type)
	}
}

func randomSerial() (*big.Int, error) {
	// Serial numbers must fit in 20 octets (RFC 5280 §4.1.2.2). Picking a
	// 128-bit random number is the standard "good enough" — collision risk
	// is negligible against the small number of certs a single operator
	// instance ever mints.
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	n, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, fmt.Errorf("generate cert serial: %w", err)
	}
	return n, nil
}
