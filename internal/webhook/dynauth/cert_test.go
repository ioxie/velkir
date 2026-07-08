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

package dynauth

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"
)

func TestGenerateCA_RoundTrip(t *testing.T) {
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	mat, err := GenerateCA(now, 5*365*24*time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	cert, err := ParseCert(mat.CertPEM)
	if err != nil {
		t.Fatalf("ParseCert: %v", err)
	}

	if !cert.IsCA {
		t.Errorf("CA cert IsCA=false, want true")
	}
	if cert.Subject.CommonName != caCommonName {
		t.Errorf("CN=%q, want %q", cert.Subject.CommonName, caCommonName)
	}
	// NotBefore is backdated 1 minute to defend against tiny clock skew on
	// pods whose system clock leads the apiserver's. Test the bound, not
	// the exact value.
	if cert.NotBefore.After(now) {
		t.Errorf("NotBefore=%v is in the future of now=%v", cert.NotBefore, now)
	}
	if got, want := cert.NotAfter, now.Add(5*365*24*time.Hour); !got.Equal(want) {
		t.Errorf("NotAfter=%v, want %v", got, want)
	}
	if cert.KeyUsage&x509.KeyUsageCertSign == 0 {
		t.Errorf("CA missing KeyUsageCertSign")
	}

	// Self-signed: cert can verify itself.
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	if _, err := cert.Verify(x509.VerifyOptions{Roots: pool, CurrentTime: now}); err != nil {
		t.Errorf("self-sign verify failed: %v", err)
	}
}

func TestGenerateLeaf_SignedByCA(t *testing.T) {
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	ca, err := GenerateCA(now, 5*365*24*time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	dnsNames := []string{
		"velkir-webhook.velkir.svc",
		"velkir-webhook.velkir.svc.cluster.local",
	}
	leaf, err := GenerateLeaf(now, 365*24*time.Hour, dnsNames, ca)
	if err != nil {
		t.Fatalf("GenerateLeaf: %v", err)
	}

	leafCert, err := ParseCert(leaf.CertPEM)
	if err != nil {
		t.Fatalf("ParseCert leaf: %v", err)
	}

	caCert, err := ParseCert(ca.CertPEM)
	if err != nil {
		t.Fatalf("ParseCert CA: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	if _, err := leafCert.Verify(x509.VerifyOptions{
		Roots:       pool,
		CurrentTime: now,
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Errorf("leaf failed CA-chain verify: %v", err)
	}

	if got, want := leafCert.DNSNames, dnsNames; !slicesEqual(got, want) {
		t.Errorf("leaf DNSNames=%v, want %v", got, want)
	}
}

func TestGenerateLeaf_RejectsEmptyDNSNames(t *testing.T) {
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	ca, err := GenerateCA(now, 5*365*24*time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	if _, err := GenerateLeaf(now, 365*24*time.Hour, nil, ca); err == nil {
		t.Errorf("GenerateLeaf with empty DNS SAN list returned nil error; want failure")
	}
}

func TestParseCert_RejectsNonCertificateBlock(t *testing.T) {
	notACert := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: []byte{0x30, 0x82}})
	if _, err := ParseCert(notACert); err == nil {
		t.Errorf("ParseCert accepted a RSA PRIVATE KEY block; want decode error")
	}
}

// TestGeneratedKeys_ArePKCS8 pins the on-disk key encoding to PKCS#8 for
// both the CA and leaf keys (the legacy format was PKCS#1).
func TestGeneratedKeys_ArePKCS8(t *testing.T) {
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	ca, err := GenerateCA(now, time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	leaf, err := GenerateLeaf(now, time.Hour, []string{"x.y.svc"}, ca)
	if err != nil {
		t.Fatalf("GenerateLeaf: %v", err)
	}
	for name, keyPEM := range map[string][]byte{"ca": ca.KeyPEM, "leaf": leaf.KeyPEM} {
		block, _ := pem.Decode(keyPEM)
		if block == nil {
			t.Fatalf("%s: decode key PEM block returned nil", name)
		}
		if block.Type != pemBlockPKCS8 {
			t.Errorf("%s: key PEM block type=%q, want %q (PKCS#8)", name, block.Type, pemBlockPKCS8)
		}
		if _, err := x509.ParsePKCS8PrivateKey(block.Bytes); err != nil {
			t.Errorf("%s: key not parseable as PKCS#8: %v", name, err)
		}
	}
}

// TestParseRSAKeyPEM_AcceptsBothEncodings is the backward-compat guard:
// a CA/leaf Secret written by a pre-PKCS#8 operator carries a PKCS#1
// ("RSA PRIVATE KEY") block and must keep parsing across the upgrade,
// while the new PKCS#8 form round-trips too.
func TestParseRSAKeyPEM_AcceptsBothEncodings(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	legacy := pem.EncodeToMemory(&pem.Block{Type: pemBlockPKCS1, Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if got, err := parseRSAKeyPEM(legacy); err != nil {
		t.Errorf("parseRSAKeyPEM(legacy PKCS#1): %v", err)
	} else if !got.Equal(key) {
		t.Errorf("legacy PKCS#1 round-trip: parsed key != original")
	}

	modern, err := marshalRSAKeyPEM(key)
	if err != nil {
		t.Fatalf("marshalRSAKeyPEM: %v", err)
	}
	if got, err := parseRSAKeyPEM(modern); err != nil {
		t.Errorf("parseRSAKeyPEM(PKCS#8): %v", err)
	} else if !got.Equal(key) {
		t.Errorf("PKCS#8 round-trip: parsed key != original")
	}
}

// TestGenerateLeaf_AcceptsLegacyPKCS1CA proves the upgrade path: a CA
// Secret still holding a legacy PKCS#1 key can sign fresh leaves without
// a forced CA reissue.
func TestGenerateLeaf_AcceptsLegacyPKCS1CA(t *testing.T) {
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	ca, err := GenerateCA(now, time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	caKey, err := parseRSAKeyPEM(ca.KeyPEM)
	if err != nil {
		t.Fatalf("parseRSAKeyPEM: %v", err)
	}
	legacyCA := CertMaterial{
		CertPEM: ca.CertPEM,
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: pemBlockPKCS1, Bytes: x509.MarshalPKCS1PrivateKey(caKey)}),
	}
	leaf, err := GenerateLeaf(now, time.Hour, []string{"x.y.svc"}, legacyCA)
	if err != nil {
		t.Fatalf("GenerateLeaf against legacy PKCS#1 CA key failed: %v", err)
	}
	// Prove the leaf is genuinely signed by the legacy-format CA — not just
	// that generation returned no error.
	leafCert, err := ParseCert(leaf.CertPEM)
	if err != nil {
		t.Fatalf("ParseCert leaf: %v", err)
	}
	caCert, err := ParseCert(legacyCA.CertPEM)
	if err != nil {
		t.Fatalf("ParseCert CA: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	if _, err := leafCert.Verify(x509.VerifyOptions{
		Roots:       pool,
		CurrentTime: now,
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Errorf("leaf signed by legacy PKCS#1 CA failed chain verify: %v", err)
	}
}

// TestParseRSAKeyPEM_RejectsNonRSAPKCS8 covers the explicit type guard:
// a valid PKCS#8 block carrying a non-RSA key is rejected, not panicked
// on at the type assertion.
func TestParseRSAKeyPEM_RejectsNonRSAPKCS8(t *testing.T) {
	_, edKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519 GenerateKey: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(edKey)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey(ed25519): %v", err)
	}
	nonRSA := pem.EncodeToMemory(&pem.Block{Type: pemBlockPKCS8, Bytes: der})
	if _, err := parseRSAKeyPEM(nonRSA); err == nil {
		t.Errorf("parseRSAKeyPEM accepted a non-RSA PKCS#8 key; want rejection")
	}
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
