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

package utils

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// The operator's /metrics endpoint is HTTPS on a ClusterIP Service (port
// 8443), reachable only from inside the cluster, and gated by a
// SubjectAccessReview on the `/metrics` non-resource URL. Scraping it from a
// host-side e2e process therefore needs three things this file provides: a
// ServiceAccount whose token carries `/metrics` GET permission, a bridge from
// the host to the ClusterIP Service (kubectl port-forward to a random local
// port — parallel-safe across ginkgo processes), and a parser for the
// Prometheus text exposition format so a single series can be asserted by its
// `{namespace,name}` label set.
//
// The ServiceAccount name is fixed — it is namespaced, so per-run operator
// namespaces already isolate it. The cluster-scoped Role + binding names are
// suffixed with the operator namespace (see metricsReaderClusterRoleName) so
// two concurrent shared-cluster runs never collide on one cluster-scoped
// object and repoint each other's binding subject. Re-apply within a run is
// idempotent; in shared-cluster mode each object is teardown-labelled (see
// metricsReaderLabels) so the cluster-scoped sweep reaps it between runs.
const (
	metricsReaderSAName            = "valkey-e2e-metrics-reader"
	metricsReaderClusterRolePrefix = "valkey-e2e-metrics-reader"
	metricsServicePort             = "8443"
	metricsReaderTokenDuration     = "10m"
)

// metricsReaderClusterRoleName is the cluster-scoped Role/Binding name for a
// given operator namespace. Suffixing with the namespace keeps concurrent
// shared-cluster runs (each with a distinct operator namespace) on distinct
// cluster-scoped objects.
func metricsReaderClusterRoleName(opNamespace string) string {
	return metricsReaderClusterRolePrefix + "-" + opNamespace
}

// EnsureMetricsReaderRBAC idempotently provisions, in opNamespace, a
// ServiceAccount plus a cluster-scoped Role granting GET on the `/metrics`
// non-resource URL and a binding to that ServiceAccount. The operator's
// metrics server authorizes scrapes via a SubjectAccessReview, so a token
// needs this permission to read /metrics. The chart renders an equivalent
// reader Role only when its ServiceMonitor is enabled, which the e2e installs
// leave off — hence this self-contained binding rather than reusing a
// chart-rendered one.
//
// The cluster-scoped Role + binding are named per operator namespace so two
// concurrent shared-cluster runs (distinct operator namespaces) own distinct
// objects; otherwise a second run's apply would repoint the shared binding's
// subject to its own SA and silently revoke the first run's /metrics grant.
func EnsureMetricsReaderRBAC(opNamespace string) error {
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(buildMetricsReaderManifest(opNamespace))
	if out, err := Run(cmd); err != nil {
		return fmt.Errorf("applying metrics-reader RBAC: %s: %w", out, err)
	}
	return nil
}

// buildMetricsReaderManifest renders the metrics-reader ServiceAccount plus the
// per-namespace cluster-scoped Role/Binding. In shared-cluster mode each object
// carries the harness teardown label (see metricsReaderLabels) so the
// cluster-scoped sweep in tools/e2e-shared.sh reaps the Role/Binding.
func buildMetricsReaderManifest(opNamespace string) string {
	clusterRoleName := metricsReaderClusterRoleName(opNamespace)
	labels := metricsReaderLabels()
	return fmt.Sprintf(`apiVersion: v1
kind: ServiceAccount
metadata:
  name: %[1]s
  namespace: %[2]s%[4]s
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: %[3]s%[4]s
rules:
  - nonResourceURLs: ["/metrics"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: %[3]s%[4]s
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: %[3]s
subjects:
  - kind: ServiceAccount
    name: %[1]s
    namespace: %[2]s
`, metricsReaderSAName, opNamespace, clusterRoleName, labels)
}

// metricsReaderLabels returns a YAML labels block, indented to sit under an
// object's metadata, carrying the shared-cluster teardown selector — or "" when
// E2E_OPERATOR_LABEL is unset (the Kind `make test-e2e` path tears the whole
// cluster down, so nothing leaks there). The harness exports the selector as
// "app.kubernetes.io/instance=<release>"; stamping it on the cluster-scoped
// Role/Binding lets the existing `kubectl delete ... -l <selector>` sweep in
// tools/e2e-shared.sh reap them instead of accumulating one orphaned pair per
// shared-cluster run.
func metricsReaderLabels() string {
	key, val, ok := strings.Cut(os.Getenv("E2E_OPERATOR_LABEL"), "=")
	if !ok || key == "" || val == "" {
		return ""
	}
	return fmt.Sprintf("\n  labels:\n    %s: %s", key, val)
}

// MintMetricsReaderToken returns a short-lived bearer token for the
// metrics-reader ServiceAccount provisioned by EnsureMetricsReaderRBAC.
func MintMetricsReaderToken(opNamespace string) (string, error) {
	out, err := Run(exec.Command(
		"kubectl", "-n", opNamespace, "create", "token", metricsReaderSAName,
		"--duration="+metricsReaderTokenDuration,
	))
	if err != nil {
		return "", fmt.Errorf("minting metrics-reader token: %w", err)
	}
	return strings.TrimSpace(out), nil
}

// ResolveMetricsServiceName finds the operator's metrics Service in
// opNamespace. The chart names it "<release>-metrics"; the kustomize deploy
// names it "<prefix>-controller-manager-metrics-service". Both expose 8443.
func ResolveMetricsServiceName(opNamespace string) (string, error) {
	out, err := Run(exec.Command(
		"kubectl", "-n", opNamespace, "get", "svc",
		"-o", "jsonpath={.items[*].metadata.name}",
	))
	if err != nil {
		return "", fmt.Errorf("listing services in %s: %w", opNamespace, err)
	}

	var candidates []string
	for name := range strings.FieldsSeq(out) {
		if strings.HasSuffix(name, "-metrics") || strings.HasSuffix(name, "metrics-service") {
			candidates = append(candidates, name)
		}
	}
	switch len(candidates) {
	case 0:
		return "", fmt.Errorf("no metrics Service found in namespace %s (services: %q)", opNamespace, strings.TrimSpace(out))
	case 1:
		return candidates[0], nil
	default:
		// Prefer the chart name shape when both a chart and kustomize
		// Service somehow coexist in the same namespace.
		for _, c := range candidates {
			if strings.HasSuffix(c, "-metrics") {
				return c, nil
			}
		}
		return candidates[0], nil
	}
}

// ScrapeOperatorMetrics port-forwards the named ClusterIP metrics Service in
// opNamespace to a random local port and GETs /metrics over TLS with the given
// bearer token, returning the parsed exposition. The metrics server presents a
// self-signed certificate, so verification is skipped. An omitted local port
// in the port-forward lets kubectl pick a free one, so concurrent ginkgo
// processes never collide.
func ScrapeOperatorMetrics(opNamespace, serviceName, token string) (*ScrapedMetrics, error) {
	cmd := exec.Command(
		"kubectl", "-n", opNamespace, "port-forward",
		"svc/"+serviceName, ":"+metricsServicePort,
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("creating port-forward stdout pipe: %w", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting kubectl port-forward: %w", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	localPort, err := readForwardedPort(stdout, 15*time.Second)
	if err != nil {
		return nil, fmt.Errorf("waiting for port-forward (stderr=%q): %w", strings.TrimSpace(stderr.String()), err)
	}

	body, err := getMetricsBody(localPort, token)
	if err != nil {
		return nil, err
	}
	return parsePromText(body), nil
}

// OperatorMetrics is the one-call entry point: it ensures the reader RBAC,
// mints a token, resolves the metrics Service, and returns the parsed scrape
// for opNamespace. Specs that scrape repeatedly can call the lower-level
// helpers directly to provision RBAC once.
func OperatorMetrics(opNamespace string) (*ScrapedMetrics, error) {
	if err := EnsureMetricsReaderRBAC(opNamespace); err != nil {
		return nil, err
	}
	token, err := MintMetricsReaderToken(opNamespace)
	if err != nil {
		return nil, err
	}
	svc, err := ResolveMetricsServiceName(opNamespace)
	if err != nil {
		return nil, err
	}
	return ScrapeOperatorMetrics(opNamespace, svc, token)
}

var forwardedPortRe = regexp.MustCompile(`Forwarding from 127\.0\.0\.1:(\d+)`)

// readForwardedPort scans kubectl port-forward's stdout for the line that
// announces the chosen local port, returning it. A goroutine drives the
// blocking scan so the caller can bound the wait with a timeout.
func readForwardedPort(r io.Reader, timeout time.Duration) (int, error) {
	type result struct {
		port int
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			if m := forwardedPortRe.FindStringSubmatch(sc.Text()); m != nil {
				p, convErr := strconv.Atoi(m[1])
				ch <- result{port: p, err: convErr}
				return
			}
		}
		if err := sc.Err(); err != nil {
			ch <- result{err: err}
			return
		}
		ch <- result{err: fmt.Errorf("port-forward stdout closed before announcing a local port")}
	}()

	select {
	case res := <-ch:
		return res.port, res.err
	case <-time.After(timeout):
		return 0, fmt.Errorf("timed out after %s waiting for port-forward to announce a local port", timeout)
	}
}

// getMetricsBody GETs https://127.0.0.1:<port>/metrics with the bearer token,
// retrying briefly because the port-forward listener can lag its announcement.
func getMetricsBody(localPort int, token string) (string, error) {
	url := fmt.Sprintf("https://127.0.0.1:%d/metrics", localPort)
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			// The operator's metrics server uses a self-signed cert; this is
			// a localhost-only e2e scrape over a port-forward, not a trust
			// decision against a real endpoint.
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // self-signed in-cluster metrics cert
		},
	}

	var lastErr error
	for range 8 {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return "", fmt.Errorf("building metrics request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			// A freshly-created reader binding can briefly 401/403 until the
			// apiserver authorizer observes it; treat a non-200 as transient
			// within the retry budget, surfacing the last one (with the body,
			// which names an authorization problem) if it persists.
			lastErr = fmt.Errorf("metrics endpoint returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return string(body), nil
	}
	return "", fmt.Errorf("scraping metrics after retries: %w", lastErr)
}

// promSample is one parsed series line from a Prometheus text-format scrape.
type promSample struct {
	Labels map[string]string
	Value  float64
}

// ScrapedMetrics holds a parsed /metrics exposition indexed by metric name.
type ScrapedMetrics struct {
	byName map[string][]promSample
}

// Has reports whether any series for the named metric was present in the
// scrape. Useful as a liveness check that the scrape returned a functional
// metrics page before asserting on a specific counter.
func (m *ScrapedMetrics) Has(name string) bool {
	return len(m.byName[name]) > 0
}

// Value returns the value of the series named `name` whose labels are a
// superset of `want`, and whether such a series was found. A fully-specified
// label set matches at most one series; absence returns (0, false). For a
// CounterVec, absence is equivalent to "never incremented" (the series is
// created lazily on first observation).
func (m *ScrapedMetrics) Value(name string, want map[string]string) (float64, bool) {
	for _, s := range m.byName[name] {
		if labelsMatch(s.Labels, want) {
			return s.Value, true
		}
	}
	return 0, false
}

func labelsMatch(have, want map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

// parsePromText parses the Prometheus text exposition format, skipping HELP/
// TYPE comment lines and any line it cannot interpret as a sample.
func parsePromText(body string) *ScrapedMetrics {
	res := &ScrapedMetrics{byName: map[string][]promSample{}}
	for line := range strings.SplitSeq(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, sample, ok := parseSampleLine(line)
		if !ok {
			continue
		}
		res.byName[name] = append(res.byName[name], sample)
	}
	return res
}

// parseSampleLine parses one exposition line of the form
// `name value [timestamp]` or `name{k="v",...} value [timestamp]`.
func parseSampleLine(line string) (string, promSample, bool) {
	var name, labelPart, rest string
	if i := strings.IndexByte(line, '{'); i >= 0 {
		j := strings.LastIndexByte(line, '}')
		if j < i {
			return "", promSample{}, false
		}
		name = line[:i]
		labelPart = line[i+1 : j]
		rest = strings.TrimSpace(line[j+1:])
	} else {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return "", promSample{}, false
		}
		name = fields[0]
		rest = strings.Join(fields[1:], " ")
	}

	valueFields := strings.Fields(rest)
	if len(valueFields) == 0 {
		return "", promSample{}, false
	}
	val, err := strconv.ParseFloat(valueFields[0], 64)
	if err != nil {
		return "", promSample{}, false
	}
	return name, promSample{Labels: parseLabels(labelPart), Value: val}, true
}

// parseLabels parses a `k="v",k2="v2"` label block, honouring quoted commas
// and the text-format escapes for backslash, double-quote, and newline.
func parseLabels(s string) map[string]string {
	labels := map[string]string{}
	s = strings.TrimSpace(s)
	if s == "" {
		return labels
	}

	var pairs []string
	var b strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			inQuote = !inQuote
			b.WriteByte(c)
		case c == ',' && !inQuote:
			pairs = append(pairs, b.String())
			b.Reset()
		default:
			b.WriteByte(c)
		}
	}
	if b.Len() > 0 {
		pairs = append(pairs, b.String())
	}

	unescape := strings.NewReplacer(`\"`, `"`, `\\`, `\`, `\n`, "\n")
	for _, p := range pairs {
		k, v, ok := strings.Cut(p, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		v = strings.TrimPrefix(v, `"`)
		v = strings.TrimSuffix(v, `"`)
		labels[k] = unescape.Replace(v)
	}
	return labels
}
