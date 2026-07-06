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

package ssa_test

import (
	"fmt"
	"os"
	"testing"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

var (
	testEnv *envtest.Environment
	cfg     *rest.Config
	k8s     client.Client
)

func TestMain(m *testing.M) {
	testEnv = &envtest.Environment{}
	var err error
	cfg, err = testEnv.Start()
	if err != nil {
		panic("envtest start: " + err.Error())
	}
	k8s, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		panic("client.New: " + err.Error())
	}
	code := m.Run()
	if stopErr := testEnv.Stop(); stopErr != nil {
		fmt.Fprintf(os.Stderr, "testEnv.Stop: %v\n", stopErr)
	}
	os.Exit(code)
}
