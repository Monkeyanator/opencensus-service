// Copyright 2018, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zipkinterceptor

import (
	"io/ioutil"
	"reflect"
	"testing"

	commonpb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/common/v1"
)

func TestConvertSpansToTraceSpans(t *testing.T) {
	// Using Adrian Cole's sample at https://gist.github.com/adriancole/e8823c19dfed64e2eb71
	blob, err := ioutil.ReadFile("./testdata/sample1.json")
	if err != nil {
		t.Fatalf("Failed to read sample JSON file: %v", err)
	}
	zi := new(ZipkinInterceptor)
	reqs, err := zi.parseAndConvertToTraceSpans(blob)
	if err != nil {
		t.Fatalf("Failed to parse convert Zipkin spans in JSON to Trace spans: %v", err)
	}

	if g, w := len(reqs), 1; g != w {
		t.Fatalf("Expecting only one request since all spans share same node/localEndpoint: %v", g)
	}

	req := reqs[0]
	wantNode := &commonpb.Node{
		ServiceInfo: &commonpb.ServiceInfo{
			Name: "frontend",
		},
		Attributes: map[string]string{
			"ipv6": "7::80:807f",
		},
	}
	if g, w := req.Node, wantNode; !reflect.DeepEqual(g, w) {
		t.Errorf("GotNode:\n\t%v\nWantNode:\n\t%v", g, w)
	}

	nonNilSpans := 0
	for _, span := range req.Spans {
		if span != nil {
			nonNilSpans += 1
		}
	}
	// Expecting 9 non-nil spans
	if g, w := nonNilSpans, 9; g != w {
		t.Fatalf("Non-nil spans: Got %d Want %d", g, w)
	}
}
