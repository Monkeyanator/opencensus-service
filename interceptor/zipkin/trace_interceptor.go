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
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"

	"go.opencensus.io/trace"

	zipkinmodel "github.com/openzipkin/zipkin-go/model"

	commonpb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/common/v1"
	agenttracepb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/trace/v1"
	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	"github.com/census-instrumentation/opencensus-service/interceptor"
	"github.com/census-instrumentation/opencensus-service/internal"
	"github.com/census-instrumentation/opencensus-service/spanreceiver"
)

type ZipkinInterceptor struct {
	spanSink spanreceiver.SpanReceiver
}

var _ interceptor.TraceInterceptor = (*ZipkinInterceptor)(nil)
var _ http.Handler = (*ZipkinInterceptor)(nil)

func New(sr spanreceiver.SpanReceiver) (*ZipkinInterceptor, error) {
	return &ZipkinInterceptor{spanSink: sr}, nil
}

func (zi *ZipkinInterceptor) StartTraceInterception(ctx context.Context, spanSink spanreceiver.SpanReceiver) error {
	zi.spanSink = spanSink
	return nil
}

func (zi *ZipkinInterceptor) parseAndConvertToTraceSpans(jsonBlob []byte) (reqs []*agenttracepb.ExportTraceServiceRequest, err error) {
	var zipkinSpans []*zipkinmodel.SpanModel
	if err = json.Unmarshal(jsonBlob, &zipkinSpans); err != nil {
		return nil, err
	}

	// *commonpb.Node instances have unique addresses hence
	// for grouping within a map, we'll use the .String() value
	byNodeGrouping := make(map[string][]*tracepb.Span)
	nodes := make([]*commonpb.Node, 0, len(zipkinSpans))
	// Now translate them into tracepb.Span
	for _, zspan := range zipkinSpans {
		span, node, err := zipkinSpanToTraceSpan(zspan)
		// TODO:(@odeke-em) record errors
		if err == nil && span != nil {
			key := node.String()
			if _, firstTime := byNodeGrouping[key]; firstTime {
				nodes = append(nodes, node)
			}
			byNodeGrouping[key] = append(byNodeGrouping[key], span)
		}
	}

	// TODO: @odeke-em perhaps maintain the relative order that the spans were received in
	for _, node := range nodes {
		key := node.String()
		spans := byNodeGrouping[key]
		if len(spans) == 0 {
			// Should never happen but nonetheless be cautious
			// not to send blank spans.
			continue
		}
		reqs = append(reqs, &agenttracepb.ExportTraceServiceRequest{
			Node:  node,
			Spans: spans,
		})
		delete(byNodeGrouping, key)
	}

	return reqs, nil
}

func (zi *ZipkinInterceptor) StopTraceInterception(ctx context.Context) error {
	return nil
}

// The ZipkinInterceptor receives spans from endpoint /api/v2 as JSON,
// unmarshals them and sends them along to the spanreceiver.
func (zi *ZipkinInterceptor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Trace this method
	ctx, span := trace.StartSpan(context.Background(), "ZipkinInterceptor.Export")
	defer span.End()

	// If the starting RPC has a parent span, then add it as a parent link.
	parentCtx := r.Context()
	internal.SetParentLink(parentCtx, span)

	blob, err := ioutil.ReadAll(r.Body)
	_ = r.Body.Close()
	ereqs, err := zi.parseAndConvertToTraceSpans(blob)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	spansMetricsFn := internal.NewReceivedSpansRecorderStreaming(ctx, "zipkin")
	// Now translate them into tracepb.Span
	for _, ereq := range ereqs {
		zi.spanSink.ReceiveSpans(ctx, ereq.Node, ereq.Spans...)
		// We MUST unconditionally record metrics from this reception.
		spansMetricsFn(ereq.Node, ereq.Spans)
	}

	// Finally send back the response "Accepted" as
	// required at https://zipkin.io/zipkin-api/#/default/post_spans
	w.WriteHeader(http.StatusAccepted)
}

var errNilZipkinSpan = errors.New("non-nil Zipkin span expected")

var blankIP net.IP

func zipkinSpanToTraceSpan(zs *zipkinmodel.SpanModel) (*tracepb.Span, *commonpb.Node, error) {
	if zs == nil {
		return nil, nil, errNilZipkinSpan
	}

	var node *commonpb.Node
	if lep := zs.LocalEndpoint; lep != nil {
		node = &commonpb.Node{
			ServiceInfo: &commonpb.ServiceInfo{
				Name: lep.ServiceName,
			},
		}

		attributes := make(map[string]string, 3)
		if !lep.IPv4.Equal(blankIP) {
			attributes["ipv4"] = lep.IPv4.String()
		}
		if !lep.IPv6.Equal(blankIP) {
			attributes["ipv6"] = lep.IPv6.String()
		}
		if lep.Port > 0 {
			attributes["port"] = fmt.Sprintf("%d", lep.Port)
		}
		if len(attributes) > 0 {
			node.Attributes = attributes
		}
	}

	traceID, err := hexStrToBytes(zs.TraceID.String())
	if err != nil {
		return nil, node, fmt.Errorf("TraceID: %v", err)
	}
	spanID, err := hexStrToBytes(zs.ID.String())
	if err != nil {
		return nil, node, fmt.Errorf("SpanID: %v", err)
	}
	var parentSpanID []byte
	if zs.ParentID != nil {
		parentSpanID, err = hexStrToBytes(zs.ParentID.String())
		if err != nil {
			return nil, node, fmt.Errorf("ParentSpanID: %v", err)
		}
	}

	pbs := &tracepb.Span{
		TraceId:      traceID,
		SpanId:       spanID,
		ParentSpanId: parentSpanID,
		Name:         &tracepb.TruncatableString{Value: zs.Name},
		StartTime:    internal.TimeToTimestamp(zs.Timestamp),
		EndTime:      internal.TimeToTimestamp(zs.Timestamp.Add(zs.Duration)),
		Kind:         zipkinSpanKindToProtoSpanKind(zs.Kind),
		Status:       extractProtoStatus(zs),
		Attributes:   zipkinTagsToTraceAttributes(zs.Tags),
		TimeEvents:   zipkinAnnotationsToProtoTimeEvents(zs.Annotations),
	}

	return pbs, node, nil
}

const statusCodeUnknown = 2

func extractProtoStatus(zs *zipkinmodel.SpanModel) *tracepb.Status {
	// The status is stored with the "error" key
	// See https://github.com/census-instrumentation/opencensus-go/blob/1eb9a13c7dd02141e065a665f6bf5c99a090a16a/exporter/zipkin/zipkin.go#L160-L165
	if zs == nil || len(zs.Tags) == 0 {
		return nil
	}
	canonicalCodeStr := zs.Tags["error"]
	message := zs.Tags["opencensus.status_description"]
	if message == "" && canonicalCodeStr == "" {
		return nil
	}
	code, set := canonicalCodesMap[canonicalCodeStr]
	if !set {
		// If not status code was set, then we should use UNKNOWN
		code = statusCodeUnknown
	}
	return &tracepb.Status{
		Message: message,
		Code:    code,
	}
}

var canonicalCodesMap = map[string]int32{
	// https://github.com/googleapis/googleapis/blob/bee79fbe03254a35db125dc6d2f1e9b752b390fe/google/rpc/code.proto#L33-L186
	"OK":                  0,
	"CANCELLED":           1,
	"UNKNOWN":             2,
	"INVALID_ARGUMENT":    3,
	"DEADLINE_EXCEEDED":   4,
	"NOT_FOUND":           5,
	"ALREADY_EXISTS":      6,
	"PERMISSION_DENIED":   7,
	"RESOURCE_EXHAUSTED":  8,
	"FAILED_PRECONDITION": 9,
	"ABORTED":             10,
	"OUT_OF_RANGE":        11,
	"UNIMPLEMENTED":       12,
	"INTERNAL":            13,
	"UNAVAILABLE":         14,
	"DATA_LOSS":           15,
	"UNAUTHENTICATED":     16,
}

func hexStrToBytes(hexStr string) ([]byte, error) {
	if len(hexStr) == 0 {
		return nil, nil
	}
	return hex.DecodeString(hexStr)
}

func zipkinSpanKindToProtoSpanKind(skind zipkinmodel.Kind) tracepb.Span_SpanKind {
	switch strings.ToUpper(string(skind)) {
	case "CLIENT":
		return tracepb.Span_CLIENT
	case "SERVER":
		return tracepb.Span_SERVER
	default:
		return tracepb.Span_SPAN_KIND_UNSPECIFIED
	}
}

var blankAnnotation zipkinmodel.Annotation

func zipkinAnnotationsToProtoTimeEvents(zas []zipkinmodel.Annotation) *tracepb.Span_TimeEvents {
	if len(zas) == 0 {
		return nil
	}
	tevs := make([]*tracepb.Span_TimeEvent, 0, len(zas))
	for _, za := range zas {
		if tev := zipkinAnnotationToProtoAnnotation(za); tev != nil {
			tevs = append(tevs, tev)
		}
	}
	if len(tevs) == 0 {
		return nil
	}
	return &tracepb.Span_TimeEvents{
		TimeEvent: tevs,
	}
}

func zipkinAnnotationToProtoAnnotation(zas zipkinmodel.Annotation) *tracepb.Span_TimeEvent {
	if zas == blankAnnotation {
		return nil
	}
	return &tracepb.Span_TimeEvent{
		Time: internal.TimeToTimestamp(zas.Timestamp),
		Value: &tracepb.Span_TimeEvent_Annotation_{
			Annotation: &tracepb.Span_TimeEvent_Annotation{
				Description: &tracepb.TruncatableString{Value: zas.Value},
			},
		},
	}
}

func zipkinTagsToTraceAttributes(tags map[string]string) *tracepb.Span_Attributes {
	if len(tags) == 0 {
		return nil
	}

	amap := make(map[string]*tracepb.AttributeValue, len(tags))
	for key, value := range tags {
		// We did a translation from "boolean" to "string"
		// in OpenCensus-Go's Zipkin exporter as per
		// https://github.com/census-instrumentation/opencensus-go/blob/1eb9a13c7dd02141e065a665f6bf5c99a090a16a/exporter/zipkin/zipkin.go#L138-L155
		switch value {
		case "true", "false":
			amap[key] = &tracepb.AttributeValue{
				Value: &tracepb.AttributeValue_BoolValue{BoolValue: value == "true"},
			}
		default:
			amap[key] = &tracepb.AttributeValue{
				Value: &tracepb.AttributeValue_StringValue{
					StringValue: &tracepb.TruncatableString{Value: value},
				},
			}
		}
	}
	return &tracepb.Span_Attributes{AttributeMap: amap}
}

func setIfNonEmpty(key, value string, dest map[string]string) {
	if value != "" {
		dest[key] = value
	}
}
