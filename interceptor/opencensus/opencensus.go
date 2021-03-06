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

package ocinterceptor

import (
	"context"
	"errors"
	"time"

	"google.golang.org/api/support/bundler"

	"go.opencensus.io/trace"

	commonpb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/common/v1"
	agenttracepb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/trace/v1"
	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	"github.com/census-instrumentation/opencensus-service/internal"
	"github.com/census-instrumentation/opencensus-service/spanreceiver"
)

type OCInterceptor struct {
	spanSink         spanreceiver.SpanReceiver
	spanBufferPeriod time.Duration
	spanBufferCount  int
}

func New(sr spanreceiver.SpanReceiver, opts ...OCOption) (*OCInterceptor, error) {
	if sr == nil {
		return nil, errors.New("needs a non-nil spanReceiver")
	}
	oci := &OCInterceptor{spanSink: sr}
	for _, opt := range opts {
		opt.WithOCInterceptor(oci)
	}
	return oci, nil
}

var _ agenttracepb.TraceServiceServer = (*OCInterceptor)(nil)

var errUnimplemented = errors.New("unimplemented")

func (oci *OCInterceptor) Config(tcs agenttracepb.TraceService_ConfigServer) error {
	// TODO: Implement when we define the config receiver/sender.
	return errUnimplemented
}

type spansAndNode struct {
	spans []*tracepb.Span
	node  *commonpb.Node
}

var errTraceExportProtocolViolation = errors.New("protocol violation: Export's first message must have a Node")

const interceptorName = "opencensus"

// Export is the gRPC method that receives streamed traces from
// OpenCensus-traceproto compatible libraries/applications.
func (oci *OCInterceptor) Export(tes agenttracepb.TraceService_ExportServer) error {
	// The bundler will receive batches of spans i.e. []*tracepb.Span
	// We need to ensure that it propagates the interceptor name as a tag
	ctxWithInterceptorName := internal.ContextWithInterceptorName(tes.Context(), interceptorName)
	traceBundler := bundler.NewBundler((*spansAndNode)(nil), func(payload interface{}) {
		oci.batchSpanExporting(ctxWithInterceptorName, payload)
	})

	spanBufferPeriod := oci.spanBufferPeriod
	if spanBufferPeriod <= 0 {
		spanBufferPeriod = 2 * time.Second // Arbitrary value
	}
	spanBufferCount := oci.spanBufferCount
	if spanBufferCount <= 0 {
		// TODO: (@odeke-em) provide an option to disable any buffering
		spanBufferCount = 50 // Arbitrary value
	}

	traceBundler.DelayThreshold = spanBufferPeriod
	traceBundler.BundleCountThreshold = spanBufferCount

	// The first message MUST have a non-nil Node.
	recv, err := tes.Recv()
	if err != nil {
		return err
	}

	// Check the condition that the first message has a non-nil Node.
	if recv.Node == nil {
		return errTraceExportProtocolViolation
	}

	spansMetricsFn := internal.NewReceivedSpansRecorderStreaming(tes.Context(), interceptorName)

	processReceivedSpans := func(ni *commonpb.Node, spans []*tracepb.Span) {
		// Firstly, we'll add them to the bundler.
		if len(spans) > 0 {
			bundlerPayload := &spansAndNode{node: ni, spans: spans}
			traceBundler.Add(bundlerPayload, len(bundlerPayload.spans))
		}

		// We MUST unconditionally record metrics from this reception.
		spansMetricsFn(ni, spans)
	}

	var lastNonNilNode *commonpb.Node
	// Now that we've got the first message with a Node, we can start to receive streamed up spans.
	for {
		// If a Node has been sent from downstream, save and use it.
		if recv.Node != nil {
			lastNonNilNode = recv.Node
		}

		processReceivedSpans(lastNonNilNode, recv.Spans)

		recv, err = tes.Recv()
		if err != nil {
			return err
		}
	}
}

func (oci *OCInterceptor) batchSpanExporting(longLivedRPCCtx context.Context, payload interface{}) {
	spnL := payload.([]*spansAndNode)
	if len(spnL) == 0 {
		return
	}

	// Trace this method
	ctx, span := trace.StartSpan(context.Background(), "OpenCensusInterceptor.Export")
	defer span.End()

	// TODO: (@odeke-em) investigate if it is necessary
	// to group nodes with their respective spans during
	// spansAndNode list unfurling then send spans grouped per node

	// If the starting RPC has a parent span, then add it as a parent link.
	parentSpanFromRPC := trace.FromContext(longLivedRPCCtx)
	if parentSpanFromRPC != nil {
		psc := parentSpanFromRPC.SpanContext()
		span.AddLink(trace.Link{
			SpanID:  psc.SpanID,
			TraceID: psc.TraceID,
			Type:    trace.LinkTypeParent,
		})
	}

	for _, spn := range spnL {
		oci.spanSink.ReceiveSpans(ctx, spn.node, spn.spans...)
	}
}
