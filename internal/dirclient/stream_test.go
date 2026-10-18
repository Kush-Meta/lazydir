// Copyright AGNTCY Contributors (https://github.com/agntcy)
// SPDX-License-Identifier: Apache-2.0

package dirclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"testing"

	corev1 "github.com/agntcy/dir/api/core/v1"
	searchv1 "github.com/agntcy/dir/api/search/v1"
	"github.com/agntcy/dir/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type summarySearchClient struct {
	searchv1.SearchServiceClient
	pages    []*searchv1.ListRecordSummariesResponse
	requests []*searchv1.ListRecordSummariesRequest
	err      error
}

func (s *summarySearchClient) ListRecordSummaries(_ context.Context, req *searchv1.ListRecordSummariesRequest, _ ...grpc.CallOption) (*searchv1.ListRecordSummariesResponse, error) {
	request, ok := proto.Clone(req).(*searchv1.ListRecordSummariesRequest)
	if !ok {
		return nil, errors.New("cloning summary request")
	}
	s.requests = append(s.requests, request)
	if s.err != nil {
		return nil, s.err
	}
	if len(s.requests) > len(s.pages) {
		return nil, errors.New("unexpected summary page request")
	}
	return s.pages[len(s.requests)-1], nil
}

func newSummaryClient(pages ...*searchv1.ListRecordSummariesResponse) (*Client, *summarySearchClient) {
	search := &summarySearchClient{pages: pages}
	return &Client{c: &client.Client{SearchServiceClient: search}}, search
}

func summaryPage(start, count int, hasMore bool) *searchv1.ListRecordSummariesResponse {
	records := make([]*searchv1.RecordSummary, count)
	for i := range count {
		records[i] = &searchv1.RecordSummary{Cid: fmt.Sprintf("cid-%03d", start+i), Name: fmt.Sprintf("agent-%03d", start+i)}
	}
	return &searchv1.ListRecordSummariesResponse{Records: records, HasMore: hasMore}
}

func TestStreamPagesIndexedSummaries(t *testing.T) {
	t.Parallel()
	c, search := newSummaryClient(summaryPage(0, 2, true), summaryPage(2, 3, false))
	c.FirstPageSize = 2
	c.BatchSize = 2
	var first, batches [][]string
	var doneCount int
	var doneErr error

	c.Stream(t.Context(), []Query{{Category: FilterSkill, Value: "planning"}}, StreamCallbacks{
		OnFirstPage: func(records []*RecordSummary) { first = append(first, summaryCIDs(records)) },
		OnBatch:     func(records []*RecordSummary) { batches = append(batches, summaryCIDs(records)) },
		OnDone:      func(err error) { doneCount++; doneErr = err },
	})

	if doneErr != nil || doneCount != 1 {
		t.Fatalf("OnDone calls = %d, error = %v", doneCount, doneErr)
	}
	if len(first) != 1 || !slices.Equal(first[0], []string{"cid-000", "cid-001"}) {
		t.Fatalf("first pages = %v", first)
	}
	if len(batches) != 2 || !slices.Equal(batches[0], []string{"cid-002", "cid-003"}) || !slices.Equal(batches[1], []string{"cid-004"}) {
		t.Fatalf("batches = %v", batches)
	}
	for i, req := range search.requests {
		wantLimit := uint32(100)
		if i == 0 {
			wantLimit = 2
		}
		if req.GetOffset() != uint32(i*2) || req.GetLimit() != wantLimit {
			t.Errorf("request %d: offset=%d limit=%d", i, req.GetOffset(), req.GetLimit())
		}
		if len(req.GetQueries()) != 1 || req.GetQueries()[0].GetType() != searchv1.RecordQueryType_RECORD_QUERY_TYPE_SKILL_NAME {
			t.Errorf("request %d has unexpected queries: %v", i, req.GetQueries())
		}
	}
}

func TestStreamAggregatesFirstPageAboveServerLimit(t *testing.T) {
	t.Parallel()
	c, search := newSummaryClient(summaryPage(0, 100, true), summaryPage(100, 20, false))
	c.FirstPageSize = 120
	var first []string
	var doneErr error
	c.Stream(t.Context(), nil, StreamCallbacks{
		OnFirstPage: func(records []*RecordSummary) { first = summaryCIDs(records) },
		OnDone:      func(err error) { doneErr = err },
	})
	if doneErr != nil || len(first) != 120 {
		t.Fatalf("first page size=%d, error=%v", len(first), doneErr)
	}
	if len(search.requests) != 2 || search.requests[0].GetLimit() != 100 || search.requests[1].GetLimit() != 20 || search.requests[1].GetOffset() != 100 {
		t.Fatalf("requests = %v", search.requests)
	}
}

func TestStreamRejectsEmptyPageWithMore(t *testing.T) {
	t.Parallel()
	c, _ := newSummaryClient(summaryPage(0, 0, true))
	var doneErr error
	c.Stream(t.Context(), nil, StreamCallbacks{OnDone: func(err error) { doneErr = err }})
	if doneErr == nil {
		t.Fatal("expected error for empty page with has_more")
	}
}

func TestStreamReportsSummaryRPCErrorOnce(t *testing.T) {
	t.Parallel()
	c, search := newSummaryClient()
	search.err = status.Error(codes.Internal, "database unavailable")
	var firstCount, doneCount int
	var doneErr error
	c.Stream(t.Context(), nil, StreamCallbacks{
		OnFirstPage: func(records []*RecordSummary) {
			firstCount++
			if len(records) != 0 {
				t.Errorf("unexpected records: %v", records)
			}
		},
		OnDone: func(err error) { doneCount++; doneErr = err },
	})
	if firstCount != 1 || doneCount != 1 || status.Code(doneErr) != codes.Internal {
		t.Fatalf("callbacks: first=%d done=%d err=%v", firstCount, doneCount, doneErr)
	}
	if len(search.requests) != 1 {
		t.Fatalf("unexpected fallback or retry: %d requests", len(search.requests))
	}
}

func TestStreamCancelledBeforeFirstPage(t *testing.T) {
	t.Parallel()
	c, search := newSummaryClient()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var doneErr error
	c.Stream(ctx, nil, StreamCallbacks{OnDone: func(err error) { doneErr = err }})
	if !errors.Is(doneErr, context.Canceled) || len(search.requests) != 0 {
		t.Fatalf("cancelled stream: err=%v requests=%d", doneErr, len(search.requests))
	}
}

func TestStreamSkipsNilSummary(t *testing.T) {
	t.Parallel()
	c, _ := newSummaryClient(&searchv1.ListRecordSummariesResponse{
		Records: []*searchv1.RecordSummary{nil, {Cid: "valid-cid", Name: "valid-agent"}},
	})
	var got []string
	c.Stream(t.Context(), nil, StreamCallbacks{
		OnFirstPage: func(records []*RecordSummary) { got = summaryCIDs(records) },
	})
	if !slices.Equal(got, []string{"valid-cid"}) {
		t.Fatalf("records = %v", got)
	}
}

func TestIndexedSummarySanitizesDisplayFields(t *testing.T) {
	t.Parallel()
	got := indexedSummary(&searchv1.RecordSummary{
		Cid: "cid-123", Version: "v1.6.1\n1.6.1", SchemaVersion: "1.0.0", Authors: []string{"alice"},
		Skills: []string{"planning"}, Domains: []string{"finance"}, Modules: []string{"integration/mcp"},
	})
	if got.Name != "cid-123" || got.Version != "v1.6.1 1.6.1" || got.SchemaVersion != "1.0.0" {
		t.Fatalf("display fields = %+v", got)
	}
	if !slices.Equal(got.Authors, []string{"alice"}) || !slices.Equal(got.Skills, []string{"planning"}) || !slices.Equal(got.Domains, []string{"finance"}) || !slices.Equal(got.Modules, []string{"integration/mcp"}) {
		t.Fatalf("metadata fields = %+v", got)
	}
}

type legacySearchServer struct {
	searchv1.UnimplementedSearchServiceServer
	record *corev1.Record
}

func (s legacySearchServer) SearchRecords(_ *searchv1.SearchRecordsRequest, stream searchv1.SearchService_SearchRecordsServer) error {
	return stream.Send(&searchv1.SearchRecordsResponse{Record: s.record})
}

func TestStreamFallsBackToLegacySearch(t *testing.T) {
	t.Parallel()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	record := makeRecord(t, "1.0.0")
	wantCID := record.GetCid()
	searchv1.RegisterSearchServiceServer(server, legacySearchServer{record: record})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })

	conn, err := grpc.NewClient("passthrough:///bufconn", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := &Client{c: &client.Client{SearchServiceClient: searchv1.NewSearchServiceClient(conn)}}
	var got []*RecordSummary
	var doneErr error
	c.Stream(t.Context(), nil, StreamCallbacks{
		OnFirstPage: func(records []*RecordSummary) { got = records },
		OnDone:      func(err error) { doneErr = err },
	})
	if doneErr != nil || len(got) != 1 || got[0].CID != wantCID || got[0].Name != "test-agent" {
		t.Fatalf("legacy fallback: records=%+v, err=%v", got, doneErr)
	}
}

func summaryCIDs(records []*RecordSummary) []string {
	cids := make([]string, len(records))
	for i, record := range records {
		cids[i] = record.CID
	}
	return cids
}
