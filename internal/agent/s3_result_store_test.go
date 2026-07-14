package agent

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/sporevm/k8s/internal/fleet"
)

func TestS3ResultStorePreservesCreateOnlySemantics(t *testing.T) {
	client := &fakeS3Client{objects: make(map[string][]byte)}
	store := &S3ResultStore{client: client}
	run := testBundleRun()
	result := successfulAttemptResult(run, 42)

	if _, ok, err := store.TerminalResult(context.Background(), run, 42); err != nil || ok {
		t.Fatalf("missing TerminalResult = ok %v err %v", ok, err)
	}
	if err := store.WriteAttemptResult(context.Background(), run, result); err != nil {
		t.Fatalf("WriteAttemptResult: %v", err)
	}
	if err := store.WriteAttemptResult(context.Background(), run, result); !errors.Is(err, ErrResultExists) {
		t.Fatalf("duplicate WriteAttemptResult error = %v", err)
	}
	committed, err := store.CommitTerminalResult(context.Background(), run, result)
	if err != nil || !committed {
		t.Fatalf("CommitTerminalResult = committed %v err %v", committed, err)
	}
	committed, err = store.CommitTerminalResult(context.Background(), run, result)
	if err != nil || committed {
		t.Fatalf("duplicate CommitTerminalResult = committed %v err %v", committed, err)
	}
	terminal, ok, err := store.TerminalResult(context.Background(), run, 42)
	if err != nil || !ok || terminal.AttemptID != result.AttemptID {
		t.Fatalf("TerminalResult = %+v ok %v err %v", terminal, ok, err)
	}
	if client.nonConditionalPuts != 0 {
		t.Fatalf("non-conditional puts = %d", client.nonConditionalPuts)
	}
}

func successfulAttemptResult(run fleet.BundleRun, childID int) fleet.AttemptResult {
	started := time.Unix(1, 0).UTC()
	return fleet.AttemptResult{
		RunID:        run.RunID,
		BundleDigest: run.Bundle.Digest,
		ChildID:      childID,
		AttemptID:    fleet.FormatAttemptID(run.RunID, childID, 1),
		AgentID:      "agent-1",
		ShardID:      "shard-1",
		Status:       fleet.AttemptSucceeded,
		StartedAt:    started,
		FinishedAt:   started.Add(time.Second),
		Terminal:     true,
		ResultURI:    TerminalResultURI(run, childID),
	}
}

type fakeS3Client struct {
	mu                 sync.Mutex
	objects            map[string][]byte
	nonConditionalPuts int
}

func (c *fakeS3Client) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	payload, ok := c.objects[aws.ToString(input.Bucket)+"/"+aws.ToString(input.Key)]
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "NoSuchKey", Message: "missing"}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(payload))}, nil
}

func (c *fakeS3Client) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if aws.ToString(input.IfNoneMatch) != "*" {
		c.nonConditionalPuts++
	}
	key := aws.ToString(input.Bucket) + "/" + aws.ToString(input.Key)
	if _, ok := c.objects[key]; ok {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "exists"}
	}
	payload, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	c.objects[key] = payload
	return &s3.PutObjectOutput{}, nil
}
