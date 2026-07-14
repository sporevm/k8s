package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/sporevm/k8s/internal/fleet"
)

// ResultStoreConfig selects local smoke storage or durable S3 object storage.
type ResultStoreConfig struct {
	Backend      string
	LocalRoot    string
	Region       string
	Endpoint     string
	UsePathStyle bool
}

// NewResultStore creates the configured result store.
func NewResultStore(ctx context.Context, cfg ResultStoreConfig) (ResultStore, error) {
	switch cfg.Backend {
	case "", "local":
		return NewLocalResultStore(cfg.LocalRoot)
	case "s3":
		return NewS3ResultStore(ctx, S3ResultStoreOptions{
			Region:       cfg.Region,
			Endpoint:     cfg.Endpoint,
			UsePathStyle: cfg.UsePathStyle,
		})
	default:
		return nil, fmt.Errorf("unsupported result store backend %q", cfg.Backend)
	}
}

// S3ResultStoreOptions configures AWS SDK loading and S3-compatible test endpoints.
type S3ResultStoreOptions struct {
	Region       string
	Endpoint     string
	UsePathStyle bool
}

type s3ObjectClient interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// S3ResultStore persists attempts and create-only terminal results in S3.
type S3ResultStore struct {
	client s3ObjectClient
}

// NewS3ResultStore loads the default AWS credential chain and creates an S3 result store.
func NewS3ResultStore(ctx context.Context, opts S3ResultStoreOptions) (*S3ResultStore, error) {
	loadOptions := []func(*config.LoadOptions) error{}
	if opts.Region != "" {
		loadOptions = append(loadOptions, config.WithRegion(opts.Region))
	}
	awsConfig, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.UsePathStyle = opts.UsePathStyle
		if opts.Endpoint != "" {
			options.BaseEndpoint = aws.String(opts.Endpoint)
		}
	})
	return &S3ResultStore{client: client}, nil
}

// TerminalResult reads a previously committed terminal result if present.
func (s *S3ResultStore) TerminalResult(ctx context.Context, run fleet.BundleRun, childID int) (fleet.AttemptResult, bool, error) {
	if err := run.Validate(); err != nil {
		return fleet.AttemptResult{}, false, err
	}
	if childID < run.Children.Start || childID >= run.Children.End() {
		return fleet.AttemptResult{}, false, fmt.Errorf("%w: terminal child id is outside the run", fleet.ErrInvalidContract)
	}
	bucket, key, err := parseS3ResultURI(TerminalResultURI(run, childID))
	if err != nil {
		return fleet.AttemptResult{}, false, err
	}
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if isS3Error(err, "NoSuchKey", "NotFound") {
		return fleet.AttemptResult{}, false, nil
	}
	if err != nil {
		return fleet.AttemptResult{}, false, fmt.Errorf("get terminal result %q: %w", key, err)
	}
	defer output.Body.Close()
	result, err := decodeTerminalResult(output.Body, run)
	if err != nil {
		return fleet.AttemptResult{}, false, err
	}
	return result, true, nil
}

// WriteAttemptResult writes an append-only attempt result document.
func (s *S3ResultStore) WriteAttemptResult(ctx context.Context, run fleet.BundleRun, result fleet.AttemptResult) error {
	if err := result.Validate(run); err != nil {
		return err
	}
	created, err := s.putJSONCreateOnly(ctx, AttemptResultURI(run, result.ChildID, result.AttemptID), result)
	if err != nil {
		return err
	}
	if !created {
		return ErrResultExists
	}
	return nil
}

// CommitTerminalResult atomically creates the terminal object when it does not exist.
func (s *S3ResultStore) CommitTerminalResult(ctx context.Context, run fleet.BundleRun, result fleet.AttemptResult) (bool, error) {
	if err := result.Validate(run); err != nil {
		return false, err
	}
	if !result.Terminal {
		return false, errors.New("cannot commit non-terminal result")
	}
	terminalURI := TerminalResultURI(run, result.ChildID)
	if result.ResultURI != terminalURI {
		return false, fmt.Errorf("terminal result URI = %q, want %q", result.ResultURI, terminalURI)
	}
	return s.putJSONCreateOnly(ctx, terminalURI, result)
}

func (s *S3ResultStore) putJSONCreateOnly(ctx context.Context, uri string, value any) (bool, error) {
	bucket, key, err := parseS3ResultURI(uri)
	if err != nil {
		return false, err
	}
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return false, err
	}
	payload = append(payload, '\n')
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		Body:        bytes.NewReader(payload),
		ContentType: aws.String("application/json"),
		IfNoneMatch: aws.String("*"),
	})
	if isS3Error(err, "PreconditionFailed") {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("create result object %q: %w", key, err)
	}
	return true, nil
}

func decodeTerminalResult(reader io.Reader, run fleet.BundleRun) (fleet.AttemptResult, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var result fleet.AttemptResult
	if err := decoder.Decode(&result); err != nil {
		return fleet.AttemptResult{}, fmt.Errorf("%w: decode terminal result: %v", ErrInvalidMachineOutput, err)
	}
	var extra struct{}
	if err := decoder.Decode(&extra); err == nil {
		return fleet.AttemptResult{}, fmt.Errorf("%w: terminal result has multiple JSON documents", ErrInvalidMachineOutput)
	} else if !errors.Is(err, io.EOF) {
		return fleet.AttemptResult{}, fmt.Errorf("%w: decode trailing terminal result: %v", ErrInvalidMachineOutput, err)
	}
	if err := result.Validate(run); err != nil {
		return fleet.AttemptResult{}, err
	}
	if !result.Terminal {
		return fleet.AttemptResult{}, fmt.Errorf("%w: committed terminal result is not terminal", ErrInvalidMachineOutput)
	}
	return result, nil
}

func isS3Error(err error, codes ...string) bool {
	if err == nil {
		return false
	}
	var apiError smithy.APIError
	if !errors.As(err, &apiError) {
		return false
	}
	for _, code := range codes {
		if apiError.ErrorCode() == code {
			return true
		}
	}
	return false
}
