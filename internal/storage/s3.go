package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

type BodyUpload struct {
	Key             string
	RequestID       uuid.UUID
	Timestamp       time.Time
	RequestMethod   string
	RequestPath     string
	RequestHeaders  map[string]string
	RequestBody     []byte
	ResponseStatus  int
	ResponseHeaders map[string]string
	ResponseBody    []byte
}

type S3BodyContent struct {
	RequestID string            `json:"request_id"`
	Timestamp string            `json:"timestamp"`
	Request   S3RequestContent  `json:"request"`
	Response  S3ResponseContent `json:"response"`
}

type S3RequestContent struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    json.RawMessage   `json:"body,omitempty"`
}

type S3ResponseContent struct {
	StatusCode int               `json:"status_code"`
	Headers    map[string]string `json:"headers,omitempty"`
	Body       json.RawMessage   `json:"body,omitempty"`
}

func toJSONRawMessage(data []byte) json.RawMessage {
	if len(data) == 0 {
		return nil
	}
	if json.Valid(data) {
		return json.RawMessage(data)
	}
	escaped, _ := json.Marshal(string(data))
	return json.RawMessage(escaped)
}

func ExtractResponseHeaders(h http.Header) map[string]string {
	result := make(map[string]string)
	for key, values := range h {
		if len(values) > 0 {
			result[key] = values[0]
		}
	}
	return result
}

// downloadS3Body fetches a gzipped JSON body from S3 and returns the parsed content.
func downloadS3Body(ctx context.Context, client *s3.Client, bucket, key string) (*S3BodyContent, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 GetObject %s/%s: %w", bucket, key, err)
	}
	defer out.Body.Close()

	compressed, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("reading s3 body: %w", err)
	}

	gz, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("gzip reader: %w", err)
	}
	defer gz.Close()

	decompressed, err := io.ReadAll(gz)
	if err != nil {
		return nil, fmt.Errorf("gzip decompress: %w", err)
	}

	var content S3BodyContent
	if err := json.Unmarshal(decompressed, &content); err != nil {
		return nil, fmt.Errorf("unmarshal body content: %w", err)
	}

	return &content, nil
}
