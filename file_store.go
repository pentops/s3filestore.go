package s3filestore

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pentops/log.go/log"
	"io"
	"net/url"
	"path"
	"strings"
)

type FileStore interface {
	Put(ctx context.Context, key string, body io.Reader, metadata map[string]string) error
	Get(ctx context.Context, key string) (io.ReadCloser, string, error)
	GetBytes(ctx context.Context, key string) ([]byte, string, error)
}

type S3FileStore struct {
	Client    S3Client
	Bucket    string
	KeyPrefix string
}

func NewS3FileStore(client S3Client, location string) (*S3FileStore, error) {
	bucketURL, err := url.Parse(location)
	if err != nil {
		return nil, err
	}

	if bucketURL.Scheme != "s3" {
		return nil, fmt.Errorf("bucket must be an s3:// url")
	}

	bucketName := bucketURL.Host
	if bucketName == "" {
		return nil, fmt.Errorf("bucket host required")
	}

	return &S3FileStore{
		Client:    client,
		Bucket:    bucketName,
		KeyPrefix: strings.TrimPrefix(bucketURL.Path, "/"),
	}, nil
}

func (s3fs *S3FileStore) ensurePrefix(key string) string {
	if s3fs.KeyPrefix != "" && !strings.HasPrefix(key, s3fs.KeyPrefix) {
		return path.Join(s3fs.KeyPrefix, key)
	}

	return key
}

func (s3fs *S3FileStore) Put(ctx context.Context, key string, body io.Reader, metadata map[string]string) error {
	finalKey := s3fs.ensurePrefix(key)

	ctx = log.WithFields(ctx, map[string]interface{}{
		"s3Bucket": s3fs.Bucket,
		"s3Key":    finalKey,
	})

	log.Debug(ctx, "uploading to s3")

	putRequest := &s3.PutObjectInput{
		Bucket:   &s3fs.Bucket,
		Key:      &finalKey,
		Body:     body,
		Metadata: metadata,
	}

	if contentType, ok := metadata["Content-Type"]; ok {
		putRequest.ContentType = aws.String(contentType)
		delete(metadata, "Content-Type")
	}

	_, err := s3fs.Client.PutObject(ctx, putRequest)
	if err != nil {
		return fmt.Errorf("failed to upload to s3: 's3://%s/%s' : %w", s3fs.Bucket, finalKey, err)
	}

	log.Info(ctx, "uploaded to s3")

	return nil
}

func (s3fs *S3FileStore) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	finalKey := s3fs.ensurePrefix(key)

	res, err := s3fs.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s3fs.Bucket),
		Key:    aws.String(finalKey),
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to download from s3: 's3://%s/%s' : %w", s3fs.Bucket, key, err)
	}

	// Extract content type
	contentType := "application/octet-stream" // Default fallback
	if res.ContentType != nil {
		contentType = *res.ContentType
	}

	return res.Body, contentType, nil
}

func (s3fs *S3FileStore) GetBytes(ctx context.Context, key string) ([]byte, string, error) {
	body, contentType, err := s3fs.Get(ctx, key)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get S3 object: %w", err)
	}

	defer body.Close()

	// Read the response body
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read S3 object body: %w", err)
	}

	return data, contentType, nil
}
