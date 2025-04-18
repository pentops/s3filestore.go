package s3fstest

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
)

type mockFile struct {
	Bytes       []byte
	ContentType string
}

type S3Client struct {
	files map[string]*mockFile
}

func NewS3Client() *S3Client {
	return &S3Client{
		files: make(map[string]*mockFile),
	}
}

func (s *S3Client) PutObject(ctx context.Context, input *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	fullPath := fmt.Sprintf("s3://%s/%s", *input.Bucket, *input.Key)
	bodyBytes, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}

	contentType := "application/octet-stream"
	if input.ContentType != nil {
		contentType = *input.ContentType
	}

	s.files[fullPath] = &mockFile{
		Bytes:       bodyBytes,
		ContentType: contentType,
	}

	return &s3.PutObjectOutput{}, nil
}

func (s *S3Client) GetObject(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	fullPath := fmt.Sprintf("s3://%s/%s", *input.Bucket, *input.Key)
	mockFile, exists := s.files[fullPath]
	if !exists {
		return nil, fmt.Errorf("file not found: %s", fullPath)
	}

	return &s3.GetObjectOutput{
		Body:        io.NopCloser(bytes.NewReader(mockFile.Bytes)),
		ContentType: &mockFile.ContentType,
	}, nil
}
