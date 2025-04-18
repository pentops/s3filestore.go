package s3fstest

import (
	"bytes"
	"context"
	"testing"
)

func TestS3FileStore(t *testing.T) {
	bucketName := "test-bucket"
	keyPrefix := "test-prefix"
	testKey := "test-key"

	fs := NewMockS3FileStore(bucketName, keyPrefix)
	testData := []byte("test-data")

	t.Run("Put object", func(t *testing.T) {
		err := fs.Put(context.Background(), testKey, bytes.NewReader(testData), map[string]string{"Content-Type": "text/plain"})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})

	t.Run("Get object", func(t *testing.T) {
		data, contentType, err := fs.GetBytes(context.Background(), testKey)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if string(data) != string(testData) {
			t.Fatalf("expected %s, got %s", testData, data)
		}

		if contentType != "text/plain" {
			t.Fatalf("expected content type %s, got %s", "text/plain", contentType)
		}
	})
}
