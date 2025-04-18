package s3fstest

import "github.com/pentops/s3filestore.go"

func NewMockS3FileStore(bucket, keyPrefix string) *s3filestore.S3FileStore {
	return &s3filestore.S3FileStore{
		Client:    NewS3Client(),
		Bucket:    bucket,
		KeyPrefix: keyPrefix,
	}
}
