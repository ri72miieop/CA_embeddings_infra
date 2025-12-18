// Download parquet data from R2 bucket
// Usage: go run download.go [--output path] [--collection name]
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"
)

type DownloadConfig struct {
	R2AccessKeyID     string
	R2SecretAccessKey string
	R2Endpoint        string
	R2Bucket          string
	CollectionName    string
	OutputPath        string
	ListOnly          bool
}

func main() {
	var config DownloadConfig
	var collectionName string

	flag.StringVar(&config.OutputPath, "output", "", "Output file path (default: ./{collection}/latest.parquet)")
	flag.StringVar(&collectionName, "collection", "", "Collection name (default: from QDRANT_COLLECTION_NAME env)")
	flag.BoolVar(&config.ListOnly, "list", false, "List files in bucket instead of downloading")
	flag.Parse()

	// Load .env files (check multiple locations)
	godotenv.Load(".env")
	godotenv.Load("../.env")
	godotenv.Load("../../.env")

	// Load configuration
	config.R2AccessKeyID = os.Getenv("R2_ACCESS_KEY_ID")
	config.R2SecretAccessKey = os.Getenv("R2_SECRET_ACCESS_KEY")
	config.R2Endpoint = os.Getenv("R2_ENDPOINT")
	config.R2Bucket = os.Getenv("R2_BUCKET")

	if collectionName != "" {
		config.CollectionName = collectionName
	} else {
		config.CollectionName = os.Getenv("QDRANT_COLLECTION_NAME")
		if config.CollectionName == "" {
			config.CollectionName = "embeddings"
		}
	}

	if config.R2AccessKeyID == "" || config.R2SecretAccessKey == "" || config.R2Endpoint == "" || config.R2Bucket == "" {
		log.Fatal("Missing R2 configuration. Set R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT, R2_BUCKET")
	}

	fmt.Println("\n=== R2 Downloader ===\n")
	fmt.Printf("  Bucket:     %s\n", config.R2Bucket)
	fmt.Printf("  Endpoint:   %s\n", config.R2Endpoint)
	fmt.Printf("  Collection: %s\n", config.CollectionName)

	ctx := context.Background()

	// Create S3 client
	s3Client := s3.New(s3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String(config.R2Endpoint),
		Credentials: credentials.NewStaticCredentialsProvider(
			config.R2AccessKeyID,
			config.R2SecretAccessKey,
			"",
		),
	})

	if config.ListOnly {
		listBucket(ctx, s3Client, config)
		return
	}

	// Download the file
	key := fmt.Sprintf("%s/latest.parquet", config.CollectionName)

	if config.OutputPath == "" {
		config.OutputPath = filepath.Join(".", config.CollectionName, "latest.parquet")
	}

	fmt.Printf("  Key:        %s\n", key)
	fmt.Printf("  Output:     %s\n\n", config.OutputPath)

	if err := downloadFromR2(ctx, s3Client, config, key); err != nil {
		log.Fatalf("Download failed: %v", err)
	}
}

func listBucket(ctx context.Context, client *s3.Client, config DownloadConfig) {
	fmt.Printf("\nListing files in bucket '%s':\n\n", config.R2Bucket)

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(config.R2Bucket),
	})

	totalSize := int64(0)
	totalFiles := 0

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			log.Fatalf("Failed to list objects: %v", err)
		}

		for _, obj := range page.Contents {
			size := *obj.Size
			totalSize += size
			totalFiles++

			sizeStr := formatSize(size)
			modified := obj.LastModified.Format("2006-01-02 15:04:05")
			fmt.Printf("  %10s  %s  %s\n", sizeStr, modified, *obj.Key)
		}
	}

	fmt.Printf("\n  Total: %d files, %s\n", totalFiles, formatSize(totalSize))
}

func downloadFromR2(ctx context.Context, client *s3.Client, config DownloadConfig, key string) error {
	// Get object metadata first
	headResp, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(config.R2Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to get object info: %w", err)
	}

	fileSize := *headResp.ContentLength
	fmt.Printf("File size: %s\n", formatSize(fileSize))

	// Create output directory
	outputDir := filepath.Dir(config.OutputPath)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Create output file
	outFile, err := os.Create(config.OutputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Buffered writer for better performance
	bufWriter := bufio.NewWriterSize(outFile, 64*1024) // 64KB buffer
	defer bufWriter.Flush()

	// Download the object
	log.Println("Downloading...")
	startTime := time.Now()

	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(config.R2Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to download: %w", err)
	}
	defer resp.Body.Close()

	// Progress tracking
	progressReader := &progressReader{
		reader:    resp.Body,
		total:     fileSize,
		startTime: startTime,
	}

	written, err := io.Copy(bufWriter, progressReader)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	duration := time.Since(startTime)
	speed := float64(written) / duration.Seconds() / (1024 * 1024)

	fmt.Printf("\n\nDownloaded %s in %v (%.2f MB/s)\n", formatSize(written), duration.Round(time.Second), speed)
	fmt.Printf("Saved to: %s\n", config.OutputPath)

	return nil
}

type progressReader struct {
	reader     io.Reader
	total      int64
	downloaded int64
	startTime  time.Time
	lastPrint  time.Time
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	pr.downloaded += int64(n)

	// Print progress every 500ms
	if time.Since(pr.lastPrint) > 500*time.Millisecond {
		pr.lastPrint = time.Now()
		elapsed := time.Since(pr.startTime).Seconds()
		speed := float64(pr.downloaded) / elapsed / (1024 * 1024)
		pct := float64(pr.downloaded) / float64(pr.total) * 100

		// Estimate remaining time
		if speed > 0 {
			remaining := float64(pr.total-pr.downloaded) / (speed * 1024 * 1024)
			fmt.Printf("\r  Progress: %s / %s (%.1f%%) - %.2f MB/s - ETA: %s",
				formatSize(pr.downloaded),
				formatSize(pr.total),
				pct,
				speed,
				formatDuration(remaining),
			)
		}
	}

	return n, err
}

func formatSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatDuration(seconds float64) string {
	if seconds < 60 {
		return fmt.Sprintf("%.0fs", seconds)
	}
	if seconds < 3600 {
		return fmt.Sprintf("%.0fm %.0fs", seconds/60, float64(int(seconds)%60))
	}
	hours := int(seconds / 3600)
	mins := int(seconds/60) % 60
	return fmt.Sprintf("%dh %dm", hours, mins)
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// Allow downloading specific files by providing them as arguments
func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Download parquet data from R2 bucket\n\n")
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s --list                           # List all files in bucket\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s                                  # Download {collection}/latest.parquet\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s --collection my-vectors          # Download my-vectors/latest.parquet\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s --output ./data/export.parquet   # Download to custom path\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nEnvironment variables (or .env file):\n")
		fmt.Fprintf(os.Stderr, "  R2_ACCESS_KEY_ID      - R2 access key\n")
		fmt.Fprintf(os.Stderr, "  R2_SECRET_ACCESS_KEY  - R2 secret key\n")
		fmt.Fprintf(os.Stderr, "  R2_ENDPOINT           - R2 endpoint URL\n")
		fmt.Fprintf(os.Stderr, "  R2_BUCKET             - R2 bucket name\n")
		fmt.Fprintf(os.Stderr, "  QDRANT_COLLECTION_NAME - Collection name (default: embeddings)\n")
	}
}
