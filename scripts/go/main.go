package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"
	"github.com/parquet-go/parquet-go"
	pb "github.com/qdrant/go-client/qdrant"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// ExportStrategy defines which export method to use
type ExportStrategy string

const (
	StrategyNDJSONGzip   ExportStrategy = "ndjson-gz"     // Default: gzipped NDJSON + DuckDB (memory-safe)
	StrategyParquetSmall ExportStrategy = "parquet-small" // Direct parquet, 1000 row groups
	StrategyParquetMed   ExportStrategy = "parquet-med"   // Direct parquet, 10000 row groups
	StrategyParquetLarge ExportStrategy = "parquet-large" // Direct parquet, 100000 row groups
)

// Config holds all configuration
type Config struct {
	QdrantURL         string
	QdrantAPIKey      string
	CollectionName    string
	R2AccessKeyID     string
	R2SecretAccessKey string
	R2Endpoint        string
	R2Bucket          string
	BatchSize         int
	ParallelWorkers   int
	VectorDim         int
	Resume            bool
	Debug             bool
	DryRun            bool
	Strategy          ExportStrategy
	RowGroupSize      int  // For parquet strategies
	StreamUpload      bool // Stream directly to R2 without storing on disk
	UseHTTP           bool // Use HTTP REST API instead of gRPC
	RetrieveBatch     int  // Batch size for retrieving full point data (with vectors)
}

// PointID holds point ID and key
type PointID struct {
	ID     uint64
	Key    string
	IsUUID bool
	UUID   string
}

func main() {
	var config Config
	var strategyStr string
	flag.StringVar(&config.CollectionName, "collection", "", "Qdrant collection name")
	flag.IntVar(&config.BatchSize, "batch-size", 10000, "Points per scroll batch")
	flag.IntVar(&config.ParallelWorkers, "parallel", runtime.NumCPU(), "Parallel workers")
	flag.IntVar(&config.VectorDim, "vector-dim", 1024, "Vector dimension")
	flag.BoolVar(&config.Resume, "resume", false, "Resume from existing files")
	flag.BoolVar(&config.Debug, "debug", false, "Debug mode")
	flag.BoolVar(&config.DryRun, "dry-run", false, "Dry run")
	flag.StringVar(&strategyStr, "strategy", "ndjson-gz", "Export strategy: ndjson-gz, parquet-small, parquet-med, parquet-large")
	flag.IntVar(&config.RowGroupSize, "row-group-size", 0, "Override row group size (0 = use strategy default)")
	flag.BoolVar(&config.StreamUpload, "stream-upload", false, "Stream directly to R2 (no disk, parquet strategies only)")
	flag.BoolVar(&config.UseHTTP, "http", false, "Use HTTP REST API instead of gRPC (for when gRPC port is blocked)")
	flag.IntVar(&config.RetrieveBatch, "retrieve-batch", 0, "Batch size for retrieving points with vectors (default: half of batch-size)")
	flag.Parse()

	// Set default retrieve batch to half of scroll batch
	if config.RetrieveBatch == 0 {
		config.RetrieveBatch = config.BatchSize / 2
		if config.RetrieveBatch < 100 {
			config.RetrieveBatch = 100
		}
	}

	// Parse strategy
	config.Strategy = ExportStrategy(strategyStr)
	switch config.Strategy {
	case StrategyNDJSONGzip, StrategyParquetSmall, StrategyParquetMed, StrategyParquetLarge:
		// Valid
	default:
		log.Fatalf("Invalid strategy: %s. Valid options: ndjson-gz, parquet-small, parquet-med, parquet-large", strategyStr)
	}

	// Set default row group sizes
	if config.RowGroupSize == 0 {
		switch config.Strategy {
		case StrategyParquetSmall:
			config.RowGroupSize = 1000
		case StrategyParquetMed:
			config.RowGroupSize = 10000
		case StrategyParquetLarge:
			config.RowGroupSize = 100000
		}
	}

	// Load .env
	for _, p := range []string{".env", "../.env", "../../.env"} {
		if err := godotenv.Load(p); err == nil {
			log.Printf("Loaded environment from %s", p)
			break
		}
	}

	// Load config
	qdrantPort := getEnv("QDRANT_PORT", "")
	if config.UseHTTP {
		config.QdrantURL = normalizeQdrantHTTPURL(getEnv("QDRANT_URL", "http://localhost:6333"), qdrantPort)
	} else {
		config.QdrantURL = normalizeQdrantURL(getEnv("QDRANT_URL", "localhost:6334"))
	}
	config.QdrantAPIKey = os.Getenv("QDRANT_API_KEY")
	if config.CollectionName == "" {
		config.CollectionName = getEnv("QDRANT_COLLECTION_NAME", "embeddings")
	}
	config.R2AccessKeyID = os.Getenv("R2_ACCESS_KEY_ID")
	config.R2SecretAccessKey = os.Getenv("R2_SECRET_ACCESS_KEY")
	config.R2Endpoint = os.Getenv("R2_ENDPOINT")
	config.R2Bucket = os.Getenv("R2_BUCKET")

	if dim := os.Getenv("VECTOR_DIMENSION"); dim != "" {
		fmt.Sscanf(dim, "%d", &config.VectorDim)
	}

	printConfig(config)

	if config.R2AccessKeyID == "" || config.R2SecretAccessKey == "" || config.R2Endpoint == "" || config.R2Bucket == "" {
		log.Fatal("Missing R2 configuration")
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("\nShutting down...")
		cancel()
	}()

	if err := runExport(ctx, config); err != nil {
		if ctx.Err() == context.Canceled {
			log.Println("Export cancelled")
			os.Exit(0)
		}
		log.Fatalf("Export failed: %v", err)
	}
}

func printConfig(config Config) {
	fmt.Println("\n=== Qdrant R2 Exporter (Go) ===\n")
	fmt.Printf("  Collection:    %s\n", config.CollectionName)
	if config.UseHTTP {
		fmt.Printf("  Qdrant:        %s (HTTP)\n", config.QdrantURL)
	} else {
		fmt.Printf("  Qdrant:        %s (gRPC)\n", config.QdrantURL)
	}
	fmt.Printf("  R2 Bucket:     %s\n", config.R2Bucket)
	fmt.Printf("  Vector Dim:    %d\n", config.VectorDim)
	fmt.Printf("  Batch Size:    %d (scroll), %d (retrieve)\n", config.BatchSize, config.RetrieveBatch)
	fmt.Printf("  Workers:       %d\n", config.ParallelWorkers)
	fmt.Printf("  Strategy:      %s\n", config.Strategy)
	if config.RowGroupSize > 0 {
		fmt.Printf("  Row Group:     %d\n", config.RowGroupSize)
	}
	if config.StreamUpload {
		fmt.Printf("  Stream Upload: %v (no disk storage)\n", config.StreamUpload)
	}
	fmt.Printf("  Resume:        %v\n", config.Resume)
	fmt.Printf("  Debug:         %v\n", config.Debug)
	fmt.Println()
}

// QdrantClient interface for both gRPC and HTTP implementations
type QdrantClient interface {
	GetPointsCount(ctx context.Context, collection string) (uint64, error)
	Scroll(ctx context.Context, collection string, limit int, offset interface{}, withPayloadFields []string, withVectors bool) ([]ScrollPoint, interface{}, error)
	GetPoints(ctx context.Context, collection string, ids []PointID) ([]RetrievedPoint, error)
	Close() error
}

// ScrollPoint from scroll response
type ScrollPoint struct {
	ID      PointID
	Payload map[string]interface{}
}

// RetrievedPoint with full data
type RetrievedPoint struct {
	ID       PointID
	Key      string
	Metadata string
	Vector   []float32
}

func runExport(ctx context.Context, config Config) error {
	startTime := time.Now()

	// Connect to Qdrant (HTTP or gRPC)
	var client QdrantClient
	var err error

	if config.UseHTTP {
		log.Printf("Connecting to Qdrant at %s (HTTP)...", config.QdrantURL)
		client = NewHTTPClient(config.QdrantURL, config.QdrantAPIKey)
	} else {
		log.Printf("Connecting to Qdrant at %s (gRPC)...", config.QdrantURL)
		client, err = NewGRPCClient(ctx, config.QdrantURL, config.QdrantAPIKey)
		if err != nil {
			return fmt.Errorf("failed to connect: %w", err)
		}
	}
	defer client.Close()

	// Get collection info
	totalPoints, err := client.GetPointsCount(ctx, config.CollectionName)
	if err != nil {
		return fmt.Errorf("failed to get collection: %w", err)
	}

	log.Printf("Collection '%s': %d points", config.CollectionName, totalPoints)

	if totalPoints == 0 {
		return nil
	}

	if config.DryRun {
		log.Printf("[DRY RUN] Would export %d points to %s/latest.parquet", totalPoints, config.CollectionName)
		return nil
	}

	// Setup temp directory
	today := time.Now().Format("2006-01-02")
	baseDir := os.Getenv("OUTPUT_TEMP_FOLDER")
	if baseDir == "" {
		baseDir = os.TempDir()
	}
	tempDir := filepath.Join(baseDir, fmt.Sprintf("qdrant-export-%s-%s", config.CollectionName, today))
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return err
	}
	log.Printf("Temp directory: %s", tempDir)

	// Generate strategy-specific filenames (useful for --debug to compare strategies)
	var filePrefix string
	switch config.Strategy {
	case StrategyNDJSONGzip:
		filePrefix = "ndjson-gz"
	case StrategyParquetSmall:
		filePrefix = fmt.Sprintf("parquet-rg%d", config.RowGroupSize)
	case StrategyParquetMed:
		filePrefix = fmt.Sprintf("parquet-rg%d", config.RowGroupSize)
	case StrategyParquetLarge:
		filePrefix = fmt.Sprintf("parquet-rg%d", config.RowGroupSize)
	default:
		filePrefix = "output"
	}

	parquetPath := filepath.Join(tempDir, fmt.Sprintf("%s.parquet", filePrefix))
	ndjsonGzPath := filepath.Join(tempDir, fmt.Sprintf("%s.ndjson.gz", filePrefix))

	var exported int64

	// Check resume - strategy-agnostic parquet check
	if config.Resume {
		if fi, err := os.Stat(parquetPath); err == nil && fi.Size() > 0 {
			log.Println("RESUME: Found parquet, skipping export")
			goto upload
		}
	}

	// Branch by strategy
	switch config.Strategy {
	case StrategyNDJSONGzip:
		// Check for NDJSON resume
		skipExport := false
		if config.Resume {
			if fi, err := os.Stat(ndjsonGzPath); err == nil && fi.Size() > 0 {
				log.Println("RESUME: Found ndjson.gz, skipping export")
				skipExport = true
			}
		}

		if !skipExport {
			log.Printf("Strategy: NDJSON-GZ (memory-safe, DuckDB conversion)")
			exported, err = runStreamingNDJSON(ctx, client, config, ndjsonGzPath, totalPoints)
			if err != nil {
				return err
			}
		}

		// Convert to Parquet using DuckDB
		log.Println("Converting to Parquet with DuckDB...")
		if err := convertToParquet(ctx, ndjsonGzPath, parquetPath); err != nil {
			return err
		}

	case StrategyParquetSmall, StrategyParquetMed, StrategyParquetLarge:
		log.Printf("Strategy: Direct Parquet (row group size: %d)", config.RowGroupSize)

		if config.StreamUpload {
			// Stream directly to R2 - no disk storage!
			log.Println("Streaming directly to R2 (no disk storage)...")
			streamKey := fmt.Sprintf("%s/latest.parquet", config.CollectionName)
			exported, err = runStreamingParquetUpload(ctx, client, config, streamKey, totalPoints)
			if err != nil {
				return err
			}
			// Skip the upload section since we already uploaded
			goto complete
		}

		exported, err = runDirectParquet(ctx, client, config, parquetPath, totalPoints)
		if err != nil {
			return err
		}

	default:
		return fmt.Errorf("unknown strategy: %s", config.Strategy)
	}

upload:

	// Get file sizes
	if fi, err := os.Stat(ndjsonGzPath); err == nil {
		log.Printf("NDJSON.gz: %.2f GB", float64(fi.Size())/(1024*1024*1024))
	}
	if fi, err := os.Stat(parquetPath); err == nil {
		log.Printf("Parquet: %.2f GB", float64(fi.Size())/(1024*1024*1024))
	}

	// Debug confirmation
	if config.Debug {
		fmt.Print("\nUpload to R2? [Y/n]: ")
		var answer string
		fmt.Scanln(&answer)
		if answer != "" && strings.ToLower(answer)[0] != 'y' {
			log.Printf("Cancelled. Files at: %s", tempDir)
			return nil
		}
	}

	// Upload
	log.Println("Uploading to R2...")
	{
		key := fmt.Sprintf("%s/latest.parquet", config.CollectionName)
		if err := uploadToR2(ctx, config, parquetPath, key); err != nil {
			return err
		}
	}

	// Cleanup
	if !config.Debug {
		os.RemoveAll(tempDir)
	} else {
		log.Printf("DEBUG: Files kept at %s", tempDir)
	}

complete:

	log.Printf("\n=== Complete ===")
	log.Printf("Exported: %d points", exported)
	log.Printf("Duration: %v", time.Since(startTime).Round(time.Second))
	log.Printf("Rate: %.0f pts/sec", float64(exported)/time.Since(startTime).Seconds())

	return nil
}

// runStreamingNDJSON writes gzipped NDJSON with constant memory usage
func runStreamingNDJSON(
	ctx context.Context,
	client QdrantClient,
	config Config,
	outputPath string,
	totalPoints uint64,
) (int64, error) {

	// Use config batch sizes
	scrollBatch := config.BatchSize
	retrieveBatch := config.RetrieveBatch

	// Progress tracking
	var retrieved, written int64
	progressDone := make(chan struct{})

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		start := time.Now()
		var memStats runtime.MemStats

		for {
			select {
			case <-ticker.C:
				r := atomic.LoadInt64(&retrieved)
				w := atomic.LoadInt64(&written)
				elapsed := time.Since(start).Seconds()
				pct := float64(w) / float64(totalPoints) * 100
				runtime.ReadMemStats(&memStats)
				heapMB := float64(memStats.HeapAlloc) / (1024 * 1024)
				fmt.Printf("\rRetrieved: %d | Written: %d/%d (%.1f%%) | %.0f pts/sec | Heap: %.0fMB   ",
					r, w, totalPoints, pct, float64(w)/elapsed, heapMB)
			case <-progressDone:
				fmt.Println()
				return
			}
		}
	}()

	// Create gzipped output file
	file, err := os.Create(outputPath)
	if err != nil {
		close(progressDone)
		return 0, fmt.Errorf("failed to create file: %w", err)
	}

	// Gzip writer with best compression
	gzWriter, err := gzip.NewWriterLevel(file, gzip.BestSpeed) // BestSpeed for faster writes, still good compression
	if err != nil {
		file.Close()
		close(progressDone)
		return 0, err
	}

	// Buffered writer on top (64KB buffer)
	bufWriter := bufio.NewWriterSize(gzWriter, 64*1024)

	// Pipeline channels - small buffers for backpressure
	idChan := make(chan []PointID, config.ParallelWorkers)
	dataChan := make(chan []byte, 4) // Pre-serialized NDJSON batches

	g, gctx := errgroup.WithContext(ctx)

	// STAGE 1: ID Producer
	g.Go(func() error {
		defer close(idChan)
		return produceIDs(gctx, client, config.CollectionName, scrollBatch, scrollBatch, idChan)
	})

	// STAGE 2: Retrieve Workers - serialize to NDJSON bytes
	var workerWg sync.WaitGroup
	for i := 0; i < config.ParallelWorkers; i++ {
		workerWg.Add(1)
		g.Go(func() error {
			defer workerWg.Done()
			return retrieveWorker(gctx, client, config.CollectionName, config.VectorDim, idChan, dataChan, retrieveBatch, &retrieved)
		})
	}

	go func() {
		workerWg.Wait()
		close(dataChan)
	}()

	// STAGE 3: Single Writer - streams to gzipped file
	g.Go(func() error {
		for data := range dataChan {
			if _, err := bufWriter.Write(data); err != nil {
				return err
			}
			// Count newlines for progress
			for _, b := range data {
				if b == '\n' {
					atomic.AddInt64(&written, 1)
				}
			}
		}
		return nil
	})

	// Wait for pipeline
	if err := g.Wait(); err != nil {
		bufWriter.Flush()
		gzWriter.Close()
		file.Close()
		close(progressDone)
		return 0, err
	}

	// Flush and close in order
	if err := bufWriter.Flush(); err != nil {
		gzWriter.Close()
		file.Close()
		close(progressDone)
		return 0, err
	}
	if err := gzWriter.Close(); err != nil {
		file.Close()
		close(progressDone)
		return 0, err
	}
	file.Close()
	close(progressDone)

	return atomic.LoadInt64(&written), nil
}

// ParquetRow represents a single row in the parquet file
type ParquetRow struct {
	Key      string    `parquet:"key"`
	Metadata string    `parquet:"metadata"`
	Vector   []float32 `parquet:"vector"` // Native float32 array
}

// runDirectParquet writes directly to parquet with configurable row group sizes
// WARNING: Memory usage depends on row group size * vector dimension * 4 bytes
func runDirectParquet(
	ctx context.Context,
	client QdrantClient,
	config Config,
	outputPath string,
	totalPoints uint64,
) (int64, error) {

	// Use config batch sizes
	scrollBatch := config.BatchSize
	retrieveBatch := config.RetrieveBatch

	// Progress tracking
	var retrieved, written int64
	progressDone := make(chan struct{})

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		start := time.Now()
		var memStats runtime.MemStats

		for {
			select {
			case <-ticker.C:
				r := atomic.LoadInt64(&retrieved)
				w := atomic.LoadInt64(&written)
				elapsed := time.Since(start).Seconds()
				pct := float64(w) / float64(totalPoints) * 100
				runtime.ReadMemStats(&memStats)
				heapMB := float64(memStats.HeapAlloc) / (1024 * 1024)
				fmt.Printf("\rRetrieved: %d | Written: %d/%d (%.1f%%) | %.0f pts/sec | Heap: %.0fMB   ",
					r, w, totalPoints, pct, float64(w)/elapsed, heapMB)
			case <-progressDone:
				fmt.Println()
				return
			}
		}
	}()

	// Create parquet file with snappy compression and configurable row group size
	file, err := os.Create(outputPath)
	if err != nil {
		close(progressDone)
		return 0, fmt.Errorf("failed to create file: %w", err)
	}

	// Configure parquet writer with small row groups for memory efficiency
	writer := parquet.NewGenericWriter[ParquetRow](file,
		parquet.Compression(&parquet.Snappy),
		parquet.PageBufferSize(64*1024),       // 64KB page buffer (small)
		parquet.WriteBufferSize(1*1024*1024),  // 1MB write buffer
	)

	// Pipeline channels
	idChan := make(chan []PointID, config.ParallelWorkers)
	dataChan := make(chan []pointDataParquet, 4)

	g, gctx := errgroup.WithContext(ctx)

	// STAGE 1: ID Producer
	g.Go(func() error {
		defer close(idChan)
		return produceIDs(gctx, client, config.CollectionName, scrollBatch, scrollBatch, idChan)
	})

	// STAGE 2: Retrieve Workers
	var workerWg sync.WaitGroup
	var workersStarted int32
	var workersFinished int32
	for i := 0; i < config.ParallelWorkers; i++ {
		workerWg.Add(1)
		workerID := i
		g.Go(func() error {
			atomic.AddInt32(&workersStarted, 1)
			defer func() {
				atomic.AddInt32(&workersFinished, 1)
				workerWg.Done()
			}()
			err := retrieveWorkerParquet(gctx, client, config.CollectionName, idChan, dataChan, retrieveBatch, &retrieved)
			if err != nil {
				log.Printf("Worker %d returned error: %v", workerID, err)
			}
			return err
		})
	}

	go func() {
		workerWg.Wait()
		log.Printf("All workers finished (started=%d, finished=%d), closing dataChan",
			atomic.LoadInt32(&workersStarted), atomic.LoadInt32(&workersFinished))
		close(dataChan)
	}()

	// STAGE 3: Single Writer - writes to parquet in batches (row groups)
	g.Go(func() error {
		rowBuffer := make([]ParquetRow, 0, config.RowGroupSize)

		flushRowGroup := func() error {
			if len(rowBuffer) == 0 {
				return nil
			}
			_, err := writer.Write(rowBuffer)
			if err != nil {
				return err
			}
			// Flush triggers row group boundary
			if err := writer.Flush(); err != nil {
				return err
			}
			atomic.AddInt64(&written, int64(len(rowBuffer)))
			rowBuffer = rowBuffer[:0] // Reset slice, keep capacity
			runtime.GC()               // Help release memory between row groups
			return nil
		}

		for batch := range dataChan {
			for _, p := range batch {
				row := ParquetRow{
					Key:      p.Key,
					Metadata: p.Metadata,
					Vector:   p.Vector, // Direct assignment - native float32 array
				}
				rowBuffer = append(rowBuffer, row)

				// Flush at row group boundary
				if len(rowBuffer) >= config.RowGroupSize {
					if err := flushRowGroup(); err != nil {
						return err
					}
				}
			}
		}

		// Flush remaining rows
		return flushRowGroup()
	})

	// Wait for pipeline
	if err := g.Wait(); err != nil {
		log.Printf("Pipeline error: %v", err)
		log.Printf("At time of error: retrieved=%d, written=%d", atomic.LoadInt64(&retrieved), atomic.LoadInt64(&written))
		writer.Close()
		file.Close()
		close(progressDone)
		return 0, err
	}

	finalWritten := atomic.LoadInt64(&written)
	finalRetrieved := atomic.LoadInt64(&retrieved)
	log.Printf("Pipeline completed: retrieved=%d, written=%d", finalRetrieved, finalWritten)

	if err := writer.Close(); err != nil {
		file.Close()
		close(progressDone)
		return 0, err
	}
	file.Close()
	close(progressDone)

	return finalWritten, nil
}

// runStreamingParquetUpload writes parquet directly to R2 using io.Pipe
// No disk storage required - data flows: Qdrant -> Parquet Writer -> Pipe -> S3 Uploader -> R2
func runStreamingParquetUpload(
	ctx context.Context,
	client QdrantClient,
	config Config,
	s3Key string,
	totalPoints uint64,
) (int64, error) {

	// Use config batch sizes
	scrollBatch := config.BatchSize
	retrieveBatch := config.RetrieveBatch

	// Progress tracking
	var retrieved, written int64
	progressDone := make(chan struct{})

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		start := time.Now()
		var memStats runtime.MemStats

		for {
			select {
			case <-ticker.C:
				r := atomic.LoadInt64(&retrieved)
				w := atomic.LoadInt64(&written)
				elapsed := time.Since(start).Seconds()
				pct := float64(w) / float64(totalPoints) * 100
				runtime.ReadMemStats(&memStats)
				heapMB := float64(memStats.HeapAlloc) / (1024 * 1024)
				fmt.Printf("\rRetrieved: %d | Uploaded: %d/%d (%.1f%%) | %.0f pts/sec | Heap: %.0fMB   ",
					r, w, totalPoints, pct, float64(w)/elapsed, heapMB)
			case <-progressDone:
				fmt.Println()
				return
			}
		}
	}()

	// Create pipe: parquet writer writes to pipeWriter, S3 uploader reads from pipeReader
	pipeReader, pipeWriter := io.Pipe()

	// S3 client
	s3Client := s3.New(s3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String(config.R2Endpoint),
		Credentials: credentials.NewStaticCredentialsProvider(
			config.R2AccessKeyID,
			config.R2SecretAccessKey,
			"",
		),
	})

	uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) {
		u.PartSize = 64 * 1024 * 1024 // 64MB parts
		u.Concurrency = 4
	})

	// Upload goroutine - runs concurrently with parquet writing
	var uploadErr error
	uploadDone := make(chan struct{})
	go func() {
		defer close(uploadDone)
		_, uploadErr = uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(config.R2Bucket),
			Key:    aws.String(s3Key),
			Body:   pipeReader,
		})
		if uploadErr != nil {
			log.Printf("Upload error: %v", uploadErr)
		}
	}()

	// Create parquet writer to pipe
	writer := parquet.NewGenericWriter[ParquetRow](pipeWriter,
		parquet.Compression(&parquet.Snappy),
		parquet.PageBufferSize(64*1024),
		parquet.WriteBufferSize(1*1024*1024),
	)

	// Pipeline channels
	idChan := make(chan []PointID, config.ParallelWorkers)
	dataChan := make(chan []pointDataParquet, 4)

	g, gctx := errgroup.WithContext(ctx)

	// STAGE 1: ID Producer
	g.Go(func() error {
		defer close(idChan)
		return produceIDs(gctx, client, config.CollectionName, scrollBatch, scrollBatch, idChan)
	})

	// STAGE 2: Retrieve Workers
	var workerWg sync.WaitGroup
	for i := 0; i < config.ParallelWorkers; i++ {
		workerWg.Add(1)
		g.Go(func() error {
			defer workerWg.Done()
			return retrieveWorkerParquet(gctx, client, config.CollectionName, idChan, dataChan, retrieveBatch, &retrieved)
		})
	}

	go func() {
		workerWg.Wait()
		close(dataChan)
	}()

	// STAGE 3: Single Writer - writes to parquet (which writes to pipe)
	g.Go(func() error {
		rowBuffer := make([]ParquetRow, 0, config.RowGroupSize)

		flushRowGroup := func() error {
			if len(rowBuffer) == 0 {
				return nil
			}
			_, err := writer.Write(rowBuffer)
			if err != nil {
				return err
			}
			if err := writer.Flush(); err != nil {
				return err
			}
			atomic.AddInt64(&written, int64(len(rowBuffer)))
			rowBuffer = rowBuffer[:0]
			runtime.GC()
			return nil
		}

		for batch := range dataChan {
			for _, p := range batch {
				row := ParquetRow{
					Key:      p.Key,
					Metadata: p.Metadata,
					Vector:   p.Vector, // Direct assignment - native float32 array
				}
				rowBuffer = append(rowBuffer, row)

				if len(rowBuffer) >= config.RowGroupSize {
					if err := flushRowGroup(); err != nil {
						return err
					}
				}
			}
		}

		return flushRowGroup()
	})

	// Wait for data pipeline
	pipelineErr := g.Wait()

	// Close parquet writer (flushes remaining data + footer)
	if err := writer.Close(); err != nil && pipelineErr == nil {
		pipelineErr = err
	}

	// Close pipe writer to signal EOF to uploader
	pipeWriter.Close()

	// Wait for upload to complete
	<-uploadDone
	close(progressDone)

	if pipelineErr != nil {
		return 0, pipelineErr
	}
	if uploadErr != nil {
		return 0, uploadErr
	}

	log.Printf("Streamed to s3://%s/%s", config.R2Bucket, s3Key)
	return atomic.LoadInt64(&written), nil
}

// pointDataParquet holds point data for parquet export
type pointDataParquet struct {
	Key      string
	Metadata string
	Vector   []float32
}

// retrieveWorkerParquet fetches full point data for parquet writing
func retrieveWorkerParquet(
	ctx context.Context,
	client QdrantClient,
	collection string,
	in <-chan []PointID,
	out chan<- []pointDataParquet,
	batchSize int,
	counter *int64,
) error {
	batchesReceived := 0
	batchesProcessed := 0
	idsProcessed := 0

	defer func() {
		log.Printf("Worker exiting: received %d ID batches, processed %d retrieve batches, %d IDs total", batchesReceived, batchesProcessed, idsProcessed)
	}()

	for ids := range in {
		batchesReceived++
		for i := 0; i < len(ids); i += batchSize {
			// Check context before each batch
			select {
			case <-ctx.Done():
				log.Printf("Worker cancelled after receiving %d batches, processing %d IDs", batchesReceived, idsProcessed)
				return ctx.Err()
			default:
			}

			end := min(i+batchSize, len(ids))
			batch := ids[i:end]

			points, err := client.GetPoints(ctx, collection, batch)
			if err != nil {
				log.Printf("Worker error after %d IDs: %v", idsProcessed, err)
				return fmt.Errorf("GetPoints failed after %d IDs: %w", idsProcessed, err)
			}

			results := make([]pointDataParquet, 0, len(points))

			for _, point := range points {
				results = append(results, pointDataParquet{
					Key:      point.Key,
					Metadata: point.Metadata,
					Vector:   point.Vector,
				})

				atomic.AddInt64(counter, 1)
				idsProcessed++
			}

			if len(results) > 0 {
				select {
				case out <- results:
					batchesProcessed++
				case <-ctx.Done():
					log.Printf("Worker cancelled while sending after %d IDs", idsProcessed)
					return ctx.Err()
				}
			}
		}
	}

	return nil
}

// produceIDs scrolls through Qdrant collecting IDs without vectors
func produceIDs(
	ctx context.Context,
	client QdrantClient,
	collection string,
	scrollBatch, sendBatch int,
	out chan<- []PointID,
) error {
	var offset interface{}
	batch := make([]PointID, 0, sendBatch)
	var totalSent int64

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		points, nextOffset, err := client.Scroll(ctx, collection, scrollBatch, offset, []string{"key"}, false)
		if err != nil {
			return err
		}

		if len(points) == 0 {
			break
		}

		for _, p := range points {
			pid := p.ID
			if keyVal, ok := p.Payload["key"].(string); ok && keyVal != "" {
				pid.Key = keyVal
			}
			batch = append(batch, pid)

			if len(batch) >= sendBatch {
				select {
				case out <- batch:
					totalSent += int64(len(batch))
					batch = make([]PointID, 0, sendBatch)
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		if len(points) < scrollBatch {
			break
		}
		offset = nextOffset
	}

	if len(batch) > 0 {
		select {
		case out <- batch:
			totalSent += int64(len(batch))
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	log.Printf("ID Producer finished: sent %d unique IDs to workers", totalSent)
	return nil
}

// retrieveWorker fetches full point data and serializes to NDJSON
func retrieveWorker(
	ctx context.Context,
	client QdrantClient,
	collection string,
	vectorDim int,
	in <-chan []PointID,
	out chan<- []byte,
	batchSize int,
	counter *int64,
) error {
	// Reusable buffer for JSON serialization
	var buf strings.Builder
	buf.Grow(512 * 1024) // 512KB initial

	batchesProcessed := 0
	idsProcessed := 0

	defer func() {
		log.Printf("NDJSON Worker exiting: processed %d batches, %d IDs", batchesProcessed, idsProcessed)
	}()

	for ids := range in {
		for i := 0; i < len(ids); i += batchSize {
			// Check context before each batch
			select {
			case <-ctx.Done():
				log.Printf("NDJSON Worker cancelled after processing %d IDs", idsProcessed)
				return ctx.Err()
			default:
			}

			end := min(i+batchSize, len(ids))
			batch := ids[i:end]

			points, err := client.GetPoints(ctx, collection, batch)
			if err != nil {
				log.Printf("NDJSON Worker error after %d IDs: %v", idsProcessed, err)
				return fmt.Errorf("GetPoints failed after %d IDs: %w", idsProcessed, err)
			}

			buf.Reset()

			for _, point := range points {
				// Build JSON manually for efficiency
				buf.WriteString(`{"key":`)

				keyJSON, _ := json.Marshal(point.Key)
				buf.Write(keyJSON)

				// Metadata
				buf.WriteString(`,"metadata":`)
				metaJSON, _ := json.Marshal(point.Metadata)
				buf.Write(metaJSON)

				// Vector as array
				buf.WriteString(`,"vector":[`)
				for j, val := range point.Vector {
					if j > 0 {
						buf.WriteByte(',')
					}
					fmt.Fprintf(&buf, "%g", val)
				}
				buf.WriteString("]}\n")

				atomic.AddInt64(counter, 1)
				idsProcessed++
			}

			if buf.Len() > 0 {
				// Copy buffer content (we reuse the buffer)
				data := make([]byte, buf.Len())
				copy(data, buf.String())

				select {
				case out <- data:
					batchesProcessed++
				case <-ctx.Done():
					log.Printf("NDJSON Worker cancelled while sending after %d IDs", idsProcessed)
					return ctx.Err()
				}
			}
		}
	}

	return nil
}

// convertToParquet uses DuckDB to convert gzipped NDJSON to Parquet
func convertToParquet(ctx context.Context, ndjsonGzPath, parquetPath string) error {
	// DuckDB can read .ndjson.gz directly!
	// Cast vector from DOUBLE[] to FLOAT[] (32-bit) to save space
	query := fmt.Sprintf(
		"COPY (SELECT key, metadata, CAST(vector AS FLOAT[]) AS vector FROM read_json_auto('%s', format='newline_delimited', compression='gzip')) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)",
		filepath.ToSlash(ndjsonGzPath),
		filepath.ToSlash(parquetPath),
	)

	log.Printf("Running DuckDB conversion...")
	cmd := exec.CommandContext(ctx, "duckdb", "-c", query)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func structToJSON(s *pb.Struct) string {
	if s == nil {
		return "{}"
	}
	var b strings.Builder
	b.WriteString("{")
	first := true
	for k, v := range s.GetFields() {
		if !first {
			b.WriteString(",")
		}
		first = false
		b.WriteString(`"`)
		b.WriteString(k)
		b.WriteString(`":`)
		writeValue(&b, v)
	}
	b.WriteString("}")
	return b.String()
}

func writeValue(b *strings.Builder, v *pb.Value) {
	if v == nil {
		b.WriteString("null")
		return
	}
	switch val := v.Kind.(type) {
	case *pb.Value_StringValue:
		b.WriteString(`"`)
		b.WriteString(strings.ReplaceAll(val.StringValue, `"`, `\"`))
		b.WriteString(`"`)
	case *pb.Value_IntegerValue:
		fmt.Fprintf(b, "%d", val.IntegerValue)
	case *pb.Value_DoubleValue:
		fmt.Fprintf(b, "%g", val.DoubleValue)
	case *pb.Value_BoolValue:
		if val.BoolValue {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	default:
		b.WriteString("null")
	}
}

func uploadToR2(ctx context.Context, config Config, localPath, key string) error {
	s3Client := s3.New(s3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String(config.R2Endpoint),
		Credentials: credentials.NewStaticCredentialsProvider(
			config.R2AccessKeyID,
			config.R2SecretAccessKey,
			"",
		),
	})

	file, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer file.Close()

	fi, _ := file.Stat()
	log.Printf("Uploading %.2f GB...", float64(fi.Size())/(1024*1024*1024))

	uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) {
		u.PartSize = 64 * 1024 * 1024 // 64MB parts for large files
		u.Concurrency = 4
	})

	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(config.R2Bucket),
		Key:    aws.String(key),
		Body:   file,
	})

	if err == nil {
		log.Printf("Uploaded to s3://%s/%s", config.R2Bucket, key)
	}
	return err
}

// Utility functions

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func normalizeQdrantURL(url string) string {
	url = strings.TrimPrefix(url, "http://")
	url = strings.TrimPrefix(url, "https://")
	url = strings.TrimSuffix(url, "/")
	if strings.HasSuffix(url, ":6333") {
		url = strings.TrimSuffix(url, ":6333") + ":6334"
	}
	if !strings.Contains(url, ":") {
		url += ":6334"
	}
	return url
}

func ptr[T any](v T) *T { return &v }

func parsePointID(id *pb.PointId) PointID {
	if id == nil {
		return PointID{}
	}
	switch v := id.PointIdOptions.(type) {
	case *pb.PointId_Num:
		return PointID{ID: v.Num, Key: fmt.Sprintf("%d", v.Num)}
	case *pb.PointId_Uuid:
		return PointID{IsUUID: true, UUID: v.Uuid, Key: v.Uuid}
	}
	return PointID{}
}

func pointIDToPB(p PointID) *pb.PointId {
	if p.IsUUID {
		return &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: p.UUID}}
	}
	return &pb.PointId{PointIdOptions: &pb.PointId_Num{Num: p.ID}}
}

func pointIDToString(id *pb.PointId) string {
	if id == nil {
		return ""
	}
	switch v := id.PointIdOptions.(type) {
	case *pb.PointId_Num:
		return fmt.Sprintf("%d", v.Num)
	case *pb.PointId_Uuid:
		return v.Uuid
	}
	return ""
}

func normalizeQdrantHTTPURL(url string, portOverride string) string {
	// Ensure URL has http:// or https:// prefix
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "https://" + url
	}
	url = strings.TrimSuffix(url, "/")

	// Extract scheme and host
	scheme := "http://"
	if strings.HasPrefix(url, "https://") {
		scheme = "https://"
	}
	hostPart := strings.TrimPrefix(strings.TrimPrefix(url, "http://"), "https://")

	// Remove existing port if any
	if idx := strings.LastIndex(hostPart, ":"); idx != -1 {
		hostPart = hostPart[:idx]
	}

	// Use QDRANT_PORT if specified, otherwise default based on scheme
	port := portOverride
	if port == "" {
		if scheme == "https://" {
			port = "443"
		} else {
			port = "6333"
		}
	}

	// Don't add port for standard ports
	if (scheme == "https://" && port == "443") || (scheme == "http://" && port == "80") {
		return scheme + hostPart
	}

	return scheme + hostPart + ":" + port
}

// =============================================================================
// gRPC Client Implementation
// =============================================================================

type GRPCClient struct {
	conn              *grpc.ClientConn
	pointsClient      pb.PointsClient
	collectionsClient pb.CollectionsClient
	ctx               context.Context
}

func NewGRPCClient(ctx context.Context, url, apiKey string) (*GRPCClient, error) {
	conn, err := grpc.NewClient(url,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100*1024*1024)),
	)
	if err != nil {
		return nil, err
	}

	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "api-key", apiKey)
	}

	return &GRPCClient{
		conn:              conn,
		pointsClient:      pb.NewPointsClient(conn),
		collectionsClient: pb.NewCollectionsClient(conn),
		ctx:               ctx,
	}, nil
}

func (c *GRPCClient) GetPointsCount(ctx context.Context, collection string) (uint64, error) {
	info, err := c.collectionsClient.Get(c.ctx, &pb.GetCollectionInfoRequest{CollectionName: collection})
	if err != nil {
		return 0, err
	}
	return info.GetResult().GetPointsCount(), nil
}

func (c *GRPCClient) Scroll(ctx context.Context, collection string, limit int, offset interface{}, withPayloadFields []string, withVectors bool) ([]ScrollPoint, interface{}, error) {
	var pbOffset *pb.PointId
	if offset != nil {
		pbOffset = offset.(*pb.PointId)
	}

	var payloadSelector *pb.WithPayloadSelector
	if len(withPayloadFields) > 0 {
		payloadSelector = &pb.WithPayloadSelector{
			SelectorOptions: &pb.WithPayloadSelector_Include{
				Include: &pb.PayloadIncludeSelector{Fields: withPayloadFields},
			},
		}
	} else {
		payloadSelector = &pb.WithPayloadSelector{
			SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: false},
		}
	}

	resp, err := c.pointsClient.Scroll(c.ctx, &pb.ScrollPoints{
		CollectionName: collection,
		Limit:          ptr(uint32(limit)),
		Offset:         pbOffset,
		WithPayload:    payloadSelector,
		WithVectors:    &pb.WithVectorsSelector{SelectorOptions: &pb.WithVectorsSelector_Enable{Enable: withVectors}},
	})
	if err != nil {
		return nil, nil, err
	}

	points := resp.GetResult()
	result := make([]ScrollPoint, len(points))
	for i, p := range points {
		result[i] = ScrollPoint{
			ID:      parsePointID(p.GetId()),
			Payload: convertPayload(p.GetPayload()),
		}
	}

	var nextOffset interface{}
	if len(points) > 0 {
		nextOffset = points[len(points)-1].GetId()
	}

	return result, nextOffset, nil
}

func (c *GRPCClient) GetPoints(ctx context.Context, collection string, ids []PointID) ([]RetrievedPoint, error) {
	pbIDs := make([]*pb.PointId, len(ids))
	for i, id := range ids {
		pbIDs[i] = pointIDToPB(id)
	}

	resp, err := c.pointsClient.Get(c.ctx, &pb.GetPoints{
		CollectionName: collection,
		Ids:            pbIDs,
		WithPayload:    &pb.WithPayloadSelector{SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true}},
		WithVectors:    &pb.WithVectorsSelector{SelectorOptions: &pb.WithVectorsSelector_Enable{Enable: true}},
	})
	if err != nil {
		return nil, err
	}

	result := make([]RetrievedPoint, len(resp.GetResult()))
	for i, p := range resp.GetResult() {
		key := pointIDToString(p.GetId())
		if keyVal := p.GetPayload()["key"]; keyVal != nil {
			if s := keyVal.GetStringValue(); s != "" {
				key = s
			}
		}

		metadata := "{}"
		if metaVal := p.GetPayload()["metadata"]; metaVal != nil {
			if structVal := metaVal.GetStructValue(); structVal != nil {
				metadata = structToJSON(structVal)
			}
		}

		var vector []float32
		if vec := p.GetVectors(); vec != nil {
			if v := vec.GetVector(); v != nil {
				vector = v.GetData()
			}
		}

		result[i] = RetrievedPoint{
			ID:       parsePointID(p.GetId()),
			Key:      key,
			Metadata: metadata,
			Vector:   vector,
		}
	}

	return result, nil
}

func (c *GRPCClient) Close() error {
	return c.conn.Close()
}

func convertPayload(payload map[string]*pb.Value) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range payload {
		if v != nil {
			switch val := v.Kind.(type) {
			case *pb.Value_StringValue:
				result[k] = val.StringValue
			case *pb.Value_IntegerValue:
				result[k] = val.IntegerValue
			case *pb.Value_DoubleValue:
				result[k] = val.DoubleValue
			case *pb.Value_BoolValue:
				result[k] = val.BoolValue
			}
		}
	}
	return result
}

// =============================================================================
// HTTP Client Implementation
// =============================================================================

type HTTPClient struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
	maxRetries int
}

func NewHTTPClient(url, apiKey string) *HTTPClient {
	return &HTTPClient{
		baseURL: url,
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout: 300 * time.Second, // 5 minutes for large batch requests
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		maxRetries: 3,
	}
}

func (c *HTTPClient) doRequest(ctx context.Context, method, path string, body interface{}) ([]byte, error) {
	var jsonBody []byte
	var err error
	if body != nil {
		jsonBody, err = json.Marshal(body)
		if err != nil {
			return nil, err
		}
	}

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 1s, 2s, 4s
			backoff := time.Duration(1<<(attempt-1)) * time.Second
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		var bodyReader io.Reader
		if jsonBody != nil {
			bodyReader = bytes.NewReader(jsonBody)
		}

		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
		if err != nil {
			return nil, err
		}

		req.Header.Set("Content-Type", "application/json")
		if c.apiKey != "" {
			req.Header.Set("api-key", c.apiKey)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			// Retry on network errors
			if ctx.Err() == nil {
				continue
			}
			return nil, err
		}

		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}

		// Retry on 5xx errors (server errors)
		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
			continue
		}

		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
		}

		return respBody, nil
	}

	return nil, fmt.Errorf("failed after %d retries: %w", c.maxRetries+1, lastErr)
}

func (c *HTTPClient) GetPointsCount(ctx context.Context, collection string) (uint64, error) {
	respBody, err := c.doRequest(ctx, "GET", "/collections/"+collection, nil)
	if err != nil {
		return 0, err
	}

	var resp struct {
		Result struct {
			PointsCount uint64 `json:"points_count"`
		} `json:"result"`
	}
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return 0, err
	}

	return resp.Result.PointsCount, nil
}

func (c *HTTPClient) Scroll(ctx context.Context, collection string, limit int, offset interface{}, withPayloadFields []string, withVectors bool) ([]ScrollPoint, interface{}, error) {
	reqBody := map[string]interface{}{
		"limit":        limit,
		"with_vectors": withVectors,
	}

	if offset != nil {
		reqBody["offset"] = offset
	}

	if len(withPayloadFields) > 0 {
		reqBody["with_payload"] = map[string]interface{}{
			"include": withPayloadFields,
		}
	} else {
		reqBody["with_payload"] = false
	}

	respBody, err := c.doRequest(ctx, "POST", "/collections/"+collection+"/points/scroll", reqBody)
	if err != nil {
		return nil, nil, err
	}

	var resp struct {
		Result struct {
			Points []struct {
				ID      interface{}            `json:"id"`
				Payload map[string]interface{} `json:"payload"`
			} `json:"points"`
			NextPageOffset interface{} `json:"next_page_offset"`
		} `json:"result"`
	}
	// Use json.Number to preserve precision for large integer IDs
	decoder := json.NewDecoder(bytes.NewReader(respBody))
	decoder.UseNumber()
	if err := decoder.Decode(&resp); err != nil {
		return nil, nil, err
	}

	result := make([]ScrollPoint, len(resp.Result.Points))
	for i, p := range resp.Result.Points {
		result[i] = ScrollPoint{
			ID:      parseHTTPPointID(p.ID),
			Payload: p.Payload,
		}
	}

	return result, resp.Result.NextPageOffset, nil
}

func (c *HTTPClient) GetPoints(ctx context.Context, collection string, ids []PointID) ([]RetrievedPoint, error) {
	// Convert PointIDs to the format expected by the API
	apiIDs := make([]interface{}, len(ids))
	for i, id := range ids {
		if id.IsUUID {
			apiIDs[i] = id.UUID
		} else {
			apiIDs[i] = id.ID
		}
	}

	reqBody := map[string]interface{}{
		"ids":          apiIDs,
		"with_payload": true,
		"with_vector":  true,
	}

	respBody, err := c.doRequest(ctx, "POST", "/collections/"+collection+"/points", reqBody)
	if err != nil {
		return nil, err
	}

	// First try standard response format with direct vector array
	var resp struct {
		Result []struct {
			ID      interface{}            `json:"id"`
			Payload map[string]interface{} `json:"payload"`
			Vector  json.RawMessage        `json:"vector"` // Use RawMessage to handle both formats
		} `json:"result"`
	}
	// Use json.Number to preserve precision for large integer IDs
	decoder := json.NewDecoder(bytes.NewReader(respBody))
	decoder.UseNumber()
	if err := decoder.Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w (first 500 chars: %s)", err, string(respBody[:min(500, len(respBody))]))
	}

	if len(resp.Result) != len(ids) && len(resp.Result) < len(ids) {
		// Log if significant mismatch (more than 1% missing)
		missingCount := len(ids) - len(resp.Result)
		hitRate := float64(len(resp.Result)) / float64(len(ids)) * 100
		if hitRate < 99 {
			log.Printf("WARNING: GetPoints requested %d IDs but got %d results (missing %d, %.1f%% hit rate)",
				len(ids), len(resp.Result), missingCount, hitRate)
		}
	}

	result := make([]RetrievedPoint, len(resp.Result))
	for i, p := range resp.Result {
		pointID := parseHTTPPointID(p.ID)

		key := pointID.Key
		if keyVal, ok := p.Payload["key"].(string); ok && keyVal != "" {
			key = keyVal
		}

		metadata := "{}"
		if metaVal, ok := p.Payload["metadata"].(map[string]interface{}); ok {
			metaBytes, _ := json.Marshal(metaVal)
			metadata = string(metaBytes)
		}

		// Parse vector - handle both array and named vector formats
		var vector []float32
		if len(p.Vector) > 0 {
			// Try direct array first
			if err := json.Unmarshal(p.Vector, &vector); err != nil {
				// Maybe it's a named vector object like {"default": [...]}
				var namedVectors map[string][]float32
				if err2 := json.Unmarshal(p.Vector, &namedVectors); err2 == nil {
					// Use the first (or "default") named vector
					if v, ok := namedVectors["default"]; ok {
						vector = v
					} else if v, ok := namedVectors[""]; ok {
						vector = v
					} else {
						// Use whatever vector is available
						for _, v := range namedVectors {
							vector = v
							break
						}
					}
				} else {
					log.Printf("WARNING: Could not parse vector for point %v: array err: %v, named err: %v, raw: %s",
						p.ID, err, err2, string(p.Vector[:min(200, len(p.Vector))]))
				}
			}
		}

		if len(vector) == 0 {
			log.Printf("WARNING: Point %v has empty vector", p.ID)
		}

		result[i] = RetrievedPoint{
			ID:       pointID,
			Key:      key,
			Metadata: metadata,
			Vector:   vector,
		}
	}

	return result, nil
}

func (c *HTTPClient) Close() error {
	return nil
}

func parseHTTPPointID(id interface{}) PointID {
	switch v := id.(type) {
	case float64:
		// WARNING: float64 can lose precision for large integers!
		// IDs > 2^53 will be corrupted when parsed as float64
		return PointID{ID: uint64(v), Key: fmt.Sprintf("%d", uint64(v))}
	case json.Number:
		// Prefer json.Number to preserve precision
		if i, err := v.Int64(); err == nil {
			return PointID{ID: uint64(i), Key: v.String()}
		}
		// If it's not an int, treat as string/UUID
		return PointID{IsUUID: true, UUID: v.String(), Key: v.String()}
	case string:
		return PointID{IsUUID: true, UUID: v, Key: v}
	case int64:
		return PointID{ID: uint64(v), Key: fmt.Sprintf("%d", v)}
	case uint64:
		return PointID{ID: v, Key: fmt.Sprintf("%d", v)}
	}
	return PointID{}
}
