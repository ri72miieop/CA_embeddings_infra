// Supabase Storage Upload via tus Protocol
// Exports Qdrant collection to Parquet and uploads to Supabase Storage
// Usage: go build -o supabase-upload.exe supabase-upload.go && ./supabase-upload.exe --collection embeddings
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/parquet-go/parquet-go"
	"golang.org/x/sync/errgroup"
)

// ============================================================================
// Configuration
// ============================================================================

type Config struct {
	// Qdrant
	QdrantURL      string
	QdrantAPIKey   string
	CollectionName string

	// Supabase Storage
	SupabaseProjectRef string
	SupabaseServiceKey string
	SupabaseBucket     string

	// Export settings
	BatchSize       int
	RetrieveBatch   int
	ParallelWorkers int
	VectorDim       int
	RowGroupSize    int

	// Feature flags
	Resume bool
	Debug  bool
	DryRun bool
}

// ============================================================================
// Types
// ============================================================================

type PointID struct {
	ID     uint64
	Key    string
	IsUUID bool
	UUID   string
}

type ScrollPoint struct {
	ID      PointID
	Payload map[string]interface{}
}

type RetrievedPoint struct {
	ID       PointID
	Key      string
	Metadata string
	Vector   []float32
}

type ParquetRow struct {
	Key      string    `parquet:"key"`
	Metadata string    `parquet:"metadata"`
	Vector   []float32 `parquet:"vector"`
}

type pointDataParquet struct {
	Key      string
	Metadata string
	Vector   []float32
}

// ============================================================================
// TUS Client for Supabase Storage
// ============================================================================

type TusClient struct {
	projectRef string
	serviceKey string
	bucket     string
	httpClient *http.Client
	chunkSize  int64
}

func NewTusClient(projectRef, serviceKey, bucket string) *TusClient {
	return &TusClient{
		projectRef: projectRef,
		serviceKey: serviceKey,
		bucket:     bucket,
		httpClient: &http.Client{
			Timeout: 5 * time.Minute,
			Transport: &http.Transport{
				MaxIdleConns:        10,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		chunkSize: 6 * 1024 * 1024, // 6MB chunks
	}
}

func (c *TusClient) baseURL() string {
	return fmt.Sprintf("https://%s.supabase.co/storage/v1", c.projectRef)
}

// CreateUpload initiates a new resumable upload
func (c *TusClient) CreateUpload(ctx context.Context, objectPath string, fileSize int64, upsert bool) (string, error) {
	url := c.baseURL() + "/upload/resumable"

	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return "", err
	}

	// tus headers
	req.Header.Set("Authorization", "Bearer "+c.serviceKey)
	req.Header.Set("Tus-Resumable", "1.0.0")
	req.Header.Set("Upload-Length", strconv.FormatInt(fileSize, 10))
	if upsert {
		req.Header.Set("x-upsert", "true")
	}

	// Upload-Metadata: base64 encoded key-value pairs
	metadata := fmt.Sprintf("bucketName %s,objectName %s,contentType %s",
		base64.StdEncoding.EncodeToString([]byte(c.bucket)),
		base64.StdEncoding.EncodeToString([]byte(objectPath)),
		base64.StdEncoding.EncodeToString([]byte("application/octet-stream")),
	)
	req.Header.Set("Upload-Metadata", metadata)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("create upload failed: HTTP %d: %s", resp.StatusCode, string(body))
	}

	location := resp.Header.Get("Location")
	if location == "" {
		return "", fmt.Errorf("no Location header in response")
	}

	return location, nil
}

// GetUploadOffset checks current upload offset (for resume)
func (c *TusClient) GetUploadOffset(ctx context.Context, uploadURL string) (int64, int64, error) {
	req, err := http.NewRequestWithContext(ctx, "HEAD", uploadURL, nil)
	if err != nil {
		return 0, 0, err
	}

	req.Header.Set("Authorization", "Bearer "+c.serviceKey)
	req.Header.Set("Tus-Resumable", "1.0.0")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return 0, 0, fmt.Errorf("get offset failed: HTTP %d", resp.StatusCode)
	}

	offsetStr := resp.Header.Get("Upload-Offset")
	lengthStr := resp.Header.Get("Upload-Length")

	offset, _ := strconv.ParseInt(offsetStr, 10, 64)
	length, _ := strconv.ParseInt(lengthStr, 10, 64)

	return offset, length, nil
}

// UploadChunk uploads a single chunk at the specified offset
func (c *TusClient) UploadChunk(ctx context.Context, uploadURL string, offset int64, data []byte) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, "PATCH", uploadURL, bytes.NewReader(data))
	if err != nil {
		return 0, err
	}

	req.Header.Set("Authorization", "Bearer "+c.serviceKey)
	req.Header.Set("Tus-Resumable", "1.0.0")
	req.Header.Set("Upload-Offset", strconv.FormatInt(offset, 10))
	req.Header.Set("Content-Type", "application/offset+octet-stream")
	req.Header.Set("Content-Length", strconv.Itoa(len(data)))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("upload chunk failed: HTTP %d: %s", resp.StatusCode, string(body))
	}

	newOffsetStr := resp.Header.Get("Upload-Offset")
	newOffset, err := strconv.ParseInt(newOffsetStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid Upload-Offset header: %s", newOffsetStr)
	}

	return newOffset, nil
}

// DeleteObject deletes an existing object from storage
func (c *TusClient) DeleteObject(ctx context.Context, objectPath string) error {
	url := c.baseURL() + "/object/" + c.bucket + "/" + objectPath

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+c.serviceKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 200 = deleted, 404 = didn't exist (both are fine)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete failed: HTTP %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// CreateStreamingUpload initiates a resumable upload without knowing the size upfront
func (c *TusClient) CreateStreamingUpload(ctx context.Context, objectPath string, upsert bool) (string, error) {
	url := c.baseURL() + "/upload/resumable"

	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("Authorization", "Bearer "+c.serviceKey)
	req.Header.Set("Tus-Resumable", "1.0.0")
	req.Header.Set("Upload-Defer-Length", "1") // Size unknown upfront
	if upsert {
		req.Header.Set("x-upsert", "true")
	}

	metadata := fmt.Sprintf("bucketName %s,objectName %s,contentType %s",
		base64.StdEncoding.EncodeToString([]byte(c.bucket)),
		base64.StdEncoding.EncodeToString([]byte(objectPath)),
		base64.StdEncoding.EncodeToString([]byte("application/octet-stream")),
	)
	req.Header.Set("Upload-Metadata", metadata)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("create streaming upload failed: HTTP %d: %s", resp.StatusCode, string(body))
	}

	location := resp.Header.Get("Location")
	if location == "" {
		return "", fmt.Errorf("no Location header in response")
	}

	return location, nil
}

// UploadStreamingChunk uploads a chunk, optionally setting the final length
func (c *TusClient) UploadStreamingChunk(ctx context.Context, uploadURL string, offset int64, data []byte, finalLength int64) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, "PATCH", uploadURL, bytes.NewReader(data))
	if err != nil {
		return 0, err
	}

	req.Header.Set("Authorization", "Bearer "+c.serviceKey)
	req.Header.Set("Tus-Resumable", "1.0.0")
	req.Header.Set("Upload-Offset", strconv.FormatInt(offset, 10))
	req.Header.Set("Content-Type", "application/offset+octet-stream")
	req.Header.Set("Content-Length", strconv.Itoa(len(data)))

	// Set final length when known (last chunk)
	if finalLength > 0 {
		req.Header.Set("Upload-Length", strconv.FormatInt(finalLength, 10))
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("upload chunk failed: HTTP %d: %s", resp.StatusCode, string(body))
	}

	newOffsetStr := resp.Header.Get("Upload-Offset")
	newOffset, err := strconv.ParseInt(newOffsetStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid Upload-Offset header: %s", newOffsetStr)
	}

	return newOffset, nil
}

// Upload performs the full file upload with chunking and progress
func (c *TusClient) Upload(ctx context.Context, objectPath string, file *os.File, fileSize int64, resume bool, onProgress func(uploaded, total int64)) error {
	var uploadURL string
	var currentOffset int64 = 0

	// State file for resume
	stateFile := filepath.Join(os.TempDir(), fmt.Sprintf("supabase-upload-%s.state", sanitizeFilename(objectPath)))

	// Try to resume existing upload
	if resume {
		if savedURL, err := os.ReadFile(stateFile); err == nil && len(savedURL) > 0 {
			offset, length, err := c.GetUploadOffset(ctx, string(savedURL))
			if err == nil && length == fileSize {
				uploadURL = string(savedURL)
				currentOffset = offset
				log.Printf("Resuming upload from offset %d/%d (%.1f%%)", offset, fileSize, float64(offset)/float64(fileSize)*100)
			}
		}
	}

	// Create new upload if not resuming
	if uploadURL == "" {
		url, err := c.CreateUpload(ctx, objectPath, fileSize, true)
		if err != nil {
			return fmt.Errorf("create upload failed: %w", err)
		}
		uploadURL = url
		currentOffset = 0
		log.Printf("Created new upload: %s", uploadURL)

		// Save state for resume
		if resume {
			os.WriteFile(stateFile, []byte(uploadURL), 0644)
		}
	}

	// Upload in chunks
	chunk := make([]byte, c.chunkSize)
	startTime := time.Now()

	for currentOffset < fileSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Seek to current position
		if _, err := file.Seek(currentOffset, io.SeekStart); err != nil {
			return fmt.Errorf("seek failed: %w", err)
		}

		// Read chunk
		n, err := file.Read(chunk)
		if err != nil && err != io.EOF {
			return fmt.Errorf("read chunk failed: %w", err)
		}
		if n == 0 {
			break
		}

		// Upload chunk with retries
		var newOffset int64
		var lastErr error
		for attempt := 0; attempt <= 3; attempt++ {
			if attempt > 0 {
				backoff := time.Duration(1<<(attempt-1)) * time.Second
				log.Printf("Retry %d after %v...", attempt, backoff)
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			newOffset, lastErr = c.UploadChunk(ctx, uploadURL, currentOffset, chunk[:n])
			if lastErr == nil {
				break
			}
		}
		if lastErr != nil {
			return fmt.Errorf("upload chunk failed at offset %d: %w", currentOffset, lastErr)
		}

		currentOffset = newOffset

		// Progress callback
		if onProgress != nil {
			onProgress(currentOffset, fileSize)
		}
	}

	// Cleanup state file on success
	if resume {
		os.Remove(stateFile)
	}

	elapsed := time.Since(startTime)
	speedMBps := float64(fileSize) / elapsed.Seconds() / (1024 * 1024)
	log.Printf("Upload complete: %.2f MB in %v (%.2f MB/s)", float64(fileSize)/(1024*1024), elapsed.Round(time.Second), speedMBps)

	return nil
}

func sanitizeFilename(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "/", "_"), "\\", "_")
}

// ============================================================================
// Qdrant HTTP Client
// ============================================================================

type QdrantClient interface {
	GetPointsCount(ctx context.Context, collection string) (uint64, error)
	Scroll(ctx context.Context, collection string, limit int, offset interface{}, withPayloadFields []string, withVectors bool) ([]ScrollPoint, interface{}, error)
	GetPoints(ctx context.Context, collection string, ids []PointID) ([]RetrievedPoint, error)
	Close() error
}

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
			Timeout: 300 * time.Second,
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

	var resp struct {
		Result []struct {
			ID      interface{}            `json:"id"`
			Payload map[string]interface{} `json:"payload"`
			Vector  json.RawMessage        `json:"vector"`
		} `json:"result"`
	}
	decoder := json.NewDecoder(bytes.NewReader(respBody))
	decoder.UseNumber()
	if err := decoder.Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
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

		var vector []float32
		if len(p.Vector) > 0 {
			if err := json.Unmarshal(p.Vector, &vector); err != nil {
				var namedVectors map[string][]float32
				if err2 := json.Unmarshal(p.Vector, &namedVectors); err2 == nil {
					if v, ok := namedVectors["default"]; ok {
						vector = v
					} else if v, ok := namedVectors[""]; ok {
						vector = v
					} else {
						for _, v := range namedVectors {
							vector = v
							break
						}
					}
				}
			}
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
		return PointID{ID: uint64(v), Key: fmt.Sprintf("%d", uint64(v))}
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return PointID{ID: uint64(i), Key: v.String()}
		}
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

// ============================================================================
// Worker Functions
// ============================================================================

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

	log.Printf("ID Producer finished: sent %d IDs to workers", totalSent)
	return nil
}

func retrieveWorkerParquet(
	ctx context.Context,
	client QdrantClient,
	collection string,
	in <-chan []PointID,
	out chan<- []pointDataParquet,
	batchSize int,
	counter *int64,
) error {
	for ids := range in {
		for i := 0; i < len(ids); i += batchSize {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			end := i + batchSize
			if end > len(ids) {
				end = len(ids)
			}
			batch := ids[i:end]

			points, err := client.GetPoints(ctx, collection, batch)
			if err != nil {
				return fmt.Errorf("GetPoints failed: %w", err)
			}

			results := make([]pointDataParquet, 0, len(points))
			for _, point := range points {
				results = append(results, pointDataParquet{
					Key:      point.Key,
					Metadata: point.Metadata,
					Vector:   point.Vector,
				})
				atomic.AddInt64(counter, 1)
			}

			if len(results) > 0 {
				select {
				case out <- results:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}

	return nil
}

// ============================================================================
// Direct Parquet Export
// ============================================================================

func runDirectParquet(
	ctx context.Context,
	client QdrantClient,
	config Config,
	outputPath string,
	totalPoints uint64,
) (int64, error) {
	scrollBatch := config.BatchSize
	retrieveBatch := config.RetrieveBatch

	var retrieved, written int64
	progressDone := make(chan struct{})

	// Progress tracking goroutine
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
				fmt.Printf("\rExporting: %d retrieved | %d/%d written (%.1f%%) | %.0f pts/sec | Heap: %.0fMB   ",
					r, w, totalPoints, pct, float64(w)/elapsed, heapMB)
			case <-progressDone:
				fmt.Println()
				return
			}
		}
	}()

	// Create parquet file
	file, err := os.Create(outputPath)
	if err != nil {
		close(progressDone)
		return 0, fmt.Errorf("failed to create file: %w", err)
	}

	writer := parquet.NewGenericWriter[ParquetRow](file,
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

	// STAGE 3: Single Writer
	g.Go(func() error {
		rowBuffer := make([]ParquetRow, 0, config.RowGroupSize)

		flushRowGroup := func() error {
			if len(rowBuffer) == 0 {
				return nil
			}
			if _, err := writer.Write(rowBuffer); err != nil {
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
				rowBuffer = append(rowBuffer, ParquetRow{
					Key:      p.Key,
					Metadata: p.Metadata,
					Vector:   p.Vector,
				})

				if len(rowBuffer) >= config.RowGroupSize {
					if err := flushRowGroup(); err != nil {
						return err
					}
				}
			}
		}

		return flushRowGroup()
	})

	// Wait for pipeline
	if err := g.Wait(); err != nil {
		writer.Close()
		file.Close()
		close(progressDone)
		return 0, err
	}

	finalWritten := atomic.LoadInt64(&written)

	if err := writer.Close(); err != nil {
		file.Close()
		close(progressDone)
		return 0, err
	}
	file.Close()
	close(progressDone)

	return finalWritten, nil
}

// ============================================================================
// Streaming Parquet Upload (no disk storage)
// ============================================================================

func runStreamingParquetUpload(
	ctx context.Context,
	client QdrantClient,
	config Config,
	tusClient *TusClient,
	objectPath string,
	totalPoints uint64,
) (int64, error) {
	scrollBatch := config.BatchSize
	retrieveBatch := config.RetrieveBatch

	var retrieved, uploaded int64
	progressDone := make(chan struct{})

	// Progress tracking
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		start := time.Now()
		var memStats runtime.MemStats

		for {
			select {
			case <-ticker.C:
				r := atomic.LoadInt64(&retrieved)
				u := atomic.LoadInt64(&uploaded)
				elapsed := time.Since(start).Seconds()
				pct := float64(r) / float64(totalPoints) * 100
				runtime.ReadMemStats(&memStats)
				heapMB := float64(memStats.HeapAlloc) / (1024 * 1024)
				fmt.Printf("\rStreaming: %d/%d (%.1f%%) | Uploaded: %.1f MB | %.0f pts/sec | Heap: %.0fMB   ",
					r, totalPoints, pct, float64(u)/(1024*1024), float64(r)/elapsed, heapMB)
			case <-progressDone:
				fmt.Println()
				return
			}
		}
	}()

	// Delete existing file first (tus streaming doesn't support upsert well)
	log.Printf("Deleting existing file if present...")
	if err := tusClient.DeleteObject(ctx, objectPath); err != nil {
		log.Printf("Warning: delete failed (may not exist): %v", err)
	}

	// Create streaming upload (size unknown)
	uploadURL, err := tusClient.CreateStreamingUpload(ctx, objectPath, true)
	if err != nil {
		close(progressDone)
		return 0, fmt.Errorf("create streaming upload failed: %w", err)
	}
	log.Printf("Created streaming upload: %s", uploadURL)

	// Create pipe: parquet writer -> tus uploader
	pipeReader, pipeWriter := io.Pipe()

	// Upload goroutine - reads from pipe and uploads chunks
	var uploadErr error
	var totalBytes int64
	uploadDone := make(chan struct{})

	go func() {
		defer close(uploadDone)

		buffer := make([]byte, 6*1024*1024) // 6MB chunks
		var offset int64

		for {
			n, err := io.ReadFull(pipeReader, buffer)
			if n > 0 {
				// Upload chunk (not final yet)
				newOffset, uploadErr := tusClient.UploadStreamingChunk(ctx, uploadURL, offset, buffer[:n], 0)
				if uploadErr != nil {
					log.Printf("Upload chunk error: %v", uploadErr)
					pipeReader.CloseWithError(uploadErr)
					return
				}
				offset = newOffset
				atomic.StoreInt64(&uploaded, offset)
			}

			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// End of data - we're done
				totalBytes = offset
				break
			}
			if err != nil {
				uploadErr = err
				log.Printf("Read error: %v", err)
				return
			}
		}
	}()

	// Configure parquet writer to write to pipe
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
	var written int64
	g.Go(func() error {
		rowBuffer := make([]ParquetRow, 0, config.RowGroupSize)

		flushRowGroup := func() error {
			if len(rowBuffer) == 0 {
				return nil
			}
			if _, err := writer.Write(rowBuffer); err != nil {
				return err
			}
			if err := writer.Flush(); err != nil {
				return err
			}
			written += int64(len(rowBuffer))
			rowBuffer = rowBuffer[:0]
			runtime.GC()
			return nil
		}

		for batch := range dataChan {
			for _, p := range batch {
				rowBuffer = append(rowBuffer, ParquetRow{
					Key:      p.Key,
					Metadata: p.Metadata,
					Vector:   p.Vector,
				})

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

	// Close parquet writer (writes footer)
	if err := writer.Close(); err != nil && pipelineErr == nil {
		pipelineErr = err
	}

	// Close pipe to signal EOF to uploader
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

	log.Printf("Streaming complete: %d points, %.2f MB uploaded", written, float64(totalBytes)/(1024*1024))
	return written, nil
}

// ============================================================================
// Main Export Flow
// ============================================================================

func runSupabaseExport(ctx context.Context, config Config) error {
	startTime := time.Now()

	// Connect to Qdrant
	log.Printf("Connecting to Qdrant at %s...", config.QdrantURL)
	client := NewHTTPClient(config.QdrantURL, config.QdrantAPIKey)
	defer client.Close()

	// Get collection info
	totalPoints, err := client.GetPointsCount(ctx, config.CollectionName)
	if err != nil {
		return fmt.Errorf("failed to get collection: %w", err)
	}

	log.Printf("Collection '%s': %d points", config.CollectionName, totalPoints)

	if totalPoints == 0 {
		log.Println("No points to export")
		return nil
	}

	if config.DryRun {
		log.Printf("[DRY RUN] Would export %d points to Supabase Storage", totalPoints)
		return nil
	}

	// Setup Supabase client
	tusClient := NewTusClient(config.SupabaseProjectRef, config.SupabaseServiceKey, config.SupabaseBucket)
	objectPath := fmt.Sprintf("%s/latest.parquet", config.CollectionName)

	// Stream directly to Supabase (no local disk storage)
	log.Println("\n=== Streaming to Supabase Storage ===")
	log.Printf("Data flow: Qdrant -> Parquet -> Supabase (no disk)")

	exported, err := runStreamingParquetUpload(ctx, client, config, tusClient, objectPath, totalPoints)
	if err != nil {
		return fmt.Errorf("streaming upload failed: %w", err)
	}

	elapsed := time.Since(startTime)
	log.Printf("\n=== Complete ===")
	log.Printf("Total time: %v", elapsed.Round(time.Second))
	log.Printf("Points exported: %d", exported)
	log.Printf("Uploaded to: %s/%s", config.SupabaseBucket, objectPath)

	return nil
}

// ============================================================================
// Utilities
// ============================================================================

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// extractSupabaseProjectRef extracts the project ref from a Supabase URL
// e.g., "https://abc123.supabase.co" -> "abc123"
func extractSupabaseProjectRef(url string) string {
	if url == "" {
		return ""
	}
	// Remove protocol
	url = strings.TrimPrefix(url, "https://")
	url = strings.TrimPrefix(url, "http://")
	// Extract subdomain (project ref)
	if idx := strings.Index(url, ".supabase.co"); idx > 0 {
		return url[:idx]
	}
	// Also handle supabase.in for some regions
	if idx := strings.Index(url, ".supabase.in"); idx > 0 {
		return url[:idx]
	}
	return ""
}

func normalizeQdrantHTTPURL(url string, portOverride string) string {
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

	// Remove any existing port from host
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

// ============================================================================
// Main
// ============================================================================

func main() {
	var config Config

	flag.StringVar(&config.CollectionName, "collection", "", "Qdrant collection name")
	flag.IntVar(&config.BatchSize, "batch-size", 10000, "Points per scroll batch")
	flag.IntVar(&config.RetrieveBatch, "retrieve-batch", 0, "Batch size for retrieving points (default: batch-size/2)")
	flag.IntVar(&config.ParallelWorkers, "parallel", runtime.NumCPU(), "Parallel workers")
	flag.IntVar(&config.VectorDim, "vector-dim", 1024, "Vector dimension")
	flag.IntVar(&config.RowGroupSize, "row-group-size", 10000, "Rows per parquet row group")
	flag.BoolVar(&config.Resume, "resume", false, "Resume interrupted upload")
	flag.BoolVar(&config.Debug, "debug", false, "Keep temp files")
	flag.BoolVar(&config.DryRun, "dry-run", false, "Show what would happen")
	flag.Parse()

	// Load .env files
	for _, p := range []string{".env", "../.env", "../../.env"} {
		if err := godotenv.Load(p); err == nil {
			log.Printf("Loaded environment from %s", p)
			break
		}
	}

	// Set defaults
	if config.RetrieveBatch == 0 {
		config.RetrieveBatch = config.BatchSize / 2
		if config.RetrieveBatch < 100 {
			config.RetrieveBatch = 100
		}
	}

	// Load configuration from environment
	qdrantPort := os.Getenv("QDRANT_PORT")
	config.QdrantURL = normalizeQdrantHTTPURL(getEnv("QDRANT_URL", "http://localhost:6333"), qdrantPort)
	config.QdrantAPIKey = os.Getenv("QDRANT_API_KEY")
	if config.CollectionName == "" {
		config.CollectionName = getEnv("QDRANT_COLLECTION_NAME", "embeddings")
	}

	// Supabase config - extract project ref from SUPABASE_URL
	supabaseURL := os.Getenv("SUPABASE_URL")
	config.SupabaseProjectRef = extractSupabaseProjectRef(supabaseURL)
	// Use service role key (bypasses RLS for storage uploads)
	config.SupabaseServiceKey = os.Getenv("SUPABASE_SERVICE_ROLE_KEY")
	config.SupabaseBucket = getEnv("SUPABASE_STORAGE_BUCKET", "backups")

	// Validate
	if config.SupabaseProjectRef == "" {
		log.Fatal("Could not extract project ref from SUPABASE_URL. Expected format: https://<project-ref>.supabase.co")
	}
	if config.SupabaseServiceKey == "" {
		log.Fatal("SUPABASE_SERVICE_ROLE_KEY is required for storage uploads")
	}

	// Print configuration
	fmt.Println("\n=== Supabase Storage Uploader ===\n")
	fmt.Printf("  Qdrant URL:        %s\n", config.QdrantURL)
	fmt.Printf("  Collection:        %s\n", config.CollectionName)
	fmt.Printf("  Supabase Project:  %s\n", config.SupabaseProjectRef)
	fmt.Printf("  Storage Bucket:    %s\n", config.SupabaseBucket)
	fmt.Printf("  Batch Size:        %d\n", config.BatchSize)
	fmt.Printf("  Row Group Size:    %d\n", config.RowGroupSize)
	fmt.Printf("  Parallel Workers:  %d\n", config.ParallelWorkers)
	fmt.Printf("  Resume:            %v\n", config.Resume)
	fmt.Println()

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("\nReceived signal %v, shutting down...", sig)
		cancel()
	}()

	// Run export
	if err := runSupabaseExport(ctx, config); err != nil {
		log.Fatalf("Export failed: %v", err)
	}
}
