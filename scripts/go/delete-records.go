// +build ignore

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	pb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func main() {
	var collectionName string
	var keepCount int
	var dryRun bool
	var batchSize int

	flag.StringVar(&collectionName, "collection", "", "Qdrant collection name")
	flag.IntVar(&keepCount, "keep", 25000, "Number of records to keep")
	flag.BoolVar(&dryRun, "dry-run", false, "Show what would be deleted without actually deleting")
	flag.IntVar(&batchSize, "batch-size", 10000, "Batch size for deletion")
	flag.Parse()

	// Load .env
	for _, p := range []string{".env", "../.env", "../../.env"} {
		if err := godotenv.Load(p); err == nil {
			log.Printf("Loaded environment from %s", p)
			break
		}
	}

	// Get config
	qdrantURL := normalizeQdrantURL(getEnv("QDRANT_URL", "localhost:6334"))
	qdrantAPIKey := os.Getenv("QDRANT_API_KEY")
	if collectionName == "" {
		collectionName = getEnv("QDRANT_COLLECTION_NAME", "embeddings")
	}

	// SAFETY CHECK: Only allow deletion on localhost
	if !isLocalhost(qdrantURL) {
		log.Fatalf("🛑 SAFETY: This script only works on localhost!\n   Current URL: %s\n   This prevents accidental deletion of production data.", qdrantURL)
	}

	fmt.Println("\n=== Qdrant Record Deletion Tool ===\n")
	fmt.Printf("  Collection:  %s\n", collectionName)
	fmt.Printf("  Qdrant:      %s\n", qdrantURL)
	fmt.Printf("  Keep:        %d records\n", keepCount)
	fmt.Printf("  Batch Size:  %d\n", batchSize)
	fmt.Printf("  Dry Run:     %v\n", dryRun)
	fmt.Println()

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

	// Connect to Qdrant
	log.Printf("Connecting to Qdrant at %s...", qdrantURL)
	conn, err := grpc.NewClient(qdrantURL,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100*1024*1024)),
	)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	if qdrantAPIKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "api-key", qdrantAPIKey)
	}

	pointsClient := pb.NewPointsClient(conn)
	collectionsClient := pb.NewCollectionsClient(conn)

	// Get collection info
	info, err := collectionsClient.Get(ctx, &pb.GetCollectionInfoRequest{CollectionName: collectionName})
	if err != nil {
		log.Fatalf("Failed to get collection: %v", err)
	}

	totalPoints := info.GetResult().GetPointsCount()
	log.Printf("Collection '%s': %d points", collectionName, totalPoints)

	if totalPoints <= uint64(keepCount) {
		log.Printf("Collection has %d points, which is <= %d. Nothing to delete.", totalPoints, keepCount)
		return
	}

	toDelete := int(totalPoints) - keepCount
	log.Printf("Will delete %d points (keeping %d)", toDelete, keepCount)

	if dryRun {
		log.Println("[DRY RUN] No actual deletion performed.")
		return
	}

	// Confirmation
	fmt.Printf("\n⚠️  WARNING: This will permanently delete %d points!\n", toDelete)
	fmt.Print("Type 'yes' to confirm: ")
	var confirm string
	fmt.Scanln(&confirm)
	if strings.ToLower(confirm) != "yes" {
		log.Println("Cancelled.")
		return
	}

	// Collect IDs to delete (skip the first 'keepCount' records)
	log.Printf("Collecting IDs to delete (skipping first %d)...", keepCount)

	var idsToDelete []*pb.PointId
	var offset *pb.PointId
	skipped := 0
	collected := 0
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			log.Println("Cancelled")
			return
		default:
		}

		resp, err := pointsClient.Scroll(ctx, &pb.ScrollPoints{
			CollectionName: collectionName,
			Limit:          ptr(uint32(batchSize)),
			Offset:         offset,
			WithPayload:    &pb.WithPayloadSelector{SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: false}},
			WithVectors:    &pb.WithVectorsSelector{SelectorOptions: &pb.WithVectorsSelector_Enable{Enable: false}},
		})
		if err != nil {
			log.Fatalf("Scroll failed: %v", err)
		}

		points := resp.GetResult()
		if len(points) == 0 {
			break
		}

		for _, p := range points {
			if skipped < keepCount {
				skipped++
				continue
			}
			idsToDelete = append(idsToDelete, p.GetId())
			collected++
		}

		fmt.Printf("\rSkipped: %d | Collected for deletion: %d", skipped, collected)

		if len(points) < batchSize {
			break
		}
		offset = points[len(points)-1].GetId()
	}
	fmt.Println()

	if len(idsToDelete) == 0 {
		log.Println("No points to delete.")
		return
	}

	log.Printf("Collected %d IDs to delete in %v", len(idsToDelete), time.Since(startTime).Round(time.Millisecond))

	// Delete in batches
	log.Printf("Deleting %d points in batches of %d...", len(idsToDelete), batchSize)
	deleted := 0
	deleteStart := time.Now()

	for i := 0; i < len(idsToDelete); i += batchSize {
		select {
		case <-ctx.Done():
			log.Printf("Cancelled after deleting %d points", deleted)
			return
		default:
		}

		end := min(i+batchSize, len(idsToDelete))
		batch := idsToDelete[i:end]

		_, err := pointsClient.Delete(ctx, &pb.DeletePoints{
			CollectionName: collectionName,
			Points: &pb.PointsSelector{
				PointsSelectorOneOf: &pb.PointsSelector_Points{
					Points: &pb.PointsIdsList{Ids: batch},
				},
			},
		})
		if err != nil {
			log.Fatalf("Delete failed at batch %d: %v", i/batchSize, err)
		}

		deleted += len(batch)
		elapsed := time.Since(deleteStart).Seconds()
		rate := float64(deleted) / elapsed
		fmt.Printf("\rDeleted: %d/%d (%.1f%%) | %.0f pts/sec", deleted, len(idsToDelete), float64(deleted)/float64(len(idsToDelete))*100, rate)
	}
	fmt.Println()

	// Verify
	info, err = collectionsClient.Get(ctx, &pb.GetCollectionInfoRequest{CollectionName: collectionName})
	if err != nil {
		log.Printf("Warning: couldn't verify final count: %v", err)
	} else {
		remaining := info.GetResult().GetPointsCount()
		log.Printf("\n=== Complete ===")
		log.Printf("Deleted: %d points", deleted)
		log.Printf("Remaining: %d points", remaining)
		log.Printf("Duration: %v", time.Since(startTime).Round(time.Second))
	}
}

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

func isLocalhost(url string) bool {
	// Extract host part (before the port)
	host := url
	if idx := strings.LastIndex(url, ":"); idx != -1 {
		host = url[:idx]
	}
	host = strings.ToLower(host)

	// Check for localhost variants
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func ptr[T any](v T) *T { return &v }
