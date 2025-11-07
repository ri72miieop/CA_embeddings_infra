# CA_Embed - Production-Ready Embeddings Service

A high-performance, scalable embeddings serving solution built with TypeScript and Bun. Supports multiple vector database backends (CoreNN and Qdrant) with comprehensive observability and production-ready features.

## 🚀 Features

- **Multi-Backend Support**: Choose between CoreNN (local, high-performance) or Qdrant (cloud-native, advanced features)
- **High Performance**: Handle billion-scale vector searches in <15ms
- **Advanced Search**: Metadata filtering and payload-based search (Qdrant)
- **Production Ready**: Rate limiting, health checks, graceful shutdown
- **Full Observability**: Metrics, tracing, structured logging, dashboards
- **Type Safe**: Full TypeScript implementation with Zod validation
- **Containerized**: Docker deployment with multi-stage builds
- **Flexible Architecture**: Plugin-based design for easy backend switching

## 📊 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     CA_Embed Service                       │
├─────────────────────────────────────────────────────────────┤
│  Fastify API Server + Middleware (CORS, Rate Limit, etc.)  │
├─────────────────────────────────────────────────────────────┤
│  Vector Store Factory (Plugin Architecture)                │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────────┐    ┌─────────────────────────┐   │
│  │ CoreNN Vector Store │ OR │ Qdrant Vector Store     │   │
│  │ (Local, Fast)       │    │ (Cloud, Feature-Rich)   │   │
│  └─────────────────────┘    └─────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                   Observability Stack                      │
├─────────────────────────────────────────────────────────────┤
│  Prometheus (Metrics) + Grafana (Dashboards)              │
│  Jaeger (Distributed Tracing) + Structured Logs          │
└─────────────────────────────────────────────────────────────┘
```

### Vector Store Backends

**CoreNN** - High-performance local vector database
- Billion-scale vector support
- Memory-efficient compression (96GB vs 3TB traditional)
- Local file-based storage (RocksDB + HNSW)
- Best for: Local deployments, maximum speed

**Qdrant** - Cloud-native vector database
- Collection-based storage with indexing
- Advanced metadata filtering and payload search
- Horizontal scalability
- Cloud and self-hosted options
- Best for: Distributed systems, advanced filtering needs

## 🛠 Quick Start

### Prerequisites

- [Bun](https://bun.sh/) (v1.2.5+)
- [Docker](https://docker.com/) (optional, for containerized deployment)

### Local Development

1. **Clone and Install**
   ```bash
   git clone <repository>
   cd ca_embed
   bun install
   ```

   **Note**: The project uses CoreNN directly from the GitHub repository since the npm package was incomplete. The installation includes the real CoreNN library with all its performance benefits.

2. **Environment Setup**
   ```bash
   # Create .env file with your configuration
   # See MIGRATION_GUIDE.md for detailed configuration options
   
   # Minimum required:
   VECTOR_STORE=corenn  # or 'qdrant'
   VECTOR_DIMENSION=1024
   
   # For CoreNN:
   CORENN_DB_PATH=./data/embeddings.db
   
   # For Qdrant:
   # QDRANT_URL=http://localhost:6333
   # QDRANT_COLLECTION_NAME=embeddings
   ```

3. **Start Development Server**
   ```bash
   bun run dev
   ```

4. **Run Tests**
   ```bash
   bun test
   ```

### Docker Deployment

1. **Full Stack with Observability**
   ```bash
   cd docker
   docker-compose up -d
   ```

2. **Access Services**
   - API: http://localhost:3000
   - Metrics: http://localhost:3000/metrics
   - Grafana: http://localhost:3001 (admin/admin)
   - Jaeger: http://localhost:16686
   - Prometheus: http://localhost:9091

## 📡 API Reference

### Insert Embeddings
```bash
POST /embeddings
Content-Type: application/json

{
  "embeddings": [
    {
      "key": "doc1",
      "vector": [0.1, 0.2, 0.3, ...],
      "metadata": {"category": "text"}
    }
  ]
}
```

### Search Similar Vectors
```bash
POST /embeddings/search
Content-Type: application/json

{
  "vector": [0.1, 0.2, 0.3, ...],
  "k": 10,
  "threshold": 0.8,
  "filter": {              # Optional: Qdrant only
    "category": "technology",
    "year": 2024
  }
}
```

**Note**: Metadata filtering (`filter`) is only supported with Qdrant backend.

### Delete Embeddings
```bash
DELETE /embeddings
Content-Type: application/json

{
  "keys": ["doc1", "doc2"]
}
```

### Health & Stats
```bash
GET /health              # Health check
GET /health/readiness    # Kubernetes readiness probe
GET /health/liveness     # Kubernetes liveness probe
GET /embeddings/stats    # Database statistics
GET /metrics             # Prometheus metrics
```

## ⚙ Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| **Server** | | |
| `PORT` | `3000` | Server port |
| `HOST` | `0.0.0.0` | Server host |
| `NODE_ENV` | `development` | Environment mode |
| **Vector Store** | | |
| `VECTOR_STORE` | `corenn` | Backend: `corenn` or `qdrant` |
| `VECTOR_DIMENSION` | `1024` | Vector dimensions |
| **CoreNN** (when `VECTOR_STORE=corenn`) | | |
| `CORENN_DB_PATH` | `./data/embeddings.db` | Database file path |
| `INDEX_TYPE` | `hnsw` | Index type |
| **Qdrant** (when `VECTOR_STORE=qdrant`) | | |
| `QDRANT_URL` | `http://localhost:6333` | Qdrant server URL |
| `QDRANT_API_KEY` | - | API key (for Qdrant Cloud) |
| `QDRANT_COLLECTION_NAME` | `embeddings` | Collection name |
| `QDRANT_TIMEOUT` | `30000` | Request timeout (ms) |
| **Observability** | | |
| `ENABLE_METRICS` | `true` | Enable Prometheus metrics |
| `ENABLE_TRACING` | `true` | Enable OpenTelemetry tracing |
| `LOG_LEVEL` | `info` | Logging level |
| **Rate Limiting** | | |
| `RATE_LIMIT_MAX` | `100` | Requests per window |
| `RATE_LIMIT_WINDOW` | `60000` | Rate limit window (ms) |

See [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) for detailed configuration and migration instructions.

## 🔧 Production Deployment

### Requirements

- **Memory**: Minimum 8GB, recommended 16GB+
- **CPU**: 4+ cores recommended
- **Storage**: SSD recommended for database
- **Network**: Low latency for optimal performance

### Deployment Steps

1. **Build Production Image**
   ```bash
   docker build -t ca_embed:latest .
   ```

2. **Deploy with Docker Compose**
   ```bash
   docker-compose -f docker/docker-compose.yml up -d
   ```

3. **Configure Monitoring**
   - Set up Prometheus scraping
   - Configure Grafana dashboards
   - Set up alerting rules

### Backup & Recovery

```bash
# Create backup
./scripts/backup.sh

# List available backups
ls /backups/

# Restore from backup
./scripts/restore.sh embeddings_backup_20231201_120000.db.gz
```

## 📈 Monitoring & Observability

### Key Metrics

- **Performance**: Request rate, latency (P95, P99), error rate
- **Business**: Vector count, search accuracy, database size
- **System**: CPU, memory, disk I/O, active connections

### Available Dashboards

1. **Service Overview**: Request metrics, error rates, latency
2. **Database Performance**: Vector operations, search times
3. **System Resources**: CPU, memory, disk usage
4. **Alerts**: Critical issues, SLA violations

### Log Structure

```json
{
  "level": "info",
  "timestamp": "2023-12-01T12:00:00.000Z",
  "correlationId": "abc-123",
  "operation": "search",
  "duration": 12.5,
  "resultCount": 10,
  "msg": "Search completed successfully"
}
```

## 🧪 Testing

```bash
# Unit tests
bun test

# Integration tests
bun run test:integration

# Load testing (requires k6)
k6 run tests/load/search.js
```

## 🚦 Health Checks

The service provides multiple health check endpoints:

- `/health` - Overall service health
- `/health/readiness` - Ready to accept traffic
- `/health/liveness` - Service is alive

## ⚠️ Current Limitations

### CoreNN Backend
- **Delete Operations**: CoreNN v0.3.1 doesn't have a remove/delete method yet. Delete requests are logged but not executed.
- **Metadata Filtering**: Metadata is stored but cannot be used for filtering searches.

### Qdrant Backend
- **Local Storage**: CoreNN is more memory-efficient for local deployments.
- **Performance**: Slightly higher latency than CoreNN for single-node deployments.

## 🔄 Migration Between Backends

See [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) for:
- Switching from CoreNN to Qdrant
- Switching from Qdrant to CoreNN
- Data migration strategies
- Feature comparison

## 🔒 Security Features

- Input validation with Zod schemas
- Rate limiting per IP
- CORS protection
- Helmet security headers
- Request correlation IDs
- Structured audit logging

## 📊 Performance Characteristics

- **Latency**: <15ms P95 for search operations
- **Throughput**: 1000+ requests/second
- **Scale**: Up to 1 billion vectors
- **Memory**: 96GB for billion vectors (vs 3TB traditional)
- **Accuracy**: High recall with HNSW indexing

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request


## 🆘 Support

- **Issues**: Create GitHub issues for bugs
- **Discussions**: Use GitHub discussions for questions
- **Documentation**: Check `/docs` for detailed guides
