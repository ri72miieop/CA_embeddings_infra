# Search API Test Requests

A collection of test request bodies for the `/embeddings/search` endpoint. Use these with Postman, curl, or any HTTP client.

**Endpoint:** `POST http://localhost:3000/embeddings/search`  
**Content-Type:** `application/json`

---

## Basic Searches (No Filters)

### Test 1: Simple Text Search

```json
{
  "searchTerm": "machine learning"
}
```

### Test 2: Search with Custom k and Threshold

```json
{
  "searchTerm": "artificial intelligence",
  "k": 20,
  "threshold": 0.7
}
```

### Test 3: High Precision Search

```json
{
  "searchTerm": "neural networks deep learning",
  "k": 5,
  "threshold": 0.9
}
```

---

## Legacy Format Tests

### Test 4: Substring Matching (Text Field)

```json
{
  "searchTerm": "LLM",
  "filter": {
    "text": "technology"
  }
}
```

### Test 5: Exact Boolean Match

```json
{
  "searchTerm": "content",
  "filter": {
    "is_truncated": false
  }
}
```

### Test 6: Exact String Match (Provider)

```json
{
  "searchTerm": "embeddings",
  "filter": {
    "provider": "deepinfra"
  }
}
```

### Test 7: Range Query - Greater Than

```json
{
  "searchTerm": "long posts",
  "filter": {
    "tokens": ">50"
  }
}
```

### Test 8: Range Query - Less Than

```json
{
  "searchTerm": "short messages",
  "filter": {
    "tokens": "<30"
  }
}
```

### Test 9: Range Query - Greater Than or Equal

```json
{
  "searchTerm": "medium content",
  "filter": {
    "tokens": ">=20"
  }
}
```

### Test 10: Range Query - Between (Exclusive)

```json
{
  "searchTerm": "medium posts",
  "filter": {
    "tokens": ">10 & <100"
  }
}
```

### Test 11: Range Query - Between (Inclusive)

```json
{
  "searchTerm": "specific range",
  "filter": {
    "tokens": ">=20 & <=80"
  }
}
```

### Test 12: Multiple Conditions (Legacy AND)

```json
{
  "searchTerm": "technical",
  "k": 25,
  "threshold": 0.6,
  "filter": {
    "tokens": ">50",
    "provider": "deepinfra",
    "is_truncated": false,
    "has_quotes": false
  }
}
```

### Test 13: Provider and Model Filter

```json
{
  "searchTerm": "content",
  "filter": {
    "provider": "deepinfra",
    "model": "Qwen/Qwen3-Embedding-4B",
    "source": "parquet_import"
  }
}
```

---

## New Format Tests - Must Clause (AND)

### Test 14: Simple Must Clause

```json
{
  "searchTerm": "optimization",
  "filter": {
    "must": [
      { "key": "provider", "match": { "value": "deepinfra" } }
    ]
  }
}
```

### Test 15: Multiple Must Conditions

```json
{
  "searchTerm": "data science",
  "filter": {
    "must": [
      { "key": "provider", "match": { "value": "deepinfra" } },
      { "key": "tokens", "range": { "gte": 50 } },
      { "key": "is_truncated", "match": { "value": false } }
    ]
  }
}
```

### Test 16: Text Substring Match (New Format)

```json
{
  "searchTerm": "controversial",
  "filter": {
    "must": [
      { "key": "text", "match": { "text": "ideas" } }
    ]
  }
}
```

### Test 17: Range Condition with Multiple Bounds

```json
{
  "searchTerm": "medium length",
  "filter": {
    "must": [
      { "key": "tokens", "range": { "gte": 30, "lte": 150 } }
    ]
  }
}
```

---

## New Format Tests - Should Clause (OR)

### Test 18: Simple Should Clause

```json
{
  "searchTerm": "embeddings",
  "filter": {
    "should": [
      { "key": "provider", "match": { "value": "deepinfra" } },
      { "key": "provider", "match": { "value": "openai" } }
    ]
  }
}
```

### Test 19: Multiple Providers (OR)

```json
{
  "searchTerm": "technical content",
  "k": 50,
  "filter": {
    "should": [
      { "key": "provider", "match": { "value": "deepinfra" } },
      { "key": "provider", "match": { "value": "openai" } },
      { "key": "provider", "match": { "value": "local" } }
    ]
  }
}
```

### Test 20: Text OR Conditions

```json
{
  "searchTerm": "development",
  "filter": {
    "should": [
      { "key": "text", "match": { "text": "python" } },
      { "key": "text", "match": { "text": "javascript" } },
      { "key": "text", "match": { "text": "typescript" } }
    ]
  }
}
```

---

## New Format Tests - Must Not Clause (NOT)

### Test 21: Simple Exclusion

```json
{
  "searchTerm": "complete guide",
  "filter": {
    "must_not": [
      { "key": "is_truncated", "match": { "value": true } }
    ]
  }
}
```

### Test 22: Multiple Exclusions

```json
{
  "searchTerm": "clean content",
  "filter": {
    "must_not": [
      { "key": "is_truncated", "match": { "value": true } },
      { "key": "has_quotes", "match": { "value": true } },
      { "key": "has_context", "match": { "value": true } }
    ]
  }
}
```

### Test 23: Exclude by Range

```json
{
  "searchTerm": "substantial content",
  "filter": {
    "must_not": [
      { "key": "tokens", "range": { "lt": 20 } }
    ]
  }
}
```

---

## New Format Tests - Combined Clauses

### Test 24: Must + Should

```json
{
  "searchTerm": "analysis",
  "filter": {
    "must": [
      { "key": "source", "match": { "value": "parquet_import" } }
    ],
    "should": [
      { "key": "provider", "match": { "value": "deepinfra" } },
      { "key": "provider", "match": { "value": "openai" } }
    ]
  }
}
```

### Test 25: Must + Must Not

```json
{
  "searchTerm": "original content",
  "filter": {
    "must": [
      { "key": "tokens", "range": { "gte": 50 } },
      { "key": "provider", "match": { "value": "deepinfra" } }
    ],
    "must_not": [
      { "key": "is_truncated", "match": { "value": true } }
    ]
  }
}
```

### Test 26: All Three Clauses

```json
{
  "searchTerm": "comprehensive",
  "k": 30,
  "threshold": 0.6,
  "filter": {
    "must": [
      { "key": "source", "match": { "value": "parquet_import" } },
      { "key": "tokens", "range": { "gte": 50, "lte": 300 } }
    ],
    "should": [
      { "key": "text", "match": { "text": "optimization" } },
      { "key": "text", "match": { "text": "performance" } },
      { "key": "text", "match": { "text": "efficiency" } }
    ],
    "must_not": [
      { "key": "is_truncated", "match": { "value": true } },
      { "key": "has_quotes", "match": { "value": true } }
    ]
  }
}
```

---

## New Format Tests - Nested Clauses

### Test 27: Nested Must in Should

```json
{
  "searchTerm": "content",
  "filter": {
    "should": [
      {
        "must": [
          { "key": "provider", "match": { "value": "deepinfra" } },
          { "key": "tokens", "range": { "gt": 50 } }
        ]
      },
      {
        "must": [
          { "key": "provider", "match": { "value": "openai" } },
          { "key": "tokens", "range": { "gt": 100 } }
        ]
      }
    ]
  }
}
```

### Test 28: Complex Nested Logic

```json
{
  "searchTerm": "advanced query",
  "filter": {
    "must": [
      { "key": "source", "match": { "value": "parquet_import" } }
    ],
    "should": [
      {
        "must": [
          { "key": "provider", "match": { "value": "deepinfra" } },
          { "key": "model", "match": { "value": "Qwen/Qwen3-Embedding-4B" } }
        ]
      },
      {
        "must": [
          { "key": "provider", "match": { "value": "openai" } },
          { "key": "tokens", "range": { "gte": 100 } }
        ],
        "must_not": [
          { "key": "is_truncated", "match": { "value": true } }
        ]
      }
    ]
  }
}
```

### Test 29: Three-Level Nesting

```json
{
  "searchTerm": "deep nesting test",
  "filter": {
    "must": [
      { "key": "source", "match": { "value": "parquet_import" } }
    ],
    "should": [
      {
        "should": [
          {
            "must": [
              { "key": "provider", "match": { "value": "deepinfra" } },
              { "key": "tokens", "range": { "gt": 50 } }
            ]
          },
          {
            "must": [
              { "key": "provider", "match": { "value": "local" } }
            ]
          }
        ]
      },
      {
        "must": [
          { "key": "provider", "match": { "value": "openai" } }
        ]
      }
    ]
  }
}
```

---

## Datetime Filtering Tests

### Test 30: Datetime Range - ISO 8601 Format

```json
{
  "searchTerm": "recent content",
  "filter": {
    "must": [
      {
        "key": "created_at",
        "range": {
          "gte": "2024-01-01T00:00:00Z",
          "lt": "2024-12-31T23:59:59Z"
        }
      }
    ]
  }
}
```

### Test 31: Datetime Range - Unix Timestamp (Milliseconds)

```json
{
  "searchTerm": "timestamped content",
  "filter": {
    "must": [
      {
        "key": "timestamp",
        "range": {
          "gte": 1704067200000,
          "lt": 1735689599000
        }
      }
    ]
  }
}
```

### Test 32: Datetime - After Specific Date

```json
{
  "searchTerm": "new content",
  "filter": {
    "must": [
      {
        "key": "created_at",
        "range": {
          "gte": "2024-06-01T00:00:00Z"
        }
      }
    ]
  }
}
```

### Test 33: Datetime - Before Specific Date

```json
{
  "searchTerm": "old content",
  "filter": {
    "must": [
      {
        "key": "created_at",
        "range": {
          "lt": "2024-01-01T00:00:00Z"
        }
      }
    ]
  }
}
```

### Test 34: Mixed Datetime Formats

```json
{
  "searchTerm": "mixed datetime",
  "filter": {
    "must": [
      {
        "key": "created_at",
        "range": {
          "gte": "2024-01-01T00:00:00Z",
          "lt": 1735689599000
        }
      }
    ]
  }
}
```

---

## Edge Cases

### Test 35: Empty Search Term with Filter (Should Fail)

```json
{
  "filter": {
    "must": [
      { "key": "provider", "match": { "value": "deepinfra" } }
    ]
  }
}
```
*Expected: Error - searchTerm or vector required*

### Test 36: Very Low Threshold

```json
{
  "searchTerm": "anything",
  "k": 100,
  "threshold": 0.1,
  "filter": {
    "must": [
      { "key": "tokens", "range": { "gt": 10 } }
    ]
  }
}
```

### Test 37: Maximum k Value

```json
{
  "searchTerm": "bulk results",
  "k": 1000,
  "threshold": 0.5
}
```

### Test 38: Minimal Filter (Single Condition)

```json
{
  "searchTerm": "simple",
  "filter": {
    "must": [
      { "key": "tokens", "range": { "gt": 0 } }
    ]
  }
}
```

### Test 39: Filter with Only Should (No Must)

```json
{
  "searchTerm": "flexible matching",
  "filter": {
    "should": [
      { "key": "text", "match": { "text": "rare term 1" } },
      { "key": "text", "match": { "text": "rare term 2" } }
    ]
  }
}
```

### Test 40: Filter with Only Must Not

```json
{
  "searchTerm": "exclude only",
  "filter": {
    "must_not": [
      { "key": "provider", "match": { "value": "unknown_provider" } }
    ]
  }
}
```

---

## Quick Copy-Paste curl Commands

### Basic Search
```bash
curl -X POST http://localhost:3000/embeddings/search \
  -H "Content-Type: application/json" \
  -d '{"searchTerm": "machine learning", "k": 10}'
```

### Legacy Filter
```bash
curl -X POST http://localhost:3000/embeddings/search \
  -H "Content-Type: application/json" \
  -d '{"searchTerm": "content", "filter": {"tokens": ">50", "provider": "deepinfra"}}'
```

### New Format Filter
```bash
curl -X POST http://localhost:3000/embeddings/search \
  -H "Content-Type: application/json" \
  -d '{"searchTerm": "content", "filter": {"must": [{"key": "tokens", "range": {"gt": 50}}, {"key": "provider", "match": {"value": "deepinfra"}}]}}'
```

### OR Query (Should)
```bash
curl -X POST http://localhost:3000/embeddings/search \
  -H "Content-Type: application/json" \
  -d '{"searchTerm": "content", "filter": {"should": [{"key": "provider", "match": {"value": "deepinfra"}}, {"key": "provider", "match": {"value": "openai"}}]}}'
```

### NOT Query (Must Not)
```bash
curl -X POST http://localhost:3000/embeddings/search \
  -H "Content-Type: application/json" \
  -d '{"searchTerm": "content", "filter": {"must_not": [{"key": "is_truncated", "match": {"value": true}}]}}'
```

