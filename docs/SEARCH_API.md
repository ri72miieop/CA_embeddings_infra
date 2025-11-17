# Embeddings Search API Guide

Complete guide for using the `/embeddings/search` endpoint with advanced filtering capabilities.

## Table of Contents
- [Endpoint Overview](#endpoint-overview)
- [Request Structure](#request-structure)
- [Filter Types](#filter-types)
- [Examples by Use Case](#examples-by-use-case)
- [Response Format](#response-format)
- [Best Practices](#best-practices)

---

## Endpoint Overview

**URL:** `POST /embeddings/search`

**Description:** Performs semantic search over your embeddings using either:
- A search term (text) that gets automatically converted to an embedding
- A direct vector for similarity search

**Authentication:** Not required for search endpoint

---

## Request Structure

### Basic Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `searchTerm` | string | Yes* | - | Text to search for (alternative to `vector`) |
| `vector` | number[] | Yes* | - | Vector to search with (alternative to `searchTerm`) |
| `k` | number | No | 10 | Number of results to return (1-1000) |
| `threshold` | number | No | 0.65 | Similarity threshold (0.0-1.0) |
| `filter` | object | No | - | Metadata filters (see below) |

*Either `searchTerm` OR `vector` must be provided, but not both.

### Minimal Request Example

```json
{
  "searchTerm": "TPOT"
}
```

---

## Filter Types

The `filter` object supports multiple filter types that can be combined:

### 1. Substring Matching (Text Fields)

String values automatically enable full-text substring matching.

**How it works:**
- Matches if the stored text contains the search string
- Case-insensitive
- Works on any text field in metadata

**Example:**
```json
{
  "searchTerm": "you can just do stuff",
  "threshold": 0.5,
  "filter":{
    "text":"tpot"
  }
}
```

This will match any embedding where `metadata.text` contains "tpot" as a substring.

---

### 2. Exact Matching (Non-Text Fields)

Non-string values (numbers, booleans) use exact matching.

**Example:**
```json
{
  "searchTerm": "yes",
  "filter": {
    "has_quotes": true,
    "has_context":false
  }
}
```

---

### 3. Numeric Range Queries

Use string expressions to filter numeric fields by ranges.

#### Supported Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `>` | Greater than | `">10"` |
| `>=` | Greater than or equal | `">=10"` |
| `<` | Less than | `"<50"` |
| `<=` | Less than or equal | `"<=50"` |
| `>X & <Y` | Between X and Y | `">10 & <50"` |
| `>=X & <=Y` | Between X and Y (inclusive) | `">=10 & <=50"` |

#### Range Query Examples

**Greater than:**
```json
{
  "searchTerm": "long",
  "filter": {
    "tokens": ">30"
  }
}
```

**Less than:**
```json
{
  "searchTerm": "short messages",
  "filter": {
    "tokens": "<20"
  }
}
```

**Between (exclusive):**
```json
{
  "searchTerm": "medium posts",
  "filter": {
    "tokens": ">10 & <50"
  }
}
```

**Between (inclusive):**
```json
{
  "searchTerm": "specific range",
  "filter": {
    "tokens": ">=20 & <=100"
  }
}
```

---

### 4. Keyword Matching (Categorical Fields)

String fields like `provider`, `model`, `source` use exact matching.

**Example:**
```json
{
  "searchTerm": "embeddings",
  "filter": {
    "provider": "deepinfra",
    "model": "Qwen/Qwen3-Embedding-4B",
    "source": "parquet_import"
  }
}
```

---

## Examples by Use Case

### Use Case 1: Basic Text Search

Find embeddings containing specific text.

```json
{
  "searchTerm": "artificial intelligence",
  "k": 5
}
```

### Use Case 2: Text Search with Substring Filter

Find embeddings semantically similar to "machine learning" that contain "neural" in their text.

```json
{
  "searchTerm": "machine learning",
  "k": 10,
  "filter": {
    "text": "neural"
  }
}
```

### Use Case 3: Filter by Exact Text Match

Find embeddings with exact text content.

```json
{
  "searchTerm": "lifeless",
  "filter": {
    "text": "the end comes, this lifeless singsong of futility, squawking furiously at water's edge for chips from sucker passersby. don't feed the beast"
  }
}
```

### Use Case 4: Filter by Token Count Range

Find embeddings with token count between 20 and 100.

```json
{
  "searchTerm": "data analysis",
  "k": 20,
  "filter": {
    "tokens": ">=20 & <=100"
  }
}
```

### Use Case 5: Find Long Documents from Specific Provider

```json
{
  "searchTerm": "guide",
  "k": 15,
  "threshold": 0.50,
  "filter": {
    "tokens": ">50",
    "provider": "deepinfra"
  }
}
```

### Use Case 6: Find Short, Untruncated Messages

```json
{
  "searchTerm": "quick update",
  "filter": {
    "tokens": "<30",
    "is_truncated": false,
    "has_quotes": false
  }
}
```

### Use Case 7: Complex Multi-Filter Query

Combine multiple filter types for precise results.

```json
{
  "searchTerm": "technical",
  "k": 25,
  "threshold": 0.5,
  "filter": {
    "text": "optimization",
    "tokens": ">50 & <200",
    "provider": "deepinfra",
    "model": "Qwen/Qwen3-Embedding-4B",
    "is_truncated": false,
    "source": "parquet_import"
  }
}
```

### Use Case 8: Search with Direct Vector

If you already have a vector, use it directly instead of `searchTerm`.

```json
{
  "vector": [0.123, -0.456, 0.789, ...],
  "k": 10,
  "threshold": 0.8,
  "filter": {
    "tokens": ">10"
  }
}
```

### Use Case 9: High Similarity Threshold

Find only very similar results.

```json
{
  "searchTerm": "exact match needed",
  "k": 5,
  "threshold": 0.95,
  "filter": {
    "provider": "deepinfra"
  }
}
```

### Use Case 10: Find All Embeddings from a Source

Use a broad search term with specific filters.

```json
{
  "searchTerm": "content",
  "k": 100,
  "threshold": 0.5,
  "filter": {
    "source": "parquet_import",
    "has_context": false
  }
}
```

---

## Response Format

### Success Response

```json
{
  "success": true,
  "results": [
    {
      "key": "1234567890",
      "distance": 0.923,
      "metadata": {
        "source": "parquet_import",
        "original_text": "Sample text content here",
        "has_context": false,
        "has_quotes": false,
        "is_truncated": false,
        "character_difference": 0,
        "text": "Sample text content here",
        "provider": "deepinfra",
        "model": "Qwen/Qwen3-Embedding-4B",
        "tokens": 35
      }
    },
    ...
  ],
  "count": 10
}
```

### Response Fields

| Field | Type | Description |
|-------|------|-------------|
| `success` | boolean | Whether the search succeeded |
| `results` | array | Array of search results |
| `results[].key` | string | Unique identifier for the embedding |
| `results[].distance` | number | Similarity score (0-1, higher is more similar) |
| `results[].metadata` | object | All metadata associated with the embedding |
| `count` | number | Number of results returned |

### Error Response

```json
{
  "success": false,
  "error": "Error message describing what went wrong"
}
```

---

## Best Practices

### 1. Use Appropriate k Values

- **Exploratory searches:** `k: 20-50`
- **Precise matches:** `k: 5-10`
- **Batch processing:** `k: 100-500`

### 2. Set Meaningful Thresholds

- **High precision needed:** `threshold: 0.85-0.95`
- **Balanced results:** `threshold: 0.70-0.80` (default: 0.65)
- **Broad search:** `threshold: 0.50-0.65`

### 3. Combine Filters Wisely

Don't over-filter! Start broad and narrow down:

```json
// Good: Start with one or two filters
{
  "searchTerm": "query",
  "filter": {
    "tokens": ">20",
    "provider": "deepinfra"
  }
}

// May be too restrictive
{
  "searchTerm": "query",
  "filter": {
    "tokens": ">20 & <25",
    "provider": "deepinfra",
    "model": "specific-model",
    "has_quotes": false,
    "has_context": true,
    "is_truncated": false
  }
}
```

### 4. Text Substring Matching

For substring matching on text fields, keep queries reasonably specific:

```json
// Good: Specific phrase
{
  "filter": { "text": "neural network optimization" }
}

// Less useful: Too generic
{
  "filter": { "text": "the" }
}
```

### 5. Performance Tips

- **Index common fields:** The following fields are automatically indexed: `text`, `original_text`, `source`, `provider`, `model`
- **Use range queries:** More efficient than multiple exact matches
- **Limit results:** Don't request more than you need

### 6. Testing Filters

Test your filters incrementally:

1. Start without filters
2. Add one filter at a time
3. Check result counts
4. Adjust threshold if needed

---

## Common Metadata Fields

Based on your data structure, common metadata fields include:

| Field | Type | Description | Filter Type |
|-------|------|-------------|-------------|
| `text` | string | The main text content | Substring |
| `original_text` | string | Original unmodified text | Substring |
| `source` | string | Data source identifier | Exact |
| `provider` | string | Embedding provider | Exact |
| `model` | string | Model used for embedding | Exact |
| `tokens` | number | Token count | Exact/Range |
| `has_context` | boolean | Whether context is included | Exact |
| `has_quotes` | boolean | Whether text contains quotes | Exact |
| `is_truncated` | boolean | Whether text was truncated | Exact |
| `character_difference` | number | Char count difference | Exact/Range |

---

## Troubleshooting

### No Results Returned

1. **Check your threshold:** Lower it to see if you get results
2. **Remove filters:** Test without filters first
3. **Verify field names:** Ensure filter keys match your metadata structure
4. **Check data:** Confirm embeddings exist with those characteristics

### Too Many Results

1. **Increase threshold:** Get more similar results only
2. **Add filters:** Narrow down by metadata
3. **Reduce k:** Limit number of results

### Filters Not Working

1. **Check field names:** Must match metadata exactly (case-sensitive)
2. **Verify data types:** Use correct filter type for the field
3. **Test incrementally:** Add one filter at a time

---

## Postman Collection Example

Here's a complete Postman request setup:

**Method:** POST
**URL:** `http://localhost:3000/embeddings/search`
**Headers:**
```
Content-Type: application/json
```

**Body (raw JSON):**
```json
{
  "searchTerm": "machine learning optimization",
  "k": 20,
  "threshold": 0.7,
  "filter": {
    "tokens": ">50 & <150",
    "provider": "deepinfra",
    "has_context": true
  }
}
```

---

## Need Help?

- Check server logs for detailed error messages
- Verify your server is running: `GET /health`
- Check database stats: `GET /embeddings/stats`
- Review the main README.md for setup instructions

For more information about other API endpoints, see the main [README.md](../README.md).
