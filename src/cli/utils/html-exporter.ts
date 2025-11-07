import fs from 'fs/promises';
import path from 'path';

export interface TweetComparisonData {
  tweet_id: string;
  full_text: string;
  processed_text: string;
  contextIncluded: boolean;
  quotesIncluded: boolean;
  truncated: boolean;
  charactersUsed: number;
  originalLength: number;
}

export class HTMLExporter {
  private static generateCSS(): string {
    return `
      <style>
        body {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
          margin: 0;
          padding: 20px;
          background-color: #f5f5f5;
          line-height: 1.6;
        }

        .container {
          max-width: 1200px;
          margin: 0 auto;
          background: white;
          border-radius: 8px;
          box-shadow: 0 2px 10px rgba(0,0,0,0.1);
          overflow: hidden;
        }

        .header {
          background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
          color: white;
          padding: 20px;
          text-align: center;
        }

        .header h1 {
          margin: 0;
          font-size: 2em;
        }

        .stats {
          background: #f8f9fa;
          padding: 15px 20px;
          border-bottom: 1px solid #dee2e6;
          display: flex;
          justify-content: space-around;
          flex-wrap: wrap;
        }

        .stat-item {
          text-align: center;
          margin: 5px 10px;
        }

        .stat-number {
          display: block;
          font-size: 2em;
          font-weight: bold;
          color: #667eea;
        }

        .stat-label {
          color: #6c757d;
          font-size: 0.9em;
        }

        .tweet-card {
          border-bottom: 1px solid #dee2e6;
          padding: 20px;
          transition: background-color 0.2s;
        }

        .tweet-card:hover {
          background-color: #f8f9fa;
        }

        .tweet-card:last-child {
          border-bottom: none;
        }

        .tweet-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: 15px;
        }

        .tweet-id {
          font-family: 'Courier New', monospace;
          background: #e9ecef;
          padding: 4px 8px;
          border-radius: 4px;
          font-size: 0.9em;
          font-weight: bold;
        }

        .tweet-badges {
          display: flex;
          gap: 8px;
        }

        .badge {
          padding: 2px 8px;
          border-radius: 12px;
          font-size: 0.75em;
          font-weight: 500;
          text-transform: uppercase;
        }

        .badge.context { background: #d4edda; color: #155724; }
        .badge.quotes { background: #d1ecf1; color: #0c5460; }
        .badge.truncated { background: #f8d7da; color: #721c24; }
        .badge.simple { background: #e2e3e5; color: #383d41; }

        .text-section {
          margin-bottom: 15px;
        }

        .text-label {
          font-weight: 600;
          color: #495057;
          margin-bottom: 5px;
          display: flex;
          align-items: center;
          gap: 8px;
        }

        .text-content {
          background: #f8f9fa;
          border: 1px solid #dee2e6;
          border-radius: 6px;
          padding: 12px;
          font-family: Georgia, serif;
          white-space: pre-wrap;
          word-wrap: break-word;
        }

        .processed-text {
          background: #e8f5e8;
          border-color: #28a745;
        }

        .character-info {
          display: flex;
          justify-content: space-between;
          font-size: 0.85em;
          color: #6c757d;
          margin-top: 5px;
        }

        .diff-indicator {
          padding: 2px 6px;
          border-radius: 3px;
          font-size: 0.8em;
          margin-left: 8px;
        }

        .diff-reduced { background: #d4edda; color: #155724; }
        .diff-increased { background: #f8d7da; color: #721c24; }
        .diff-same { background: #e2e3e5; color: #383d41; }

        .context-parts {
          background: #fff3cd;
          border: 1px solid #ffeaa7;
          border-radius: 6px;
          padding: 10px;
          margin-top: 10px;
          font-size: 0.9em;
        }

        .context-parts strong {
          color: #856404;
        }

        @media (max-width: 768px) {
          .container {
            margin: 10px;
            border-radius: 0;
          }

          .stats {
            flex-direction: column;
          }

          .tweet-header {
            flex-direction: column;
            align-items: flex-start;
            gap: 10px;
          }
        }
      </style>
    `;
  }

  static async exportToHTML(
    tweets: TweetComparisonData[],
    outputPath: string,
    title: string = 'Tweet Processing Validation'
  ): Promise<void> {
    const totalTweets = tweets.length;
    const contextProcessed = tweets.filter(t => t.contextIncluded).length;
    const quotesIncluded = tweets.filter(t => t.quotesIncluded).length;
    const truncated = tweets.filter(t => t.truncated).length;
    const avgReduction = tweets.reduce((sum, t) => sum + (t.originalLength - t.charactersUsed), 0) / totalTweets;

    const html = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>${title}</title>
    ${this.generateCSS()}
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🐦 ${title}</h1>
            <p>Processing validation report generated on ${new Date().toLocaleString()}</p>
        </div>

        <div class="stats">
            <div class="stat-item">
                <span class="stat-number">${totalTweets}</span>
                <span class="stat-label">Total Tweets</span>
            </div>
            <div class="stat-item">
                <span class="stat-number">${contextProcessed}</span>
                <span class="stat-label">With Context</span>
            </div>
            <div class="stat-item">
                <span class="stat-number">${quotesIncluded}</span>
                <span class="stat-label">With Quotes</span>
            </div>
            <div class="stat-item">
                <span class="stat-number">${truncated}</span>
                <span class="stat-label">Truncated</span>
            </div>
            <div class="stat-item">
                <span class="stat-number">${avgReduction.toFixed(0)}</span>
                <span class="stat-label">Avg Char Reduction</span>
            </div>
        </div>

        ${tweets.map(tweet => this.generateTweetCard(tweet)).join('')}
    </div>
</body>
</html>
    `;

    await fs.writeFile(outputPath, html, 'utf-8');
  }

  private static generateTweetCard(tweet: TweetComparisonData): string {
    const characterDiff = tweet.charactersUsed - tweet.originalLength;
    const diffClass = characterDiff < 0 ? 'diff-reduced' : characterDiff > 0 ? 'diff-increased' : 'diff-same';
    const diffText = characterDiff === 0 ? 'Same' : characterDiff > 0 ? `+${characterDiff}` : `${characterDiff}`;

    const badges = [];
    if (tweet.contextIncluded) badges.push('<span class="badge context">Context</span>');
    if (tweet.quotesIncluded) badges.push('<span class="badge quotes">Quotes</span>');
    if (tweet.truncated) badges.push('<span class="badge truncated">Truncated</span>');
    if (!tweet.contextIncluded && !tweet.quotesIncluded) badges.push('<span class="badge simple">Simple</span>');

    // Detect if processed text has conversation markers
    const hasConversationMarkers = /\[(root|context|current|quoted)\]/.test(tweet.processed_text);
    let contextInfo = '';

    if (hasConversationMarkers) {
      const markers = tweet.processed_text.match(/\[(root|context|current|quoted)\]/g) || [];
      const uniqueMarkers = [...new Set(markers)];
      contextInfo = `
        <div class="context-parts">
          <strong>Conversation Parts:</strong> ${uniqueMarkers.join(', ')}
        </div>
      `;
    }

    return `
      <div class="tweet-card">
        <div class="tweet-header">
          <div class="tweet-id">ID: ${tweet.tweet_id}</div>
          <div class="tweet-badges">
            ${badges.join('')}
          </div>
        </div>

        <div class="text-section">
          <div class="text-label">
            📝 Original Text
            <span class="diff-indicator diff-same">${tweet.originalLength} chars</span>
          </div>
          <div class="text-content">${this.escapeHtml(tweet.full_text)}</div>
        </div>

        <div class="text-section">
          <div class="text-label">
            ⚡ Processed Text
            <span class="diff-indicator ${diffClass}">${tweet.charactersUsed} chars (${diffText})</span>
          </div>
          <div class="text-content processed-text">${this.escapeHtml(tweet.processed_text)}</div>
          ${contextInfo}
        </div>
      </div>
    `;
  }

  private static escapeHtml(text: string): string {
    return text
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }
}