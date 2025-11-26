/**
 * Shared tweet text processing utility
 * Extracted from TweetProcessor.processTweetSimple() to be reusable across contexts
 */

export interface ProcessedTweetResult {
  originalText: string;
  processedText: string;
  quotesIncluded: boolean;
  truncated: boolean;
  charactersUsed: number;
}

/**
 * Process tweet text with optional quoted tweet text
 * Uses the exact logic from TweetProcessor.processTweetSimple()
 * 
 * @param mainTweetText - The main tweet's full_text
 * @param quotedTweetText - Optional quoted tweet's full_text (single quote)
 * @param maxChars - Maximum character limit (default 1024)
 * @returns Processed tweet result with metadata
 */
export function processTweetText(
  mainTweetText: string,
  quotedTweetText: string | null = null,
  maxChars: number = 1024
): ProcessedTweetResult {
  // Clean the main tweet text
  const cleanedMainText = cleanTweetText(mainTweetText);

  let processedText = cleanedMainText;
  let quotesIncluded = false;
  let truncated = false;

  // Add quoted tweet if available (keeping exact logic from processTweetSimple)
  if (quotedTweetText && quotedTweetText.trim()) {
    const cleanedQuotedText = cleanTweetText(quotedTweetText);
    const combinedText = `${cleanedMainText}\n[quoted] ${cleanedQuotedText}`;

    // Check if combined text fits within maxChars
    if (combinedText.length <= maxChars) {
      processedText = combinedText;
      quotesIncluded = true;
    } else {
      // Smart truncation: prioritize main tweet, but give quoted tweet some space
      truncated = true;

      // Calculate allocation
      const quotedPrefix = '\n[quoted] ';
      const ellipsis = '...';

      // Main tweet gets 70% of space (but at least 512 chars or maxChars if less)
      const minMainChars = Math.min(512, maxChars);
      const mainAllocation = Math.max(
        minMainChars,
        Math.floor(maxChars * 0.7)
      );

      // Quoted tweet gets the remaining space
      const quotedAllocation = maxChars - mainAllocation - quotedPrefix.length - ellipsis.length;

      // Truncate main text if needed
      let truncatedMain = cleanedMainText;
      if (cleanedMainText.length > mainAllocation) {
        truncatedMain = cleanedMainText.substring(0, mainAllocation - ellipsis.length) + ellipsis;
      }

      // Truncate quoted text if we have space for it
      if (quotedAllocation > 20) { // Only include quoted if we have meaningful space
        const truncatedQuoted = cleanedQuotedText.substring(0, quotedAllocation) + ellipsis;
        processedText = `${truncatedMain}${quotedPrefix}${truncatedQuoted}`;
        quotesIncluded = true;
      } else {
        // Not enough space for quoted tweet, just use main tweet
        processedText = truncatedMain;
      }
    }
  } else if (cleanedMainText.length > maxChars) {
    // No quoted tweet, but main tweet is too long
    processedText = cleanedMainText.substring(0, maxChars - 3) + '...';
    truncated = true;
  }

  return {
    originalText: mainTweetText,
    processedText,
    quotesIncluded,
    truncated,
    charactersUsed: processedText.length
  };
}

/**
 * Clean tweet text for embedding
 * Removes URLs, normalizes whitespace, removes mentions at start, etc.
 * 
 * This function is also used to normalize filter text in search queries
 * to ensure consistent matching against stored processed text.
 */
export function cleanTweetText(text: string): string {
  if (!text) return '';
  
  return text
    .replace(/https?:\/\/\S+/g, '')       // Remove URLs
    .replace(/\s+/g, ' ')                  // Remove extra whitespace
    .trim()                                // Remove leading/trailing whitespace
    .replace(/^(@\w+\s*)+/, '')           // Remove all mentions at start
    .replace(/[""]/g, '"')                // Normalize unicode quotes
    .replace(/['']/g, "'");
}

