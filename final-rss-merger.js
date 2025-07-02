/**
 * Improved RSS Feed Merger - Enhanced Matching Version v2.0
 * 
 * Key improvements:
 * - Better CDATA handling in titles
 * - Improved column parsing and text normalization
 * - More flexible matching strategies
 * - Better debugging output
 * - Fixed secondary feed filtering logic
 * - Lower thresholds for better recall
 * - Enhanced prefix handling for various title formats
 * - Improved author extraction from secondary feed
 */

const https = require('https');
const fs = require('fs');
const { DOMParser, XMLSerializer } = require('xmldom');

// Configuration object - adjusted for better matching
const CONFIG = {
  primaryFeedUrl: 'https://www.thearknewspaper.com/blog-feed.xml',
  secondaryFeedUrl: 'https://thearknewspaper-ca.newsmemory.com/rss.php?edition=The%20Ark&section=Main&device=std&images=none&content=abstract',
  
  // Matching thresholds - made more permissive
  exactMatchThreshold: 1.0,
  fuzzyMatchThreshold: 0.55,  // Lowered from 0.65
  wordOverlapThreshold: 0.4,   // Lowered from 0.5
  levenshteinThreshold: 0.6,   // Lowered from 0.7
  
  // Scoring weights
  titleSimilarityWeight: 0.7,  // Increased title weight
  authorMatchWeight: 0.2,      // Reduced author weight
  columnMatchWeight: 0.05,     // Reduced column weight
  dateProximityWeight: 0.05,
  
  // Network settings
  requestTimeout: 10000,
  maxRetries: 3,
  retryDelay: 1000,
  
  // Output settings
  outputFile: 'merged_rss_feed.xml',
  generateReport: true,
  reportFile: 'merge_report.json',
  
  // Debug settings
  verboseLogging: true
};

// Column names that might appear as prefixes in titles
const COLUMN_NAMES = [
  'New Business',
  'Sports Shout', 
  'Everyday Encounter',
  'Everyday Encounters',
  'Notes from an Appraiser',
  'Garden Plot',
  'Travel Bug',
  'Wildflower Watch'
];

// Common title variations and normalizations
const TITLE_NORMALIZATIONS = new Map([
  ['town council', 'city council'],
  ['tiburon town council', 'tiburon city council'],
  ['&amp;', 'and'],
  ['&', 'and'],
  ['w/', 'with'],
  ['st.', 'street'],
  ['rd.', 'road'],
  ['ave.', 'avenue'],
  ['\u2019', "'"], // right single quotation mark
  ['\u201c', '"'], // left double quotation mark
  ['\u201d', '"'], // right double quotation mark
  ['\u2013', '-'], // en dash
  ['\u2014', '-']  // em dash
]);

// Compiled regex patterns for performance
const PATTERNS = {
  htmlEntity: /&#?\w+;/g,
  cdata: /^\s*<!\[CDATA\[(.*?)\]\]>\s*$/,
  cdataContent: /<!\[CDATA\[(.*?)\]\]>/,
  newsmemoryLink: /newsmemory\.com/,
  bylineExtraction: /<!\[CDATA\[By\s+([A-Z\s]+)/i,
  whitespace: /\s+/g,
  punctuation: /[^\w\s]/g,
  leadingArticles: /^(the|a|an)\s+/i,
  commonPrepositions: /\s+(to|in|at|on|by|with|for|of|and|or)\s+/gi,
  extraSpaces: /\s{2,}/g
};

/**
 * Enhanced HTML entity decoder
 */
function decodeHtmlEntities(text) {
  if (!text) return text;
  
  const entityMap = new Map([
    ['&#38;', '&'], ['&amp;', '&'],
    ['&#39;', "'"], ['&apos;', "'"],
    ['&#34;', '"'], ['&quot;', '"'],
    ['&#60;', '<'], ['&lt;', '<'],
    ['&#62;', '>'], ['&gt;', '>'],
    ['&#160;', ' '], ['&nbsp;', ' '],
    ['&#8217;', "'"], ['&rsquo;', "'"],
    ['&#8216;', "'"], ['&lsquo;', "'"],
    ['&#8220;', '"'], ['&ldquo;', '"'],
    ['&#8221;', '"'], ['&rdquo;', '"'],
    ['&#8211;', '–'], ['&ndash;', '–'],
    ['&#8212;', '—'], ['&mdash;', '—']
  ]);
  
  let decoded = text;
  for (const [entity, replacement] of entityMap) {
    decoded = decoded.replace(new RegExp(entity, 'g'), replacement);
  }
  
  return decoded;
}

/**
 * Enhanced CDATA and title extraction
 */
function extractCleanTitle(titleElement) {
  if (!titleElement) return '';
  
  let title = titleElement.textContent || '';
  
  // Handle CDATA sections
  const cdataMatch = title.match(PATTERNS.cdata);
  if (cdataMatch) {
    title = cdataMatch[1];
  } else {
    // Look for CDATA content within the text
    const cdataContentMatch = title.match(PATTERNS.cdataContent);
    if (cdataContentMatch) {
      title = cdataContentMatch[1];
    }
  }
  
  // Decode HTML entities
  title = decodeHtmlEntities(title);
  
  // Clean up extra whitespace
  title = title.replace(PATTERNS.extraSpaces, ' ').trim();
  
  if (CONFIG.verboseLogging && title) {
    console.log('    Extracted title: "' + title + '"');
  }
  
  return title;
}

/**
 * Calculate Levenshtein distance between two strings
 */
function levenshteinDistance(str1, str2) {
  const matrix = Array(str2.length + 1).fill(null).map(() => Array(str1.length + 1).fill(null));
  
  for (let i = 0; i <= str1.length; i++) matrix[0][i] = i;
  for (let j = 0; j <= str2.length; j++) matrix[j][0] = j;
  
  for (let j = 1; j <= str2.length; j++) {
    for (let i = 1; i <= str1.length; i++) {
      const indicator = str1[i - 1] === str2[j - 1] ? 0 : 1;
      matrix[j][i] = Math.min(
        matrix[j][i - 1] + 1,
        matrix[j - 1][i] + 1,
        matrix[j - 1][i - 1] + indicator
      );
    }
  }
  
  return matrix[str2.length][str1.length];
}

/**
 * Calculate string similarity ratio (0-1) using Levenshtein distance
 */
function stringSimilarity(str1, str2) {
  if (!str1 || !str2) return 0;
  if (str1 === str2) return 1;
  
  const maxLength = Math.max(str1.length, str2.length);
  if (maxLength === 0) return 1;
  
  return 1 - (levenshteinDistance(str1, str2) / maxLength);
}

/**
 * Calculate word overlap similarity
 */
function wordOverlapSimilarity(str1, str2) {
  if (!str1 || !str2) return 0;
  
  const words1 = str1.toLowerCase().split(/\s+/).filter(w => w.length > 2);
  const words2 = str2.toLowerCase().split(/\s+/).filter(w => w.length > 2);
  
  if (words1.length === 0 && words2.length === 0) return 1;
  if (words1.length === 0 || words2.length === 0) return 0;
  
  const set1 = new Set(words1);
  const set2 = new Set(words2);
  
  const intersection = new Set([...set1].filter(x => set2.has(x)));
  const union = new Set([...set1, ...set2]);
  
  return intersection.size / union.size;
}

/**
 * HTTP request with timeout and retry logic
 */
async function fetchURLWithRetry(url, retries = CONFIG.maxRetries) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      console.log('Fetching ' + url + ' (attempt ' + attempt + '/' + retries + ')');
      
      const data = await new Promise((resolve, reject) => {
        const request = https.get(url, { timeout: CONFIG.requestTimeout }, (res) => {
          if (res.statusCode !== 200) {
            reject(new Error('HTTP ' + res.statusCode + ': ' + res.statusMessage));
            return;
          }
          
          let data = '';
          res.on('data', chunk => data += chunk);
          res.on('end', () => resolve(data));
          res.on('error', reject);
        });
        
        request.on('timeout', () => {
          request.destroy();
          reject(new Error('Request timeout'));
        });
        
        request.on('error', reject);
      });
      
      return data;
    } catch (error) {
      console.log('Attempt ' + attempt + ' failed: ' + error.message);
      
      if (attempt === retries) {
        throw new Error('Failed to fetch ' + url + ' after ' + retries + ' attempts: ' + error.message);
      }
      
      await new Promise(resolve => setTimeout(resolve, CONFIG.retryDelay * attempt));
    }
  }
}

/**
 * Parse publication date from various formats
 */
function parsePublicationDate(dateString) {
  if (!dateString) return null;
  
  try {
    let date = new Date(dateString);
    if (!isNaN(date.getTime())) return date;
    
    const formats = [
      /(\d{4})-(\d{2})-(\d{2})/,
      /(\d{2})\/(\d{2})\/(\d{4})/,
      /(\d{1,2})\s+(\w+)\s+(\d{4})/
    ];
    
    for (const format of formats) {
      const match = dateString.match(format);
      if (match) {
        date = new Date(dateString);
        if (!isNaN(date.getTime())) return date;
      }
    }
  } catch (error) {
    console.warn('Failed to parse date: ' + dateString);
  }
  
  return null;
}

/**
 * Enhanced title parser that handles various prefixes beyond just column names
 */
function parseColumnTitle(title) {
  if (!title) {
    return { columnName: null, coreTitle: title || '', fullTitle: title || '' };
  }
  
  const cleanTitle = title.trim();
  
  // Simple approach - check each column name directly
  for (let i = 0; i < COLUMN_NAMES.length; i++) {
    const columnName = COLUMN_NAMES[i];
    
    // Check for "ColumnName: Title" format
    if (cleanTitle.toLowerCase().startsWith(columnName.toLowerCase() + ':')) {
      const coreTitle = cleanTitle.substring(columnName.length + 1).trim();
      if (coreTitle.length > 0) {
        return {
          columnName: columnName,
          coreTitle: coreTitle,
          fullTitle: cleanTitle
        };
      }
    }
    
    // Check for "ColumnName Title" format (without colon)
    if (cleanTitle.toLowerCase().startsWith(columnName.toLowerCase() + ' ')) {
      const coreTitle = cleanTitle.substring(columnName.length + 1).trim();
      if (coreTitle.length > 0) {
        return {
          columnName: columnName,
          coreTitle: coreTitle,
          fullTitle: cleanTitle
        };
      }
    }
  }
  
  // Handle other common prefixes
  const commonPrefixes = [
    'July 4 holiday',
    'Fourth of July',
    'Independence Day',
    'Holiday',
    'Breaking',
    'Update',
    'News',
    'Local'
  ];
  
  for (let i = 0; i < commonPrefixes.length; i++) {
    const prefix = commonPrefixes[i];
    if (cleanTitle.toLowerCase().startsWith(prefix.toLowerCase() + ':')) {
      const coreTitle = cleanTitle.substring(prefix.length + 1).trim();
      if (coreTitle.length > 0) {
        return {
          columnName: prefix,
          coreTitle: coreTitle,
          fullTitle: cleanTitle
        };
      }
    }
  }
  
  // Handle generic "word:" patterns
  const colonIndex = cleanTitle.indexOf(':');
  if (colonIndex > 0 && colonIndex < 30) {
    const potentialPrefix = cleanTitle.substring(0, colonIndex).trim();
    const potentialCore = cleanTitle.substring(colonIndex + 1).trim();
    
    if (potentialCore.length > 10 && potentialPrefix.length <= 25) {
      return {
        columnName: potentialPrefix,
        coreTitle: potentialCore,
        fullTitle: cleanTitle
      };
    }
  }
  
  return {
    columnName: null,
    coreTitle: cleanTitle,
    fullTitle: cleanTitle
  };
}

/**
 * Enhanced text normalization
 */
function normalizeText(text, options = {}) {
  if (!text) return '';
  
  const {
    stripColumnNames = true,
    applyNormalizations = true,
    removeStopWords = true
  } = options;
  
  let workingText = text;
  
  if (stripColumnNames) {
    const parsed = parseColumnTitle(text);
    workingText = parsed.coreTitle;
  } else {
    workingText = decodeHtmlEntities(text);
  }
  
  if (applyNormalizations) {
    for (const [from, to] of TITLE_NORMALIZATIONS) {
      workingText = workingText.replace(new RegExp(from, 'gi'), to);
    }
  }
  
  let normalized = workingText.trim()
    .replace(PATTERNS.whitespace, ' ')
    .replace(PATTERNS.punctuation, ' ')
    .toLowerCase();
  
  if (removeStopWords) {
    normalized = normalized
      .replace(PATTERNS.leadingArticles, '')
      .replace(PATTERNS.commonPrepositions, ' ')
      .replace(/\b(sf|san francisco|bay area)\b/g, '')
      .replace(/\b(award winning|awardwinning)\b/g, '');
  }
  
  return normalized.replace(PATTERNS.extraSpaces, ' ').trim();
}

/**
 * Extract comprehensive metadata from an RSS item
 */
function extractItemMetadata(item, feedType = 'unknown') {
  const metadata = {
    title: null,
    parsedTitle: null,
    link: null,
    author: null,
    extractedAuthor: null,
    pubDate: null,
    description: null,
    guid: null,
    feedType: feedType
  };
  
  if (CONFIG.verboseLogging) {
    console.log('  Extracting metadata from ' + feedType + ' item...');
  }
  
  // Extract title
  const titleElements = item.getElementsByTagName('title');
  if (titleElements.length > 0) {
    metadata.title = extractCleanTitle(titleElements[0]);
    metadata.parsedTitle = parseColumnTitle(metadata.title);
    
    if (CONFIG.verboseLogging) {
      console.log('    Parsed title - Column: "' + metadata.parsedTitle.columnName + '", Core: "' + metadata.parsedTitle.coreTitle + '"');
    }
  }
  
  // Extract link
  const linkElements = item.getElementsByTagName('link');
  if (linkElements.length > 0) {
    metadata.link = linkElements[0].textContent.trim();
  }
  
  // Extract author from dc:creator
  const creatorElements = item.getElementsByTagName('dc:creator');
  if (creatorElements.length > 0) {
    metadata.author = creatorElements[0].textContent.trim();
  }
  
  // Extract author from <full> content (NewMemory specific)
  const fullElements = item.getElementsByTagName('full');
  if (fullElements.length > 0) {
    const fullContent = fullElements[0].textContent;
    if (fullContent) {
      // Multiple patterns to catch different author formats
      const authorPatterns = [
        // Standard: "By FIRSTNAME LASTNAME" (followed by email or other text)
        /By\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)(?:[a-z@\.\s]|$)/i,
        // All caps: "By FIRSTNAME LASTNAME" 
        /By\s+([A-Z]{2,}\s+[A-Z]{2,}(?:\s+[A-Z]{2,})*)/,
        // Mixed case with email: "By John Smithjsmith@email.com"
        /By\s+([A-Z][a-z]+\s+[A-Z][a-z]+)(?:[a-z]+@|$)/i,
        // Simple pattern: "By" followed by reasonable name characters
        /By\s+([A-Z][A-Za-z\s]{3,30})(?=\s*[a-z]*@|\s*———|$)/i,
        // Handle cases where there's text before "By"
        /(?:^|[^A-Za-z])By\s+([A-Z][A-Za-z\s]{3,25})(?=\s*[a-z]*@|\s*———|$)/i
      ];
      
      for (const pattern of authorPatterns) {
        const match = fullContent.match(pattern);
        if (match && match[1]) {
          let authorName = match[1].trim();
          
          // Clean up the extracted name
          authorName = authorName
            .replace(/\s+/g, ' ')  // normalize spaces
            .replace(/[^A-Za-z\s]/g, '') // remove non-letter chars
            .trim();
          
          // Validate it looks like a reasonable name (2-4 words, reasonable length)
          const words = authorName.split(/\s+/);
          if (words.length >= 2 && words.length <= 4 && authorName.length >= 4 && authorName.length <= 30) {
            // Convert to proper case
            metadata.extractedAuthor = words
              .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
              .join(' ');
            
            if (CONFIG.verboseLogging) {
              console.log('    Extracted author: "' + metadata.extractedAuthor + '" from pattern: ' + pattern);
            }
            break;
          }
        }
      }
    }
  }
  
  // Extract publication date
  const pubDateElements = item.getElementsByTagName('pubDate');
  if (pubDateElements.length > 0) {
    metadata.pubDate = parsePublicationDate(pubDateElements[0].textContent);
  }
  
  // Extract description
  const descElements = item.getElementsByTagName('description');
  if (descElements.length > 0) {
    metadata.description = descElements[0].textContent.trim();
  }
  
  // Extract GUID
  const guidElements = item.getElementsByTagName('guid');
  if (guidElements.length > 0) {
    metadata.guid = guidElements[0].textContent.trim();
  }
  
  return metadata;
}

/**
 * Calculate comprehensive match score between two articles
 */
function calculateMatchScore(primary, secondary) {
  const scores = {
    titleSimilarity: 0,
    authorMatch: 0,
    columnMatch: 0,
    dateProximity: 0,
    total: 0,
    details: {}
  };
  
  // Title similarity using multiple methods
  if (primary.parsedTitle && secondary.parsedTitle) {
    const primaryCore = normalizeText(primary.parsedTitle.coreTitle, { stripColumnNames: false });
    const secondaryCore = normalizeText(secondary.parsedTitle.coreTitle, { stripColumnNames: false });
    
    if (CONFIG.verboseLogging) {
      console.log('    Comparing titles:');
      console.log('      Primary core: "' + primaryCore + '"');
      console.log('      Secondary core: "' + secondaryCore + '"');
    }
    
    // Exact match check
    if (primaryCore === secondaryCore) {
      scores.titleSimilarity = 1.0;
      scores.details.exactMatch = true;
    } else {
      // Substring match
      if ((primaryCore.includes(secondaryCore) && secondaryCore.length > 10) || 
          (secondaryCore.includes(primaryCore) && primaryCore.length > 10)) {
        scores.titleSimilarity = 0.95;
        scores.details.substringMatch = true;
      } else {
        // Levenshtein similarity
        const levenshteinSim = stringSimilarity(primaryCore, secondaryCore);
        
        // Word overlap similarity
        const wordOverlapSim = wordOverlapSimilarity(primaryCore, secondaryCore);
        
        // Use the better of the two
        scores.titleSimilarity = Math.max(levenshteinSim, wordOverlapSim);
        scores.details.levenshteinSim = levenshteinSim;
        scores.details.wordOverlapSim = wordOverlapSim;
      }
    }
  }
  
  // Author matching
  const primaryAuthor = (primary.author || primary.extractedAuthor || '').toLowerCase();
  const secondaryAuthor = (secondary.author || secondary.extractedAuthor || '').toLowerCase();
  
  if (primaryAuthor && secondaryAuthor) {
    if (primaryAuthor === secondaryAuthor) {
      scores.authorMatch = 1.0;
    } else if (primaryAuthor.includes(secondaryAuthor) || secondaryAuthor.includes(primaryAuthor)) {
      scores.authorMatch = 0.8;
    } else {
      scores.authorMatch = stringSimilarity(primaryAuthor, secondaryAuthor);
    }
  }
  
  // Column name matching
  if (primary.parsedTitle?.columnName && secondary.parsedTitle?.columnName) {
    const col1 = primary.parsedTitle.columnName.toLowerCase();
    const col2 = secondary.parsedTitle.columnName.toLowerCase();
    scores.columnMatch = col1 === col2 ? 1.0 : stringSimilarity(col1, col2);
  }
  
  // Date proximity (less important)
  if (primary.pubDate && secondary.pubDate) {
    const diffMs = Math.abs(primary.pubDate.getTime() - secondary.pubDate.getTime());
    const diffDays = diffMs / (1000 * 60 * 60 * 24);
    scores.dateProximity = Math.exp(-diffDays / 7);
  }
  
  // Calculate weighted total
  scores.total = 
    scores.titleSimilarity * CONFIG.titleSimilarityWeight +
    scores.authorMatch * CONFIG.authorMatchWeight +
    scores.columnMatch * CONFIG.columnMatchWeight +
    scores.dateProximity * CONFIG.dateProximityWeight;
  
  if (CONFIG.verboseLogging && scores.total > 0.3) {
    console.log('    Match scores: Total=' + scores.total.toFixed(3) + ', Title=' + scores.titleSimilarity.toFixed(3) + ', Author=' + scores.authorMatch.toFixed(3));
  }
  
  return scores;
}

/**
 * Build efficient lookup indexes for secondary feed items
 */
function buildSecondaryIndexes(secondaryItems) {
  const indexes = {
    byNormalizedTitle: new Map(),
    byNormalizedCore: new Map(),
    byAuthor: new Map(),
    byColumn: new Map(),
    items: []
  };
  
  console.log('\n🔍 Building indexes for ' + secondaryItems.length + ' secondary items...');
  
  for (const item of secondaryItems) {
    const metadata = extractItemMetadata(item, 'secondary');
    
    if (!metadata.title) {
      if (CONFIG.verboseLogging) {
        console.log('  Skipping item with no title');
      }
      continue;
    }
    
    const normalizedFull = normalizeText(metadata.title, { stripColumnNames: false });
    const normalizedCore = normalizeText(metadata.parsedTitle?.coreTitle, { stripColumnNames: false });
    const author = (metadata.author || metadata.extractedAuthor || '').toLowerCase();
    const column = metadata.parsedTitle?.columnName?.toLowerCase();
    
    const indexedItem = {
      metadata,
      normalizedFull,
      normalizedCore,
      author,
      column,
      originalItem: item
    };
    
    indexes.items.push(indexedItem);
    
    // Build lookup maps for fast exact matching
    if (normalizedFull) {
      if (!indexes.byNormalizedTitle.has(normalizedFull)) {
        indexes.byNormalizedTitle.set(normalizedFull, []);
      }
      indexes.byNormalizedTitle.get(normalizedFull).push(indexedItem);
    }
    
    if (normalizedCore && normalizedCore !== normalizedFull) {
      if (!indexes.byNormalizedCore.has(normalizedCore)) {
        indexes.byNormalizedCore.set(normalizedCore, []);
      }
      indexes.byNormalizedCore.get(normalizedCore).push(indexedItem);
    }
    
    if (author) {
      if (!indexes.byAuthor.has(author)) {
        indexes.byAuthor.set(author, []);
      }
      indexes.byAuthor.get(author).push(indexedItem);
    }
    
    if (column) {
      if (!indexes.byColumn.has(column)) {
        indexes.byColumn.set(column, []);
      }
      indexes.byColumn.get(column).push(indexedItem);
    }
    
    if (CONFIG.verboseLogging) {
      console.log('  Indexed: "' + metadata.title + '"');
      console.log('    Core: "' + normalizedCore + '"');
      console.log('    Author: "' + author + '"');
      console.log('    Column: "' + column + '"');
    }
  }
  
  console.log('📊 Built indexes:');
  console.log('  - ' + indexes.items.length + ' total items');
  console.log('  - ' + indexes.byNormalizedTitle.size + ' unique normalized titles');
  console.log('  - ' + indexes.byNormalizedCore.size + ' unique normalized core titles');
  console.log('  - ' + indexes.byAuthor.size + ' unique authors');
  console.log('  - ' + indexes.byColumn.size + ' unique columns');
  
  return indexes;
}

/**
 * Find best match for a primary item using multiple strategies
 */
function findBestMatch(primaryMetadata, secondaryIndexes) {
  if (!primaryMetadata.title) return null;
  
  console.log('\n🔍 Finding match for: "' + primaryMetadata.title + '"');
  
  const normalizedFull = normalizeText(primaryMetadata.title, { stripColumnNames: false });
  const normalizedCore = normalizeText(primaryMetadata.parsedTitle?.coreTitle, { stripColumnNames: false });
  
  console.log('  Normalized full: "' + normalizedFull + '"');
  console.log('  Normalized core: "' + normalizedCore + '"');
  
  // Strategy 1: Exact match by normalized full title
  const exactFullMatches = secondaryIndexes.byNormalizedTitle.get(normalizedFull) || [];
  if (exactFullMatches.length > 0) {
    console.log('  ✅ Found exact full title match!');
    return {
      match: exactFullMatches[0],
      score: calculateMatchScore(primaryMetadata, exactFullMatches[0].metadata),
      strategy: 'exact_full'
    };
  }
  
  // Strategy 2: Exact match by normalized core title
  const exactCoreMatches = secondaryIndexes.byNormalizedCore.get(normalizedCore) || [];
  if (exactCoreMatches.length > 0) {
    console.log('  ✅ Found exact core title match!');
    return {
      match: exactCoreMatches[0],
      score: calculateMatchScore(primaryMetadata, exactCoreMatches[0].metadata),
      strategy: 'exact_core'
    };
  }
  
  // Strategy 3: Check if any secondary title matches our core (prefix removal)
  for (const [secondaryNormalized, candidates] of secondaryIndexes.byNormalizedTitle) {
    if (secondaryNormalized === normalizedCore) {
      console.log('  ✅ Found prefix-removed match! Secondary "' + candidates[0].metadata.title + '" matches our core');
      return {
        match: candidates[0],
        score: calculateMatchScore(primaryMetadata, candidates[0].metadata),
        strategy: 'prefix_removed'
      };
    }
  }
  
  // Strategy 4: Check if our full title matches any secondary core (reverse prefix removal)
  for (const candidate of secondaryIndexes.items) {
    if (candidate.normalizedCore === normalizedFull) {
      console.log('  ✅ Found reverse prefix match! Our full matches their core "' + candidate.metadata.title + '"');
      return {
        match: candidate,
        score: calculateMatchScore(primaryMetadata, candidate.metadata),
        strategy: 'reverse_prefix'
      };
    }
  }
  
  // Strategy 5: Fuzzy matching with comprehensive scoring
  console.log('  🔄 Performing fuzzy matching across ' + secondaryIndexes.items.length + ' items...');
  
  let bestMatch = null;
  let bestScore = null;
  
  for (const candidate of secondaryIndexes.items) {
    const score = calculateMatchScore(primaryMetadata, candidate.metadata);
    
    if (score.total > CONFIG.fuzzyMatchThreshold && (!bestScore || score.total > bestScore.total)) {
      bestMatch = candidate;
      bestScore = score;
    }
  }
  
  if (bestMatch) {
    console.log('  ✅ Found fuzzy match with score ' + bestScore.total.toFixed(3));
    console.log('    Secondary title: "' + bestMatch.metadata.title + '"');
    return {
      match: bestMatch,
      score: bestScore,
      strategy: 'fuzzy'
    };
  }
  
  console.log('  ❌ No suitable match found');
  return null;
}

/**
 * Format newsmemory URLs to cleaner format
 */
function reformatNewsmemoryLink(url) {
  // Extract components from the URL
  const match = url.match(/date=(\d+).*?page=\d+theark(\d+).*?id=art_(\d+)\.xml/);
  
  if (match) {
    const [, date, page, artid] = match;
    return 'https://thearknewspaper-ca.newsmemory.com?selDate=' + date + '&goTo=' + page.padStart(2, '0') + '&artid=' + artid + '&editionStart=The%20Ark';
  }
  
  console.log('Could not reformat URL: ' + url);
  return url;
}

/**
 * Main merge function with enhanced processing
 */
async function mergeFeeds() {
  const startTime = Date.now();
  const report = {
    startTime: new Date().toISOString(),
    config: CONFIG,
    stats: {
      primaryItems: 0,
      secondaryItems: 0,
      exactMatches: 0,
      fuzzyMatches: 0,
      noMatches: 0,
      duplicatesSkipped: 0
    },
    matches: [],
    unmatched: [],
    errors: []
  };
  
  try {
    console.log('🚀 Starting improved RSS feed merger...');
    
    // Fetch feeds
    console.log('\n📥 Fetching feeds...');
    const [primaryFeedXML, secondaryFeedXML] = await Promise.all([
      fetchURLWithRetry(CONFIG.primaryFeedUrl),
      fetchURLWithRetry(CONFIG.secondaryFeedUrl)
    ]);
    
    // Parse XML documents
    const parser = new DOMParser();
    const primaryDoc = parser.parseFromString(primaryFeedXML, 'text/xml');
    const secondaryDoc = parser.parseFromString(secondaryFeedXML, 'text/xml');
    
    // Validate XML structure
    const primaryChannel = primaryDoc.getElementsByTagName('channel')[0];
    const secondaryChannel = secondaryDoc.getElementsByTagName('channel')[0];
    
    if (!primaryChannel || !secondaryChannel) {
      throw new Error('Invalid RSS structure: missing channel element');
    }
    
    // Extract items
    const primaryItems = Array.from(primaryDoc.getElementsByTagName('item'));
    const secondaryItems = Array.from(secondaryDoc.getElementsByTagName('item'));
    
    report.stats.primaryItems = primaryItems.length;
    report.stats.secondaryItems = secondaryItems.length;
    
    console.log('📊 Found ' + primaryItems.length + ' primary items, ' + secondaryItems.length + ' secondary items');
    
    // Build efficient indexes for secondary items
    const secondaryIndexes = buildSecondaryIndexes(secondaryItems);
    
    // Create result document
    const resultDoc = parser.parseFromString(primaryFeedXML, 'text/xml');
    const resultChannel = resultDoc.getElementsByTagName('channel')[0];
    
    // Fix self-reference links
    const atomLinks = resultDoc.getElementsByTagName('atom:link');
    for (const link of Array.from(atomLinks || [])) {
      if (link.getAttribute('rel') === 'self') {
        link.setAttribute('href', 'https://arkeditor.github.io/rss-feed-merger/merged_rss_feed.xml');
        console.log('🔧 Fixed self-reference link');
      }
    }
    
    // Remove existing items
    const existingItems = resultDoc.getElementsByTagName('item');
    while (existingItems.length > 0) {
      resultChannel.removeChild(existingItems[0]);
    }
    
    // Process primary items
    console.log('\n🔄 Processing primary items...');
    const processedCores = new Set();
    
    for (let i = 0; i < primaryItems.length; i++) {
      const item = primaryItems[i];
      const metadata = extractItemMetadata(item, 'primary');
      
      if (!metadata.title) {
        console.warn('⚠️ Skipping item ' + (i + 1) + ': no title found');
        continue;
      }
      
      console.log('\n--- Processing ' + (i + 1) + '/' + primaryItems.length + ': "' + metadata.title + '" ---');
      
      // Check for duplicates based on core title
      const normalizedCore = normalizeText(metadata.parsedTitle?.coreTitle, { stripColumnNames: false });
      if (processedCores.has(normalizedCore)) {
        console.log('⚠️ Skipping duplicate core title');
        report.stats.duplicatesSkipped++;
        continue;
      }
      
      // Find best match
      const matchResult = findBestMatch(metadata, secondaryIndexes);
      
      if (matchResult) {
        // Clone and update the item
        const newItem = item.cloneNode(true);
        
        // Update title (clean up CDATA issues)
        const titleElements = newItem.getElementsByTagName('title');
        if (titleElements.length > 0) {
          const titleElement = titleElements[0];
          const cleanTitle = metadata.title;
          
          // Clear and update title
          while (titleElement.firstChild) {
            titleElement.removeChild(titleElement.firstChild);
          }
          titleElement.appendChild(resultDoc.createTextNode(cleanTitle));
        }
        
        // Update link
        const linkElements = newItem.getElementsByTagName('link');
        const cleanLink = reformatNewsmemoryLink(matchResult.match.metadata.link);
        
        if (linkElements.length > 0) {
          linkElements[0].textContent = cleanLink;
        } else {
          const linkElement = resultDoc.createElement('link');
          linkElement.textContent = cleanLink;
          newItem.insertBefore(linkElement, newItem.firstChild);
        }
        
        // Add to result
        resultChannel.appendChild(newItem);
        processedCores.add(normalizedCore);
        
        // Update stats and report
        if (matchResult.strategy === 'exact_full' || matchResult.strategy === 'exact_core') {
          report.stats.exactMatches++;
        } else {
          report.stats.fuzzyMatches++;
        }
        
        report.matches.push({
          primaryTitle: metadata.title,
          secondaryTitle: matchResult.match.metadata.title,
          strategy: matchResult.strategy,
          score: matchResult.score,
          link: cleanLink
        });
        
        console.log('✅ Matched using ' + matchResult.strategy + ' strategy');
      } else {
        report.stats.noMatches++;
        report.unmatched.push({
          title: metadata.title,
          author: metadata.author || metadata.extractedAuthor,
          pubDate: metadata.pubDate?.toISOString()
        });
        
        console.log('❌ No suitable match found');
      }
    }
    
    // Generate output
    const serializer = new XMLSerializer();
    const resultXml = serializer.serializeToString(resultDoc);
    
    fs.writeFileSync(CONFIG.outputFile, resultXml);
    
    // Compile final statistics
    const endTime = Date.now();
    const totalMatched = report.stats.exactMatches + report.stats.fuzzyMatches;
    
    report.endTime = new Date().toISOString();
    report.durationMs = endTime - startTime;
    report.finalItemCount = totalMatched;
    
    console.log('\n📈 MERGE COMPLETE');
    console.log('⏱️  Duration: ' + (report.durationMs / 1000).toFixed(2) + 's');
    console.log('📝 Final feed: ' + totalMatched + ' items');
    console.log('🎯 Exact matches: ' + report.stats.exactMatches);
    console.log('🔍 Fuzzy matches: ' + report.stats.fuzzyMatches);
    console.log('❌ No matches: ' + report.stats.noMatches);
    console.log('⚠️  Duplicates skipped: ' + report.stats.duplicatesSkipped);
    
    // Generate detailed report
    if (CONFIG.generateReport) {
      fs.writeFileSync(CONFIG.reportFile, JSON.stringify(report, null, 2));
      console.log('📊 Detailed report saved to ' + CONFIG.reportFile);
    }
    
    console.log('\n📄 Merged RSS feed saved to ' + CONFIG.outputFile);
    
    return resultXml;
    
  } catch (error) {
    report.errors.push({
      message: error.message,
      stack: error.stack,
      timestamp: new Date().toISOString()
    });
    
    console.error('💥 MERGE FAILED:', error.message);
    
    if (CONFIG.generateReport) {
      fs.writeFileSync(CONFIG.reportFile, JSON.stringify(report, null, 2));
    }
    
    throw error;
  }
}

// Execute the merger
if (require.main === module) {
  console.log('🎬 Starting improved RSS feed merger...');
  mergeFeeds()
    .then(() => console.log('🎉 Script completed successfully!'))
    .catch(err => {
      console.error('💥 Script failed:', err);
      process.exit(1);
    });
}

module.exports = { mergeFeeds, CONFIG };
