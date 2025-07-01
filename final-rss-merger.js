/**
 * Advanced RSS Feed Merger - Significantly Enhanced Version
 * 
 * Key improvements:
 * - Better string similarity algorithms (Levenshtein distance)
 * - Performance optimizations (indexing, caching)
 * - Date-based matching for disambiguation
 * - Configurable matching thresholds
 * - Better error handling and retry logic
 * - More sophisticated scoring system
 * - Comprehensive match reporting
 */

const https = require('https');
const fs = require('fs');
const { DOMParser, XMLSerializer } = require('xmldom');

// Configuration object for easy tuning
const CONFIG = {
  primaryFeedUrl: 'https://www.thearknewspaper.com/blog-feed.xml',
  secondaryFeedUrl: 'https://thearknewspaper-ca.newsmemory.com/rss.php?edition=The%20Ark&section=Main&device=std&images=none&content=abstract',
  
  // Matching thresholds
  exactMatchThreshold: 1.0,
  fuzzyMatchThreshold: 0.65,
  wordOverlapThreshold: 0.5,
  levenshteinThreshold: 0.7,
  
  // Scoring weights
  titleSimilarityWeight: 0.6,
  authorMatchWeight: 0.25,
  columnMatchWeight: 0.1,
  dateProximityWeight: 0.05,
  
  // Network settings
  requestTimeout: 10000,
  maxRetries: 3,
  retryDelay: 1000,
  
  // Output settings
  outputFile: 'merged_rss_feed.xml',
  generateReport: true,
  reportFile: 'merge_report.json'
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
  ['&', 'and'],
  ['w/', 'with'],
  ['st.', 'street'],
  ['rd.', 'road'],
  ['ave.', 'avenue']
]);

// Compiled regex patterns for performance
const PATTERNS = {
  htmlEntity: /&#?\w+;/g,
  cdata: /\s*<!\[CDATA\[(.*?)\]\]>\s*/,
  newsmemoryLink: /newsmemory\.com\/rss\.php\?date=(\d+)&edition=([^&]+)&subsection=Main&page=\d+theark(\d+)_w-o[^\.]+\.pdf\.(\d+)&id=art_(\d+)\.xml/,
  bylineExtraction: /<!\[CDATA\[By\s+([A-Z]+\s+[A-Z]+)/i,
  whitespace: /\s+/g,
  punctuation: /[^\w\s]/g,
  leadingArticles: /^(the|a|an)\s+/i,
  commonPrepositions: /\s+(to|in|at|on|by|with|for)\s+/gi
};

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
        matrix[j][i - 1] + 1,     // deletion
        matrix[j - 1][i] + 1,     // insertion
        matrix[j - 1][i - 1] + indicator // substitution
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
  const maxLength = Math.max(str1.length, str2.length);
  if (maxLength === 0) return 1;
  return 1 - (levenshteinDistance(str1, str2) / maxLength);
}

/**
 * Enhanced HTML entity decoder with more entities
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
 * HTTP request with timeout and retry logic
 */
async function fetchURLWithRetry(url, retries = CONFIG.maxRetries) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      console.log(`Fetching ${url} (attempt ${attempt}/${retries})`);
      
      const data = await new Promise((resolve, reject) => {
        const request = https.get(url, { timeout: CONFIG.requestTimeout }, (res) => {
          if (res.statusCode !== 200) {
            reject(new Error(`HTTP ${res.statusCode}: ${res.statusMessage}`));
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
      console.log(`Attempt ${attempt} failed: ${error.message}`);
      
      if (attempt === retries) {
        throw new Error(`Failed to fetch ${url} after ${retries} attempts: ${error.message}`);
      }
      
      // Wait before retry
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
    // Try standard RFC formats first
    let date = new Date(dateString);
    if (!isNaN(date.getTime())) return date;
    
    // Try parsing other common formats
    const formats = [
      /(\d{4})-(\d{2})-(\d{2})/,  // YYYY-MM-DD
      /(\d{2})\/(\d{2})\/(\d{4})/, // MM/DD/YYYY
      /(\d{1,2})\s+(\w+)\s+(\d{4})/ // D Month YYYY
    ];
    
    for (const format of formats) {
      const match = dateString.match(format);
      if (match) {
        date = new Date(dateString);
        if (!isNaN(date.getTime())) return date;
      }
    }
  } catch (error) {
    console.warn(`Failed to parse date: ${dateString}`);
  }
  
  return null;
}

/**
 * Calculate date proximity score (closer dates = higher score)
 */
function calculateDateProximity(date1, date2) {
  if (!date1 || !date2) return 0;
  
  const diffMs = Math.abs(date1.getTime() - date2.getTime());
  const diffDays = diffMs / (1000 * 60 * 60 * 24);
  
  // Score decreases exponentially with distance
  return Math.exp(-diffDays / 7); // 50% score at 5 days, ~0% at 3 weeks
}

/**
 * Enhanced column title parser with better error handling
 */
function parseColumnTitle(title) {
  if (!title) return { columnName: null, coreTitle: title || '', fullTitle: title || '' };
  
  const decodedTitle = decodeHtmlEntities(title.trim());
  
  for (const columnName of COLUMN_NAMES) {
    const pattern = new RegExp(`^${columnName}\\s*:\\s*(.+)$`, 'i');
    const match = decodedTitle.match(pattern);
    
    if (match) {
      return {
        columnName: columnName,
        coreTitle: match[1].trim(),
        fullTitle: decodedTitle
      };
    }
  }
  
  return {
    columnName: null,
    coreTitle: decodedTitle,
    fullTitle: decodedTitle
  };
}

/**
 * Advanced text normalization with better handling
 */
function normalizeText(text, options = {}) {
  if (!text) return '';
  
  const {
    stripColumnNames = true,
    applyNormalizations = true,
    removeStopWords = true
  } = options;
  
  let workingText = text;
  
  // Parse column information if stripping is enabled
  if (stripColumnNames) {
    const parsed = parseColumnTitle(text);
    workingText = parsed.coreTitle;
  } else {
    workingText = decodeHtmlEntities(text);
  }
  
  // Apply title normalizations
  if (applyNormalizations) {
    for (const [from, to] of TITLE_NORMALIZATIONS) {
      workingText = workingText.replace(new RegExp(from, 'gi'), to);
    }
  }
  
  // Normalize text structure
  let normalized = workingText.trim()
    .replace(PATTERNS.whitespace, ' ')
    .replace(PATTERNS.punctuation, '')
    .toLowerCase();
  
  // Remove common words that don't add meaning
  if (removeStopWords) {
    normalized = normalized
      .replace(PATTERNS.leadingArticles, '')
      .replace(PATTERNS.commonPrepositions, ' ')
      .replace(/\b(sf|san francisco|bay area)\b/g, '') // Location normalization
      .replace(/\b(award winning|awardwinning)\b/g, '');
  }
  
  return normalized.replace(PATTERNS.whitespace, ' ').trim();
}

/**
 * Extract comprehensive metadata from an RSS item
 */
function extractItemMetadata(item) {
  const metadata = {
    title: null,
    parsedTitle: null,
    link: null,
    author: null,
    extractedAuthor: null,
    pubDate: null,
    description: null,
    guid: null
  };
  
  // Extract title
  const titleElements = item.getElementsByTagName('title');
  if (titleElements.length > 0) {
    let title = titleElements[0].textContent;
    
    if (title.includes('CDATA')) {
      const cdataMatch = title.match(PATTERNS.cdata);
      title = cdataMatch ? cdataMatch[1].trim() : title.trim();
    }
    
    metadata.title = decodeHtmlEntities(title);
    metadata.parsedTitle = parseColumnTitle(metadata.title);
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
  
  // Extract author from <full> content
  const fullElements = item.getElementsByTagName('full');
  if (fullElements.length > 0) {
    const fullContent = fullElements[0].textContent;
    if (fullContent && fullContent.includes('<![CDATA[By')) {
      const authorMatch = fullContent.match(PATTERNS.bylineExtraction);
      if (authorMatch && authorMatch[1]) {
        metadata.extractedAuthor = authorMatch[1].trim()
          .split(/\s+/)
          .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
          .join(' ');
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
    total: 0
  };
  
  // Title similarity using multiple methods
  if (primary.parsedTitle && secondary.parsedTitle) {
    const primaryCore = normalizeText(primary.parsedTitle.coreTitle, { stripColumnNames: false });
    const secondaryCore = normalizeText(secondary.parsedTitle.coreTitle, { stripColumnNames: false });
    
    // Direct substring match gets highest score
    if (primaryCore.includes(secondaryCore) || secondaryCore.includes(primaryCore)) {
      scores.titleSimilarity = 1.0;
    } else {
      // Use Levenshtein similarity
      scores.titleSimilarity = stringSimilarity(primaryCore, secondaryCore);
      
      // Also check word overlap for additional validation
      const words1 = primaryCore.split(/\s+/).filter(w => w.length > 3);
      const words2 = secondaryCore.split(/\s+/).filter(w => w.length > 3);
      
      let matchCount = 0;
      for (const word of words1) {
        if (words2.includes(word)) matchCount++;
      }
      
      const wordOverlap = matchCount / Math.max(words1.length, words2.length, 1);
      
      // Use the better of the two similarity measures
      scores.titleSimilarity = Math.max(scores.titleSimilarity, wordOverlap);
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
  
  // Date proximity
  if (primary.pubDate && secondary.pubDate) {
    scores.dateProximity = calculateDateProximity(primary.pubDate, secondary.pubDate);
  }
  
  // Calculate weighted total
  scores.total = 
    scores.titleSimilarity * CONFIG.titleSimilarityWeight +
    scores.authorMatch * CONFIG.authorMatchWeight +
    scores.columnMatch * CONFIG.columnMatchWeight +
    scores.dateProximity * CONFIG.dateProximityWeight;
  
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
  
  for (const item of secondaryItems) {
    const metadata = extractItemMetadata(item);
    
    // Only process items with newsmemory links
    if (!metadata.link?.includes('newsmemory.com')) continue;
    
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
  }
  
  console.log(`Built indexes for ${indexes.items.length} secondary items`);
  console.log(`- ${indexes.byNormalizedTitle.size} unique normalized titles`);
  console.log(`- ${indexes.byNormalizedCore.size} unique normalized core titles`);
  console.log(`- ${indexes.byAuthor.size} unique authors`);
  console.log(`- ${indexes.byColumn.size} unique columns`);
  
  return indexes;
}

/**
 * Find best match for a primary item using multiple strategies
 */
function findBestMatch(primaryMetadata, secondaryIndexes) {
  const normalizedFull = normalizeText(primaryMetadata.title, { stripColumnNames: false });
  const normalizedCore = normalizeText(primaryMetadata.parsedTitle?.coreTitle, { stripColumnNames: false });
  
  // Strategy 1: Exact match by normalized full title
  const exactFullMatches = secondaryIndexes.byNormalizedTitle.get(normalizedFull) || [];
  if (exactFullMatches.length > 0) {
    console.log(`Found exact full title match`);
    return {
      match: exactFullMatches[0],
      score: calculateMatchScore(primaryMetadata, exactFullMatches[0].metadata),
      strategy: 'exact_full'
    };
  }
  
  // Strategy 2: Exact match by normalized core title
  const exactCoreMatches = secondaryIndexes.byNormalizedCore.get(normalizedCore) || [];
  if (exactCoreMatches.length > 0) {
    console.log(`Found exact core title match`);
    return {
      match: exactCoreMatches[0],
      score: calculateMatchScore(primaryMetadata, exactCoreMatches[0].metadata),
      strategy: 'exact_core'
    };
  }
  
  // Strategy 3: Fuzzy matching with comprehensive scoring
  console.log(`Performing fuzzy matching across ${secondaryIndexes.items.length} items...`);
  
  let bestMatch = null;
  let bestScore = null;
  
  // Narrow down candidates using author or column if available
  let candidates = secondaryIndexes.items;
  
  const primaryAuthor = (primaryMetadata.author || primaryMetadata.extractedAuthor || '').toLowerCase();
  const primaryColumn = primaryMetadata.parsedTitle?.columnName?.toLowerCase();
  
  if (primaryAuthor && secondaryIndexes.byAuthor.has(primaryAuthor)) {
    candidates = secondaryIndexes.byAuthor.get(primaryAuthor);
    console.log(`Narrowed to ${candidates.length} candidates by author: ${primaryAuthor}`);
  } else if (primaryColumn && secondaryIndexes.byColumn.has(primaryColumn)) {
    candidates = secondaryIndexes.byColumn.get(primaryColumn);
    console.log(`Narrowed to ${candidates.length} candidates by column: ${primaryColumn}`);
  }
  
  for (const candidate of candidates) {
    const score = calculateMatchScore(primaryMetadata, candidate.metadata);
    
    if (score.total > CONFIG.fuzzyMatchThreshold && (!bestScore || score.total > bestScore.total)) {
      bestMatch = candidate;
      bestScore = score;
    }
  }
  
  if (bestMatch) {
    console.log(`Best fuzzy match found with score ${bestScore.total.toFixed(3)}`);
    console.log(`  Title similarity: ${bestScore.titleSimilarity.toFixed(3)}`);
    console.log(`  Author match: ${bestScore.authorMatch.toFixed(3)}`);
    console.log(`  Column match: ${bestScore.columnMatch.toFixed(3)}`);
    console.log(`  Date proximity: ${bestScore.dateProximity.toFixed(3)}`);
    
    return {
      match: bestMatch,
      score: bestScore,
      strategy: 'fuzzy'
    };
  }
  
  return null;
}

/**
 * Format newsmemory URLs to cleaner format
 */
function reformatNewsmemoryLink(url) {
  const match = url.match(PATTERNS.newsmemoryLink);
  
  if (match) {
    const [, date, edition, page, , artid] = match;
    const cleanEdition = decodeURIComponent(edition.replace(/\+/g, ' '));
    
    return `https://thearknewspaper-ca.newsmemory.com?selDate=${date}&goTo=${page}&artid=${artid}&editionStart=${encodeURIComponent(cleanEdition)}`;
  }
  
  console.warn(`Could not reformat URL: ${url}`);
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
      secondaryWithLinks: 0,
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
    console.log('🚀 Starting advanced RSS feed merger...');
    
    // Fetch feeds with retry logic
    console.log('📥 Fetching feeds...');
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
    
    console.log(`📊 Found ${primaryItems.length} primary items, ${secondaryItems.length} secondary items`);
    
    // Build efficient indexes for secondary items
    console.log('🔍 Building secondary feed indexes...');
    const secondaryIndexes = buildSecondaryIndexes(secondaryItems);
    report.stats.secondaryWithLinks = secondaryIndexes.items.length;
    
    // Create result document
    const resultDoc = parser.parseFromString(primaryFeedXML, 'text/xml');
    const resultChannel = resultDoc.getElementsByTagName('channel')[0];
    
    // Fix self-reference links
    const atomLinks = resultDoc.getElementsByTagName('atom:link');
    for (const link of atomLinks) {
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
    console.log('🔄 Processing primary items...');
    const processedCores = new Set();
    
    for (let i = 0; i < primaryItems.length; i++) {
      const item = primaryItems[i];
      const metadata = extractItemMetadata(item);
      
      if (!metadata.title) {
        console.warn(`Skipping item ${i + 1}: no title found`);
        continue;
      }
      
      console.log(`\n--- Processing: "${metadata.title}" ---`);
      
      // Check for duplicates based on core title
      const normalizedCore = normalizeText(metadata.parsedTitle?.coreTitle, { stripColumnNames: false });
      if (processedCores.has(normalizedCore)) {
        console.log(`⚠️ Skipping duplicate core title`);
        report.stats.duplicatesSkipped++;
        continue;
      }
      
      // Find best match
      const matchResult = findBestMatch(metadata, secondaryIndexes);
      
      if (matchResult) {
        // Clone and update the item
        const newItem = item.cloneNode(true);
        
        // Fix HTML entities in title
        const titleElements = newItem.getElementsByTagName('title');
        if (titleElements.length > 0) {
          const titleElement = titleElements[0];
          const cleanTitle = decodeHtmlEntities(metadata.title);
          
          // Clear and update title
          while (titleElement.firstChild) {
            titleElement.removeChild(titleElement.firstChild);
          }
          
          if (metadata.title.includes('CDATA')) {
            titleElement.appendChild(resultDoc.createCDATASection(cleanTitle));
          } else {
            titleElement.appendChild(resultDoc.createTextNode(cleanTitle));
          }
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
        
        console.log(`✅ Matched using ${matchResult.strategy} strategy`);
      } else {
        report.stats.noMatches++;
        report.unmatched.push({
          title: metadata.title,
          author: metadata.author || metadata.extractedAuthor,
          pubDate: metadata.pubDate?.toISOString()
        });
        
        console.log(`❌ No suitable match found`);
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
    
    console.log(`\n📈 MERGE COMPLETE`);
    console.log(`⏱️  Duration: ${(report.durationMs / 1000).toFixed(2)}s`);
    console.log(`📝 Final feed: ${totalMatched} items`);
    console.log(`🎯 Exact matches: ${report.stats.exactMatches}`);
    console.log(`🔍 Fuzzy matches: ${report.stats.fuzzyMatches}`);
    console.log(`❌ No matches: ${report.stats.noMatches}`);
    console.log(`⚠️  Duplicates skipped: ${report.stats.duplicatesSkipped}`);
    
    // Generate detailed report
    if (CONFIG.generateReport) {
      fs.writeFileSync(CONFIG.reportFile, JSON.stringify(report, null, 2));
      console.log(`📊 Detailed report saved to ${CONFIG.reportFile}`);
    }
    
    // Final validation
    await validateOutput(resultXml, report);
    
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

/**
 * Validate the output feed
 */
async function validateOutput(xml, report) {
  console.log('\n🔍 Validating output...');
  
  const parser = new DOMParser();
  const doc = parser.parseFromString(xml, 'text/xml');
  const items = doc.getElementsByTagName('item');
  
  const issues = [];
  const titleSet = new Set();
  
  for (let i = 0; i < items.length; i++) {
    const item = items[i];
    
    // Check for proper links
    const linkElements = item.getElementsByTagName('link');
    if (linkElements.length === 0) {
      issues.push(`Item ${i + 1}: Missing link element`);
    } else {
      const link = linkElements[0].textContent;
      if (link.includes('thearknewspaper.com')) {
        issues.push(`Item ${i + 1}: Still contains thearknewspaper.com link`);
      }
      if (!link.includes('newsmemory.com')) {
        issues.push(`Item ${i + 1}: Missing newsmemory.com link`);
      }
    }
    
    // Check for title issues
    const titleElements = item.getElementsByTagName('title');
    if (titleElements.length > 0) {
      const title = titleElements[0].textContent;
      
      if (PATTERNS.htmlEntity.test(title)) {
        issues.push(`Item ${i + 1}: Title contains unresolved HTML entities: ${title}`);
      }
      
      const normalizedTitle = normalizeText(title);
      if (titleSet.has(normalizedTitle)) {
        issues.push(`Item ${i + 1}: Duplicate title detected: ${title}`);
      }
      titleSet.add(normalizedTitle);
    } else {
      issues.push(`Item ${i + 1}: Missing title element`);
    }
  }
  
  if (issues.length > 0) {
    console.log(`⚠️  Found ${issues.length} validation issues:`);
    issues.forEach(issue => console.log(`   ${issue}`));
    report.validationIssues = issues;
  } else {
    console.log('✅ Validation passed - no issues found');
  }
}

// Execute the merger
if (require.main === module) {
  console.log('🎬 Starting enhanced RSS feed merger...');
  mergeFeeds()
    .then(() => console.log('🎉 Script completed successfully!'))
    .catch(err => {
      console.error('💥 Script failed:', err);
      process.exit(1);
    });
}

module.exports = { mergeFeeds, CONFIG };
