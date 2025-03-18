/**
 * Simple and Reliable RSS Feed Merger
 * 
 * This script takes a direct approach with clear requirements:
 * 1. Keep ONLY items that have newsmemory.com links
 * 2. No duplicate titles allowed
 * 3. No thearknewspaper.com links allowed
 */

const https = require('https');
const fs = require('fs');
const { DOMParser, XMLSerializer } = require('xmldom');

// Define feed URLs
const PRIMARY_FEED_URL = 'https://www.thearknewspaper.com/blog-feed.xml';
const SECONDARY_FEED_URL = 'https://thearknewspaper-ca.newsmemory.com/rss.php?edition=The%20Ark&section=Main&device=std&images=none&content=abstract';

// Function to fetch the content of a URL
function fetchURL(url) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      if (res.statusCode !== 200) {
        reject(new Error(`Request failed with status code ${res.statusCode}`));
        return;
      }
      
      let data = '';
      res.on('data', (chunk) => {
        data += chunk;
      });
      res.on('end', () => {
        resolve(data);
      });
    }).on('error', (err) => {
      reject(err);
    });
  });
}

// Clean text for comparison - more flexible matching
function cleanText(text) {
  if (!text) return '';
  
  // First normalize the text
  let cleaned = text.trim()
    .replace(/\s+/g, ' ')        // Normalize whitespace
    .replace(/[^\w\s]/g, '')     // Remove punctuation
    .toLowerCase();              // Convert to lowercase
  
  // Remove common prefixes/suffixes and filler words
  cleaned = cleaned
    .replace(/^(the|a|an) /, '')                // Remove leading articles
    .replace(/ (to|in|at|on|by|with|for) /g, ' ') // Remove common prepositions
    .replace(/sf|san francisco/g, '')           // Normalize location references
    
    // Remove words that might appear in one title but not the other
    .replace(/awardwinning|award winning/g, '')
    
    // Normalize whitespace again after all the replacements
    .replace(/\s+/g, ' ')
    .trim();
  
  return cleaned;
}

// Main function
async function mergeFeeds() {
  try {
    console.log('Starting RSS feed merger...');
    
    // Fetch both feeds
    console.log('Fetching primary feed...');
    const primaryFeedXML = await fetchURL(PRIMARY_FEED_URL);
    
    console.log('Fetching secondary feed...');
    const secondaryFeedXML = await fetchURL(SECONDARY_FEED_URL);
    
    // Parse XML
    const parser = new DOMParser();
    const primaryDoc = parser.parseFromString(primaryFeedXML, 'text/xml');
    const secondaryDoc = parser.parseFromString(secondaryFeedXML, 'text/xml');
    
    // Find channel element
    const primaryChannel = primaryDoc.getElementsByTagName('channel')[0];
    const secondaryChannel = secondaryDoc.getElementsByTagName('channel')[0];
    
    if (!primaryChannel || !secondaryChannel) {
      throw new Error('Could not find channel element in one of the feeds');
    }
    
    // Fix self-reference link
    const atomLinks = primaryDoc.getElementsByTagName('atom:link');
    for (let i = 0; i < atomLinks.length; i++) {
      const link = atomLinks[i];
      if (link.getAttribute('rel') === 'self') {
        link.setAttribute('href', 'https://arkeditor.github.io/rss-feed-merger/merged_rss_feed.xml');
      }
    }
    
    // Extract items from both feeds
    const primaryItems = primaryDoc.getElementsByTagName('item');
    const secondaryItems = secondaryDoc.getElementsByTagName('item');
    
    console.log(`Found ${primaryItems.length} items in primary feed`);
    console.log(`Found ${secondaryItems.length} items in secondary feed`);
    
    // Build a map of titles to secondary feed links
    const newsmemoryLinksByTitle = {};
    const secondaryTitleDetails = [];
    
    for (let i = 0; i < secondaryItems.length; i++) {
      const item = secondaryItems[i];
      
      // Get title
      const titleElements = item.getElementsByTagName('title');
      if (titleElements.length === 0) continue;
      
      const titleElement = titleElements[0];
      let title = titleElement.textContent;
      
      // Handle CDATA
      if (title.includes('CDATA')) {
        const cdataMatch = title.match(/\s*<!\[CDATA\[(.*?)\]\]>\s*/);
        title = cdataMatch ? cdataMatch[1].trim() : title.trim();
      }
      
      // Get creator if available for additional matching
      let creator = null;
      const creatorElements = item.getElementsByTagName('dc:creator');
      if (creatorElements.length > 0) {
        creator = creatorElements[0].textContent.trim();
      }
      
      const cleanedTitle = cleanText(title);
      
      // Get link
      const linkElements = item.getElementsByTagName('link');
      if (linkElements.length === 0) continue;
      
      const linkElement = linkElements[0];
      const link = linkElement.textContent.trim();
      
      // Only keep newsmemory links
      if (link.includes('newsmemory.com')) {
        newsmemoryLinksByTitle[cleanedTitle] = link;
        
        // Store additional details for better matching
        secondaryTitleDetails.push({
          originalTitle: title,
          cleanedTitle: cleanedTitle,
          creator: creator,
          link: link
        });
      }
    }
    
    // Sort secondary titles by length (descending) to prioritize longer, more specific titles in matching
    secondaryTitleDetails.sort((a, b) => b.cleanedTitle.length - a.cleanedTitle.length);
    
    console.log(`Found ${Object.keys(newsmemoryLinksByTitle).length} titles with newsmemory links`);
    
    // Create a new result document starting with primary feed
    const resultDoc = parser.parseFromString(primaryFeedXML, 'text/xml');
    const resultChannel = resultDoc.getElementsByTagName('channel')[0];
    
    // Remove all existing items from result document
    const existingItems = resultDoc.getElementsByTagName('item');
    while (existingItems.length > 0) {
      resultChannel.removeChild(existingItems[0]);
    }
    
    // Process primary items
    const processedTitles = new Set();
    let matchCount = 0;
    
    // Advanced title matching: store all secondary titles for fuzzy matching
    const allSecondaryTitles = Object.keys(newsmemoryLinksByTitle);
    
    for (let i = 0; i < primaryItems.length; i++) {
      const item = primaryItems[i];
      
      // Get title
      const titleElements = item.getElementsByTagName('title');
      if (titleElements.length === 0) continue;
      
      const titleElement = titleElements[0];
      let title = titleElement.textContent;
      
      // Handle CDATA
      if (title.includes('CDATA')) {
        const cdataMatch = title.match(/\s*<!\[CDATA\[(.*?)\]\]>\s*/);
        title = cdataMatch ? cdataMatch[1].trim() : title.trim();
      }
      
      const cleanedTitle = cleanText(title);
      
      // Skip if we've already processed this title
      if (processedTitles.has(cleanedTitle)) {
        console.log(`Skipping duplicate title: "${title}"`);
        continue;
      }
      
      // Get creator/author if available (for additional matching criteria)
      let creator = null;
      const creatorElements = item.getElementsByTagName('dc:creator');
      if (creatorElements.length > 0) {
        creator = creatorElements[0].textContent.trim().toLowerCase();
      }
      
      // Find best match in secondary feed using more flexible matching
      let bestMatchTitle = null;
      let bestMatchScore = 0;
      
      for (const secondaryTitle of allSecondaryTitles) {
        // Calculate match score (simple similarity check)
        const secondaryClean = secondaryTitle;
        
        // Check if one title contains most of the words from the other
        const words1 = cleanedTitle.split(' ');
        const words2 = secondaryClean.split(' ');
        
        // Count matching words
        let matchingWords = 0;
        for (const word of words1) {
          if (word.length > 3 && words2.includes(word)) { // Only count substantial words
            matchingWords++;
          }
        }
        
        const score = matchingWords / Math.max(words1.length, words2.length);
        
        // Consider it a match if:
        // 1. At least 60% of significant words match, OR
        // 2. We have a creator name that matches and at least 40% of words match
        if ((score > 0.6) || (creator && secondaryTitle.includes(creator) && score > 0.4)) {
          if (score > bestMatchScore) {
            bestMatchScore = score;
            bestMatchTitle = secondaryTitle;
          }
        }
      }
      
      // Use direct match if available, otherwise use best fuzzy match
      let newsmemoryLink = newsmemoryLinksByTitle[cleanedTitle];
      
      if (!newsmemoryLink && bestMatchTitle) {
        newsmemoryLink = newsmemoryLinksByTitle[bestMatchTitle];
        console.log(`Fuzzy matched: "${title}" with secondary title (${bestMatchScore.toFixed(2)} score)`);
      }
      
      if (newsmemoryLink) {
        // Clone the item
        const newItem = item.cloneNode(true);
        
        // Replace or add link
        const linkElements = newItem.getElementsByTagName('link');
        
        if (linkElements.length > 0) {
          // Replace existing link
          const linkElement = linkElements[0];
          linkElement.textContent = newsmemoryLink;
        } else {
          // Add new link element after title
          const link = resultDoc.createElement('link');
          link.textContent = newsmemoryLink;
          
          // Insert after title
          newItem.insertBefore(link, titleElement.nextSibling);
        }
        
        // Add to result document
        resultChannel.appendChild(newItem);
        processedTitles.add(cleanedTitle);
        matchCount++;
        
        console.log(`✓ Added item with newsmemory link: "${title}"`);
      } else {
        console.log(`✗ No newsmemory link for: "${title}" - Skipping`);
      }
    }
    
    console.log(`\nCompleted merge with ${matchCount} items with newsmemory links`);
    
    // Final verification
    const finalItems = resultDoc.getElementsByTagName('item');
    console.log(`Final feed contains ${finalItems.length} items`);
    
    // Convert result document back to XML string
    const serializer = new XMLSerializer();
    const resultXml = serializer.serializeToString(resultDoc);
    
    // Save output
    fs.writeFileSync('merged_rss_feed.xml', resultXml);
    console.log(`\nMerged feed saved to merged_rss_feed.xml`);
    
    // Perform a final check to verify contents
    console.log("\nPerforming final verification...");
    
    let errorFound = false;
    
    // Check for thearknewspaper.com links
    for (let i = 0; i < finalItems.length; i++) {
      const item = finalItems[i];
      const linkElements = item.getElementsByTagName('link');
      
      if (linkElements.length > 0) {
        const link = linkElements[0].textContent;
        
        if (link.includes('thearknewspaper.com')) {
          console.log(`ERROR: Item #${i+1} still has a thearknewspaper.com link`);
          errorFound = true;
        }
        
        if (!link.includes('newsmemory.com')) {
          console.log(`ERROR: Item #${i+1} doesn't have a newsmemory.com link`);
          errorFound = true;
        }
      }
    }
    
    // Check for duplicate titles
    const titleSet = new Set();
    for (let i = 0; i < finalItems.length; i++) {
      const item = finalItems[i];
      const titleElements = item.getElementsByTagName('title');
      
      if (titleElements.length > 0) {
        let title = titleElements[0].textContent;
        
        // Handle CDATA
        if (title.includes('CDATA')) {
          const cdataMatch = title.match(/\s*<!\[CDATA\[(.*?)\]\]>\s*/);
          title = cdataMatch ? cdataMatch[1].trim() : title.trim();
        }
        
        const cleanedTitle = cleanText(title);
        
        if (titleSet.has(cleanedTitle)) {
          console.log(`ERROR: Duplicate title found: "${title}"`);
          errorFound = true;
        }
        
        titleSet.add(cleanedTitle);
      }
    }
    
    if (errorFound) {
      console.log("\n⚠️ WARNING: Verification errors found. The feed may not be correct.");
    } else {
      console.log("\n✅ SUCCESS: All verification checks passed!");
    }
    
    return resultXml;
  } catch (error) {
    console.error('Error:', error.message);
    console.error(error.stack);
    throw error;
  }
}

// Run the script
console.log('Starting enhanced RSS feed merger...');
mergeFeeds().then(() => {
  console.log('Script completed successfully!');
}).catch(err => {
  console.error('Script failed with error:', err);
  process.exit(1);
});
