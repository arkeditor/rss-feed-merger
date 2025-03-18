/**
 * Improved RSS Feed Merger
 * 
 * This script merges two RSS feeds:
 * 1. It uses the primary feed for all metadata and content
 * 2. It replaces links with those from the secondary feed when available
 * 3. Prevents duplicate items
 * 4. Only includes items with newsmemory.com links
 */

const https = require('https');
const fs = require('fs');

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

// Clean title for comparison
function cleanTitle(title) {
  return title.trim()
    .replace(/\s+/g, ' ')          // Normalize whitespace
    .replace(/[^\w\s]/g, '')       // Remove punctuation
    .toLowerCase();                 // Convert to lowercase
}

// Extract title from an item
function extractTitle(item) {
  const titleMatch = item.match(/<title>\s*(?:<!\[CDATA\[(.*?)\]\]>|([^<]*))\s*<\/title>/s);
  if (!titleMatch) return null;
  
  const titleContent = titleMatch[1] || titleMatch[2];
  if (!titleContent) return null;
  
  return titleContent.trim();
}

// Extract link from an item
function extractLink(item) {
  const linkMatch = item.match(/<link>\s*(.*?)\s*<\/link>/s);
  if (!linkMatch) return null;
  
  return linkMatch[1].trim();
}

// Main function
async function mergeFeeds() {
  try {
    console.log('Fetching feeds...');
    const primaryFeedXML = await fetchURL(PRIMARY_FEED_URL);
    const secondaryFeedXML = await fetchURL(SECONDARY_FEED_URL);
    
    // Extract the header (everything before the first item)
    const primaryHeaderMatch = primaryFeedXML.match(/([\s\S]*?)<item>/);
    let headerXML = primaryHeaderMatch ? primaryHeaderMatch[1] : '';
    
    // Fix self-reference link in header
    headerXML = headerXML.replace(
      /<atom:link[^>]*rel="self"[^>]*\/>/,
      '<atom:link href="https://arkeditor.github.io/rss-feed-merger/merged_rss_feed.xml" rel="self" type="application/rss+xml"/>'
    );
    
    // Extract the footer (everything after the last item)
    const primaryFooterMatch = primaryFeedXML.match(/<\/item>\s*([\s\S]*?)$/);
    const footerXML = primaryFooterMatch ? primaryFooterMatch[1] : '</channel>\n</rss>';
    
    // Extract all items from primary feed
    const primaryItemsMatches = [...primaryFeedXML.matchAll(/<item>([\s\S]*?)<\/item>/g)];
    const primaryItems = primaryItemsMatches.map(match => match[0]);
    
    // Extract all items from secondary feed
    const secondaryItemsMatches = [...secondaryFeedXML.matchAll(/<item>([\s\S]*?)<\/item>/g)];
    const secondaryItems = secondaryItemsMatches.map(match => match[0]);
    
    console.log(`Found ${primaryItems.length} items in primary feed`);
    console.log(`Found ${secondaryItems.length} items in secondary feed`);
    
    // Build a map of titles to secondary links
    const secondaryLinksByTitle = {};
    
    for (const item of secondaryItems) {
      const title = extractTitle(item);
      if (!title) continue;
      
      const cleanedTitle = cleanTitle(title);
      
      // Extract link (only keep links to newsmemory)
      const link = extractLink(item);
      if (!link || !link.includes('newsmemory.com')) continue;
      
      secondaryLinksByTitle[cleanedTitle] = link;
    }
    
    console.log(`Found ${Object.keys(secondaryLinksByTitle).length} unique titles with newsmemory links`);
    
    // Process primary items
    let mergedFeedXML = headerXML;
    const processedTitles = new Set();
    let matchCount = 0;
    
    // First pass: Group items by title to avoid duplicates
    const primaryItemsByTitle = {};
    for (const item of primaryItems) {
      const title = extractTitle(item);
      if (!title) continue;
      
      const cleanedTitle = cleanTitle(title);
      if (!primaryItemsByTitle[cleanedTitle]) {
        primaryItemsByTitle[cleanedTitle] = item;
      }
    }
    
    // Second pass: Process each unique primary item
    for (const [cleanedTitle, item] of Object.entries(primaryItemsByTitle)) {
      // Check if we have a matching link
      const secondaryLink = secondaryLinksByTitle[cleanedTitle];
      
      if (secondaryLink) {
        // Replace the link in the item
        let modifiedItem = item;
        
        // Check if the item has a link tag
        const hasLink = /<link>[\s\S]*?<\/link>/i.test(modifiedItem);
        
        if (hasLink) {
          // Replace existing link - using regex with lookahead/lookbehind to ensure we get the whole tag
          modifiedItem = modifiedItem.replace(
            /<link>[\s\S]*?<\/link>/i,
            `<link>${secondaryLink}</link>`
          );
        } else {
          // Add a link tag after the title
          modifiedItem = modifiedItem.replace(
            /<\/title>/,
            `</title>\n  <link>${secondaryLink}</link>`
          );
        }
        
        // Add to merged feed
        mergedFeedXML += modifiedItem;
        matchCount++;
        
        const title = extractTitle(item);
        console.log(`✓ Added item with secondary link: "${title}"`);
      } else {
        const title = extractTitle(item);
        console.log(`✗ No secondary link for: "${title}"`);
      }
    }
    
    // Add footer
    mergedFeedXML += footerXML;
    
    console.log(`\nCompleted merge with ${matchCount} items`);
    
    // Perform verification to ensure all links are from newsmemory
    const finalItemsMatches = [...mergedFeedXML.matchAll(/<item>([\s\S]*?)<\/item>/g)];
    const finalItems = finalItemsMatches.map(match => match[0]);
    
    console.log(`Final feed contains ${finalItems.length} items`);
    
    // Check that all links are from newsmemory
    const links = finalItems.map(item => extractLink(item)).filter(Boolean);
    const nonNewsmemoryLinks = links.filter(link => !link.includes('newsmemory.com'));
    
    if (nonNewsmemoryLinks.length > 0) {
      console.log(`Warning: Found ${nonNewsmemoryLinks.length} links not from newsmemory.com`);
      console.log('Non-newsmemory links:', nonNewsmemoryLinks);
      
      // Remove items with non-newsmemory links
      console.log('Removing items with non-newsmemory links...');
      
      // Re-extract all items
      const cleanItemsMatches = [...mergedFeedXML.matchAll(/<item>([\s\S]*?)<\/item>/g)];
      const allItems = cleanItemsMatches.map(match => match[0]);
      
      // Filter to keep only items with newsmemory links
      let cleanedFeedXML = headerXML;
      let keepCount = 0;
      
      for (const item of allItems) {
        const link = extractLink(item);
        if (link && link.includes('newsmemory.com')) {
          cleanedFeedXML += item;
          keepCount++;
        }
      }
      
      cleanedFeedXML += footerXML;
      mergedFeedXML = cleanedFeedXML;
      
      console.log(`Kept ${keepCount} items with newsmemory links`);
    } else {
      console.log('All links are from newsmemory.com: PASSED');
    }
    
    // Save the output to a file
    fs.writeFileSync('merged_rss_feed.xml', mergedFeedXML);
    console.log(`\nMerged feed saved to merged_rss_feed.xml`);
    
    return mergedFeedXML;
  } catch (error) {
    console.error('Error:', error.message);
    console.error(error.stack);
    throw error;
  }
}

// Run the script
console.log('Starting RSS feed merger...');
mergeFeeds().then(() => {
  console.log('Script completed successfully!');
}).catch(err => {
  console.error('Script failed with error:', err);
  process.exit(1);
});
