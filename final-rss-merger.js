/**
 * Direct XML Link Replacement
 * 
 * This script takes a brute-force approach:
 * 1. Only keep items from the primary feed
 * 2. For each item, search for a matching title in the secondary feed
 * 3. If found, directly replace the link text
 * 4. Drop all non-matching items 
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
      // Extract title - handle CDATA properly
      const titleMatch = item.match(/<title>\s*(?:<!\[CDATA\[(.*?)\]\]>|([^<]*))\s*<\/title>/s);
      if (!titleMatch) continue;
      
      const titleContent = titleMatch[1] || titleMatch[2];
      if (!titleContent) continue;
      
      const cleanedTitle = cleanTitle(titleContent);
      
      // Extract link (only keep links to newsmemory)
      const linkMatch = item.match(/<link>\s*(.*?)\s*<\/link>/s);
      if (!linkMatch) continue;
      
      const link = linkMatch[1].trim();
      if (!link.includes('newsmemory.com')) continue;
      
      secondaryLinksByTitle[cleanedTitle] = link;
    }
    
    console.log(`Found ${Object.keys(secondaryLinksByTitle).length} unique titles with newsmemory links`);
    
    // Process primary items
    let mergedFeedXML = headerXML;
    const processedTitles = new Set();
    let matchCount = 0;
    
    for (const item of primaryItems) {
      // Extract title
      const titleMatch = item.match(/<title>\s*(?:<!\[CDATA\[(.*?)\]\]>|([^<]*))\s*<\/title>/s);
      if (!titleMatch) continue;
      
      const titleContent = titleMatch[1] || titleMatch[2];
      if (!titleContent) continue;
      
      const cleanedTitle = cleanTitle(titleContent);
      
      // Skip duplicates
      if (processedTitles.has(cleanedTitle)) {
        console.log(`Skipping duplicate: "${titleContent.trim()}"`);
        continue;
      }
      
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
            `<link>\n${secondaryLink}\n</link>`
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
        processedTitles.add(cleanedTitle);
        matchCount++;
        
        console.log(`✓ Added item with secondary link: "${titleContent.trim()}"`);
      } else {
        console.log(`✗ No secondary link for: "${titleContent.trim()}"`);
      }
    }
    
    // Add footer
    mergedFeedXML += footerXML;
    
    console.log(`\nCompleted merge with ${matchCount} items`);
    
    // Perform one final verification to ensure no duplicate items
    const finalItemsMatches = [...mergedFeedXML.matchAll(/<item>([\s\S]*?)<\/item>/g)];
    const finalItems = finalItemsMatches.map(match => match[0]);
    
    console.log(`Final feed contains ${finalItems.length} items`);
    
    // Check that all links are from newsmemory
    const linkPatterns = finalItems.map(item => {
      const linkMatch = item.match(/<link>\s*(.*?)\s*<\/link>/s);
      return linkMatch ? linkMatch[1].trim() : null;
    }).filter(Boolean);
    
    const nonNewsmemoryLinks = linkPatterns.filter(link => !link.includes('newsmemory.com'));
    
    if (nonNewsmemoryLinks.length > 0) {
      console.log(`Warning: Found ${nonNewsmemoryLinks.length} links not from newsmemory.com`);
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
console.log('Starting direct XML link replacement...');
mergeFeeds().then(() => {
  console.log('Script completed successfully!');
}).catch(err => {
  console.error('Script failed with error:', err);
  process.exit(1);
});
