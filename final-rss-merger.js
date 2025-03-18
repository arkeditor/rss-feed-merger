/**
 * Enhanced RSS Feed Merger with Post-Processing Verification
 * 
 * This script strictly enforces the following rules:
 * 1. Only include items from the primary feed structure
 * 2. Replace links with those from the secondary feed when available
 * 3. Filter out duplicates by normalized title
 * 4. Only include items with newsmemory.com links
 * 5. Completely remove any items that don't have newsmemory.com links
 * 6. Post-process the feed to catch and fix any remaining issues:
 *    - Remove any items still containing thearknewspaper.com links
 *    - Check for and remove duplicate titles, keeping only newsmemory.com versions
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
  if (!title) return '';
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

// Check if an item has a link to newsmemory
function hasNewsmemoryLink(item) {
  const linkMatch = item.match(/<link>\s*(.*?)\s*<\/link>/s);
  if (!linkMatch) return false;
  
  const link = linkMatch[1].trim();
  return link.includes('newsmemory.com');
}

// Check if an item has a link to thearknewspaper.com
function hasArkLink(item) {
  const linkMatch = item.match(/<link>\s*(.*?)\s*<\/link>/s);
  if (!linkMatch) return false;
  
  const link = linkMatch[1].trim();
  return link.includes('thearknewspaper.com');
}

// Extract link from an item
function extractLink(item) {
  const linkMatch = item.match(/<link>\s*(.*?)\s*<\/link>/s);
  if (!linkMatch) return null;
  
  return linkMatch[1].trim();
}

// Extract GUID from an item
function extractGuid(item) {
  const guidMatch = item.match(/<guid[^>]*>(.*?)<\/guid>/s);
  return guidMatch ? guidMatch[1].trim() : null;
}

// Extract items from XML content
function extractItems(xmlContent) {
  const itemMatches = [...xmlContent.matchAll(/<item>([\s\S]*?)<\/item>/g)];
  return itemMatches.map(match => match[0]);
}

// Main function
async function mergeFeeds() {
  try {
    console.log('Starting enhanced feed merger...');
    
    console.log('Fetching primary feed...');
    const primaryFeedXML = await fetchURL(PRIMARY_FEED_URL);
    
    console.log('Fetching secondary feed...');
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
    
    // Extract all items from both feeds
    const primaryItems = extractItems(primaryFeedXML);
    const secondaryItems = extractItems(secondaryFeedXML);
    
    console.log(`Found ${primaryItems.length} items in primary feed`);
    console.log(`Found ${secondaryItems.length} items in secondary feed`);
    
    // Build a map of cleaned titles to secondary feed links
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
    
    // Process primary items - first group by title to eliminate duplicates
    const titleToItem = {};
    const processedGuids = new Set();
    
    for (const item of primaryItems) {
      const title = extractTitle(item);
      if (!title) continue;
      
      const cleanedTitle = cleanTitle(title);
      
      // Extract GUID to avoid duplicates
      const guid = extractGuid(item);
      
      // Skip if we've already seen this GUID
      if (guid && processedGuids.has(guid)) continue;
      if (guid) processedGuids.add(guid);
      
      // Only keep the first occurrence of each title
      if (!titleToItem[cleanedTitle]) {
        titleToItem[cleanedTitle] = item;
      }
    }
    
    console.log(`Reduced to ${Object.keys(titleToItem).length} unique items from primary feed`);
    
    // Now replace links with newsmemory links where available
    const finalItems = [];
    let matchCount = 0;
    
    for (const [cleanedTitle, item] of Object.entries(titleToItem)) {
      const secondaryLink = secondaryLinksByTitle[cleanedTitle];
      
      if (secondaryLink) {
        // Replace the link in the item
        let modifiedItem = item;
        
        // Check if the item has a link tag
        const hasLink = /<link>[\s\S]*?<\/link>/i.test(modifiedItem);
        
        if (hasLink) {
          // Replace existing link with the secondary link
          modifiedItem = modifiedItem.replace(
            /<link>\s*[\s\S]*?\s*<\/link>/i,
            `<link>\n${secondaryLink}\n</link>`
          );
        } else {
          // Add a link tag after the title
          modifiedItem = modifiedItem.replace(
            /<\/title>/,
            `</title>\n  <link>${secondaryLink}</link>`
          );
        }
        
        finalItems.push(modifiedItem);
        matchCount++;
        
        const title = extractTitle(item);
        console.log(`✓ Added item with newsmemory link: "${title}"`);
      } else {
        const title = extractTitle(item);
        console.log(`✗ No newsmemory link for: "${title}" - SKIPPING`);
      }
    }
    
    // Build the initial merged XML
    let mergedFeedXML = headerXML;
    for (const item of finalItems) {
      mergedFeedXML += item;
    }
    mergedFeedXML += footerXML;
    
    console.log(`\nCompleted initial merge with ${matchCount} items`);
    
    // ===== POST-PROCESSING VERIFICATION =====
    console.log("\n===== PERFORMING POST-PROCESSING VERIFICATION =====");
    
    // Extract items from merged feed for verification
    const mergedItems = extractItems(mergedFeedXML);
    console.log(`Verifying ${mergedItems.length} items in merged feed`);
    
    // PHASE 1: Check for and remove any items with thearknewspaper.com links
    const itemsWithArkLinks = mergedItems.filter(item => hasArkLink(item));
    
    if (itemsWithArkLinks.length > 0) {
      console.log(`Found ${itemsWithArkLinks.length} items with thearknewspaper.com links that need removal`);
      
      // Filter out items with ark links and rebuild the feed
      const cleanedItems = mergedItems.filter(item => !hasArkLink(item));
      
      // Rebuild the feed
      let cleanedFeedXML = headerXML;
      for (const item of cleanedItems) {
        cleanedFeedXML += item;
      }
      cleanedFeedXML += footerXML;
      
      mergedFeedXML = cleanedFeedXML;
      console.log(`Removed ${itemsWithArkLinks.length} items with thearknewspaper.com links`);
    } else {
      console.log("No items with thearknewspaper.com links found: PASSED");
    }
    
    // PHASE 2: Check for duplicate titles
    const titlesToItems = {};
    const duplicateTitles = new Set();
    const cleanedItems = extractItems(mergedFeedXML);
    
    // First find any duplicate titles
    for (const item of cleanedItems) {
      const title = extractTitle(item);
      if (!title) continue;
      
      const cleanedTitle = cleanTitle(title);
      
      if (titlesToItems[cleanedTitle]) {
        duplicateTitles.add(cleanedTitle);
      } else {
        titlesToItems[cleanedTitle] = [];
      }
      
      titlesToItems[cleanedTitle].push(item);
    }
    
    if (duplicateTitles.size > 0) {
      console.log(`Found ${duplicateTitles.size} sets of duplicate titles - resolving...`);
      
      // Resolve duplicates - for each duplicate title:
      // 1. Keep only the version with newsmemory.com link
      // 2. If multiple have newsmemory links or none do, keep the first one
      const finalCleanedItems = [];
      
      for (const [title, items] of Object.entries(titlesToItems)) {
        if (items.length === 1) {
          // No duplicates for this title
          finalCleanedItems.push(items[0]);
        } else {
          // We have duplicates to resolve
          console.log(`Resolving duplicates for title: "${extractTitle(items[0])}"`);
          
          // First try to find items with newsmemory links
          const itemsWithNewsmemoryLinks = items.filter(item => hasNewsmemoryLink(item));
          
          if (itemsWithNewsmemoryLinks.length > 0) {
            // Keep only the first item with a newsmemory link
            finalCleanedItems.push(itemsWithNewsmemoryLinks[0]);
            console.log(`  Kept 1 item with newsmemory link out of ${items.length} duplicates`);
          } else {
            // No items have newsmemory links (shouldn't happen), keep the first one
            finalCleanedItems.push(items[0]);
            console.log(`  Warning: No items had newsmemory links, kept first item`);
          }
        }
      }
      
      // Rebuild the feed again
      let finalFeedXML = headerXML;
      for (const item of finalCleanedItems) {
        finalFeedXML += item;
      }
      finalFeedXML += footerXML;
      
      mergedFeedXML = finalFeedXML;
      console.log(`Resolved all duplicate titles, now have ${finalCleanedItems.length} items`);
    } else {
      console.log("No duplicate titles found: PASSED");
    }
    
    // FINAL VERIFICATION
    const finalItems = extractItems(mergedFeedXML);
    
    // Check that all links are from newsmemory
    const nonNewsmemoryLinks = finalItems.filter(item => !hasNewsmemoryLink(item));
    
    if (nonNewsmemoryLinks.length > 0) {
      console.log(`CRITICAL ERROR: Found ${nonNewsmemoryLinks.length} items without newsmemory links!`);
      
      // Remove them in a last-ditch effort
      const actuallyFinalItems = finalItems.filter(item => hasNewsmemoryLink(item));
      
      // Rebuild one last time
      let actuallyFinalFeedXML = headerXML;
      for (const item of actuallyFinalItems) {
        actuallyFinalFeedXML += item;
      }
      actuallyFinalFeedXML += footerXML;
      
      mergedFeedXML = actuallyFinalFeedXML;
      console.log(`Emergency correction: Removed ${nonNewsmemoryLinks.length} non-newsmemory items`);
    }
    
    // CHECK FOR THEARKNEWSPAPER.COM ONE LAST TIME
    const arkLinksCheck = finalItems.some(item => {
      const link = extractLink(item);
      return link && link.includes('thearknewspaper.com');
    });
    
    if (arkLinksCheck) {
      console.log("CRITICAL ERROR: Still found thearknewspaper.com links in final output!");
    } else {
      console.log("Final check for thearknewspaper.com links: PASSED");
    }
    
    console.log(`\nFinal feed contains ${finalItems.length} valid items`);
    
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
console.log('Starting Enhanced RSS feed merger with verification...');
mergeFeeds().then(() => {
  console.log('Script completed successfully!');
}).catch(err => {
  console.error('Script failed with error:', err);
  process.exit(1);
});
