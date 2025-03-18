/**
 * Robust RSS Feed Merger Script
 * 
 * Fixes:
 * 1. Ensures consistent link replacement from secondary feed
 * 2. Prevents duplicate entries
 * 3. Better debugging and error handling
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

// Extract items from RSS feed
function extractItems(feedContent) {
  const itemRegex = /<item>([\s\S]*?)<\/item>/g;
  const items = [];
  let match;
  
  while ((match = itemRegex.exec(feedContent)) !== null) {
    items.push(match[0]);
  }
  
  return items;
}

// Extract a specific tag value from an item
function extractTagValue(item, tagName) {
  const regex = new RegExp(`<${tagName}[^>]*>(.*?)<\/${tagName}>`, 's');
  const match = item.match(regex);
  return match ? match[1].trim() : '';
}

// Extract creator from item
function extractCreator(item) {
  const creatorRegex = /<([^:]+:)?creator[^>]*>(.*?)<\/([^:]+:)?creator>/s;
  const match = item.match(creatorRegex);
  return match ? match[2].trim() : '';
}

// Create a unique identifier for an item based on title and creator
function createItemIdentifier(title, creator) {
  return `${title}|${creator}`.toLowerCase().replace(/[^a-z0-9]/g, '');
}

// Extract creator from secondary feed's "By [Name]" format
function extractCreatorFromByLine(description) {
  if (!description) return null;
  const match = description.match(/By\s+([^\.]+)/i);
  return match ? match[1].trim() : null;
}

// Check if titles are similar
function areTitlesSimilar(title1, title2) {
  if (!title1 || !title2) return false;
  
  const t1 = title1.toLowerCase().trim();
  const t2 = title2.toLowerCase().trim();
  
  if (t1.includes(t2) || t2.includes(t1)) {
    return true;
  }
  
  const words1 = t1.split(/\s+/).filter(w => w.length > 2);
  const words2 = t2.split(/\s+/).filter(w => w.length > 2);
  
  if (words1.length === 0 || words2.length === 0) return false;
  
  let matchingWords = 0;
  for (const word1 of words1) {
    if (words2.some(word2 => word2.includes(word1) || word1.includes(word2))) {
      matchingWords++;
    }
  }
  
  const shortestLength = Math.min(words1.length, words2.length);
  return matchingWords / shortestLength >= 0.4;
}

// Replace link in an item more robustly
function replaceLink(item, newLink) {
  // First try the standard <link> tag format
  const standardLinkRegex = /<link>(.*?)<\/link>/;
  if (standardLinkRegex.test(item)) {
    return item.replace(standardLinkRegex, `<link>${newLink}</link>`);
  }
  
  // Try alternate formats with attributes
  const linkWithAttrsRegex = /<link[^>]*>(.*?)<\/link>/;
  if (linkWithAttrsRegex.test(item)) {
    return item.replace(linkWithAttrsRegex, `<link>${newLink}</link>`);
  }
  
  // If no link tag found, add one before the end of the item
  return item.replace(/<\/item>/, `<link>${newLink}</link>\n</item>`);
}

// Main function
async function processFeedsAndCreateNew() {
  try {
    console.log('Fetching primary feed...');
    const primaryFeedXML = await fetchURL(PRIMARY_FEED_URL);
    console.log('Fetching secondary feed...');
    const secondaryFeedXML = await fetchURL(SECONDARY_FEED_URL);
    
    // Extract the XML declaration, RSS tag, and channel opening content
    const headerMatch = primaryFeedXML.match(/([\s\S]*?)<item>/);
    let header = headerMatch ? headerMatch[1] : '<?xml version="1.0" encoding="UTF-8"?>\n<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom" xmlns:dc="http://purl.org/dc/elements/1.1/">\n<channel>\n';
    
    // Update the self-reference link
    header = header.replace(
      /<atom:link[^>]*rel="self"[^>]*\/>/,
      `<atom:link href="https://arkeditor.github.io/rss-feed-merger/merged_rss_feed.xml" rel="self" type="application/rss+xml"/>`
    );
    
    // Get the closing tags
    const footerMatch = primaryFeedXML.match(/<\/item>([\s\S]*?)$/);
    const footer = footerMatch ? footerMatch[1] : '</channel>\n</rss>';
    
    console.log('Extracting items from feeds...');
    const primaryItems = extractItems(primaryFeedXML);
    const secondaryItems = extractItems(secondaryFeedXML);
    
    console.log(`Primary feed has ${primaryItems.length} items`);
    console.log(`Secondary feed has ${secondaryItems.length} items`);
    
    // Start building the new feed
    let newFeedXML = header;
    let matchedCount = 0;
    
    // Track processed items to avoid duplicates
    const processedItemIds = new Set();
    
    console.log('\nMatching items...');
    
    // Process each item from primary feed
    for (let i = 0; i < primaryItems.length; i++) {
      let primaryItem = primaryItems[i];
      
      const primaryTitle = extractTagValue(primaryItem, 'title');
      const primaryCreator = extractCreator(primaryItem);
      
      // Create a unique identifier for this item
      const itemId = createItemIdentifier(primaryTitle, primaryCreator);
      
      // Skip if we've already processed this item (prevents duplicates)
      if (processedItemIds.has(itemId)) {
        console.log(`\nSkipping duplicate item: "${primaryTitle}"`);
        continue;
      }
      
      console.log(`\nProcessing item (${i+1}/${primaryItems.length}):`);
      console.log(`  - Title: "${primaryTitle}"`);
      console.log(`  - Creator: "${primaryCreator}"`);
      console.log(`  - Item ID: ${itemId}`);
      
      let matchFound = false;
      let matchedSecondaryLink = '';
      
      // Find matching item in secondary feed
      for (let j = 0; j < secondaryItems.length; j++) {
        const secondaryItem = secondaryItems[j];
        const secondaryTitle = extractTagValue(secondaryItem, 'title');
        const secondaryDescription = extractTagValue(secondaryItem, 'description');
        const secondaryLink = extractTagValue(secondaryItem, 'link');
        const secondaryCreator = extractCreatorFromByLine(secondaryDescription);
        
        const titleMatch = areTitlesSimilar(primaryTitle, secondaryTitle);
        
        if (titleMatch) {
          console.log(`  - Title match with: "${secondaryTitle}"`);
          console.log(`  - Secondary creator: "${secondaryCreator || 'unknown'}"`);
        }
        
        // Check creator match
        let creatorMatch = false;
        if (primaryCreator && secondaryCreator) {
          const primaryParts = primaryCreator.toLowerCase().split(/\s+/);
          const secondaryParts = secondaryCreator.toLowerCase().split(/\s+|\,\s*/);
          
          creatorMatch = primaryParts.some(part => 
            secondaryParts.some(secPart => 
              part.length > 2 && secPart.length > 2 && 
              (part.includes(secPart) || secPart.includes(part))
            )
          );
          
          if (titleMatch) {
            console.log(`  - Creator match: ${creatorMatch}`);
          }
        } else {
          // If we don't have creator info, rely on a strong title match
          creatorMatch = titleMatch && (primaryTitle.length > 15 || secondaryTitle.length > 15);
        }
        
        if (titleMatch && (creatorMatch || !secondaryCreator || !primaryCreator)) {
          matchFound = true;
          matchedSecondaryLink = secondaryLink;
          console.log(`  ✓ Match found! Using link: ${matchedSecondaryLink}`);
          break;
        }
      }
      
      if (matchFound && matchedSecondaryLink) {
        // Mark this item as processed
        processedItemIds.add(itemId);
        
        // Get the current link (for debugging)
        const currentLink = extractTagValue(primaryItem, 'link');
        console.log(`  - Replacing link: "${currentLink}" -> "${matchedSecondaryLink}"`);
        
        // Replace the link tag
        const modifiedItem = replaceLink(primaryItem, matchedSecondaryLink);
        
        // Verify the replacement worked
        const newLink = extractTagValue(modifiedItem, 'link');
        if (newLink !== matchedSecondaryLink) {
          console.log(`  ! WARNING: Link replacement failed. Expected: "${matchedSecondaryLink}", Got: "${newLink}"`);
        }
        
        // Add the modified item to the new feed
        newFeedXML += modifiedItem;
        matchedCount++;
      } else {
        console.log(`  ✗ No match found - excluding from output`);
      }
    }
    
    // Finish the new feed
    newFeedXML += footer;
    
    console.log(`\nMatching complete: ${matchedCount} of ${primaryItems.length} unique items matched`);
    console.log(`Processed ${processedItemIds.size} unique items`);
    
    // Verify we don't have duplicate link tags in items
    const duplicateLinkCheck = /<item>[\s\S]*?<link>.*?<\/link>[\s\S]*?<link>.*?<\/link>[\s\S]*?<\/item>/g;
    const duplicateLinks = newFeedXML.match(duplicateLinkCheck);
    if (duplicateLinks && duplicateLinks.length > 0) {
      console.log(`\n! WARNING: Found ${duplicateLinks.length} items with duplicate link tags`);
    }
    
    // Save the output to a file
    fs.writeFileSync('merged_rss_feed.xml', newFeedXML);
    console.log(`\nMerged feed saved to merged_rss_feed.xml`);
    
    return newFeedXML;
  } catch (error) {
    console.error('Error:', error.message);
    throw error;
  }
}

// Run the script
console.log('Starting robust RSS feed merger...');
processFeedsAndCreateNew().then(() => {
  console.log('Script completed successfully!');
}).catch(err => {
  console.error('Script failed with error:', err);
  process.exit(1);
});
