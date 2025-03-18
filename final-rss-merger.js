/**
 * Aggressive RSS Feed Merger Script
 * 
 * Forcefully replaces links and aggressively prevents duplicates
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

// Extract creator from item (handles namespaced tags like dc:creator)
function extractCreator(item) {
  const creatorRegex = /<([^:]+:)?creator[^>]*>(.*?)<\/([^:]+:)?creator>/s;
  const match = item.match(creatorRegex);
  return match ? match[2].trim() : '';
}

// Extract creator from secondary feed's "By [Name]" format
function extractCreatorFromByLine(description) {
  if (!description) return null;
  const match = description.match(/By\s+([^\.]+)/i);
  return match ? match[1].trim() : null;
}

// Create a very specific unique identifier for an item
function createItemIdentifier(title, creator) {
  // Normalize the text to make matching more reliable
  const normalized = (title + "|" + creator)
    .toLowerCase()
    .replace(/\s+/g, '') // Remove all whitespace
    .replace(/[^a-z0-9]/g, ''); // Remove non-alphanumeric chars
  
  return normalized;
}

// Check if titles are similar enough to be a match
function areTitlesSimilar(title1, title2) {
  if (!title1 || !title2) return false;
  
  const t1 = title1.toLowerCase().trim();
  const t2 = title2.toLowerCase().trim();
  
  // Check if one title contains the other
  if (t1.includes(t2) || t2.includes(t1)) {
    return true;
  }
  
  // Split into words and check for significant word overlap
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

// AGGRESSIVELY replace all link tags in an item
function forceReplaceLinks(item, newLink) {
  // First remove ALL link tags from the item
  let modifiedItem = item.replace(/<link[^>]*>.*?<\/link>/g, '');
  
  // Then insert our new link tag after the title tag
  const titleTagRegex = /(<title[^>]*>.*?<\/title>)/;
  if (titleTagRegex.test(modifiedItem)) {
    modifiedItem = modifiedItem.replace(titleTagRegex, `$1\n    <link>${newLink}</link>`);
  } else {
    // If no title tag, add link at the beginning of the item
    modifiedItem = modifiedItem.replace(/<item>/, `<item>\n    <link>${newLink}</link>`);
  }
  
  return modifiedItem;
}

// Main function
async function processFeedsAndCreateNew() {
  try {
    console.log('Fetching primary feed...');
    const primaryFeedXML = await fetchURL(PRIMARY_FEED_URL);
    console.log('Fetching secondary feed...');
    const secondaryFeedXML = await fetchURL(SECONDARY_FEED_URL);
    
    // Extract header (everything before the first item)
    const headerMatch = primaryFeedXML.match(/([\s\S]*?)<item>/);
    let header = headerMatch ? headerMatch[1] : '<?xml version="1.0" encoding="UTF-8"?>\n<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom" xmlns:dc="http://purl.org/dc/elements/1.1/">\n<channel>\n';
    
    // Update the self-reference link
    header = header.replace(
      /<atom:link[^>]*rel="self"[^>]*\/>/,
      `<atom:link href="https://arkeditor.github.io/rss-feed-merger/merged_rss_feed.xml" rel="self" type="application/rss+xml"/>`
    );
    
    // Extract footer (everything after the last item)
    const footerMatch = primaryFeedXML.match(/<\/item>([\s\S]*?)$/);
    const footer = footerMatch ? footerMatch[1] : '</channel>\n</rss>';
    
    console.log('Extracting items from feeds...');
    const primaryItems = extractItems(primaryFeedXML);
    const secondaryItems = extractItems(secondaryFeedXML);
    
    console.log(`Primary feed has ${primaryItems.length} items`);
    console.log(`Secondary feed has ${secondaryItems.length} items`);
    
    // Build arrays of identifiable items from both feeds
    const primaryItemsData = primaryItems.map((item, index) => {
      const title = extractTagValue(item, 'title');
      const creator = extractCreator(item);
      return {
        index,
        item,
        title,
        creator,
        id: createItemIdentifier(title, creator)
      };
    });
    
    const secondaryItemsData = secondaryItems.map((item, index) => {
      const title = extractTagValue(item, 'title');
      const desc = extractTagValue(item, 'description');
      const creator = extractCreatorFromByLine(desc);
      const link = extractTagValue(item, 'link');
      return {
        index,
        item,
        title,
        creator,
        link,
        id: createItemIdentifier(title, creator || '')
      };
    });
    
    // Start building the new feed
    let newFeedXML = header;
    let matchedCount = 0;
    
    // Track processed items to avoid duplicates
    const processedIds = new Set();
    
    console.log('\nProcessing and matching items...');
    
    // Go through primary items and find matches
    for (const primaryData of primaryItemsData) {
      // Skip if already processed
      if (processedIds.has(primaryData.id)) {
        console.log(`\nSkipping duplicate primary item: "${primaryData.title}" (ID: ${primaryData.id})`);
        continue;
      }
      
      console.log(`\nProcessing item #${primaryData.index + 1}: "${primaryData.title}"`);
      console.log(`  - Creator: "${primaryData.creator || 'unknown'}"`);
      console.log(`  - ID: ${primaryData.id}`);
      
      // Find matching secondary item
      let bestMatch = null;
      let bestMatchScore = 0;
      
      for (const secondaryData of secondaryItemsData) {
        const titleMatch = areTitlesSimilar(primaryData.title, secondaryData.title);
        
        if (titleMatch) {
          console.log(`  - Title match with secondary #${secondaryData.index + 1}: "${secondaryData.title}"`);
          
          // Check creator match
          let creatorMatch = false;
          let creatorScore = 0;
          
          if (primaryData.creator && secondaryData.creator) {
            const primaryParts = primaryData.creator.toLowerCase().split(/\s+/);
            const secondaryParts = secondaryData.creator.toLowerCase().split(/\s+|\,\s*/);
            
            // Count how many parts match
            let matchedParts = 0;
            for (const part of primaryParts) {
              if (part.length <= 2) continue; // Skip very short parts
              if (secondaryParts.some(secPart => 
                secPart.length > 2 && (part.includes(secPart) || secPart.includes(part))
              )) {
                matchedParts++;
              }
            }
            
            if (matchedParts > 0) {
              creatorMatch = true;
              creatorScore = matchedParts / primaryParts.length;
            }
            
            console.log(`    - Secondary creator: "${secondaryData.creator}"`);
            console.log(`    - Creator match: ${creatorMatch} (score: ${creatorScore.toFixed(2)})`);
          } else {
            // If we don't have creator info, rely on title similarity
            creatorMatch = titleMatch;
            creatorScore = 0.5; // Default score
          }
          
          // Calculate overall match score (title match is more important)
          const overallScore = titleMatch ? (0.7 + (creatorMatch ? 0.3 * creatorScore : 0)) : 0;
          
          if (overallScore > bestMatchScore) {
            bestMatchScore = overallScore;
            bestMatch = secondaryData;
          }
        }
      }
      
      // If we found a good match
      if (bestMatch && bestMatch.link) {
        // Mark this item as processed
        processedIds.add(primaryData.id);
        
        console.log(`  ✓ Best match found: "${bestMatch.title}" with link: ${bestMatch.link}`);
        
        // Aggressively replace links
        const modifiedItem = forceReplaceLinks(primaryData.item, bestMatch.link);
        
        // Verify link replacement worked
        const newLinkCheck = modifiedItem.match(/<link>(.*?)<\/link>/);
        const newLink = newLinkCheck ? newLinkCheck[1].trim() : '';
        
        if (newLink !== bestMatch.link) {
          console.log(`  ! ERROR: Link replacement failed! Expected: "${bestMatch.link}", Got: "${newLink}"`);
        } else {
          console.log(`  - Link replacement successful`);
        }
        
        // Add the modified item to the new feed
        newFeedXML += modifiedItem;
        matchedCount++;
      } else {
        console.log(`  ✗ No good match found - excluding from output`);
      }
    }
    
    // Finish the new feed
    newFeedXML += footer;
    
    console.log(`\nMatching complete: ${matchedCount} of ${primaryItems.length} unique items matched`);
    console.log(`Processed ${processedIds.size} unique items`);
    
    // Final validation to ensure no duplicate items or link issues
    console.log('\nValidating final output...');
    
    // Check for items with multiple link tags
    const multiLinkRegex = /<item>[\s\S]*?<link>.*?<\/link>[\s\S]*?<link>.*?<\/link>[\s\S]*?<\/item>/g;
    const multiLinkMatches = newFeedXML.match(multiLinkRegex) || [];
    
    if (multiLinkMatches.length > 0) {
      console.log(`! WARNING: Found ${multiLinkMatches.length} items with multiple link tags`);
    } else {
      console.log('- No items with multiple link tags found: PASSED');
    }
    
    // Check for missing link tags
    const itemsInOutput = extractItems(newFeedXML);
    const itemsWithoutLinks = itemsInOutput.filter(item => !/<link>.*?<\/link>/.test(item));
    
    if (itemsWithoutLinks.length > 0) {
      console.log(`! WARNING: Found ${itemsWithoutLinks.length} items without link tags`);
    } else {
      console.log('- All items have link tags: PASSED');
    }
    
    // Check for potential duplicates based on title
    const titlesInOutput = itemsInOutput.map(item => extractTagValue(item, 'title'));
    const titleCounts = {};
    
    for (const title of titlesInOutput) {
      titleCounts[title] = (titleCounts[title] || 0) + 1;
    }
    
    const possibleDuplicates = Object.entries(titleCounts).filter(([title, count]) => count > 1);
    
    if (possibleDuplicates.length > 0) {
      console.log(`! WARNING: Found ${possibleDuplicates.length} possible duplicate titles:`);
      for (const [title, count] of possibleDuplicates) {
        console.log(`  - "${title}" appears ${count} times`);
      }
    } else {
      console.log('- No duplicate titles found: PASSED');
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
console.log('Starting aggressive RSS feed merger...');
processFeedsAndCreateNew().then(() => {
  console.log('Script completed successfully!');
}).catch(err => {
  console.error('Script failed with error:', err);
  process.exit(1);
});
