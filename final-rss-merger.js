/**
 * Final RSS Feed Merger Script
 * Fixed validation issues with namespaces and self-reference
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

// Generate a unique ID
function generateUniqueId(index) {
  return `unique-item-${Date.now()}-${index}-${Math.random().toString(36).substring(2, 10)}`;
}

// Main function
async function processFeedsAndCreateNew() {
  try {
    console.log('Fetching primary feed...');
    const primaryFeedXML = await fetchURL(PRIMARY_FEED_URL);
    console.log('Fetching secondary feed...');
    const secondaryFeedXML = await fetchURL(SECONDARY_FEED_URL);
    
    console.log('Extracting items from feeds...');
    const primaryItems = extractItems(primaryFeedXML);
    const secondaryItems = extractItems(secondaryFeedXML);
    
    console.log(`Primary feed has ${primaryItems.length} items`);
    console.log(`Secondary feed has ${secondaryItems.length} items`);
    
    // Create a new feed
    // FIX: Added proper namespace declarations for both atom and dc
    let newFeedXML = '<?xml version="1.0" encoding="UTF-8"?>\n';
    newFeedXML += '<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom" xmlns:dc="http://purl.org/dc/elements/1.1/">\n';
    newFeedXML += '<channel>\n';
    newFeedXML += '  <title>The Ark Newspaper</title>\n';
    newFeedXML += '  <description>Merged RSS Feed</description>\n';
    newFeedXML += '  <link>https://www.thearknewspaper.com</link>\n';
    // Note: When deploying, change this URL to match your actual hosting location
    const selfReferenceUrl = "http://example.com/merged_rss_feed.xml"; // Use generic placeholder
    newFeedXML += `  <atom:link href="${selfReferenceUrl}" rel="self" type="application/rss+xml"/>\n`;
    
    let matchedCount = 0;
    
    console.log('\nMatching items...');
    
    // Process each item
    for (let i = 0; i < primaryItems.length; i++) {
      const primaryItem = primaryItems[i];
      
      const primaryTitle = extractTagValue(primaryItem, 'title');
      const primaryCreator = extractCreator(primaryItem);
      
      console.log(`\nProcessing item (${i+1}/${primaryItems.length}):`);
      console.log(`  - Title: "${primaryTitle}"`);
      
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
        } else {
          creatorMatch = titleMatch && (primaryTitle.length > 15 || secondaryTitle.length > 15);
        }
        
        if (titleMatch && (creatorMatch || !secondaryCreator || !primaryCreator)) {
          matchFound = true;
          matchedSecondaryLink = secondaryLink;
          console.log(`  ✓ Match found! Using link: ${secondaryLink}`);
          break;
        }
      }
      
      if (matchFound && matchedSecondaryLink) {
        // Generate a unique ID
        const uniqueId = generateUniqueId(i);
        
        // Build a new item
        let newItem = '  <item>\n';
        
        // Add title (preserve CDATA if present)
        if (primaryTitle.includes('<![CDATA[')) {
          newItem += `    <title>${primaryTitle}</title>\n`;
        } else {
          newItem += `    <title><![CDATA[${primaryTitle}]]></title>\n`;
        }
        
        // Add link
        newItem += `    <link>${matchedSecondaryLink}</link>\n`;
        
        // Add description if available
        const primaryDescription = extractTagValue(primaryItem, 'description');
        if (primaryDescription) {
          if (primaryDescription.includes('<![CDATA[')) {
            newItem += `    <description>${primaryDescription}</description>\n`;
          } else {
            newItem += `    <description><![CDATA[${primaryDescription}]]></description>\n`;
          }
        }
        
        // Add creator if available
        if (primaryCreator) {
          if (primaryCreator.includes('<![CDATA[')) {
            newItem += `    <dc:creator>${primaryCreator}</dc:creator>\n`;
          } else {
            newItem += `    <dc:creator><![CDATA[${primaryCreator}]]></dc:creator>\n`;
          }
        }
        
        // Add pubDate if available
        const primaryPubDate = extractTagValue(primaryItem, 'pubDate');
        if (primaryPubDate) {
          newItem += `    <pubDate>${primaryPubDate}</pubDate>\n`;
        }
        
        // Add our unique GUID
        newItem += `    <guid isPermaLink="false">${uniqueId}</guid>\n`;
        
        // Close the item
        newItem += '  </item>\n';
        
        // Add to feed
        newFeedXML += newItem;
        matchedCount++;
      } else {
        console.log(`  ✗ No match found - excluding from output`);
      }
    }
    
    // Close the channel and rss tags
    newFeedXML += '</channel>\n</rss>';
    
    console.log(`\nMatching complete: ${matchedCount} of ${primaryItems.length} items matched`);
    
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
console.log('Starting RSS feed merger...');
processFeedsAndCreateNew().then(() => {
  console.log('Script completed successfully!');
}).catch(err => {
  console.error('Script failed with error:', err);
  process.exit(1);
});
