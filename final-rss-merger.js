/**
 * Minimal RSS Feed Merger
 * 
 * A completely different approach focused only on link replacement
 * with no possibility of duplicates.
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

// Main function
async function processFeedsAndCreateNew() {
  try {
    console.log('Fetching feeds...');
    const primaryFeedXML = await fetchURL(PRIMARY_FEED_URL);
    const secondaryFeedXML = await fetchURL(SECONDARY_FEED_URL);
    
    // Step 1: Extract only the channel content (everything between <channel> and </channel>)
    const primaryChannelMatch = primaryFeedXML.match(/<channel>([\s\S]*?)<\/channel>/);
    const secondaryChannelMatch = secondaryFeedXML.match(/<channel>([\s\S]*?)<\/channel>/);
    
    if (!primaryChannelMatch || !secondaryChannelMatch) {
      throw new Error('Could not extract channel content from feeds');
    }
    
    const primaryChannel = primaryChannelMatch[1];
    const secondaryChannel = secondaryChannelMatch[1];
    
    // Step 2: Extract header (everything before the first <item>)
    const headerMatch = primaryChannel.match(/([\s\S]*?)<item>/);
    const header = headerMatch ? headerMatch[1] : '';
    
    // Step 3: Extract individual items from both feeds
    const primaryItemRegex = /<item>([\s\S]*?)<\/item>/g;
    const secondaryItemRegex = /<item>([\s\S]*?)<\/item>/g;
    
    const primaryItems = [];
    const secondaryItems = [];
    
    let primaryMatch;
    while ((primaryMatch = primaryItemRegex.exec(primaryChannel)) !== null) {
      primaryItems.push(primaryMatch[1]);
    }
    
    let secondaryMatch;
    while ((secondaryMatch = secondaryItemRegex.exec(secondaryChannel)) !== null) {
      secondaryItems.push(secondaryMatch[1]);
    }
    
    console.log(`Found ${primaryItems.length} items in primary feed`);
    console.log(`Found ${secondaryItems.length} items in secondary feed`);
    
    // Step 4: Index secondary items by title for faster matching
    const secondaryItemsByTitle = {};
    for (const item of secondaryItems) {
      const titleMatch = item.match(/<title>([\s\S]*?)<\/title>/);
      if (titleMatch) {
        const title = titleMatch[1].trim();
        secondaryItemsByTitle[title] = item;
      }
    }
    
    // Step 5: Create a secondary link lookup
    const secondaryLinks = {};
    for (const item of secondaryItems) {
      const titleMatch = item.match(/<title>([\s\S]*?)<\/title>/);
      const linkMatch = item.match(/<link>([\s\S]*?)<\/link>/);
      
      if (titleMatch && linkMatch) {
        const title = titleMatch[1].trim();
        const link = linkMatch[1].trim();
        
        // Only store links from newsmemory
        if (link.includes('newsmemory.com')) {
          secondaryLinks[title] = link;
        }
      }
    }
    
    // Step 6: Process primary items, replacing links with secondary links when possible
    const processedItems = [];
    const processedTitles = new Set(); // For deduplication
    
    for (const item of primaryItems) {
      const titleMatch = item.match(/<title>([\s\S]*?)<\/title>/);
      if (!titleMatch) continue;
      
      const title = titleMatch[1].trim();
      
      // Skip duplicates
      if (processedTitles.has(title)) {
        console.log(`Skipping duplicate: "${title}"`);
        continue;
      }
      
      processedTitles.add(title);
      
      // Check if we have a matching link in the secondary feed
      const secondaryLink = secondaryLinks[title];
      
      if (secondaryLink) {
        // Replace the link in the item
        let modifiedItem = item;
        
        // First, remove any existing link tags
        modifiedItem = modifiedItem.replace(/<link>[\s\S]*?<\/link>/g, '');
        
        // Then add the secondary link after the title
        modifiedItem = modifiedItem.replace(/<\/title>/, '</title>\n  <link>' + secondaryLink + '</link>');
        
        processedItems.push(modifiedItem);
        console.log(`✓ Replaced link for: "${title}"`);
      } else {
        // Skip items without a match in the secondary feed
        console.log(`✗ No secondary link found for: "${title}"`);
      }
    }
    
    console.log(`\nCreated ${processedItems.length} items with secondary links`);
    
    // Step 7: Build the final XML feed
    let newFeedXML = '<?xml version="1.0" encoding="UTF-8"?>\n';
    newFeedXML += '<rss xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:content="http://purl.org/rss/1.0/modules/content/" xmlns:atom="http://www.w3.org/2005/Atom" version="2.0">\n';
    newFeedXML += '<channel>\n';
    
    // Add header with updated self-reference
    let updatedHeader = header.replace(
      /<atom:link[^>]*rel="self"[^>]*\/>/,
      '<atom:link href="https://arkeditor.github.io/rss-feed-merger/merged_rss_feed.xml" rel="self" type="application/rss+xml"/>'
    );
    
    newFeedXML += updatedHeader;
    
    // Add all processed items
    for (const item of processedItems) {
      newFeedXML += '<item>\n' + item + '\n</item>\n';
    }
    
    // Close the tags
    newFeedXML += '</channel>\n</rss>';
    
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
console.log('Starting minimal RSS feed merger...');
processFeedsAndCreateNew().then(() => {
  console.log('Script completed successfully!');
}).catch(err => {
  console.error('Script failed with error:', err);
  process.exit(1);
});
