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

// Reformat newsmemory links to a cleaner format
function reformatNewsmemoryLink(url) {
  // Check if it's a newsmemory link with the right format
  const regex = /newsmemory\.com\/rss\.php\?date=(\d+)&edition=([^&]+)&subsection=Main&page=\d+theark(\d+)_w-or9\.pdf\.(\d+)&id=art_(\d+)\.xml/;
  const match = url.match(regex);
  
  if (match) {
    const date = match[1];  // e.g., 20250319
    const edition = match[2]; // e.g., The+Ark
    const page = match[3];   // e.g., 01
    const artid = match[5];  // e.g., 0
    
    // Create reformatted URL
    const newUrl = `https://thearknewspaper-ca.newsmemory.com?selDate=${date}&goTo=${page}&artid=${artid}&editionStart=${encodeURIComponent(edition.replace(/\+/g, ' '))}`;
    
    console.log(`Reformatted link:\n  From: ${url}\n  To:   ${newUrl}`);
    return newUrl;
  }
  
  // If no match, return the original URL
  return url;
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
    console.log(`Found ${secondaryItems.length} total items in secondary feed`);
    
    // Count items that pass the <full> tag filter
    let validSecondaryItemCount = 0;
    for (let i = 0; i < secondaryItems.length; i++) {
      const item = secondaryItems[i];
      const fullElements = item.getElementsByTagName('full');
      
      if (fullElements.length > 0) {
        const fullContent = fullElements[0].textContent;
        if (fullContent && fullContent.includes('<![CDATA[By')) {
          validSecondaryItemCount++;
        }
      }
    }
    
    console.log(`Found ${validSecondaryItemCount} items in secondary feed with valid <full> tags`);
    
    // Build a map of titles to secondary feed links
    const newsmemoryLinksByTitle = {};
    const secondaryTitleDetails = [];
    
    for (let i = 0; i < secondaryItems.length; i++) {
      const item = secondaryItems[i];
      
      // Check if the <full> tag starts with "<![CDATA[By"
      const fullElements = item.getElementsByTagName('full');
      let isValidSecondaryItem = false;
      let extractedAuthor = null;
      
      if (fullElements.length > 0) {
        const fullContent = fullElements[0].textContent;
        if (fullContent && fullContent.includes('<![CDATA[By')) {
          isValidSecondaryItem = true;
          
          // Extract author name from the <full> tag
          const authorMatch = fullContent.match(/<!\[CDATA\[By\s+([A-Z]+\s+[A-Z]+)/i);
          if (authorMatch && authorMatch[1]) {
            extractedAuthor = authorMatch[1].trim();
            // Normalize author name to match format in dc:creator
            extractedAuthor = extractedAuthor.split(/\s+/)
              .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
              .join(' ');
          }
        }
      }
      
      // Skip this item if it doesn't meet the criteria
      if (!isValidSecondaryItem) {
        continue;
      }
      
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
      let link = linkElement.textContent.trim();
      
      // Only keep newsmemory links
      if (link.includes('newsmemory.com')) {
        // Reformat the link to the cleaner format
        link = reformatNewsmemoryLink(link);
        
        newsmemoryLinksByTitle[cleanedTitle] = link;
        
        // Store additional details for better matching
        secondaryTitleDetails.push({
          originalTitle: title,
          cleanedTitle: cleanedTitle,
          creator: creator || extractedAuthor, // Use extracted author if no creator tag
          extractedAuthor: extractedAuthor,    // Always store extracted author for matching
          link: link
        });
      }
    }
    
    // Sort secondary titles by length (descending) to prioritize longer, more specific titles in matching
    secondaryTitleDetails.sort((a, b) => b.cleanedTitle.length - a.cleanedTitle.length);
    
    console.log(`Found ${Object.keys(newsmemoryLinksByTitle).length} unique titles with newsmemory links after filtering`);
    
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
      let bestMatchItem = null;
      
      for (const secondaryItem of secondaryTitleDetails) {
        // Calculate match score (simple similarity check)
        const secondaryClean = secondaryItem.cleanedTitle;
        
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
        
        let score = matchingWords / Math.max(words1.length, words2.length);
        
        // Boost score if authors match (primary dc:creator matches secondary extracted author)
        if (creator && 
            (secondaryItem.creator === creator || 
             secondaryItem.extractedAuthor === creator || 
             (secondaryItem.extractedAuthor && creator.includes(secondaryItem.extractedAuthor)) ||
             (secondaryItem.extractedAuthor && secondaryItem.extractedAuthor.includes(creator)))) {
          score += 0.3; // Significant boost for author match
          console.log(`Author match boost: "${title}" - Primary: ${creator}, Secondary: ${secondaryItem.extractedAuthor}`);
        }
        
        // Consider it a match if:
        // 1. At least 60% of significant words match, OR
        // 2. We have a creator name that matches and at least 40% of words match
        if ((score > 0.6) || 
            ((creator && secondaryItem.extractedAuthor && 
              (creator.includes(secondaryItem.extractedAuthor) || 
               secondaryItem.extractedAuthor.includes(creator))) && 
             score > 0.4)) {
          if (score > bestMatchScore) {
            bestMatchScore = score;
            bestMatchTitle = secondaryItem.cleanedTitle;
            bestMatchItem = secondaryItem;
          }
        }
      }
      
      // Use direct match if available, otherwise use best fuzzy match
      let newsmemoryLink = newsmemoryLinksByTitle[cleanedTitle];
      
      if (!newsmemoryLink && bestMatchItem) {
        newsmemoryLink = bestMatchItem.link;
        console.log(`Fuzzy matched: "${title}" with secondary title "${bestMatchItem.originalTitle}" (${bestMatchScore.toFixed(2)} score)`);
        
        if (creator && bestMatchItem.extractedAuthor) {
          console.log(`  Authors: Primary="${creator}", Secondary="${bestMatchItem.extractedAuthor}"`);
        }
      }
      
      if (newsmemoryLink) {
        // Clone the item
        const newItem = item.cloneNode(true);
        
        // Replace or add link
        const linkElements = newItem.getElementsByTagName('link');
        
        if (linkElements.length > 0) {
          // Replace existing link
          const linkElement = linkElements[0];
          
          // Make sure it's a reformatted link
          let linkToUse = newsmemoryLink;
          if (linkToUse.includes('newsmemory.com/rss.php')) {
            linkToUse = reformatNewsmemoryLink(linkToUse);
          }
          
          linkElement.textContent = linkToUse;
        } else {
          // Add new link element after title
          const link = resultDoc.createElement('link');
          
          // Make sure it's a reformatted link
          let linkToUse = newsmemoryLink;
          if (linkToUse.includes('newsmemory.com/rss.php')) {
            linkToUse = reformatNewsmemoryLink(linkToUse);
          }
          
          link.textContent = linkToUse;
          
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
