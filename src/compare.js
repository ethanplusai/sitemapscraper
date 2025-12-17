/**
 * Compare crawl results with ScreamingFrog URLs
 * 
 * This script compares the crawl results CSV (crawl-results.csv) with
 * the ScreamingFrog CSV (screamingfrog.csv) to identify missing and extra URLs.
 * 
 * Usage: node src/compare.js
 * 
 * The script can be run standalone or will automatically run after a crawl completes.
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');

// File paths
const CRAWL_RESULTS_CSV = path.join(__dirname, '../crawl-results.csv');
const SCREAMING_FROG_CSV = path.join(__dirname, '../screamingfrog.csv');

/**
 * Normalizes a URL for comparison (matches compare.js logic)
 * @param {string} url - The URL to normalize
 * @returns {string|null} - The normalized URL, or null if invalid
 */
function normalize(url) {
  if (!url) return null;

  return url
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, '')
    .replace(/\/$/, '');
}

/**
 * Parses a CSV line into an array of fields
 * Handles quoted fields and escaped quotes
 * @param {string} line - The CSV line to parse
 * @returns {Array<string>} - Array of field values
 */
function parseCsvLine(line) {
  const fields = [];
  let currentField = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    const nextChar = line[i + 1];
    
    if (char === '"') {
      if (inQuotes && nextChar === '"') {
        // Escaped quote
        currentField += '"';
        i++; // Skip next quote
      } else {
        // Toggle quote state
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      // Field separator
      fields.push(currentField);
      currentField = '';
    } else {
      currentField += char;
    }
  }
  
  // Add the last field
  fields.push(currentField);
  
  return fields;
}

/**
 * Reads URLs from crawl results CSV
 * @returns {Promise<{Set<string>, Map<string, string>}>} - Set of normalized URLs and map of normalized -> original
 */
function readCrawlResults() {
  return new Promise((resolve, reject) => {
    const urlSet = new Set();
    const urlMap = new Map(); // normalized -> original URL mapping

    if (!fs.existsSync(CRAWL_RESULTS_CSV)) {
      console.error(`Crawl results file not found: ${CRAWL_RESULTS_CSV}`);
      console.error('Please run a crawl first to generate crawl-results.csv');
      process.exit(1);
    }

    try {
      const content = fs.readFileSync(CRAWL_RESULTS_CSV, 'utf8');
      const lines = content.split(/\r?\n/);
      
      // Skip header row (first line)
      for (let i = 1; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue; // Skip empty lines
        
        const fields = parseCsvLine(line);
        const original = fields[0]?.trim(); // First column is normalized_url
        
        if (original) {
          const normalized = normalize(original);
          if (normalized) {
            urlSet.add(normalized);
            // Store original URL (keep first occurrence if duplicates)
            if (!urlMap.has(normalized)) {
              urlMap.set(normalized, original);
            }
          }
        }
      }
      
      resolve({ urlSet, urlMap });
    } catch (err) {
      reject(new Error(`Error reading crawl results CSV: ${err.message}`));
    }
  });
}

/**
 * Reads URLs from ScreamingFrog CSV
 * @returns {Promise<{Set<string>, Map<string, string>}>} - Set of normalized URLs and map of normalized -> original
 */
function readScreamingFrogUrls() {
  return new Promise((resolve, reject) => {
    const urlSet = new Set();
    const urlMap = new Map(); // normalized -> original URL mapping

    if (!fs.existsSync(SCREAMING_FROG_CSV)) {
      reject(new Error(`ScreamingFrog CSV file not found: ${SCREAMING_FROG_CSV}`));
      return;
    }

    try {
      const content = fs.readFileSync(SCREAMING_FROG_CSV, 'utf8');
      const lines = content.split(/\r?\n/);
      
      // Skip header row (first line) and process data rows
      for (let i = 1; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue; // Skip empty lines
        
        const fields = parseCsvLine(line);
        const original = fields[0]?.trim(); // First column contains the URL
        
        if (original) {
          const normalized = normalize(original);
          if (normalized) {
            urlSet.add(normalized);
            // Store original URL (keep first occurrence if duplicates)
            if (!urlMap.has(normalized)) {
              urlMap.set(normalized, original);
            }
          }
        }
      }
      
      resolve({ urlSet, urlMap });
    } catch (err) {
      reject(new Error(`Error reading ScreamingFrog CSV: ${err.message}`));
    }
  });
}

/**
 * Main comparison function
 */
async function compareUrls() {
  try {
    console.log(`Reading crawl results from: ${CRAWL_RESULTS_CSV}`);
    const { urlSet: crawlSet, urlMap: crawlMap } = await readCrawlResults();
    console.log(`Found ${crawlSet.size} unique URLs in crawl results`);

    console.log(`Reading ScreamingFrog URLs from: ${SCREAMING_FROG_CSV}`);
    const { urlSet: sfSet, urlMap: sfMap } = await readScreamingFrogUrls();
    console.log(`Found ${sfSet.size} unique URLs in ScreamingFrog data\n`);

    // Find differences
    const missingFromCrawl = [...sfSet].filter(u => !crawlSet.has(u));
    const extraInCrawl = [...crawlSet].filter(u => !sfSet.has(u));
    const overlap = sfSet.size - missingFromCrawl.length;

    // Output comparison summary
    console.log('===== COMPARISON SUMMARY =====');
    console.log(`Crawl Results URLs:  ${crawlSet.size}`);
    console.log(`ScreamingFrog URLs:  ${sfSet.size}`);
    console.log(`Overlap:            ${overlap}`);
    console.log(`Missing in Crawl:   ${missingFromCrawl.length}`);
    console.log(`Extra in Crawl:     ${extraInCrawl.length}`);
    console.log('==============================\n');

    if (missingFromCrawl.length > 0) {
      console.log('--- Missing in Crawl Results (found in ScreamingFrog but not in crawl) ---');
      missingFromCrawl.forEach(u => {
        const originalUrl = sfMap.get(u) || u;
        console.log(originalUrl);
      });
      console.log('');
    }

    if (extraInCrawl.length > 0) {
      console.log('--- Extra in Crawl Results (found in crawl but not in ScreamingFrog) ---');
      extraInCrawl.forEach(u => {
        const originalUrl = crawlMap.get(u) || u;
        console.log(originalUrl);
      });
      console.log('');
    }

    if (missingFromCrawl.length === 0 && extraInCrawl.length === 0) {
      console.log('✓ Perfect match! All URLs are present in both datasets.\n');
    }

  } catch (error) {
    console.error('Error comparing URLs:', error.message);
    if (require.main === module) {
      process.exit(1);
    } else {
      throw error; // Re-throw if called as a module
    }
  }
}

// Run comparison if executed directly
if (require.main === module) {
  compareUrls();
}

module.exports = { compareUrls, normalize };

