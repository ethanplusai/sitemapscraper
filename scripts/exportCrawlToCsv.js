/**
 * Export crawl results to CSV
 * 
 * This script exports normalized_url values from a crawl job to Column A
 * of an existing CSV file, preserving the header and Column B.
 * 
 * Usage: node scripts/exportCrawlToCsv.js <crawl_job_id> [csv_file_path]
 * 
 * Example: node scripts/exportCrawlToCsv.js 123
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const supabase = require('../src/supabase');

// Default CSV file path (relative to sitemap-compare folder)
const DEFAULT_CSV_PATH = path.join(__dirname, '../../sitemap-compare/pages_rows - Sheet1.csv');

/**
 * Escapes a CSV field value (handles commas, quotes, newlines)
 * @param {string} value - The value to escape
 * @returns {string} - The escaped value
 */
function escapeCsvField(value) {
  if (value === null || value === undefined) {
    return '';
  }
  
  const str = String(value);
  
  // If the value contains comma, quote, or newline, wrap it in quotes and escape internal quotes
  if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  
  return str;
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
 * Reads a CSV file and returns rows as arrays
 * @param {string} filePath - Path to the CSV file
 * @returns {Promise<Array<Array<string>>>} - Array of rows, each row is an array of fields
 */
async function readCsvFile(filePath) {
  return new Promise((resolve, reject) => {
    const rows = [];
    
    fs.readFile(filePath, 'utf8', (err, data) => {
      if (err) {
        reject(err);
        return;
      }
      
      const lines = data.split(/\r?\n/);
      
      for (const line of lines) {
        if (line.trim() === '') {
          continue; // Skip empty lines
        }
        rows.push(parseCsvLine(line));
      }
      
      resolve(rows);
    });
  });
}

/**
 * Writes rows to a CSV file
 * @param {string} filePath - Path to the CSV file
 * @param {Array<Array<string>>} rows - Array of rows to write
 */
async function writeCsvFile(filePath, rows) {
  const lines = rows.map(row => {
    return row.map(field => escapeCsvField(field)).join(',');
  });
  
  const content = lines.join('\n') + '\n';
  
  return new Promise((resolve, reject) => {
    fs.writeFile(filePath, content, 'utf8', (err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

/**
 * Main export function
 * @param {string} crawlJobId - The crawl job ID to export
 * @param {string} csvFilePath - Path to the CSV file (optional)
 */
async function exportCrawlToCsv(crawlJobId, csvFilePath = DEFAULT_CSV_PATH) {
  try {
    console.log(`Exporting crawl job ${crawlJobId} to CSV...`);
    console.log(`CSV file: ${csvFilePath}`);
    
    // Query Supabase for normalized_url values
    console.log('Querying Supabase for pages...');
    const { data: pages, error } = await supabase
      .from('pages')
      .select('normalized_url')
      .eq('crawl_job_id', crawlJobId)
      .not('normalized_url', 'is', null)
      .order('normalized_url', { ascending: true });
    
    if (error) {
      throw new Error(`Failed to query Supabase: ${error.message}`);
    }
    
    if (!pages || pages.length === 0) {
      console.log(`No pages found for crawl job ${crawlJobId}`);
      return;
    }
    
    // Extract and sort normalized URLs
    const normalizedUrls = pages
      .map(page => page.normalized_url)
      .filter(url => url && url.trim() !== '')
      .sort(); // Alphabetical sort
    
    console.log(`Found ${normalizedUrls.length} normalized URLs`);
    
    // Read existing CSV file
    console.log('Reading existing CSV file...');
    const existingRows = await readCsvFile(csvFilePath);
    
    if (existingRows.length === 0) {
      throw new Error('CSV file is empty or invalid');
    }
    
    // Preserve header row (row 1)
    const headerRow = existingRows[0];
    const columnBValues = existingRows.slice(1).map(row => row[1] || ''); // Column B values
    
    // Create new rows: header + data rows
    const newRows = [headerRow]; // Start with header
    
    // Determine the maximum number of rows (use the larger of URLs or existing Column B)
    const maxRows = Math.max(normalizedUrls.length, columnBValues.length);
    
    // Build rows starting from row 2
    for (let i = 0; i < maxRows; i++) {
      const columnA = normalizedUrls[i] || ''; // Column A: normalized URL (or empty if we run out)
      const columnB = columnBValues[i] || ''; // Column B: preserve existing value (or empty if we run out)
      newRows.push([columnA, columnB]);
    }
    
    // Write updated CSV back to disk
    console.log('Writing updated CSV file...');
    await writeCsvFile(csvFilePath, newRows);
    
    console.log(`✓ Successfully exported ${normalizedUrls.length} URLs to Column A of ${csvFilePath}`);
    console.log(`  Total rows: ${newRows.length - 1} (excluding header)`);
    
  } catch (error) {
    console.error('Error exporting crawl to CSV:', error.message);
    process.exit(1);
  }
}

// Main execution
if (require.main === module) {
  const crawlJobId = process.argv[2];
  const csvFilePath = process.argv[3];
  
  if (!crawlJobId) {
    console.error('Usage: node scripts/exportCrawlToCsv.js <crawl_job_id> [csv_file_path]');
    console.error('Example: node scripts/exportCrawlToCsv.js 123');
    process.exit(1);
  }
  
  exportCrawlToCsv(crawlJobId, csvFilePath);
}

module.exports = { exportCrawlToCsv };

