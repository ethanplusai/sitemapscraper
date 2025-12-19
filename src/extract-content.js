/**
 * Content extraction for batch processing
 * 
 * This module handles extracting structured content from HTML pages.
 * It is separate from the initial crawl and runs as a batch job.
 * Extracts clean text, headings, and other structured content.
 */

const cheerio = require('cheerio');
const { fetchPage } = require('./fetch');

/**
 * Extracts clean text from HTML, removing scripts, styles, and other non-content elements
 * @param {string} html - The HTML content
 * @returns {string} - Clean text content
 */
function extractCleanText(html) {
  const $ = cheerio.load(html);
  
  // Remove script, style, and other non-content elements
  $('script, style, noscript, iframe, embed, object, svg').remove();
  
  // Get text from body (or html if no body)
  const body = $('body').length > 0 ? $('body') : $('html');
  
  // Extract text and clean it up
  let text = body.text();
  
  // Normalize whitespace: replace multiple spaces/newlines/tabs with single space
  text = text.replace(/\s+/g, ' ');
  
  // Trim
  text = text.trim();
  
  return text;
}

/**
 * Extracts headings from HTML
 * @param {string} html - The HTML content
 * @returns {Object} - Object with h1, h2, h3 arrays
 */
function extractHeadings(html) {
  const $ = cheerio.load(html);
  
  const headings = {
    h1: [],
    h2: [],
    h3: []
  };
  
  // Extract h1 headings
  $('h1').each((i, elem) => {
    const text = $(elem).text().trim();
    if (text) {
      headings.h1.push(text);
    }
  });
  
  // Extract h2 headings
  $('h2').each((i, elem) => {
    const text = $(elem).text().trim();
    if (text) {
      headings.h2.push(text);
    }
  });
  
  // Extract h3 headings
  $('h3').each((i, elem) => {
    const text = $(elem).text().trim();
    if (text) {
      headings.h3.push(text);
    }
  });
  
  return headings;
}

/**
 * Extracts structured content from a page
 * @param {string} url - The URL to extract content from
 * @param {Object} options - Extraction options
 * @param {boolean} options.includeRawHtml - Whether to include raw HTML in response
 * @returns {Promise<Object>} - Extracted content object
 */
async function extractPageContent(url, options = {}) {
  const { includeRawHtml = false } = options;
  
  try {
    // Fetch the page
    const fetchResult = await fetchPage(url);
    
    if (!fetchResult.html) {
      throw new Error(`Failed to fetch HTML content from ${url}`);
    }
    
    // Extract clean text
    const cleanText = extractCleanText(fetchResult.html);
    
    // Extract headings
    const headings = extractHeadings(fetchResult.html);
    
    // Build content object
    const content = {
      normalized_url: url, // Will be normalized by caller
      fetched_at: new Date().toISOString(),
      clean_text: cleanText,
      headings: headings
    };
    
    // Optionally include raw HTML
    if (includeRawHtml) {
      content.raw_html = fetchResult.html;
    }
    
    return content;
  } catch (error) {
    throw new Error(`Content extraction failed for ${url}: ${error.message}`);
  }
}

module.exports = {
  extractPageContent,
  extractCleanText,
  extractHeadings
};

