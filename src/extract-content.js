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
  
  // Extract h1 headings - preserve order of appearance
  $('h1').each((i, elem) => {
    const text = $(elem).text().trim();
    if (text) {
      headings.h1.push(text);
    }
  });
  
  // Extract h2 headings - preserve order of appearance
  $('h2').each((i, elem) => {
    const text = $(elem).text().trim();
    if (text) {
      headings.h2.push(text);
    }
  });
  
  // Extract h3 headings - preserve order of appearance
  $('h3').each((i, elem) => {
    const text = $(elem).text().trim();
    if (text) {
      headings.h3.push(text);
    }
  });
  
  return headings;
}

/**
 * Extracts SEO metadata from HTML
 * Extracts title, meta description, canonical URL, robots, and Open Graph tags
 * Do NOT infer or auto-generate missing values - store null if not present
 * 
 * @param {string} html - The HTML content
 * @returns {Object} - SEO object with title, meta_description, canonical_url, robots, and og
 */
function extractSEO(html) {
  const $ = cheerio.load(html);
  
  // Extract title from <title> tag
  let title = $('title').first().text();
  title = title ? title.trim() : null;
  title = title || null; // Convert empty string to null
  
  // Extract meta description
  let metaDescription = $('meta[name="description"]').attr('content');
  metaDescription = metaDescription ? metaDescription.trim() : null;
  metaDescription = metaDescription || null; // Convert empty string to null
  
  // Extract canonical URL from <link rel="canonical">
  let canonicalUrl = $('link[rel="canonical"]').attr('href');
  canonicalUrl = canonicalUrl ? canonicalUrl.trim() : null;
  canonicalUrl = canonicalUrl || null; // Convert empty string to null
  
  // Extract robots meta tag
  let robots = $('meta[name="robots"]').attr('content');
  robots = robots ? robots.trim() : null;
  robots = robots || null; // Convert empty string to null
  
  // Extract Open Graph tags
  const og = {
    title: null,
    description: null,
    image: null
  };
  
  // og:title
  let ogTitle = $('meta[property="og:title"]').attr('content');
  ogTitle = ogTitle ? ogTitle.trim() : null;
  og.title = ogTitle || null;
  
  // og:description
  let ogDescription = $('meta[property="og:description"]').attr('content');
  ogDescription = ogDescription ? ogDescription.trim() : null;
  og.description = ogDescription || null;
  
  // og:image
  let ogImage = $('meta[property="og:image"]').attr('content');
  ogImage = ogImage ? ogImage.trim() : null;
  og.image = ogImage || null;
  
  return {
    title,
    meta_description: metaDescription,
    canonical_url: canonicalUrl,
    robots,
    og
  };
}

/**
 * Extracts JSON-LD schema from HTML
 * Finds all <script type="application/ld+json"> tags and parses them as raw JSON
 * Do NOT normalize, validate, merge, or classify - store as-is
 * 
 * @param {string} html - The HTML content
 * @returns {Object} - Schema object with json_ld array
 */
function extractSchema(html) {
  const $ = cheerio.load(html);
  const jsonLdScripts = [];
  
  // Find all script tags with type="application/ld+json"
  $('script[type="application/ld+json"]').each((i, elem) => {
    const scriptContent = $(elem).html();
    
    if (!scriptContent) {
      return; // Skip empty scripts
    }
    
    try {
      // Parse JSON safely - preserve as raw object, do not validate or normalize
      const parsed = JSON.parse(scriptContent.trim());
      
      // Store as-is - could be object or array
      // If it's an array, we'll store each item separately
      // If it's an object, we'll store it as a single item in the array
      if (Array.isArray(parsed)) {
        // Multiple schemas in one script tag
        jsonLdScripts.push(...parsed);
      } else {
        // Single schema object
        jsonLdScripts.push(parsed);
      }
    } catch (error) {
      // Log parsing error but continue extraction - do not fail the page
      console.warn(`[EXTRACT SCHEMA] Failed to parse JSON-LD script at index ${i}: ${error.message}`);
      // Skip this block and continue
    }
  });
  
  return {
    json_ld: jsonLdScripts
  };
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
    
    // Extract headings (already structured as h1, h2, h3 arrays)
    const headings = extractHeadings(fetchResult.html);
    
    // Extract SEO metadata (title, meta_description, canonical_url, robots, og tags)
    // Failures in SEO extraction must NOT fail the page extraction
    let seo = null;
    try {
      seo = extractSEO(fetchResult.html);
    } catch (error) {
      console.warn(`[EXTRACT CONTENT] SEO extraction failed for ${url}: ${error.message}`);
      // Set default null values if extraction fails
      seo = {
        title: null,
        meta_description: null,
        canonical_url: null,
        robots: null,
        og: {
          title: null,
          description: null,
          image: null
        }
      };
    }
    
    // Extract JSON-LD schema (raw, non-interpreted)
    // Failures in schema extraction must NOT fail the page extraction
    let schema = null;
    try {
      schema = extractSchema(fetchResult.html);
    } catch (error) {
      console.warn(`[EXTRACT CONTENT] Schema extraction failed for ${url}: ${error.message}`);
      // Set default empty array if extraction fails
      schema = {
        json_ld: []
      };
    }
    
    // Build content object with schema versioning
    const content = {
      normalized_url: url, // Will be normalized by caller
      fetched_at: new Date().toISOString(),
      content_schema_version: '1.0', // Version field for future schema changes
      clean_text: cleanText,
      headings: headings, // Already structured as { h1: [], h2: [], h3: [] }
      seo: seo, // SEO metadata object
      schema: schema // Schema object with json_ld array
    };
    
    // Optionally include raw HTML
    if (includeRawHtml) {
      content.raw_html = fetchResult.html;
      // DEBUG: Log that we're including raw HTML
      console.log(`[EXTRACT CONTENT] Including raw HTML for ${url}:`, {
        html_length: fetchResult.html ? fetchResult.html.length : 0,
        html_preview: fetchResult.html ? fetchResult.html.substring(0, 100) : 'null',
        html_type: typeof fetchResult.html
      });
    } else {
      console.log(`[EXTRACT CONTENT] NOT including raw HTML for ${url} (includeRawHtml=false)`);
    }
    
    return content;
  } catch (error) {
    throw new Error(`Content extraction failed for ${url}: ${error.message}`);
  }
}

module.exports = {
  extractPageContent,
  extractCleanText,
  extractHeadings,
  extractSEO,
  extractSchema
};

