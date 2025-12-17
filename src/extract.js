/**
 * HTML metadata extraction
 * 
 * This file handles parsing HTML content and extracting relevant metadata
 * such as page titles, descriptions, links, images, and other structured data.
 * It will parse the HTML DOM and extract information needed for site mapping
 * and content analysis.
 */

const cheerio = require('cheerio');

/**
 * Extracts metadata from HTML content
 * @param {Object} params - The page data
 * @param {string} params.url - The URL of the page
 * @param {string} params.html - The HTML content
 * @returns {Object} - Object with url, title, metaDescription, and h1
 */
function extractPageData({ url, html }) {
  const $ = cheerio.load(html);

  // Extract title
  let title = $('title').first().text();
  title = title ? title.trim() : null;
  title = title || null; // Convert empty string to null

  // Extract meta description
  let metaDescription = $('meta[name="description"]').attr('content');
  metaDescription = metaDescription ? metaDescription.trim() : null;
  metaDescription = metaDescription || null; // Convert empty string to null

  // Extract first h1
  let h1 = $('h1').first().text();
  h1 = h1 ? h1.trim() : null;
  h1 = h1 || null; // Convert empty string to null

  // Extract canonical URL
  let canonical = $('link[rel="canonical"]').attr('href');
  canonical = canonical ? canonical.trim() : null;
  canonical = canonical || null; // Convert empty string to null

  return {
    url,
    title,
    metaDescription,
    h1,
    canonical
  };
}

/**
 * Extracts all anchor links from HTML content
 * @param {string} html - The HTML content
 * @returns {Array<string>} - Array of href attribute values
 */
function extractLinks(html) {
  const $ = cheerio.load(html);
  const links = [];

  $('a[href]').each((i, elem) => {
    const href = $(elem).attr('href');
    if (href) {
      links.push(href);
    }
  });

  return links;
}

module.exports = {
  extractPageData,
  extractLinks
};

