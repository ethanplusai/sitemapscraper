/**
 * URL normalization + filtering
 * 
 * This file provides utilities for normalizing URLs to ensure consistent
 * representation and avoid duplicate crawling. It handles URL canonicalization,
 * filtering out unwanted URLs (e.g., external links, file types), and
 * determining which URLs should be included in the crawl scope.
 */

const { URL } = require('url');

/**
 * Normalizes a URL according to canonicalization rules:
 * - lowercase hostname
 * - strip leading www.
 * - remove default ports
 * - remove trailing slash except /
 * - remove query string and hash for identity (but keep a representative original_url)
 * @param {string} url - The URL to normalize (must be absolute)
 * @returns {string|null} - The normalized URL, or null if invalid
 */
function normalizeUrl(url) {
  try {
    const parsedUrl = new URL(url);

    // Lowercase protocol and hostname
    parsedUrl.protocol = parsedUrl.protocol.toLowerCase();
    let hostname = parsedUrl.hostname.toLowerCase();
    
    // Normalize www. and non-www hostnames to be identical (remove www. prefix)
    if (hostname.startsWith('www.')) {
      hostname = hostname.substring(4);
    }
    parsedUrl.hostname = hostname;

    // Remove default ports (http:80, https:443)
    if ((parsedUrl.protocol === 'http:' && parsedUrl.port === '80') ||
        (parsedUrl.protocol === 'https:' && parsedUrl.port === '443')) {
      parsedUrl.port = '';
    }

    // Remove hash fragments
    parsedUrl.hash = '';

    // Remove query string for identity (all query params removed)
    parsedUrl.search = '';

    // Normalize pathname - remove duplicate slashes
    let pathname = parsedUrl.pathname.replace(/\/+/g, '/');
    
    // Normalize trailing slashes - remove trailing slash unless it's the root
    if (pathname.length > 1 && pathname.endsWith('/')) {
      pathname = pathname.slice(0, -1);
    }
    
    // Ensure pathname starts with /
    if (!pathname.startsWith('/')) {
      pathname = '/' + pathname;
    }

    // Build the normalized URL without query string
    const normalized = `${parsedUrl.protocol}//${parsedUrl.hostname}${parsedUrl.port ? ':' + parsedUrl.port : ''}${pathname}`;

    return normalized;
  } catch (error) {
    // Invalid URL, return null
    return null;
  }
}

/**
 * Extracts the primary domain from a URL (normalized, without www.)
 * @param {string} url - The URL to extract domain from
 * @returns {string|null} - The normalized primary domain, or null if invalid
 */
function extractPrimaryDomain(url) {
  try {
    const parsedUrl = new URL(url);
    let hostname = parsedUrl.hostname.toLowerCase();
    
    // Remove www. prefix
    if (hostname.startsWith('www.')) {
      hostname = hostname.substring(4);
    }
    
    return hostname;
  } catch (error) {
    return null;
  }
}

/**
 * Checks if a URL belongs to the primary domain (exact match only, no subdomains)
 * @param {string} url - The URL to check
 * @param {string} primaryDomain - The primary domain (normalized, without www.)
 * @returns {boolean} - True if URL belongs to primary domain
 */
function isPrimaryDomain(url, primaryDomain) {
  try {
    const urlDomain = extractPrimaryDomain(url);
    return urlDomain === primaryDomain;
  } catch (error) {
    return false;
  }
}

/**
 * Normalizes a URL and filters by domain (for link discovery during crawling)
 * @param {string} url - The URL to normalize (can be relative or absolute)
 * @param {string} baseUrl - The base URL to resolve relative URLs against
 * @param {string} primaryDomain - The primary domain (normalized, without www.)
 * @returns {Object} - { normalized: string|null, isExternal: boolean }
 */
function normalizeAndClassifyUrl(url, baseUrl, primaryDomain) {
  try {
    // Resolve relative URLs against baseUrl
    const parsedUrl = new URL(url, baseUrl);

    // Normalize the URL
    const normalized = normalizeUrl(parsedUrl.href);
    if (!normalized) {
      return { normalized: null, isExternal: true };
    }

    // Check if URL belongs to primary domain (exact match only)
    const urlDomain = extractPrimaryDomain(normalized);
    const isExternal = urlDomain !== primaryDomain;

    return { normalized, isExternal };
  } catch (error) {
    return { normalized: null, isExternal: true };
  }
}

module.exports = {
  normalizeUrl,
  extractPrimaryDomain,
  isPrimaryDomain,
  normalizeAndClassifyUrl
};

