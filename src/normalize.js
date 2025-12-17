/**
 * URL normalization + filtering
 * 
 * This file provides utilities for normalizing URLs to ensure consistent
 * representation and avoid duplicate crawling. It handles URL canonicalization,
 * filtering out unwanted URLs (e.g., external links, file types), and
 * determining which URLs should be included in the crawl scope.
 */

const { URL } = require('url');

// Pagination query parameters to preserve during normalization
const PAGINATION_PARAMS = new Set(['page', 'p', 'pagenum', 'paged', 'offset', 'start']);

/**
 * Normalizes a URL by removing hash fragments, tracking parameters, and normalizing trailing slashes
 * Preserves pagination query parameters to allow paginated URLs to be treated as separate pages
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

    // Remove hash fragments
    parsedUrl.hash = '';

    // Remove tracking parameters but preserve pagination parameters
    const trackingParams = ['fbclid', 'gclid'];
    const paramsToRemove = new Set();
    
    // Find and remove utm_* parameters and other tracking params
    parsedUrl.searchParams.forEach((value, key) => {
      const keyLower = key.toLowerCase();
      // Keep pagination params, remove tracking params
      if (!PAGINATION_PARAMS.has(keyLower) && 
          (keyLower.startsWith('utm_') || trackingParams.includes(keyLower))) {
        paramsToRemove.add(key);
      }
    });

    // Remove the tracking parameters
    paramsToRemove.forEach(param => {
      parsedUrl.searchParams.delete(param);
    });

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

    // Build the normalized URL with preserved query params (pagination)
    const normalized = `${parsedUrl.protocol}//${parsedUrl.hostname}${pathname}${parsedUrl.search}`;

    return normalized;
  } catch (error) {
    // Invalid URL, return null
    return null;
  }
}

/**
 * Normalizes a URL and filters by domain (for link discovery during crawling)
 * @param {string} url - The URL to normalize (can be relative or absolute)
 * @param {string} baseDomain - The base domain to resolve relative URLs against
 * @returns {string|null} - The normalized absolute URL, or null if external or invalid
 */
function normalizeAndFilterUrl(url, baseDomain) {
  try {
    // Extract just the domain from baseDomain (remove protocol and path if present)
    let baseDomainOnly = baseDomain.replace(/^https?:\/\//, '').split('/')[0].toLowerCase();
    
    // Normalize www. and non-www hostnames to be identical (remove www. prefix)
    if (baseDomainOnly.startsWith('www.')) {
      baseDomainOnly = baseDomainOnly.substring(4);
    }
    
    // Resolve relative URLs against baseDomain
    // Ensure baseDomain has a protocol for URL resolution
    const baseUrl = baseDomain.startsWith('http') 
      ? baseDomain 
      : `https://${baseDomainOnly}`;
    
    const parsedUrl = new URL(url, baseUrl);

    // Lowercase protocol and hostname
    parsedUrl.protocol = parsedUrl.protocol.toLowerCase();
    let hostname = parsedUrl.hostname.toLowerCase();
    
    // Normalize www. and non-www hostnames to be identical (remove www. prefix)
    if (hostname.startsWith('www.')) {
      hostname = hostname.substring(4);
    }
    parsedUrl.hostname = hostname;

    // Remove hash fragments
    parsedUrl.hash = '';

    // Remove tracking parameters (utm_*, fbclid, gclid)
    const trackingParams = ['fbclid', 'gclid'];
    const paramsToRemove = new Set();
    
    // Find and remove utm_* parameters and other tracking params
    parsedUrl.searchParams.forEach((value, key) => {
      const keyLower = key.toLowerCase();
      if (keyLower.startsWith('utm_') || trackingParams.includes(keyLower)) {
        paramsToRemove.add(key);
      }
    });

    // Remove the tracking parameters
    paramsToRemove.forEach(param => {
      parsedUrl.searchParams.delete(param);
    });

    // Check if URL is same domain as baseDomain
    // Both hostname and baseDomainOnly are already normalized (www. removed)
    const urlDomain = parsedUrl.hostname; // Already normalized above

    // Check if domains match exactly or if urlDomain is a subdomain of baseDomain
    // e.g., blog.example.com matches example.com, but evil.com.example.com does not
    // Note: www. has already been normalized away, so www.example.com and example.com both become example.com
    if (urlDomain !== baseDomainOnly && !urlDomain.endsWith('.' + baseDomainOnly)) {
      return null; // External URL, ignore
    }

    // Normalize the URL
    const normalized = normalizeUrl(`${parsedUrl.protocol}//${parsedUrl.hostname}${parsedUrl.pathname}`);
    
    return normalized;
  } catch (error) {
    // Invalid URL, return null
    return null;
  }
}

module.exports = {
  normalizeUrl,
  normalizeAndFilterUrl
};

