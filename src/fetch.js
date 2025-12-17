/**
 * HTTP fetching + retries
 * 
 * This file is responsible for making HTTP requests to fetch web pages.
 * It handles retry logic for failed requests, timeout management, and
 * error handling. Provides a robust mechanism for downloading HTML content
 * from URLs with appropriate retry strategies and error recovery.
 */

const axios = require('axios');

/**
 * Fetches a web page and returns its HTML content
 * @param {string} url - The URL to fetch
 * @returns {Promise<Object>} - Object with url, status, and html (string or null)
 * @throws {Error} - Throws descriptive error on network failure
 */
async function fetchPage(url) {
  try {
    const response = await axios.get(url, {
      timeout: 10000, // 10 seconds
      maxRedirects: 10, // Follow redirects
      validateStatus: function (status) {
        // Accept any status code (we'll check it in the response)
        return status >= 200 && status < 600;
      },
      responseType: 'text', // Get response as text
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; SiteMapScraper/1.0)'
      }
    });

    // Check if response is HTML based on content-type header
    const contentType = response.headers['content-type'] || '';
    const isHTML = contentType.includes('text/html');

    // Get the final URL after redirects
    const finalUrl = response.request.res.responseUrl || url;

    return {
      url: finalUrl,
      status: response.status,
      html: isHTML ? response.data : null
    };
  } catch (error) {
    // Handle network errors and timeouts
    if (error.code === 'ECONNABORTED') {
      throw new Error(`Request timeout: Failed to fetch ${url} within 10 seconds`);
    } else if (error.code === 'ENOTFOUND' || error.code === 'ECONNREFUSED') {
      throw new Error(`Network error: Unable to connect to ${url}`);
    } else if (error.response) {
      // Server responded with error status
      throw new Error(`HTTP error ${error.response.status}: Failed to fetch ${url}`);
    } else {
      // Other errors
      throw new Error(`Failed to fetch ${url}: ${error.message}`);
    }
  }
}

module.exports = {
  fetchPage
};

