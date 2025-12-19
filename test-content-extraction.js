#!/usr/bin/env node

/**
 * Test script for content extraction feature
 * Tests the content extraction API against the live endpoint
 * 
 * Usage:
 *   node test-content-extraction.js <API_BASE_URL> <CRAWL_JOB_ID>
 * 
 * Example:
 *   node test-content-extraction.js https://api.example.com abc123-def456
 */

require('dotenv').config();
const axios = require('axios');

// Get command line arguments
const args = process.argv.slice(2);

if (args.length < 2) {
  console.error('Usage: node test-content-extraction.js <API_BASE_URL> <CRAWL_JOB_ID>');
  console.error('');
  console.error('Example:');
  console.error('  node test-content-extraction.js https://api.example.com abc123-def456');
  console.error('');
  console.error('Or if testing locally:');
  console.error('  node test-content-extraction.js http://localhost:3000 abc123-def456');
  process.exit(1);
}

const API_BASE_URL = args[0].replace(/\/$/, ''); // Remove trailing slash
const CRAWL_JOB_ID = args[1];
const ONLY_MISSING = args[2] !== 'false'; // Default to true unless explicitly set to false

console.log('='.repeat(60));
console.log('Content Extraction Test');
console.log('='.repeat(60));
console.log(`API Base URL: ${API_BASE_URL}`);
console.log(`Crawl Job ID (Sitemap ID): ${CRAWL_JOB_ID}`);
console.log(`Only Missing: ${ONLY_MISSING}`);
console.log('');

// Step 1: Check if crawl job exists
async function checkCrawlJob() {
  console.log('Step 1: Checking crawl job status...');
  try {
    const response = await axios.get(`${API_BASE_URL}/crawl/${CRAWL_JOB_ID}`);
    console.log('✓ Crawl job found:');
    console.log(`  - Status: ${response.data.status}`);
    console.log(`  - Domain: ${response.data.domain}`);
    console.log(`  - Pages crawled: ${response.data.pages_crawled || 'N/A'}`);
    console.log(`  - Pages discovered: ${response.data.pages_discovered || 'N/A'}`);
    console.log('');
    
    if (response.data.status !== 'completed') {
      console.warn('⚠ Warning: Crawl job is not completed. Content extraction may have limited pages.');
      console.log('');
    }
    
    return true;
  } catch (error) {
    if (error.response && error.response.status === 404) {
      console.error('✗ Error: Crawl job not found');
      console.error(`  Make sure the crawl_job_id "${CRAWL_JOB_ID}" exists.`);
    } else {
      console.error('✗ Error checking crawl job:', error.message);
      if (error.response) {
        console.error(`  Status: ${error.response.status}`);
        console.error(`  Response: ${JSON.stringify(error.response.data, null, 2)}`);
      }
    }
    return false;
  }
}

// Step 2: Trigger content extraction
async function triggerExtraction() {
  console.log('Step 2: Triggering content extraction...');
  try {
    const response = await axios.post(
      `${API_BASE_URL}/api/sitemaps/${CRAWL_JOB_ID}/extract-content`,
      { onlyMissing: ONLY_MISSING },
      { headers: { 'Content-Type': 'application/json' } }
    );
    
    console.log('✓ Content extraction job started:');
    console.log(`  - Extraction Job ID: ${response.data.extraction_job_id}`);
    console.log(`  - Status: ${response.data.status}`);
    console.log(`  - Message: ${response.data.message}`);
    console.log('');
    
    return response.data.extraction_job_id;
  } catch (error) {
    console.error('✗ Error triggering content extraction:', error.message);
    if (error.response) {
      console.error(`  Status: ${error.response.status}`);
      console.error(`  Response: ${JSON.stringify(error.response.data, null, 2)}`);
    }
    return null;
  }
}

// Step 3: Poll for status
async function pollStatus(extractionJobId) {
  console.log('Step 3: Polling extraction job status...');
  console.log(`  Extraction Job ID: ${extractionJobId}`);
  console.log('');
  
  let lastStatus = null;
  let pollCount = 0;
  const maxPolls = 120; // 10 minutes max (5 second intervals)
  
  const pollInterval = setInterval(async () => {
    pollCount++;
    
    try {
      const response = await axios.get(
        `${API_BASE_URL}/api/sitemaps/${CRAWL_JOB_ID}/extract-content/${extractionJobId}`
      );
      
      const status = response.data;
      
      // Only log if status changed or every 10 polls
      if (JSON.stringify(status) !== JSON.stringify(lastStatus) || pollCount % 10 === 0) {
        console.log(`[${new Date().toLocaleTimeString()}] Status: ${status.status}`);
        if (status.pages_total !== null) {
          console.log(`  - Total pages: ${status.pages_total}`);
        }
        if (status.pages_extracted !== null) {
          console.log(`  - Extracted: ${status.pages_extracted}`);
        }
        if (status.pages_failed !== null) {
          console.log(`  - Failed: ${status.pages_failed}`);
        }
        console.log('');
      }
      
      lastStatus = status;
      
      // Check if completed or failed
      if (status.status === 'completed') {
        clearInterval(pollInterval);
        console.log('='.repeat(60));
        console.log('✓ Content extraction completed!');
        console.log('='.repeat(60));
        console.log(`  - Total pages: ${status.pages_total || 0}`);
        console.log(`  - Successfully extracted: ${status.pages_extracted || 0}`);
        console.log(`  - Failed: ${status.pages_failed || 0}`);
        if (status.completed_at) {
          console.log(`  - Completed at: ${new Date(status.completed_at).toLocaleString()}`);
        }
        console.log('');
        return true;
      } else if (status.status === 'failed') {
        clearInterval(pollInterval);
        console.log('='.repeat(60));
        console.error('✗ Content extraction failed!');
        console.log('='.repeat(60));
        if (status.error_message) {
          console.error(`  Error: ${status.error_message}`);
        }
        console.log('');
        return false;
      }
      
      // Safety check
      if (pollCount >= maxPolls) {
        clearInterval(pollInterval);
        console.log('='.repeat(60));
        console.warn('⚠ Polling timeout reached');
        console.log('='.repeat(60));
        console.log('  Extraction may still be running. Check status manually:');
        console.log(`  curl ${API_BASE_URL}/api/sitemaps/${CRAWL_JOB_ID}/extract-content/${extractionJobId}`);
        console.log('');
        return false;
      }
    } catch (error) {
      console.error('✗ Error polling status:', error.message);
      if (error.response) {
        console.error(`  Status: ${error.response.status}`);
        console.error(`  Response: ${JSON.stringify(error.response.data, null, 2)}`);
      }
      clearInterval(pollInterval);
      return false;
    }
  }, 5000); // Poll every 5 seconds
}

// Main execution
async function main() {
  // Check crawl job
  const crawlJobExists = await checkCrawlJob();
  if (!crawlJobExists) {
    process.exit(1);
  }
  
  // Trigger extraction
  const extractionJobId = await triggerExtraction();
  if (!extractionJobId) {
    process.exit(1);
  }
  
  // Poll for status
  await pollStatus(extractionJobId);
  
  console.log('Test complete!');
  console.log('');
  console.log('To check status again:');
  console.log(`  curl ${API_BASE_URL}/api/sitemaps/${CRAWL_JOB_ID}/extract-content/${extractionJobId}`);
  console.log('');
}

// Run
main().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});

