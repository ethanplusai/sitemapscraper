#!/usr/bin/env node

/**
 * Check what the API actually returns for a crawl job
 * Compares with what should be returned after filtering
 */

const axios = require('axios');

const API_BASE_URL = 'https://sitemapscraper-production.up.railway.app';
const CRAWL_JOB_ID = process.argv[2] || '872d4854-c8f5-4a63-bfec-1efea89862f2';

async function checkAPIResponse() {
  console.log('='.repeat(80));
  console.log('API RESPONSE CHECK');
  console.log('='.repeat(80));
  console.log(`Crawl Job ID: ${CRAWL_JOB_ID}`);
  console.log(`API URL: ${API_BASE_URL}`);
  console.log('');

  try {
    // Get crawl job status first
    console.log('Fetching crawl job status...');
    const jobResponse = await axios.get(`${API_BASE_URL}/crawl/${CRAWL_JOB_ID}`);
    const job = jobResponse.data;
    
    console.log('Crawl Job Info:');
    console.log(`  - Domain: ${job.domain}`);
    console.log(`  - Status: ${job.status}`);
    console.log(`  - Pages Crawled: ${job.pages_crawled || 'N/A'}`);
    console.log(`  - Pages Discovered: ${job.pages_discovered || 'N/A'}`);
    console.log('');

    // Extract primary domain
    const domain = job.domain;
    const primaryDomain = domain.replace(/^https?:\/\//, '').split('/')[0].toLowerCase().replace(/^www\./, '');
    console.log(`Primary Domain (normalized): ${primaryDomain}`);
    console.log('');

    // Get pages from API
    console.log('Fetching pages from API...');
    const pagesResponse = await axios.get(`${API_BASE_URL}/crawl/${CRAWL_JOB_ID}/pages`);
    const apiData = pagesResponse.data;
    const pages = apiData.pages || [];

    console.log(`API returned: ${pages.length} pages`);
    console.log('');

    // Analyze the response
    console.log('='.repeat(80));
    console.log('ANALYSIS');
    console.log('='.repeat(80));

    // Check for duplicates
    const urlSet = new Set();
    const duplicates = [];
    pages.forEach((page, idx) => {
      const url = page.normalized_url;
      if (urlSet.has(url)) {
        duplicates.push({ url, index: idx });
      } else {
        urlSet.add(url);
      }
    });

    console.log(`\n📊 Duplicate Analysis:`);
    console.log(`  - Unique URLs: ${urlSet.size}`);
    console.log(`  - Total pages returned: ${pages.length}`);
    console.log(`  - Duplicates found: ${duplicates.length}`);

    if (duplicates.length > 0) {
      console.log(`\n⚠️  DUPLICATES IN API RESPONSE:`);
      duplicates.forEach(({ url, index }) => {
        console.log(`  - Index ${index}: ${url}`);
      });
    }

    // Check for external domains
    const externalDomains = [];
    const domainCounts = new Map();
    
    pages.forEach((page, idx) => {
      const url = page.normalized_url;
      if (!url) return;
      
      try {
        const urlDomain = url.replace(/^https?:\/\//, '').split('/')[0].toLowerCase().replace(/^www\./, '');
        if (urlDomain !== primaryDomain) {
          if (!domainCounts.has(urlDomain)) {
            domainCounts.set(urlDomain, []);
          }
          domainCounts.get(urlDomain).push({ url, index: idx });
          externalDomains.push({ url, index: idx, domain: urlDomain });
        }
      } catch (e) {
        // Invalid URL format
      }
    });

    console.log(`\n📊 Domain Analysis:`);
    console.log(`  - Primary domain: ${primaryDomain}`);
    console.log(`  - Pages from primary domain: ${pages.length - externalDomains.length}`);
    console.log(`  - Pages from external domains: ${externalDomains.length}`);

    if (externalDomains.length > 0) {
      console.log(`\n⚠️  EXTERNAL DOMAINS IN API RESPONSE:`);
      domainCounts.forEach((instances, domain) => {
        console.log(`  - ${domain} (${instances.length} pages)`);
        instances.slice(0, 5).forEach(({ url, index }) => {
          console.log(`    [${index}] ${url}`);
        });
        if (instances.length > 5) {
          console.log(`    ... and ${instances.length - 5} more`);
        }
      });
    }

    // List all URLs
    console.log('\n' + '='.repeat(80));
    console.log('ALL URLs RETURNED BY API');
    console.log('='.repeat(80));
    pages.forEach((page, idx) => {
      const marker = duplicates.some(d => d.index === idx) ? ' [DUPLICATE]' : 
                     externalDomains.some(e => e.index === idx) ? ' [EXTERNAL]' : '';
      console.log(`${(idx + 1).toString().padStart(4)}. ${page.normalized_url}${marker}`);
    });

    // Summary
    console.log('\n' + '='.repeat(80));
    console.log('SUMMARY');
    console.log('='.repeat(80));
    console.log(`API returned: ${pages.length} pages`);
    console.log(`  - Unique URLs: ${urlSet.size}`);
    console.log(`  - Primary domain: ${pages.length - externalDomains.length}`);
    console.log(`  - External domains: ${externalDomains.length}`);
    console.log(`  - Duplicates: ${duplicates.length}`);
    console.log('');

    if (duplicates.length > 0 || externalDomains.length > 0) {
      console.log('❌ ISSUES FOUND IN API RESPONSE:');
      if (duplicates.length > 0) {
        console.log(`  - ${duplicates.length} duplicate URLs`);
      }
      if (externalDomains.length > 0) {
        console.log(`  - ${externalDomains.length} pages from external domains`);
      }
      console.log('\nThis indicates the API filtering is not working correctly.');
    } else {
      console.log('✅ No issues detected - API response looks correct');
    }

  } catch (error) {
    console.error('❌ Error:', error.message);
    if (error.response) {
      console.error('Response status:', error.response.status);
      console.error('Response data:', JSON.stringify(error.response.data, null, 2));
    }
    process.exit(1);
  }
}

checkAPIResponse();

