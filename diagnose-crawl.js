#!/usr/bin/env node

/**
 * Diagnostic script to check crawl results
 * Shows what's in the database vs what the API would return
 */

// Load .env from project root
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });

// Check if env vars are loaded
if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_KEY) {
  console.error('❌ Error: Missing Supabase environment variables');
  console.error('Make sure .env file exists with SUPABASE_URL and SUPABASE_SERVICE_KEY');
  console.error('Current working directory:', __dirname);
  process.exit(1);
}

const supabase = require('./src/supabase');
const { extractPrimaryDomain, isPrimaryDomain } = require('./src/normalize');

const CRAWL_JOB_ID = process.argv[2] || '872d4854-c8f5-4a63-bfec-1efea89862f2';

async function diagnoseCrawl() {
  console.log('='.repeat(80));
  console.log('CRAWL DIAGNOSTIC REPORT');
  console.log('='.repeat(80));
  console.log(`Crawl Job ID: ${CRAWL_JOB_ID}`);
  console.log('');

  try {
    // Get crawl job info
    const { data: job, error: jobError } = await supabase
      .from('crawl_jobs')
      .select('*')
      .eq('id', CRAWL_JOB_ID)
      .single();

    if (jobError || !job) {
      console.error('❌ Error: Crawl job not found');
      console.error(jobError);
      process.exit(1);
    }

    console.log('Crawl Job Info:');
    console.log(`  - Domain: ${job.domain}`);
    console.log(`  - Status: ${job.status}`);
    console.log(`  - Pages Crawled: ${job.pages_crawled || 'N/A'}`);
    console.log(`  - Pages Discovered: ${job.pages_discovered || 'N/A'}`);
    console.log('');

    // Extract primary domain
    const primaryDomain = extractPrimaryDomain(job.domain);
    console.log(`Primary Domain (normalized): ${primaryDomain}`);
    console.log('');

    // Get all pages from database
    console.log('Fetching pages from database...');
    const { data: pages, error: pagesError } = await supabase
      .from('pages')
      .select('normalized_url, url, status_code, title')
      .eq('crawl_job_id', CRAWL_JOB_ID)
      .order('normalized_url', { ascending: true });

    if (pagesError) {
      console.error('❌ Error fetching pages:', pagesError);
      process.exit(1);
    }

    if (!pages || pages.length === 0) {
      console.log('No pages found in database');
      process.exit(0);
    }

    console.log(`Total pages in database: ${pages.length}`);
    console.log('');

    // Analyze pages
    console.log('='.repeat(80));
    console.log('DATABASE ANALYSIS');
    console.log('='.repeat(80));

    // Check for duplicates
    const urlCounts = new Map();
    const duplicates = [];
    pages.forEach(page => {
      const normalizedUrl = page.normalized_url;
      if (!urlCounts.has(normalizedUrl)) {
        urlCounts.set(normalizedUrl, []);
      }
      urlCounts.get(normalizedUrl).push(page);
    });

    urlCounts.forEach((instances, url) => {
      if (instances.length > 1) {
        duplicates.push({ url, count: instances.length, instances });
      }
    });

    console.log(`\n📊 Duplicate Analysis:`);
    console.log(`  - Unique normalized URLs: ${urlCounts.size}`);
    console.log(`  - Total pages: ${pages.length}`);
    console.log(`  - Duplicate URLs found: ${duplicates.length}`);

    if (duplicates.length > 0) {
      console.log(`\n⚠️  DUPLICATES FOUND:`);
      duplicates.forEach(({ url, count, instances }) => {
        console.log(`  - ${url} (appears ${count} times)`);
        instances.forEach((instance, idx) => {
          console.log(`    [${idx + 1}] Original URL: ${instance.url || 'N/A'}`);
        });
      });
    }

    // Check for external domains
    const externalDomains = [];
    const domainCounts = new Map();
    
    pages.forEach(page => {
      if (!page.normalized_url) return;
      
      const pageDomain = extractPrimaryDomain(page.normalized_url);
      if (pageDomain && pageDomain !== primaryDomain) {
        if (!domainCounts.has(pageDomain)) {
          domainCounts.set(pageDomain, []);
        }
        domainCounts.get(pageDomain).push(page);
        externalDomains.push(page);
      }
    });

    console.log(`\n📊 Domain Analysis:`);
    console.log(`  - Primary domain: ${primaryDomain}`);
    console.log(`  - Pages from primary domain: ${pages.length - externalDomains.length}`);
    console.log(`  - Pages from external domains: ${externalDomains.length}`);

    if (externalDomains.length > 0) {
      console.log(`\n⚠️  EXTERNAL DOMAINS FOUND:`);
      domainCounts.forEach((instances, domain) => {
        console.log(`  - ${domain} (${instances.length} pages)`);
        instances.slice(0, 5).forEach(page => {
          console.log(`    - ${page.normalized_url}`);
        });
        if (instances.length > 5) {
          console.log(`    ... and ${instances.length - 5} more`);
        }
      });
    }

    // Show what API would return (with filtering)
    console.log('\n' + '='.repeat(80));
    console.log('API RESPONSE SIMULATION (with filtering)');
    console.log('='.repeat(80));

    // Apply API filtering logic
    const primaryDomainPages = pages.filter(page => 
      page.normalized_url && isPrimaryDomain(page.normalized_url, primaryDomain)
    );

    const pagesMap = new Map();
    const seenUrls = new Set();
    for (const page of primaryDomainPages) {
      const normalizedUrl = page.normalized_url;
      const normalizedKey = normalizedUrl.toLowerCase().trim();
      
      if (pagesMap.has(normalizedKey) || seenUrls.has(normalizedKey)) {
        continue;
      }
      
      pagesMap.set(normalizedKey, page);
      seenUrls.add(normalizedKey);
    }

    console.log(`\nAPI would return: ${pagesMap.size} pages`);
    console.log(`  - Filtered from ${pages.length} total pages`);
    console.log(`  - Removed ${pages.length - primaryDomainPages.length} external domain pages`);
    console.log(`  - Removed ${primaryDomainPages.length - pagesMap.size} duplicate pages`);
    console.log('');

    // List all URLs that API would return
    console.log('URLs that API would return:');
    console.log('-'.repeat(80));
    const apiUrls = Array.from(pagesMap.values())
      .map(p => p.normalized_url)
      .sort();
    
    apiUrls.forEach((url, idx) => {
      console.log(`${(idx + 1).toString().padStart(4)}. ${url}`);
    });

    // Summary
    console.log('\n' + '='.repeat(80));
    console.log('SUMMARY');
    console.log('='.repeat(80));
    console.log(`Database contains: ${pages.length} pages`);
    console.log(`  - Primary domain: ${pages.length - externalDomains.length}`);
    console.log(`  - External domains: ${externalDomains.length}`);
    console.log(`  - Duplicates: ${pages.length - urlCounts.size}`);
    console.log(`API would return: ${pagesMap.size} pages (after filtering)`);
    console.log('');

    if (duplicates.length > 0 || externalDomains.length > 0) {
      console.log('⚠️  ISSUES DETECTED:');
      if (duplicates.length > 0) {
        console.log(`  - ${duplicates.length} duplicate URLs in database`);
      }
      if (externalDomains.length > 0) {
        console.log(`  - ${externalDomains.length} pages from external domains in database`);
      }
      console.log('\nThe API filtering should prevent these from showing in the UI.');
      console.log('If you see them in the UI, the issue is in the API filtering logic.');
    } else {
      console.log('✅ No issues detected - all pages are from primary domain and unique');
    }

  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
}

diagnoseCrawl();

