#!/usr/bin/env node

/**
 * Helper script to list crawl jobs from Supabase
 * Helps you find a crawl_job_id to test content extraction with
 */

require('dotenv').config();
const supabase = require('./src/supabase');

async function listCrawlJobs() {
  console.log('Fetching crawl jobs...\n');
  
  try {
    const { data: jobs, error } = await supabase
      .from('crawl_jobs')
      .select('id, domain, status, pages_crawled, pages_discovered, started_at, completed_at')
      .order('started_at', { ascending: false })
      .limit(20);
    
    if (error) {
      console.error('Error fetching crawl jobs:', error.message);
      process.exit(1);
    }
    
    if (!jobs || jobs.length === 0) {
      console.log('No crawl jobs found.');
      console.log('\nTo create a new crawl job, use:');
      console.log('  curl -X POST <API_URL>/crawl -H "Content-Type: application/json" -d \'{"domain":"https://example.com","projectId":"test"}\'');
      process.exit(0);
    }
    
    console.log('Available Crawl Jobs:');
    console.log('='.repeat(100));
    console.log('');
    
    jobs.forEach((job, index) => {
      console.log(`${index + 1}. Crawl Job ID: ${job.id}`);
      console.log(`   Domain: ${job.domain}`);
      console.log(`   Status: ${job.status}`);
      console.log(`   Pages Crawled: ${job.pages_crawled || 'N/A'}`);
      console.log(`   Pages Discovered: ${job.pages_discovered || 'N/A'}`);
      console.log(`   Started: ${job.started_at ? new Date(job.started_at).toLocaleString() : 'N/A'}`);
      console.log(`   Completed: ${job.completed_at ? new Date(job.completed_at).toLocaleString() : 'N/A'}`);
      console.log('');
    });
    
    console.log('='.repeat(100));
    console.log('');
    console.log('To test content extraction, use a completed crawl job:');
    console.log(`  node test-content-extraction.js <API_URL> ${jobs[0].id}`);
    console.log('');
    
  } catch (error) {
    console.error('Fatal error:', error.message);
    process.exit(1);
  }
}

listCrawlJobs();

