/**
 * Crawl orchestration + job lifecycle
 * 
 * This file handles the orchestration of website crawling operations.
 * It manages the crawl job lifecycle including job creation, execution,
 * progress tracking, and completion. Coordinates between fetching, extraction,
 * and normalization modules to crawl websites systematically.
 */

// Load environment variables from .env file
require('dotenv').config();

const supabase = require('./supabase');
const { fetchPage } = require('./fetch');
const { extractPageData, extractLinks } = require('./extract');
const { normalizeUrl } = require('./normalize');
const { compareUrls } = require('./compare');
const { URL } = require('url');
const fs = require('fs');
const path = require('path');

/**
 * Starts a crawl job for the given domain and project
 * Creates a job record in Supabase and returns it immediately
 * @param {Object} params - The crawl parameters
 * @param {string} params.domain - The domain to crawl
 * @param {string} params.projectId - The project ID
 * @returns {Promise<Object>} - The created job record
 */
async function startCrawl({ domain, projectId }) {
  // Create a crawl job record in Supabase
  const { data: job, error } = await supabase
    .from('crawl_jobs')
    .insert({
      domain,
      project_id: projectId,
      status: 'running',
      started_at: new Date().toISOString()
    })
    .select()
    .single();

  if (error) {
    throw new Error(`Failed to create crawl job: ${error.message}`);
  }

  // Return the created job record
  return job;
}

// Crawl limits
const MAX_PAGES = 1000;
const MAX_DEPTH = 10;

// Non-HTML file extensions to skip
const NON_HTML_EXTENSIONS = new Set([
  'pdf', 'jpg', 'jpeg', 'png', 'gif', 'svg', 'webp', 'ico',
  'css', 'js', 'json', 'xml', 'zip', 'tar', 'gz', 'rar',
  'mp4', 'mp3', 'avi', 'mov', 'wmv', 'flv',
  'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx'
]);

/**
 * Checks if a URL should be skipped based on protocol or extension
 * @param {string} url - The URL to check
 * @returns {Object} - { skip: boolean, reason: string|null }
 */
function shouldSkipUrl(url) {
  // Skip non-HTTP protocols
  if (url.startsWith('mailto:')) {
    return { skip: true, reason: 'non-HTTP protocol (mailto)' };
  }
  if (url.startsWith('tel:')) {
    return { skip: true, reason: 'non-HTTP protocol (tel)' };
  }
  if (url.startsWith('javascript:')) {
    return { skip: true, reason: 'non-HTTP protocol (javascript)' };
  }

  // Skip hash-only links
  if (url.startsWith('#')) {
    return { skip: true, reason: 'hash-only link' };
  }

  // Check file extension
  try {
    const parsed = new URL(url);
    const pathname = parsed.pathname.toLowerCase();
    const extension = pathname.split('.').pop();
    if (extension && NON_HTML_EXTENSIONS.has(extension)) {
      return { skip: true, reason: `non-HTML extension (.${extension})` };
    }
  } catch (e) {
    // If URL parsing fails, we'll let it through and handle it later
  }

  return { skip: false, reason: null };
}

/**
 * Resolves a relative URL to absolute using a base URL
 * @param {string} url - The URL to resolve (can be relative or absolute)
 * @param {string} baseUrl - The base URL to resolve against
 * @returns {string|null} - The absolute URL or null if invalid
 */
function resolveUrl(url, baseUrl) {
  try {
    return new URL(url, baseUrl).href;
  } catch (error) {
    return null;
  }
}

/**
 * Escapes a CSV field value (handles commas, quotes, newlines)
 * @param {string} value - The value to escape
 * @returns {string} - The escaped value
 */
function escapeCsvField(value) {
  if (value === null || value === undefined) {
    return '';
  }
  
  const str = String(value);
  
  // If the value contains comma, quote, or newline, wrap it in quotes and escape internal quotes
  if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  
  return str;
}

/**
 * Exports crawl results to CSV file
 * Clears existing file and writes unique original URLs (preserving www.)
 * @param {Set<string>} normalizedUrls - Set of normalized URLs (for deduplication)
 * @param {Map<string, string>} originalUrlsMap - Map of normalized -> original URLs
 * @param {string} csvFilePath - Path to the CSV file
 */
async function exportCrawlResultsToCsv(normalizedUrls, originalUrlsMap, csvFilePath, jobId) {
  try {
    // Convert normalized URLs to original URLs, preserving www. if present
    const originalUrls = Array.from(normalizedUrls)
      .map(normalized => originalUrlsMap.get(normalized) || normalized)
      .sort();
    
    // Create CSV content with header
    const lines = ['normalized_url'];
    
    // Add each original URL as a row
    originalUrls.forEach(url => {
      lines.push(escapeCsvField(url));
    });
    
    // Write to file (this will overwrite existing file)
    const content = lines.join('\n') + '\n';
    
    // Ensure directory exists
    const dir = path.dirname(csvFilePath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    
    fs.writeFileSync(csvFilePath, content, 'utf8');
    console.log(`[CRAWL COMPLETION] Job ${jobId} - ✓ Exported ${originalUrls.length} unique URLs to ${csvFilePath}`);
  } catch (error) {
    console.error(`[ERROR] Job ${jobId} - Error exporting crawl results to CSV: ${error.message}`);
    // Don't throw - allow crawl to complete even if CSV export fails
  }
}

/**
 * Updates crawl job status in Supabase
 * @param {string} jobId - The job ID
 * @param {string} status - The status ('running', 'completed', 'failed')
 * @param {Object} metadata - Optional metadata to store
 */
async function updateJobStatus(jobId, status, metadata = {}) {
  const updateData = {
    status,
    ...metadata
  };

  // Add completed_at or failed_at timestamp
  if (status === 'completed') {
    updateData.completed_at = new Date().toISOString();
  } else if (status === 'failed') {
    updateData.failed_at = new Date().toISOString();
  }

  // Always update last_activity_at when status is 'running'
  if (status === 'running') {
    updateData.last_activity_at = new Date().toISOString();
  }

  const { error } = await supabase
    .from('crawl_jobs')
    .update(updateData)
    .eq('id', jobId);

  if (error) {
    console.error(`Error updating job status:`, error);
  }
}

/**
 * Runs the actual crawl for a job
 * @param {Object} job - The job object to process
 */
async function runCrawl(job) {
  const domain = job.domain;
  const seedUrl = domain.startsWith('http') ? domain : `https://${domain}`;
  
  console.log(`[CRAWL START] Job ${job.id} - Starting crawl for domain: ${domain}`);
  console.log(`[CRAWL START] Job ${job.id} - Seed URL: ${seedUrl}`);
  console.log(`[CRAWL START] Job ${job.id} - Max pages: ${MAX_PAGES}, Max depth: ${MAX_DEPTH}`);

  // Initialize BFS queue and data structures first
  const queue = [{ url: seedUrl, depth: 0 }];
  const seenUrls = new Set(); // URLs that have been enqueued (normalized)
  const visitedNormalizedUrls = new Set(); // URLs that have been crawled (normalized) - global deduplication
  const originalUrlsMap = new Map(); // Maps normalized URL -> original URL (for CSV export)
  
  // Crawl statistics - declare before timeout/stall detection
  let pagesCrawled = 0;
  let totalUrlsDiscovered = 0;
  let totalUrlsEnqueued = 0;
  let totalDuplicatesSkipped = 0;
  let maxDepthReached = 0;

  // Timeout and stall detection
  const MAX_RUNTIME_MS = 15 * 60 * 1000; // 15 minutes
  const STALL_DETECTION_MS = 2 * 60 * 1000; // 2 minutes
  const crawlStartTime = Date.now();
  let lastActivityTime = Date.now();
  let timeoutId = null;
  let stallCheckInterval = null;

  // Set hard timeout (15 minutes)
  timeoutId = setTimeout(async () => {
    console.error(`[CRAWL FAILED] Job ${job.id} - Timeout: Exceeded 15 minute runtime limit`);
    if (stallCheckInterval) clearInterval(stallCheckInterval);
    await updateJobStatus(job.id, 'failed', {
      error_message: 'Crawl exceeded maximum runtime of 15 minutes',
      stop_reason: 'timeout',
      pages_discovered: visitedNormalizedUrls.size,
      pages_crawled: pagesCrawled,
      duplicates_skipped: totalDuplicatesSkipped
    });
    console.error(`[CRAWL FAILED] Job ${job.id} - Job marked as failed due to timeout`);
    process.exit(1); // Force exit on timeout
  }, MAX_RUNTIME_MS);

  // Stall detection - check every 30 seconds
  stallCheckInterval = setInterval(async () => {
    const timeSinceLastActivity = Date.now() - lastActivityTime;
    if (timeSinceLastActivity > STALL_DETECTION_MS) {
      console.error(`[CRAWL FAILED] Job ${job.id} - Stalled: No activity for ${Math.round(timeSinceLastActivity / 1000)}s`);
      clearInterval(stallCheckInterval);
      clearTimeout(timeoutId);
      await updateJobStatus(job.id, 'failed', {
        error_message: `Crawl stalled - no activity for ${Math.round(timeSinceLastActivity / 1000)} seconds`,
        stop_reason: 'stalled',
        pages_discovered: visitedNormalizedUrls.size,
        pages_crawled: pagesCrawled,
        duplicates_skipped: totalDuplicatesSkipped
      });
      console.error(`[CRAWL FAILED] Job ${job.id} - Job marked as failed due to stall`);
      process.exit(1);
    }
  }, 30000); // Check every 30 seconds

  // Load already crawled URLs from Supabase for this job
  const { data: existingPages } = await supabase
    .from('pages')
    .select('normalized_url, url')
    .eq('crawl_job_id', job.id);

  if (existingPages) {
    existingPages.forEach(page => {
      if (page.normalized_url) {
        // Add to both sets since they've been both seen and visited
        seenUrls.add(page.normalized_url);
        visitedNormalizedUrls.add(page.normalized_url);
        // Store original URL if available, otherwise use normalized
        const originalUrl = page.url || page.normalized_url;
        originalUrlsMap.set(page.normalized_url, originalUrl);
      }
    });
    console.log(`[CRAWL START] Job ${job.id} - Found ${existingPages.length} existing pages for this job`);
  }

  // Normalize and add seed URL to seenUrls (it's been enqueued)
  const seedNormalized = normalizeUrl(seedUrl);
  if (seedNormalized) {
    seenUrls.add(seedNormalized);
    // Store original seed URL (preserves www. if present)
    originalUrlsMap.set(seedNormalized, seedUrl);
  }

  // Extract hostname from seed URL for same-domain filtering
  // Normalize www. and non-www hostnames to be identical
  let seedHostname;
  try {
    seedHostname = new URL(seedUrl).hostname.toLowerCase();
    // Remove www. prefix for consistent comparison
    if (seedHostname.startsWith('www.')) {
      seedHostname = seedHostname.substring(4);
    }
  } catch (error) {
    console.error(`[CRAWL FAILED] Job ${job.id} - Invalid seed URL: ${seedUrl}`);
    await updateJobStatus(job.id, 'failed', {
      error_message: `Invalid seed URL: ${seedUrl}`,
      stop_reason: 'invalid_seed_url'
    });
    return;
  }

  // BFS crawl loop
  while (queue.length > 0 && pagesCrawled < MAX_PAGES) {
    const { url: currentUrl, depth } = queue.shift();

    // Skip if depth exceeds limit
    if (depth > MAX_DEPTH) {
      console.log(`[SKIP] Job ${job.id} - ${absoluteUrl || currentUrl} - skipped due to depth (${depth} > ${MAX_DEPTH})`);
      continue;
    }

    // Resolve relative URLs to absolute
    const absoluteUrl = resolveUrl(currentUrl, seedUrl);
    if (!absoluteUrl) {
      console.log(`[SKIP] Job ${job.id} - Skipping invalid URL: ${currentUrl}`);
      continue;
    }

    // Normalize the URL
    const normalizedUrl = normalizeUrl(absoluteUrl);
    if (!normalizedUrl) {
      console.log(`[SKIP] Job ${job.id} - Skipping URL that failed normalization: ${absoluteUrl}`);
      continue;
    }

    // Skip if already crawled (shouldn't happen if deduplication works, but safety check)
    if (visitedNormalizedUrls.has(normalizedUrl)) {
      console.log(`[SKIP] Job ${job.id} - ${absoluteUrl} - skipped due to duplicate normalized URL (already crawled): ${normalizedUrl}`);
      totalDuplicatesSkipped++;
      continue;
    }

    // Check if URL should be skipped (protocol, extension, etc.)
    const skipCheck = shouldSkipUrl(absoluteUrl);
    if (skipCheck.skip) {
      console.log(`[SKIP] Job ${job.id} - ${absoluteUrl} - skipped due to ${skipCheck.reason}`);
      continue;
    }

    // Check if URL is on the same hostname (www. and non-www are treated as identical)
    try {
      let urlHostname = new URL(absoluteUrl).hostname.toLowerCase();
      // Normalize www. prefix for comparison
      if (urlHostname.startsWith('www.')) {
        urlHostname = urlHostname.substring(4);
      }
      // seedHostname is already normalized (www. removed)
      if (urlHostname !== seedHostname && !urlHostname.endsWith('.' + seedHostname)) {
        console.log(`[SKIP] Job ${job.id} - ${absoluteUrl} - skipped due to external domain (${urlHostname} vs ${seedHostname})`);
        continue; // Skip external URLs
      }
    } catch (error) {
      console.log(`[SKIP] Job ${job.id} - ${absoluteUrl} - skipped due to invalid URL format`);
      continue; // Skip invalid URLs
    }

    // Mark as visited (crawled) - this URL is now being processed
    visitedNormalizedUrls.add(normalizedUrl);
    pagesCrawled++;
    
    // Update max depth reached
    if (depth > maxDepthReached) {
      maxDepthReached = depth;
    }

    // Update last activity time
    lastActivityTime = Date.now();

    try {
      // Log progress with metrics
      console.log(`[PROGRESS] Job ${job.id} - Crawling page ${pagesCrawled}/${MAX_PAGES} (depth ${depth}): ${absoluteUrl}`);
      console.log(`[PROGRESS] Discovered: ${visitedNormalizedUrls.size} | Crawled: ${pagesCrawled} | Duplicates skipped: ${totalDuplicatesSkipped}`);
      
      // Update job progress in database (non-blocking)
      updateJobStatus(job.id, 'running', {
        pages_discovered: visitedNormalizedUrls.size,
        pages_crawled: pagesCrawled,
        duplicates_skipped: totalDuplicatesSkipped,
        last_activity_at: new Date().toISOString()
      }).catch(err => {
        console.error(`[WARNING] Job ${job.id} - Failed to update job progress:`, err.message);
      });

      // Fetch the page
      const fetchResult = await fetchPage(absoluteUrl);

      // Skip if not HTML
      if (!fetchResult.html) {
        console.log(`[SKIP] Job ${job.id} - Skipping non-HTML: ${absoluteUrl}`);
        continue;
      }

      // Extract page data
      const pageData = extractPageData({
        url: fetchResult.url,
        html: fetchResult.html
      });

      // Store original URL (final URL after redirects) for CSV export
      // Use the final URL from fetchResult, which preserves www. if present
      const originalUrl = fetchResult.url;
      originalUrlsMap.set(normalizedUrl, originalUrl);

      // Store the page in Supabase
      const { error: insertError } = await supabase
        .from('pages')
        .insert({
          crawl_job_id: job.id,
          url: pageData.url,
          normalized_url: normalizedUrl,
          status_code: fetchResult.status,
          title: pageData.title,
          meta_description: pageData.metaDescription,
          h1: pageData.h1,
          canonical: pageData.canonical,
          depth: depth
        });

      if (insertError) {
        console.error(`[ERROR] Job ${job.id} - Error storing page ${pageData.url}:`, insertError);
      } else {
        console.log(`[PROGRESS] Job ${job.id} - ✓ Stored page: ${normalizedUrl}`);
      }

      // Extract links from the page
      const links = extractLinks(fetchResult.html);
      totalUrlsDiscovered += links.length;
      console.log(`[PROGRESS] Job ${job.id} - Found ${links.length} links on ${absoluteUrl}`);

      // Process and enqueue new links
      let enqueuedCount = 0;
      let skippedCount = 0;
      let duplicatesSkipped = 0;
      for (const link of links) {
        // Skip if should be skipped
        const skipCheck = shouldSkipUrl(link);
        if (skipCheck.skip) {
          skippedCount++;
          continue;
        }

        // Resolve relative URL to absolute using current page URL as base
        const resolvedLink = resolveUrl(link, fetchResult.url);
        if (!resolvedLink) {
          skippedCount++;
          continue;
        }

        // Check if on same hostname first (before normalization to avoid unnecessary work)
        // www. and non-www are treated as identical
        let isExternal = false;
        try {
          let linkHostname = new URL(resolvedLink).hostname.toLowerCase();
          // Normalize www. prefix for comparison
          if (linkHostname.startsWith('www.')) {
            linkHostname = linkHostname.substring(4);
          }
          // seedHostname is already normalized (www. removed)
          if (linkHostname !== seedHostname && !linkHostname.endsWith('.' + seedHostname)) {
            isExternal = true;
          }
        } catch (error) {
          skippedCount++;
          continue; // Skip invalid URLs
        }

        if (isExternal) {
          skippedCount++;
          continue; // Skip external URLs
        }

        // Normalize the link BEFORE checking if already seen
        const normalizedLink = normalizeUrl(resolvedLink);
        if (!normalizedLink) {
          skippedCount++;
          continue;
        }

        // Check if already seen (enqueued) or visited (crawled) - deduplication check
        if (seenUrls.has(normalizedLink)) {
          console.log(`[SKIP] Job ${job.id} - ${resolvedLink} - skipped due to duplicate normalized URL (already enqueued or crawled): ${normalizedLink}`);
          skippedCount++;
          duplicatesSkipped++;
          continue;
        }

        // Check depth limit before enqueueing
        if (depth >= MAX_DEPTH) {
          skippedCount++;
          continue;
        }

        // Add to seenUrls immediately to prevent duplicate enqueueing
        seenUrls.add(normalizedLink);
        
        // Store the resolved absolute URL in the queue
        queue.push({ url: resolvedLink, depth: depth + 1 });
        enqueuedCount++;
        totalUrlsEnqueued++;
      }

      totalDuplicatesSkipped += duplicatesSkipped;
      console.log(`[PROGRESS] Job ${job.id} - Enqueued ${enqueuedCount} new links, skipped ${skippedCount} links (${duplicatesSkipped} duplicates) from ${absoluteUrl}`);

    } catch (error) {
      console.error(`[ERROR] Job ${job.id} - Error processing ${absoluteUrl}:`, error.message);
      // Continue with next URL
      continue;
    }
  }

  // Clear timeout and stall detection
  if (timeoutId) clearTimeout(timeoutId);
  if (stallCheckInterval) clearInterval(stallCheckInterval);

  // Determine stop reason
  const stopReason = pagesCrawled >= MAX_PAGES 
    ? `MAX_PAGES limit reached (${MAX_PAGES})` 
    : 'Queue exhausted';

  console.log(`[CRAWL COMPLETION] Job ${job.id} - Crawl finished. Reason: ${stopReason}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Final stats: ${pagesCrawled} pages crawled, ${visitedNormalizedUrls.size} unique URLs discovered, ${totalDuplicatesSkipped} duplicates skipped`);

  // Update crawl job status to "completed" with summary
  await updateJobStatus(job.id, 'completed', {
    pages_discovered: visitedNormalizedUrls.size,
    pages_crawled: pagesCrawled,
    duplicates_skipped: totalDuplicatesSkipped,
    stop_reason: stopReason
  });

  // Export crawl results to CSV
  const csvFilePath = path.join(__dirname, '../crawl-results.csv');
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Exporting crawl results to CSV`);
  await exportCrawlResultsToCsv(visitedNormalizedUrls, originalUrlsMap, csvFilePath, job.id);

  // Run comparison with ScreamingFrog URLs
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Running comparison with ScreamingFrog URLs`);
  try {
    await compareUrls();
  } catch (error) {
    console.error(`[WARNING] Job ${job.id} - Comparison failed: ${error.message}`);
    console.log(`[WARNING] Job ${job.id} - (This is non-fatal - crawl completed successfully)`);
  }

  // Output crawl summary
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Final Summary:`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Domain: ${domain}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Seed URL: ${seedUrl}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Total pages discovered: ${seenUrls.size}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Total unique normalized URLs stored: ${visitedNormalizedUrls.size}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Total skipped as duplicates: ${totalDuplicatesSkipped}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Total pages crawled: ${pagesCrawled}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Total URLs discovered (raw links): ${totalUrlsDiscovered}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Total URLs enqueued: ${totalUrlsEnqueued}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Max depth reached: ${maxDepthReached}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Stop reason: ${stopReason}`);
  console.log(`[CRAWL COMPLETED] Job ${job.id} - Successfully completed`);
  console.log(`[CRAWL COMPLETED] Job ${job.id} - Final stats: ${pagesCrawled} pages crawled, ${visitedNormalizedUrls.size} unique URLs discovered, ${totalDuplicatesSkipped} duplicates skipped`);
}

/**
 * Wrapper to run crawl with error handling
 * Updates job status to 'failed' if crawl throws an error
 * @param {Object} job - The job object to process
 */
async function runCrawlWithErrorHandling(job) {
  try {
    await runCrawl(job);
  } catch (error) {
    console.error(`[CRAWL FAILED] Job ${job.id} - Crawl failed with error: ${error.message}`);
    console.error(`[CRAWL FAILED] Job ${job.id} - Stack trace:`, error.stack);
    console.error(`[CRAWL FAILED] Job ${job.id} - Marking job as failed in database`);
    
    // Update job status to failed - ensure it always ends in completed or failed
    await updateJobStatus(job.id, 'failed', {
      error_message: error.message,
      stop_reason: 'error'
    });
    
    console.error(`[CRAWL FAILED] Job ${job.id} - Job status updated to failed`);
  }
}

/**
 * Gets crawl job status and summary from Supabase
 * @param {string} jobId - The job ID
 * @returns {Promise<Object|null>} - The job record with summary, or null if not found
 */
async function getJobStatus(jobId) {
  const { data: job, error } = await supabase
    .from('crawl_jobs')
    .select('*')
    .eq('id', jobId)
    .single();

  if (error) {
    if (error.code === 'PGRST116') {
      // Not found
      return null;
    }
    throw new Error(`Failed to get job status: ${error.message}`);
  }

  // Get page count for this job
  const { count: pageCount } = await supabase
    .from('pages')
    .select('*', { count: 'exact', head: true })
    .eq('crawl_job_id', jobId);

  // Build response with summary - include all progress fields
  const response = {
    crawl_job_id: job.id,
    domain: job.domain,
    project_id: job.project_id,
    status: job.status,
    started_at: job.started_at,
    completed_at: job.completed_at,
    failed_at: job.failed_at,
    last_activity_at: job.last_activity_at || null,
    pages_discovered: job.pages_discovered !== null && job.pages_discovered !== undefined ? job.pages_discovered : (pageCount || 0),
    pages_crawled: job.pages_crawled !== null && job.pages_crawled !== undefined ? job.pages_crawled : null,
    duplicates_skipped: job.duplicates_skipped !== null && job.duplicates_skipped !== undefined ? job.duplicates_skipped : null,
    stop_reason: job.stop_reason || null,
    error_message: job.error_message || null
  };

  return response;
}

module.exports = {
  startCrawl,
  runCrawl,
  runCrawlWithErrorHandling,
  updateJobStatus,
  getJobStatus
};

