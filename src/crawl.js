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
const { normalizeUrl, extractPrimaryDomain, isPrimaryDomain, normalizeAndClassifyUrl } = require('./normalize');
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

  // Extract primary domain (normalized, without www.)
  const primaryDomain = extractPrimaryDomain(seedUrl);
  if (!primaryDomain) {
    console.error(`[CRAWL FAILED] Job ${job.id} - Invalid seed URL: ${seedUrl}`);
    await updateJobStatus(job.id, 'failed', {
      error_message: `Invalid seed URL: ${seedUrl}`,
      stop_reason: 'invalid_seed_url'
    });
    return;
  }

  // Initialize BFS queue and data structures first
  // Queue must only contain normalized URLs
  const seedNormalizedForQueue = normalizeUrl(seedUrl);
  if (!seedNormalizedForQueue || !isPrimaryDomain(seedNormalizedForQueue, primaryDomain)) {
    console.error(`[CRAWL FAILED] Job ${job.id} - Seed URL does not belong to primary domain: ${seedUrl}`);
    await updateJobStatus(job.id, 'failed', {
      error_message: `Seed URL does not belong to primary domain: ${seedUrl}`,
      stop_reason: 'invalid_seed_url'
    });
    return;
  }
  
  const queue = [{ normalized_url: seedNormalizedForQueue, depth: 0, original_url: seedUrl }];
  const seenUrls = new Set(); // URLs that have been enqueued (normalized) - use normalized_url consistently
  const pagesMap = new Map(); // Map<normalized_url, Page> - primary deduplication store
  const originalUrlsMap = new Map(); // Maps normalized URL -> original URL (for CSV export)
  
  // External link registry: Map<normalized_external_url, { occurrences: number, pages_found_on: Set<normalized_internal_url> }>
  const externalLinksRegistry = new Map();
  
  // Link metrics tracking: Map<normalized_url, { internal_links_out: number, external_links_out: number }>
  const linkMetricsMap = new Map();
  
  // Incoming links tracking: Map<normalized_target_url, { internal_count: number, external_count: number }>
  const incomingLinksMap = new Map();
  
  // Crawl statistics - declare before timeout/stall detection
  let pagesCrawled = 0;
  let totalUrlsDiscovered = 0;
  let totalUrlsEnqueued = 0;
  let totalDuplicatesSkipped = 0;
  let maxDepthReached = 0;
  
  // Initialize link metrics for seed URL
  linkMetricsMap.set(normalizeUrl(seedUrl) || seedUrl, {
    internal_links_out: 0,
    external_links_out: 0
  });

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
      pages_discovered: pagesMap.size,
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
        pages_discovered: pagesMap.size,
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
    .select('normalized_url, url, internal_links_out, external_links_out, internal_links_in, external_links_in')
    .eq('crawl_job_id', job.id);

  if (existingPages) {
    existingPages.forEach(page => {
      if (page.normalized_url) {
        // Only load pages that belong to primary domain
        if (isPrimaryDomain(page.normalized_url, primaryDomain)) {
          // Add to seenUrls since they've been enqueued
          seenUrls.add(page.normalized_url);
          // Store in pagesMap
          pagesMap.set(page.normalized_url, {
            normalized_url: page.normalized_url,
            original_url: page.url || page.normalized_url,
            status_code: page.status_code,
            title: page.title,
            h1: page.h1,
            meta_description: page.meta_description,
            internal_links_out: page.internal_links_out || 0,
            external_links_out: page.external_links_out || 0,
            internal_links_in: page.internal_links_in || 0,
            external_links_in: page.external_links_in || 0
          });
          // Store original URL
          const originalUrl = page.url || page.normalized_url;
          originalUrlsMap.set(page.normalized_url, originalUrl);
          // Initialize link metrics
          linkMetricsMap.set(page.normalized_url, {
            internal_links_out: page.internal_links_out || 0,
            external_links_out: page.external_links_out || 0
          });
        }
      }
    });
    console.log(`[CRAWL START] Job ${job.id} - Found ${pagesMap.size} existing pages for this job (filtered to primary domain)`);
  }

  // Add seed URL to seenUrls (it's been enqueued) - use normalized_url consistently
  seenUrls.add(seedNormalizedForQueue);
  // Store original seed URL (preserves www. if present)
  originalUrlsMap.set(seedNormalizedForQueue, seedUrl);
  
  console.log(`[CRAWL START] Job ${job.id} - Primary domain: ${primaryDomain}`);
  console.log(`[CRAWL START] Job ${job.id} - Seed normalized: ${seedNormalizedForQueue}`);
  
  // Fail-safe: Track iterations with no new pages added
  let iterationsWithoutNewPages = 0;
  const MAX_ITERATIONS_WITHOUT_NEW_PAGES = 5;

  // BFS crawl loop - queue contains only normalized URLs
  while (queue.length > 0 && pagesCrawled < MAX_PAGES) {
    // TEMPORARY LOGGING: Queue size, visited count, pages count
    console.log(`[QUEUE STATUS] Job ${job.id} - Queue: ${queue.length} | Visited: ${seenUrls.size} | Pages: ${pagesMap.size} | Crawled: ${pagesCrawled}`);
    
    const { normalized_url: normalizedUrl, depth, original_url: queueOriginalUrl } = queue.shift();

    // Skip if depth exceeds limit
    if (depth > MAX_DEPTH) {
      console.log(`[SKIP] Job ${job.id} - ${normalizedUrl} - skipped due to depth (${depth} > ${MAX_DEPTH})`);
      continue;
    }

    // Queue already contains normalized URLs, so we can use it directly
    // But we need the original URL for fetching - use stored original or reconstruct
    let absoluteUrl = queueOriginalUrl;
    if (!absoluteUrl) {
      // Reconstruct from normalized URL (may lose query params, but that's ok for fetching)
      // Try to get from originalUrlsMap first
      absoluteUrl = originalUrlsMap.get(normalizedUrl) || normalizedUrl;
    }

    // PRIMARY DOMAIN ALLOWLIST: Only allow pages from primary domain (exact match, no subdomains)
    // This should already be enforced when enqueueing, but double-check for safety
    if (!isPrimaryDomain(normalizedUrl, primaryDomain)) {
      console.log(`[SKIP] Job ${job.id} - ${normalizedUrl} - skipped due to external domain (not primary domain: ${primaryDomain})`);
      continue; // Skip external URLs
    }

    // Skip if already crawled (Map-based deduplication)
    if (pagesMap.has(normalizedUrl)) {
      console.log(`[SKIP] Job ${job.id} - ${normalizedUrl} - skipped due to duplicate normalized URL (already crawled)`);
      totalDuplicatesSkipped++;
      continue;
    }

    // Check if URL should be skipped (protocol, extension, etc.) - check original URL
    const skipCheck = shouldSkipUrl(absoluteUrl);
    if (skipCheck.skip) {
      console.log(`[SKIP] Job ${job.id} - ${normalizedUrl} - skipped due to ${skipCheck.reason}`);
      continue;
    }

    // Mark as being processed
    pagesCrawled++;
    
    // Update max depth reached
    if (depth > maxDepthReached) {
      maxDepthReached = depth;
    }

    // Update last activity time
    lastActivityTime = Date.now();

    try {
      // Log progress with metrics - TEMPORARY LOGGING
      console.log(`[PROGRESS] Job ${job.id} - Crawling page ${pagesCrawled}/${MAX_PAGES} (depth ${depth}): ${normalizedUrl}`);
      console.log(`[PROGRESS] Discovered: ${pagesMap.size} | Crawled: ${pagesCrawled} | Duplicates skipped: ${totalDuplicatesSkipped} | Queue: ${queue.length}`);
      
      // Update job progress in database (non-blocking)
      updateJobStatus(job.id, 'running', {
        pages_discovered: pagesMap.size,
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

      // Initialize link metrics for this page
      let internalLinksOut = 0;
      let externalLinksOut = 0;

      // Extract links from the page
      const links = extractLinks(fetchResult.html);
      totalUrlsDiscovered += links.length;
      console.log(`[PROGRESS] Job ${job.id} - Found ${links.length} links on ${absoluteUrl}`);

      // Process links to classify and track metrics
      for (const link of links) {
        const skipCheck = shouldSkipUrl(link);
        if (skipCheck.skip) {
          continue;
        }

        // Resolve relative URL to absolute
        const resolvedLink = resolveUrl(link, fetchResult.url);
        if (!resolvedLink) {
          continue;
        }

        // Normalize and classify the link
        const { normalized: normalizedLink, isExternal } = normalizeAndClassifyUrl(link, fetchResult.url, primaryDomain);
        
        if (isExternal) {
          // External link - track in registry
          if (normalizedLink) {
            if (!externalLinksRegistry.has(normalizedLink)) {
              externalLinksRegistry.set(normalizedLink, {
                occurrences: 0,
                pages_found_on: new Set()
              });
            }
            const registryEntry = externalLinksRegistry.get(normalizedLink);
            registryEntry.occurrences++;
            registryEntry.pages_found_on.add(normalizedUrl);
          }
          externalLinksOut++;
          
          // Track external link pointing TO this page (if someone links to us externally)
          // This is rare but we track it
        } else {
          // Internal link
          internalLinksOut++;
          
          // Track incoming internal link for the target page
          if (normalizedLink && normalizedLink !== normalizedUrl) {
            if (!incomingLinksMap.has(normalizedLink)) {
              incomingLinksMap.set(normalizedLink, { internal_count: 0, external_count: 0 });
            }
            incomingLinksMap.get(normalizedLink).internal_count++;
          }
        }
      }

      // Store link metrics
      linkMetricsMap.set(normalizedUrl, {
        internal_links_out: internalLinksOut,
        external_links_out: externalLinksOut
      });

      // Get incoming link counts
      const incomingLinks = incomingLinksMap.get(normalizedUrl) || { internal_count: 0, external_count: 0 };
      
      // Create page object
      const pageObject = {
        normalized_url: normalizedUrl,
        original_url: originalUrl,
        status_code: fetchResult.status,
        title: pageData.title,
        h1: pageData.h1,
        meta_description: pageData.metaDescription,
        internal_links_out: internalLinksOut,
        external_links_out: externalLinksOut,
        internal_links_in: incomingLinks.internal_count,
        external_links_in: incomingLinks.external_count
      };

      // Store in pagesMap (deduplication)
      pagesMap.set(normalizedUrl, pageObject);

      // Store the page in Supabase
      const { error: insertError } = await supabase
        .from('pages')
        .insert({
          crawl_job_id: job.id,
          url: originalUrl,
          normalized_url: normalizedUrl,
          status_code: fetchResult.status,
          title: pageData.title,
          meta_description: pageData.metaDescription,
          h1: pageData.h1,
          canonical: pageData.canonical,
          depth: depth,
          internal_links_out: internalLinksOut,
          external_links_out: externalLinksOut,
          internal_links_in: incomingLinks.internal_count,
          external_links_in: incomingLinks.external_count
        });

      if (insertError) {
        console.error(`[ERROR] Job ${job.id} - Error storing page ${pageData.url}:`, insertError);
      } else {
        console.log(`[PROGRESS] Job ${job.id} - ✓ Stored page: ${normalizedUrl} (internal: ${internalLinksOut}, external: ${externalLinksOut})`);
      }

      // Process and enqueue new internal links (external links already tracked above)
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

        // Normalize and classify the link
        const { normalized: normalizedLink, isExternal } = normalizeAndClassifyUrl(link, fetchResult.url, primaryDomain);
        
        // DO NOT enqueue external domains
        if (isExternal || !normalizedLink) {
          // External links already tracked above, skip enqueueing
          skippedCount++;
          continue;
        }

        // PRIMARY DOMAIN ALLOWLIST: Only enqueue internal links from primary domain
        // This check is critical - do NOT enqueue external domains
        if (!isPrimaryDomain(normalizedLink, primaryDomain)) {
          skippedCount++;
          continue;
        }

        // DO NOT enqueue URLs already visited or already queued - use normalized_url consistently
        if (seenUrls.has(normalizedLink) || pagesMap.has(normalizedLink)) {
          console.log(`[SKIP] Job ${job.id} - ${normalizedLink} - skipped due to duplicate normalized URL (already enqueued or crawled)`);
          skippedCount++;
          duplicatesSkipped++;
          continue;
        }

        // Check depth limit before enqueueing
        if (depth >= MAX_DEPTH) {
          skippedCount++;
          continue;
        }

        // Add to seenUrls immediately to prevent duplicate enqueueing - use normalized_url consistently
        seenUrls.add(normalizedLink);
        
        // Queue must only contain normalized URLs - store normalized_url, not resolvedLink
        queue.push({ 
          normalized_url: normalizedLink, 
          depth: depth + 1,
          original_url: resolvedLink // Store original for fetching
        });
        enqueuedCount++;
        totalUrlsEnqueued++;
      }

      // Fail-safe: Track if new pages were added
      if (enqueuedCount === 0) {
        iterationsWithoutNewPages++;
        if (iterationsWithoutNewPages >= MAX_ITERATIONS_WITHOUT_NEW_PAGES && queue.length === 0) {
          console.log(`[CRAWL COMPLETION] Job ${job.id} - No new pages added after ${iterationsWithoutNewPages} iterations, queue empty - terminating crawl`);
          break; // Exit loop - queue will be empty
        }
      } else {
        iterationsWithoutNewPages = 0; // Reset counter when new pages are added
      }

      totalDuplicatesSkipped += duplicatesSkipped;
      console.log(`[PROGRESS] Job ${job.id} - Enqueued ${enqueuedCount} new links, skipped ${skippedCount} links (${duplicatesSkipped} duplicates) from ${normalizedUrl}`);

    } catch (error) {
      console.error(`[ERROR] Job ${job.id} - Error processing ${normalizedUrl}:`, error.message);
      // Continue with next URL
      continue;
    }
  }

  // Clear timeout and stall detection
  if (timeoutId) clearTimeout(timeoutId);
  if (stallCheckInterval) clearInterval(stallCheckInterval);
  
  // TEMPORARY LOGGING: Final queue status
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Final queue status: ${queue.length} remaining | Visited: ${seenUrls.size} | Pages: ${pagesMap.size}`);
  
  // Ensure loop terminated correctly
  if (queue.length > 0 && pagesCrawled < MAX_PAGES) {
    console.log(`[CRAWL COMPLETION] Job ${job.id} - Loop terminated but queue not empty - this should not happen`);
  }

  // Update pages in database with final link metrics (incoming links are already calculated)
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Updating link metrics in database...`);
  for (const [normalizedUrl, page] of pagesMap.entries()) {
    // Get final incoming link counts (may have been updated by later pages)
    const incomingLinks = incomingLinksMap.get(normalizedUrl) || { internal_count: 0, external_count: 0 };
    page.internal_links_in = incomingLinks.internal_count;
    page.external_links_in = incomingLinks.external_count;
    
    await supabase
      .from('pages')
      .update({
        internal_links_out: page.internal_links_out,
        external_links_out: page.external_links_out,
        internal_links_in: page.internal_links_in,
        external_links_in: page.external_links_in
      })
      .eq('crawl_job_id', job.id)
      .eq('normalized_url', normalizedUrl);
  }

  // Store external links registry in database (we'll create a table for this or store as JSON)
  // For now, we'll return it in the API response. We can store it in a separate table later.
  // Store as JSON in crawl_jobs table or create external_links table
  const externalLinksData = {};
  for (const [normalizedExternalUrl, registry] of externalLinksRegistry.entries()) {
    externalLinksData[normalizedExternalUrl] = {
      occurrences: registry.occurrences,
      pages_found_on: Array.from(registry.pages_found_on)
    };
  }
  
  // Update job with external links registry (store as JSON)
  await supabase
    .from('crawl_jobs')
    .update({
      external_links: externalLinksData
    })
    .eq('id', job.id);

  // Determine stop reason
  const stopReason = pagesCrawled >= MAX_PAGES 
    ? `MAX_PAGES limit reached (${MAX_PAGES})` 
    : queue.length === 0
    ? 'Queue exhausted'
    : `No new pages added after ${iterationsWithoutNewPages} iterations`;

  console.log(`[CRAWL COMPLETION] Job ${job.id} - Crawl finished. Reason: ${stopReason}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Final stats: ${pagesCrawled} pages crawled, ${pagesMap.size} unique URLs discovered, ${totalDuplicatesSkipped} duplicates skipped`);

  // Update crawl job status to "completed" with summary
  await updateJobStatus(job.id, 'completed', {
    pages_discovered: pagesMap.size,
    pages_crawled: pagesCrawled,
    duplicates_skipped: totalDuplicatesSkipped,
    stop_reason: stopReason
  });

  // Export crawl results to CSV
  const csvFilePath = path.join(__dirname, '../crawl-results.csv');
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Exporting crawl results to CSV`);
  await exportCrawlResultsToCsv(new Set(pagesMap.keys()), originalUrlsMap, csvFilePath, job.id);

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
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Primary Domain: ${primaryDomain}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Seed URL: ${seedUrl}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Total pages discovered: ${pagesMap.size}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Total unique normalized URLs stored: ${pagesMap.size}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Total skipped as duplicates: ${totalDuplicatesSkipped}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Total pages crawled: ${pagesCrawled}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Total URLs discovered (raw links): ${totalUrlsDiscovered}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Total URLs enqueued: ${totalUrlsEnqueued}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - External links found: ${externalLinksRegistry.size}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Max depth reached: ${maxDepthReached}`);
  console.log(`[CRAWL COMPLETION] Job ${job.id} - Stop reason: ${stopReason}`);
  console.log(`[CRAWL COMPLETED] Job ${job.id} - Successfully completed`);
  console.log(`[CRAWL COMPLETED] Job ${job.id} - Final stats: ${pagesCrawled} pages crawled, ${pagesMap.size} unique URLs discovered, ${totalDuplicatesSkipped} duplicates skipped`);
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

