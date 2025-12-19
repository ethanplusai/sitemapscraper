/**
 * HTTP API Server for SiteMapScraper
 * 
 * This is the main entry point for the crawler API service.
 * It exposes HTTP endpoints for triggering crawls and checking job status.
 * 
 * Usage: node src/server.js
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { startCrawl, runCrawlWithErrorHandling, getJobStatus } = require('./crawl');
const { startContentExtraction, runContentExtractionWithErrorHandling, getExtractionJobStatus } = require('./content-extraction');
const supabase = require('./supabase');
const { extractPrimaryDomain, isPrimaryDomain } = require('./normalize');

const app = express();
const PORT = process.env.PORT || 3000;

// CORS middleware - MUST be first to handle OPTIONS preflight requests
// Allows all origins for Base44 frontend integration
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: false
}));

// Debug header middleware - temporary version identifier
app.use((req, res, next) => {
  res.setHeader('X-Sitemap-Scraper-Version', 'cors-enabled');
  next();
});

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'sitemap-scraper' });
});

/**
 * POST /crawl
 * Starts a new crawl job
 * 
 * Request body:
 *   { domain: string, projectId: string }
 * 
 * Response:
 *   { crawl_job_id: string, status: "running" }
 */
app.post('/crawl', async (req, res) => {
  try {
    const { domain, projectId } = req.body;

    // Validate required fields
    if (!domain) {
      return res.status(400).json({ 
        error: 'domain is required',
        message: 'Please provide a domain to crawl'
      });
    }

    if (!projectId) {
      return res.status(400).json({ 
        error: 'projectId is required',
        message: 'Please provide a project ID'
      });
    }

    // Create crawl job
    const job = await startCrawl({ domain, projectId });

    // Start crawl asynchronously (non-blocking)
    // Don't await - let it run in the background
    runCrawlWithErrorHandling(job).catch(err => {
      // Error is already logged and job status updated in runCrawlWithErrorHandling
      console.error(`[API] Background crawl error for job ${job.id}:`, err.message);
    });

    // Return immediately with job ID and status
    res.status(202).json({
      crawl_job_id: job.id,
      status: 'running',
      message: 'Crawl job started successfully'
    });

  } catch (error) {
    console.error('[API] Error starting crawl:', error);
    res.status(500).json({ 
      error: 'Failed to start crawl',
      message: error.message 
    });
  }
});

/**
 * GET /crawl/:jobId
 * Gets the status and summary of a crawl job
 * 
 * Response:
 *   {
 *     crawl_job_id: string,
 *     domain: string,
 *     project_id: string,
 *     status: "running" | "completed" | "failed",
 *     started_at: ISO timestamp,
 *     completed_at: ISO timestamp | null,
 *     failed_at: ISO timestamp | null,
 *     pages_discovered: number,
 *     pages_crawled: number | null,
 *     duplicates_skipped: number | null,
 *     stop_reason: string | null,
 *     error_message: string | null
 *   }
 */
app.get('/crawl/:jobId', async (req, res) => {
  try {
    const { jobId } = req.params;

    if (!jobId) {
      return res.status(400).json({ 
        error: 'jobId is required',
        message: 'Please provide a crawl job ID'
      });
    }

    const jobStatus = await getJobStatus(jobId);

    if (!jobStatus) {
      return res.status(404).json({ 
        error: 'Job not found',
        message: `Crawl job ${jobId} does not exist`
      });
    }

    res.json(jobStatus);

  } catch (error) {
    console.error('[API] Error getting job status:', error);
    res.status(500).json({ 
      error: 'Failed to get job status',
      message: error.message 
    });
  }
});

/**
 * GET /crawl/:jobId/pages
 * Gets all crawled pages for a specific crawl job
 * 
 * Response:
 *   {
 *     pages: [
 *       {
 *         normalized_url: string,
 *         original_url: string,
 *         status_code: number,
 *         title: string | null,
 *         h1: string | null,
 *         meta_description: string | null,
 *         internal_links_out: number,
 *         external_links_out: number,
 *         internal_links_in: number,
 *         external_links_in: number
 *       },
 *       ...
 *     ],
 *     external_links: {
 *       "https://external.com/some-page": {
 *         "occurrences": 6,
 *         "pages_found_on": [
 *           "https://example.com/",
 *           "https://example.com/about"
 *         ]
 *       }
 *     }
 *   }
 */
app.get('/crawl/:jobId/pages', async (req, res) => {
  try {
    const { jobId } = req.params;

    if (!jobId) {
      console.error('[API] GET /crawl/:jobId/pages - Missing jobId parameter');
      return res.status(400).json({ 
        error: 'jobId is required',
        message: 'Please provide a crawl job ID'
      });
    }

    console.log(`[API] GET /crawl/:jobId/pages - Fetching pages for job ${jobId}`);

    // Get job to extract primary domain and external_links
    const { data: job, error: jobError } = await supabase
      .from('crawl_jobs')
      .select('domain, external_links')
      .eq('id', jobId)
      .single();

    if (jobError || !job) {
      console.error(`[API] GET /crawl/:jobId/pages - Job not found: ${jobId}`);
      return res.status(404).json({ 
        error: 'Job not found',
        message: `Crawl job ${jobId} does not exist`
      });
    }

    // Extract primary domain
    const primaryDomain = extractPrimaryDomain(job.domain);
    if (!primaryDomain) {
      return res.status(500).json({ 
        error: 'Invalid job domain',
        message: 'Unable to extract primary domain from job'
      });
    }

    // Query Supabase pages table filtered by crawl_job_id
    const { data: pages, error } = await supabase
      .from('pages')
      .select('normalized_url, url, status_code, title, h1, meta_description, internal_links_out, external_links_out, internal_links_in, external_links_in')
      .eq('crawl_job_id', jobId)
      .order('normalized_url', { ascending: true });

    if (error) {
      console.error(`[API] GET /crawl/:jobId/pages - Supabase error for job ${jobId}:`, error);
      return res.status(500).json({ 
        error: 'Failed to fetch pages',
        message: error.message 
      });
    }

    if (!pages || pages.length === 0) {
      console.log(`[API] GET /crawl/:jobId/pages - No pages found for job ${jobId}`);
      return res.json({
        pages: [],
        external_links: job.external_links || {}
      });
    }

    // UI GUARDRAILS: Filter and deduplicate (CRITICAL - must be bulletproof)
    // 1. Filter by primary domain (exact match only) - STRICT filtering
    const primaryDomainPages = pages.filter(page => {
      if (!page.normalized_url) {
        console.warn(`[API] GET /crawl/:jobId/pages - Page missing normalized_url, skipping`);
        return false;
      }
      const isPrimary = isPrimaryDomain(page.normalized_url, primaryDomain);
      if (!isPrimary) {
        console.warn(`[API] GET /crawl/:jobId/pages - Filtered out external domain: ${page.normalized_url} (primary: ${primaryDomain})`);
      }
      return isPrimary;
    });

    // 2. Deduplicate by normalized_url using Map - STRICT deduplication
    const pagesMap = new Map();
    const seenUrls = new Set(); // Track for duplicate detection
    for (const page of primaryDomainPages) {
      const normalizedUrl = page.normalized_url;
      
      // Normalize the normalized_url to ensure consistency (lowercase, trim)
      const normalizedKey = normalizedUrl.toLowerCase().trim();
      
      // Check for duplicates
      if (pagesMap.has(normalizedKey) || seenUrls.has(normalizedKey)) {
        console.warn(`[API] GET /crawl/:jobId/pages - Duplicate detected and removed: ${normalizedUrl}`);
        continue; // Skip duplicates
      }
      
      // Store in both Map and Set for tracking
      pagesMap.set(normalizedKey, page);
      seenUrls.add(normalizedKey);
    }
    
    console.log(`[API] GET /crawl/:jobId/pages - Filtered: ${pages.length} -> ${primaryDomainPages.length} (domain) -> ${pagesMap.size} (deduped)`);

    // Transform the data to match the required response format
    const formattedPages = Array.from(pagesMap.values()).map(page => ({
      normalized_url: page.normalized_url,
      original_url: page.url || page.normalized_url, // url is the original URL
      status_code: page.status_code,
      title: page.title,
      h1: page.h1,
      meta_description: page.meta_description,
      internal_links_out: page.internal_links_out || 0,
      external_links_out: page.external_links_out || 0,
      internal_links_in: page.internal_links_in || 0,
      external_links_in: page.external_links_in || 0
    }));

    // Get external links registry from job (stored as JSON)
    const externalLinks = job.external_links || {};

    console.log(`[API] GET /crawl/:jobId/pages - Returning ${formattedPages.length} pages (filtered from ${pages.length} total) for job ${jobId}`);

    res.json({
      pages: formattedPages,
      external_links: externalLinks
    });

  } catch (error) {
    console.error(`[API] GET /crawl/:jobId/pages - Unexpected error:`, error);
    res.status(500).json({ 
      error: 'Failed to get pages',
      message: error.message 
    });
  }
});

/**
 * POST /api/sitemaps/:sitemapId/extract-content
 * Triggers batch content extraction for a sitemap (crawl job)
 * 
 * Request params:
 *   sitemapId: string - The crawl_job_id (sitemap identifier)
 * 
 * Request body (optional):
 *   { onlyMissing: boolean } - Only extract content for pages missing extracted content (default: true)
 * 
 * Response:
 *   {
 *     extraction_job_id: string,
 *     status: "running",
 *     message: "Content extraction job started successfully"
 *   }
 */
app.post('/api/sitemaps/:sitemapId/extract-content', async (req, res) => {
  try {
    const { sitemapId } = req.params;
    const { onlyMissing = true } = req.body;

    if (!sitemapId) {
      return res.status(400).json({ 
        error: 'sitemapId is required',
        message: 'Please provide a sitemap ID (crawl_job_id)'
      });
    }

    console.log(`[API] POST /api/sitemaps/:sitemapId/extract-content - Starting extraction for sitemap ${sitemapId}`);

    // Start content extraction job
    const extractionJob = await startContentExtraction(sitemapId, { onlyMissing });

    // Start extraction asynchronously (non-blocking)
    // Don't await - let it run in the background
    runContentExtractionWithErrorHandling(extractionJob).catch(err => {
      // Error is already logged and job status updated in runContentExtractionWithErrorHandling
      console.error(`[API] Background content extraction error for job ${extractionJob.id}:`, err.message);
    });

    // Return immediately with job ID and status
    res.status(202).json({
      extraction_job_id: extractionJob.id,
      status: 'running',
      message: 'Content extraction job started successfully',
      only_missing: onlyMissing
    });

  } catch (error) {
    console.error('[API] Error starting content extraction:', error);
    res.status(500).json({ 
      error: 'Failed to start content extraction',
      message: error.message 
    });
  }
});

/**
 * GET /api/sitemaps/:sitemapId/extract-content/:jobId
 * Gets the status of a content extraction job
 * 
 * Response:
 *   {
 *     extraction_job_id: string,
 *     crawl_job_id: string,
 *     status: "running" | "completed" | "failed",
 *     started_at: ISO timestamp,
 *     completed_at: ISO timestamp | null,
 *     failed_at: ISO timestamp | null,
 *     pages_total: number | null,
 *     pages_extracted: number | null,
 *     pages_failed: number | null,
 *     error_message: string | null
 *   }
 */
app.get('/api/sitemaps/:sitemapId/extract-content/:jobId', async (req, res) => {
  try {
    const { sitemapId, jobId } = req.params;

    if (!jobId) {
      return res.status(400).json({ 
        error: 'jobId is required',
        message: 'Please provide an extraction job ID'
      });
    }

    const jobStatus = await getExtractionJobStatus(jobId);

    if (!jobStatus) {
      return res.status(404).json({ 
        error: 'Extraction job not found',
        message: `Content extraction job ${jobId} does not exist`
      });
    }

    // Verify the job belongs to the sitemap
    if (jobStatus.crawl_job_id !== sitemapId) {
      return res.status(400).json({ 
        error: 'Job mismatch',
        message: `Extraction job ${jobId} does not belong to sitemap ${sitemapId}`
      });
    }

    res.json({
      extraction_job_id: jobStatus.id,
      crawl_job_id: jobStatus.crawl_job_id,
      status: jobStatus.status,
      started_at: jobStatus.started_at,
      completed_at: jobStatus.completed_at,
      failed_at: jobStatus.failed_at,
      last_activity_at: jobStatus.last_activity_at,
      pages_total: jobStatus.pages_total,
      pages_extracted: jobStatus.pages_extracted,
      pages_failed: jobStatus.pages_failed,
      error_message: jobStatus.error_message
    });

  } catch (error) {
    console.error('[API] Error getting extraction job status:', error);
    res.status(500).json({ 
      error: 'Failed to get extraction job status',
      message: error.message 
    });
  }
});

// Start server
const server = app.listen(PORT, () => {
  console.log(`[SERVER] SiteMapScraper API server running on port ${PORT}`);
  console.log(`[SERVER] Health check: GET /health`);
  console.log(`[SERVER] POST /crawl - Start a new crawl`);
  console.log(`[SERVER] GET /crawl/:jobId - Get crawl job status`);
  console.log(`[SERVER] GET /crawl/:jobId/pages - Get crawled pages for a job`);
  console.log(`[SERVER] POST /api/sitemaps/:sitemapId/extract-content - Start content extraction`);
  console.log(`[SERVER] GET /api/sitemaps/:sitemapId/extract-content/:jobId - Get extraction job status`);
});

// Graceful shutdown handling
const gracefulShutdown = (signal) => {
  console.log(`\n[SERVER] Received ${signal}, shutting down gracefully...`);
  
  server.close(() => {
    console.log('[SERVER] HTTP server closed');
    console.log('[SERVER] Shutdown complete');
    process.exit(0);
  });

  // Force shutdown after 10 seconds
  setTimeout(() => {
    console.error('[SERVER] Forced shutdown after timeout');
    process.exit(1);
  }, 10000);
};

// Handle shutdown signals
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// Handle uncaught errors
process.on('unhandledRejection', (reason, promise) => {
  console.error('[SERVER] Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('uncaughtException', (error) => {
  console.error('[SERVER] Uncaught Exception:', error);
  process.exit(1);
});

module.exports = app;

