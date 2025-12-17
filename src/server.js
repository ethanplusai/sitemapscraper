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
const supabase = require('./supabase');

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
 *   Array of page objects:
 *   [
 *     {
 *       normalized_url: string,
 *       original_url: string,
 *       status_code: number,
 *       title: string | null,
 *       h1: string | null,
 *       meta_title: string | null,
 *       meta_description: string | null
 *     },
 *     ...
 *   ]
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

    // Query Supabase pages table filtered by crawl_job_id
    const { data: pages, error } = await supabase
      .from('pages')
      .select('normalized_url, url, status_code, title, h1, meta_description')
      .eq('crawl_job_id', jobId)
      .order('normalized_url', { ascending: true });

    if (error) {
      console.error(`[API] GET /crawl/:jobId/pages - Supabase error for job ${jobId}:`, error);
      return res.status(500).json({ 
        error: 'Failed to fetch pages',
        message: error.message 
      });
    }

    if (!pages) {
      console.log(`[API] GET /crawl/:jobId/pages - No pages found for job ${jobId}`);
      return res.json([]);
    }

    // Transform the data to match the required response format
    // Note: meta_title doesn't exist in the database schema, so we return null
    const formattedPages = pages.map(page => ({
      normalized_url: page.normalized_url,
      original_url: page.url || page.normalized_url, // url is the original URL
      status_code: page.status_code,
      title: page.title,
      h1: page.h1,
      meta_title: null, // Not stored in database, returning null
      meta_description: page.meta_description
    }));

    console.log(`[API] GET /crawl/:jobId/pages - Returning ${formattedPages.length} pages for job ${jobId}`);

    res.json(formattedPages);

  } catch (error) {
    console.error(`[API] GET /crawl/:jobId/pages - Unexpected error:`, error);
    res.status(500).json({ 
      error: 'Failed to get pages',
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

