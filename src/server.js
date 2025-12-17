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

const app = express();
const PORT = process.env.PORT || 3000;

// CORS middleware - allow all origins with GET and POST methods
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: false
}));

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

// Start server
const server = app.listen(PORT, () => {
  console.log(`[SERVER] SiteMapScraper API server running on port ${PORT}`);
  console.log(`[SERVER] Health check: GET /health`);
  console.log(`[SERVER] POST /crawl - Start a new crawl`);
  console.log(`[SERVER] GET /crawl/:jobId - Get crawl job status`);
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

