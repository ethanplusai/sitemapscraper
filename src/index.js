/**
 * Express API entry point
 * 
 * This file serves as the main entry point for the Node.js website crawler service.
 * It will set up the Express server, define API routes, and handle HTTP requests
 * for initiating crawls, checking job status, and retrieving crawl results.
 */

// Load environment variables from .env file
require('dotenv').config();

const express = require('express');
const { startCrawl, runCrawl } = require('./crawl');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// POST /start endpoint
app.post('/start', async (req, res) => {
  try {
    const { domain } = req.body;

    if (!domain) {
      return res.status(400).json({ error: 'domain is required' });
    }

    // Start crawl asynchronously (non-blocking)
    const job = await startCrawl(domain);

    // Return immediately with job_id and status
    res.json({
      job_id: job.id,
      status: 'started'
    });
  } catch (error) {
    console.error('Error starting crawl:', error);
    res.status(500).json({ error: 'Failed to start crawl' });
  }
});

// Main crawling logic
async function performCrawl() {
  // Hardcoded domain for now
  const domain = 'https://www.drwisehair.com/';
  const projectId = 'default-project'; // Hardcoded for now

  try {
    console.log(`Starting crawl for domain: ${domain}`);

    // Start a crawl job
    const job = await startCrawl({ domain, projectId });
    console.log(`Created crawl job: ${job.id}`);

    // Run the crawl
    await runCrawl(job);

  } catch (error) {
    console.error('Crawl error:', error);
  }
}

// Start server
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

// Run crawl if this file is executed directly
if (require.main === module) {
  performCrawl();
}

module.exports = app;

