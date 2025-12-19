/**
 * Batch content extraction orchestration
 * 
 * This module handles batch content extraction jobs.
 * It runs separately from the initial crawl and processes pages
 * to extract structured content (clean text, headings, etc.)
 */

require('dotenv').config();
const supabase = require('./supabase');
const { extractPageContent } = require('./extract-content');
const { normalizeUrl } = require('./normalize');

/**
 * Starts a content extraction job for a sitemap (crawl_job)
 * @param {string} sitemapId - The crawl_job_id (sitemap identifier)
 * @param {Object} options - Extraction options
 * @param {boolean} options.onlyMissing - Only extract content for pages missing extracted content
 * @returns {Promise<Object>} - The created extraction job record
 */
async function startContentExtraction(sitemapId, options = {}) {
  const { onlyMissing = true } = options;
  
  // Verify the crawl job exists
  const { data: crawlJob, error: jobError } = await supabase
    .from('crawl_jobs')
    .select('id, domain, status')
    .eq('id', sitemapId)
    .single();
  
  if (jobError || !crawlJob) {
    throw new Error(`Crawl job ${sitemapId} not found`);
  }
  
  // Create content extraction job record
  const { data: extractionJob, error } = await supabase
    .from('content_extraction_jobs')
    .insert({
      crawl_job_id: sitemapId,
      status: 'running',
      started_at: new Date().toISOString(),
      only_missing: onlyMissing
    })
    .select()
    .single();
  
  if (error) {
    throw new Error(`Failed to create content extraction job: ${error.message}`);
  }
  
  return extractionJob;
}

/**
 * Updates content extraction job status
 * @param {string} jobId - The extraction job ID
 * @param {string} status - The status ('running', 'completed', 'failed')
 * @param {Object} metadata - Optional metadata to store
 */
async function updateExtractionJobStatus(jobId, status, metadata = {}) {
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
    .from('content_extraction_jobs')
    .update(updateData)
    .eq('id', jobId);
  
  if (error) {
    console.error(`Error updating extraction job status:`, error);
  }
}

/**
 * Gets content extraction job status
 * @param {string} jobId - The extraction job ID
 * @returns {Promise<Object|null>} - The job record, or null if not found
 */
async function getExtractionJobStatus(jobId) {
  const { data: job, error } = await supabase
    .from('content_extraction_jobs')
    .select('*')
    .eq('id', jobId)
    .single();
  
  if (error) {
    if (error.code === 'PGRST116') {
      return null;
    }
    throw new Error(`Failed to get extraction job status: ${error.message}`);
  }
  
  return job;
}

/**
 * Runs batch content extraction for a sitemap
 * @param {Object} extractionJob - The extraction job object
 */
async function runContentExtraction(extractionJob) {
  const { id: jobId, crawl_job_id: sitemapId, only_missing: onlyMissing } = extractionJob;
  
  console.log(`[CONTENT EXTRACTION START] Job ${jobId} - Starting content extraction for sitemap ${sitemapId}`);
  console.log(`[CONTENT EXTRACTION START] Job ${jobId} - Only missing: ${onlyMissing}`);
  
  try {
    // Get all pages for this sitemap
    let query = supabase
      .from('pages')
      .select('normalized_url, url')
      .eq('crawl_job_id', sitemapId);
    
    const { data: allPages, error: pagesError } = await query.order('normalized_url', { ascending: true });
    
    if (pagesError) {
      throw new Error(`Failed to fetch pages: ${pagesError.message}`);
    }
    
    if (!allPages || allPages.length === 0) {
      console.log(`[CONTENT EXTRACTION] Job ${jobId} - No pages found for sitemap ${sitemapId}`);
      await updateExtractionJobStatus(jobId, 'completed', {
        pages_total: 0,
        pages_extracted: 0,
        pages_failed: 0
      });
      return;
    }
    
    // If only extracting missing content, filter to pages without extracted content
    let pages = allPages;
    if (onlyMissing) {
      // Get pages that already have extracted content
      const { data: existingContent } = await supabase
        .from('page_content')
        .select('normalized_url')
        .eq('crawl_job_id', sitemapId);
      
      const existingUrls = new Set(
        (existingContent || []).map(c => c.normalized_url)
      );
      
      // Filter to only pages without extracted content
      pages = allPages.filter(page => {
        const normalizedUrl = page.normalized_url;
        return !existingUrls.has(normalizedUrl);
      });
      
      console.log(`[CONTENT EXTRACTION] Job ${jobId} - Filtered to ${pages.length} pages missing content (out of ${allPages.length} total)`);
    }
    
    if (!pages || pages.length === 0) {
      console.log(`[CONTENT EXTRACTION] Job ${jobId} - No pages to extract (all may already have content)`);
      await updateExtractionJobStatus(jobId, 'completed', {
        pages_total: allPages.length,
        pages_extracted: 0,
        pages_failed: 0,
        pages_skipped: allPages.length
      });
      return;
    }
    
    console.log(`[CONTENT EXTRACTION] Job ${jobId} - Found ${pages.length} pages to process`);
    
    // Track progress
    let pagesExtracted = 0;
    let pagesFailed = 0;
    const failedUrls = [];
    
    // Process pages in batches to avoid overwhelming the server
    const BATCH_SIZE = 10;
    const DELAY_BETWEEN_BATCHES = 1000; // 1 second
    
    for (let i = 0; i < pages.length; i += BATCH_SIZE) {
      const batch = pages.slice(i, i + BATCH_SIZE);
      
      // Process batch in parallel
      const batchPromises = batch.map(async (page) => {
        try {
          // Use original URL for fetching
          const url = page.url || page.normalized_url;
          
          // Extract content (include raw HTML)
          const content = await extractPageContent(url, { includeRawHtml: true });
          
          // Normalize URL for storage
          const normalizedUrl = normalizeUrl(url) || page.normalized_url;
          content.normalized_url = normalizedUrl;
          
          // DEBUG: Log raw_html status before upsert
          console.log(`[CONTENT EXTRACTION] Job ${jobId} - Content extracted for ${normalizedUrl}:`, {
            has_raw_html: content.raw_html !== undefined && content.raw_html !== null,
            raw_html_length: content.raw_html ? content.raw_html.length : 0,
            raw_html_type: typeof content.raw_html,
            raw_html_preview: content.raw_html ? content.raw_html.substring(0, 100) : 'null/undefined'
          });
          
          // Store/update extracted content (upsert by normalized_url and crawl_job_id)
          // Try upsert with explicit conflict resolution
          const upsertData = {
            crawl_job_id: sitemapId,
            normalized_url: normalizedUrl,
            fetched_at: content.fetched_at,
            content_schema_version: content.content_schema_version, // Schema versioning for future changes
            clean_text: content.clean_text,
            headings: content.headings, // Structured as { h1: [], h2: [], h3: [] }
            seo: content.seo, // SEO metadata object (title, meta_description, canonical_url, robots, og)
            schema: content.schema, // Schema object with json_ld array (raw, non-interpreted)
            raw_html: content.raw_html // Store raw HTML
          };
          
          // DEBUG: Log what we're about to upsert
          console.log(`[CONTENT EXTRACTION] Job ${jobId} - Upsert data for ${normalizedUrl}:`, {
            has_raw_html: upsertData.raw_html !== undefined && upsertData.raw_html !== null,
            raw_html_length: upsertData.raw_html ? upsertData.raw_html.length : 0,
            raw_html_in_payload: 'raw_html' in upsertData
          });
          
          console.log(`[CONTENT EXTRACTION] Job ${jobId} - Attempting to upsert content for ${normalizedUrl}`);
          
          // Try upsert - Supabase may need the constraint name or column order
          const { data: upsertResult, error: insertError } = await supabase
            .from('page_content')
            .upsert(upsertData, {
              onConflict: 'normalized_url,crawl_job_id' // Try reversed order
            });
          
          if (insertError) {
            // Log detailed error information
            console.error(`[CONTENT EXTRACTION] Job ${jobId} - Database upsert error for ${normalizedUrl}:`, {
              error: insertError.message,
              code: insertError.code,
              details: insertError.details,
              hint: insertError.hint,
              fullError: JSON.stringify(insertError, null, 2)
            });
            
            // Try alternative: insert with conflict handling
            console.log(`[CONTENT EXTRACTION] Job ${jobId} - Trying alternative insert approach for ${normalizedUrl}`);
            
            // First, try to delete existing record
            const { error: deleteError } = await supabase
              .from('page_content')
              .delete()
              .eq('crawl_job_id', sitemapId)
              .eq('normalized_url', normalizedUrl);
            
            if (deleteError) {
              console.error(`[CONTENT EXTRACTION] Job ${jobId} - Delete error (non-fatal):`, deleteError.message);
            }
            
            // Then insert fresh
            const { error: insertError2 } = await supabase
              .from('page_content')
              .insert(upsertData);
            
            if (insertError2) {
              throw new Error(`Database error (upsert failed, insert also failed): ${insertError2.message}. Original: ${insertError.message}`);
            } else {
              console.log(`[CONTENT EXTRACTION] Job ${jobId} - ✓ Successfully inserted content for ${normalizedUrl} (using delete+insert method)`);
            }
          } else {
            console.log(`[CONTENT EXTRACTION] Job ${jobId} - ✓ Successfully upserted content for ${normalizedUrl}`);
          }
          
          // Verify the data was actually saved - check raw_html, seo, and schema too
          const { data: verifyData, error: verifyError } = await supabase
            .from('page_content')
            .select('normalized_url, fetched_at, raw_html, seo, schema, content_schema_version')
            .eq('crawl_job_id', sitemapId)
            .eq('normalized_url', normalizedUrl)
            .single();
          
          if (verifyError || !verifyData) {
            console.warn(`[CONTENT EXTRACTION] Job ${jobId} - ⚠ Warning: Could not verify saved content for ${normalizedUrl}`);
          } else {
            console.log(`[CONTENT EXTRACTION] Job ${jobId} - ✓ Verified content saved for ${normalizedUrl} at ${verifyData.fetched_at}`, {
              raw_html_in_db: verifyData.raw_html !== null && verifyData.raw_html !== undefined,
              raw_html_length_in_db: verifyData.raw_html ? verifyData.raw_html.length : 0,
              seo_in_db: verifyData.seo !== null && verifyData.seo !== undefined,
              schema_in_db: verifyData.schema !== null && verifyData.schema !== undefined,
              schema_version_in_db: verifyData.content_schema_version
            });
            
            // Alert if raw_html is missing in DB but was present in payload
            if (upsertData.raw_html && !verifyData.raw_html) {
              console.error(`[CONTENT EXTRACTION] Job ${jobId} - ⚠⚠⚠ CRITICAL: raw_html was in payload but is NULL in database for ${normalizedUrl}!`);
            }
            
            // Alert if SEO is missing in DB but was present in payload
            if (upsertData.seo && !verifyData.seo) {
              console.error(`[CONTENT EXTRACTION] Job ${jobId} - ⚠⚠⚠ CRITICAL: seo was in payload but is NULL in database for ${normalizedUrl}!`);
            }
            
            // Alert if schema is missing in DB but was present in payload
            if (upsertData.schema && !verifyData.schema) {
              console.error(`[CONTENT EXTRACTION] Job ${jobId} - ⚠⚠⚠ CRITICAL: schema was in payload but is NULL in database for ${normalizedUrl}!`);
            }
          }
          
          pagesExtracted++;
          console.log(`[CONTENT EXTRACTION] Job ${jobId} - ✓ Extracted content for ${normalizedUrl} (${pagesExtracted}/${pages.length})`);
          
        } catch (error) {
          pagesFailed++;
          const url = page.url || page.normalized_url;
          failedUrls.push({ url, error: error.message });
          console.error(`[CONTENT EXTRACTION] Job ${jobId} - ✗ Failed to extract content for ${url}: ${error.message}`);
        }
      });
      
      // Wait for batch to complete
      await Promise.all(batchPromises);
      
      // Update progress
      await updateExtractionJobStatus(jobId, 'running', {
        pages_total: pages.length,
        pages_extracted: pagesExtracted,
        pages_failed: pagesFailed,
        last_activity_at: new Date().toISOString()
      });
      
      // Delay between batches (except for last batch)
      if (i + BATCH_SIZE < pages.length) {
        await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_BATCHES));
      }
    }
    
    // Verify final count in database
    const { count: dbCount, error: countError } = await supabase
      .from('page_content')
      .select('*', { count: 'exact', head: true })
      .eq('crawl_job_id', sitemapId);
    
    if (countError) {
      console.error(`[CONTENT EXTRACTION] Job ${jobId} - Error counting saved records:`, countError.message);
    } else {
      console.log(`[CONTENT EXTRACTION] Job ${jobId} - Database verification: ${dbCount} records found in page_content table`);
      if (dbCount !== pagesExtracted) {
        console.warn(`[CONTENT EXTRACTION] Job ${jobId} - ⚠ Mismatch: Expected ${pagesExtracted} records, found ${dbCount} in database`);
      }
    }
    
    // Mark job as completed
    await updateExtractionJobStatus(jobId, 'completed', {
      pages_total: pages.length,
      pages_extracted: pagesExtracted,
      pages_failed: pagesFailed,
      pages_in_database: dbCount || null,
      failed_urls: failedUrls.length > 0 ? failedUrls : null
    });
    
    console.log(`[CONTENT EXTRACTION COMPLETION] Job ${jobId} - Completed`);
    console.log(`[CONTENT EXTRACTION COMPLETION] Job ${jobId} - Total: ${pages.length}, Extracted: ${pagesExtracted}, Failed: ${pagesFailed}, In DB: ${dbCount || 'unknown'}`);
    
    if (failedUrls.length > 0) {
      console.log(`[CONTENT EXTRACTION COMPLETION] Job ${jobId} - Failed URLs:`);
      failedUrls.forEach(({ url, error }) => {
        console.log(`  - ${url}: ${error}`);
      });
    }
    
  } catch (error) {
    console.error(`[CONTENT EXTRACTION FAILED] Job ${jobId} - Error: ${error.message}`);
    await updateExtractionJobStatus(jobId, 'failed', {
      error_message: error.message
    });
    throw error;
  }
}

/**
 * Wrapper to run content extraction with error handling
 * @param {Object} extractionJob - The extraction job object
 */
async function runContentExtractionWithErrorHandling(extractionJob) {
  try {
    await runContentExtraction(extractionJob);
  } catch (error) {
    console.error(`[CONTENT EXTRACTION FAILED] Job ${extractionJob.id} - Extraction failed: ${error.message}`);
    // Error already logged and job status updated in runContentExtraction
  }
}

module.exports = {
  startContentExtraction,
  runContentExtraction,
  runContentExtractionWithErrorHandling,
  updateExtractionJobStatus,
  getExtractionJobStatus
};

