/**
 * Supabase client setup
 * 
 * This file configures and exports the Supabase client for database operations.
 * It handles authentication, connection setup, and provides database access
 * for storing crawl jobs, results, and other persistent data related to
 * the crawling service.
 * 
 * This is the single database access point for the application.
 * All Supabase operations should use this exported client.
 */

// Load environment variables from .env file if not already loaded
require('dotenv').config();

const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_KEY;

if (!supabaseUrl || !supabaseServiceKey) {
  throw new Error('Missing required Supabase environment variables: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set');
}

const supabase = createClient(supabaseUrl, supabaseServiceKey);

module.exports = supabase;

