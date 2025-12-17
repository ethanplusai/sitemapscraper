#!/bin/bash
# Git setup script for SiteMapScraper

set -e  # Exit on error

echo "=== Setting up Git repository ==="
cd "$(dirname "$0")"

# Check if git is installed
if ! command -v git &> /dev/null; then
    echo "Error: Git is not installed. Please install Git first."
    exit 1
fi

# Initialize git if not already initialized
if [ ! -d .git ]; then
    echo "Initializing Git repository..."
    git init
else
    echo "Git repository already initialized"
fi

# Check current status
echo ""
echo "=== Current Git Status ==="
git status --short || echo "No files to show"

# Add all files
echo ""
echo "=== Adding all files ==="
git add .

# Show what will be committed
echo ""
echo "=== Files staged for commit ==="
git status --short

# Create initial commit
echo ""
echo "=== Creating initial commit ==="
git commit -m "Initial commit: SiteMapScraper API service with crawl functionality" || echo "Commit may have failed or nothing to commit"

# Check if remote exists
echo ""
echo "=== Checking remote configuration ==="
if git remote get-url origin &> /dev/null; then
    echo "Remote 'origin' already exists:"
    git remote -v
    read -p "Do you want to update it? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git remote set-url origin https://github.com/ethanplusai/sitemapscraper.git
        echo "Remote updated"
    fi
else
    echo "Adding remote 'origin'..."
    git remote add origin https://github.com/ethanplusai/sitemapscraper.git
    echo "Remote added"
fi

# Set branch to main
echo ""
echo "=== Setting branch to main ==="
git branch -M main

# Show final status
echo ""
echo "=== Final Status ==="
echo "Remote:"
git remote -v
echo ""
echo "Branch:"
git branch
echo ""
echo "Latest commit:"
git log --oneline -1 || echo "No commits yet"

echo ""
echo "=== Ready to push ==="
echo "To push to GitHub, run:"
echo "  git push -u origin main"
echo ""
echo "Note: You may need to authenticate with GitHub (Personal Access Token or SSH key)"

