#!/bin/bash

# This script lists all PRs merged after a specific commit in a Git repository
# It requires the commit hash and optionally the branch name (defaults to current branch)

set -e

# Function to display usage information
usage() {
  echo "Usage: $0 -c COMMIT_HASH [-b BRANCH_NAME] [-o OUTPUT_FORMAT] [-r REPO] [-d|--detail]"
  echo ""
  echo "Options:"
  echo "  -c COMMIT_HASH      The commit hash to start from (required)"
  echo "  -b BRANCH_NAME      The branch name to analyze (default: current branch)"
  echo "  -d, --detail        Gather detailed PR information and output to file"
  echo "  -o OUTPUT_FORMAT    Output format for detailed mode: md, html, or json (default: md)"
  echo "  -r REPO             Repository name in the format 'owner/repo' (default: derived from remote origin)"
  echo ""
  echo "Examples:"
  echo "  $0 -c 15e1e38ade1b00ba2ca9ce202902422f249cdeff"
  echo "  $0 -c 15e1e38ade1b00ba2ca9ce202902422f249cdeff -b slack-19.0 -d -o html"
  exit 1
}

# Default values
BRANCH=$(git branch --show-current)
OUTPUT_FORMAT="md"
REPO=""
DETAIL_MODE=false

# Parse command line options
while [[ $# -gt 0 ]]; do
  case $1 in
    -c)
      COMMIT_HASH="$2"
      shift 2
      ;;
    -b)
      BRANCH="$2"
      shift 2
      ;;
    -o)
      OUTPUT_FORMAT="$2"
      if [[ "$OUTPUT_FORMAT" != "md" && "$OUTPUT_FORMAT" != "html" && "$OUTPUT_FORMAT" != "json" ]]; then
        echo "Error: Output format must be md, html, or json"
        usage
      fi
      shift 2
      ;;
    -r)
      REPO="$2"
      shift 2
      ;;
    -d|--detail)
      DETAIL_MODE=true
      shift
      ;;
    -h|--help)
      usage
      ;;
    *)
      echo "Unknown option: $1"
      usage
      ;;
  esac
done

# Check if commit hash is provided
if [ -z "$COMMIT_HASH" ]; then
  echo "Error: Commit hash is required"
  usage
fi

# Validate the commit hash
if ! git rev-parse --quiet --verify "$COMMIT_HASH^{commit}" > /dev/null; then
  echo "Error: Invalid commit hash"
  exit 1
fi

# Validate the branch
if ! git show-ref --verify --quiet "refs/heads/$BRANCH"; then
  echo "Error: Branch '$BRANCH' does not exist"
  exit 1
fi

# If no repo is provided, try to get it from the remote origin
if [ -z "$REPO" ]; then
  REMOTE_URL=$(git remote get-url origin)
  if [[ $REMOTE_URL =~ github\.com[:/](.+)\.git ]]; then
    REPO=${BASH_REMATCH[1]}
  elif [[ $REMOTE_URL =~ github\.com[:/](.+) ]]; then
    REPO=${BASH_REMATCH[1]}
  else
    echo "Error: Could not determine repository from remote URL. Please provide using -r option."
    exit 1
  fi
fi

echo "Analyzing commits after $COMMIT_HASH on branch $BRANCH in repository $REPO"

# Get all commits after the specified commit
COMMITS=$(git log ${COMMIT_HASH}..${BRANCH} --pretty=format:"%h %s" --reverse)

# Function to extract PR numbers from a commit message
extract_pr_numbers() {
  local message="$1"
  local numbers=""
  
  # Look for pattern (#123)
  if [[ $message =~ \(#([0-9]+)\) ]]; then
    numbers+=" ${BASH_REMATCH[1]}"
  fi
  
  # Look for pattern " #123 " (with space before)
  if [[ $message =~ [[:space:]]#([0-9]+)[[:space:]] ]]; then
    numbers+=" ${BASH_REMATCH[1]}"
  fi
  
  echo "$numbers" | tr ' ' '\n' | grep -v '^$' | sort -u
}

# Extract all PR numbers from commit messages
PR_NUMBERS=""
while read -r commit_hash message; do
  PR_NUMS=$(extract_pr_numbers "$message")
  PR_NUMBERS+=" $PR_NUMS"
done < <(echo "$COMMITS")

# Make the PR numbers unique
PR_NUMBERS=$(echo "$PR_NUMBERS" | tr ' ' '\n' | grep -v '^$' | sort -u)

# Count the total number of unique PR numbers
PR_COUNT=$(echo "$PR_NUMBERS" | wc -l)

# Create temp directory for storing PR data
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

echo "Found references to $PR_COUNT PRs across $(echo "$COMMITS" | wc -l) commits"

# Function to get PR details using GitHub CLI
get_pr_details() {
  local pr_number=$1
  local repo=$2
  local output_file=$3
  
  # Skip large PR numbers which might be references to upstream PRs
  if [[ $pr_number -gt 10000 && $repo =~ slackhq/ ]]; then
    # Try upstream repo
    UPSTREAM_REPO=$(echo "$repo" | sed 's|slackhq/|vitessio/|')
    gh pr view $pr_number --repo $UPSTREAM_REPO --json number,title,url,body,mergedAt,author,baseRefName,mergeCommit 2>/dev/null > "$output_file" || {
      # If upstream fails, try original repo
      gh pr view $pr_number --repo $repo --json number,title,url,body,mergedAt,author,baseRefName,mergeCommit 2>/dev/null > "$output_file" || {
        echo "PR #$pr_number not found in either repository"
        return 1
      }
    }
  else
    # Try the provided repo first
    gh pr view $pr_number --repo $repo --json number,title,url,body,mergedAt,author,baseRefName,mergeCommit 2>/dev/null > "$output_file" || {
      echo "PR #$pr_number not found in $repo"
      return 1
    }
  fi
  return 0
}

# Simple output mode - just print PR numbers and titles
if [ "$DETAIL_MODE" = false ]; then
  echo "PRs merged after $COMMIT_HASH on branch $BRANCH:"
  echo "--------------------------------------------"
  
  # Process all commits and extract PR information
  TOTAL_COMMITS=$(echo "$COMMITS" | wc -l)
  
  echo "$COMMITS" | while read -r commit_hash message; do
    # Get all PR numbers from this commit message
    PR_NUMS=$(extract_pr_numbers "$message")
    
    if [ -z "$PR_NUMS" ]; then
      # No PR numbers found in this commit, skip it
      continue
    fi
    
    # For display purposes, use the first PR number found
    # and clean up the message
    PR_NUMBER=$(echo "$PR_NUMS" | awk '{print $1}')
    
    # Extract a clean title by removing standard PR references
    PR_TITLE=$(echo "$message" | sed 's/(#[0-9][0-9]*)//' | sed 's/ #[0-9][0-9]*//g' | xargs)
    
    COMMIT_DATE=$(git show -s --format=%cd --date=short $commit_hash)
    printf "%-8s %-10s %-15s %s\n" "$commit_hash" "#$PR_NUMBER" "[$COMMIT_DATE]" "$PR_TITLE"
  done
  exit 0
fi

# Detailed mode - gather PR information and generate reports
echo "Detailed mode: gathering PR information..."

# Check if GitHub CLI is available
if ! command -v gh &> /dev/null; then
  echo "Error: GitHub CLI is not installed. Please install it first."
  exit 1
fi

# Check GitHub authentication
if ! gh auth status &> /dev/null; then
  echo "Error: Not authenticated with GitHub. Please run 'gh auth login' first."
  exit 1
fi

# Process all PR numbers
for PR_NUMBER in $PR_NUMBERS; do
  PR_FILE="$TEMP_DIR/pr_${PR_NUMBER}.json"
  get_pr_details "$PR_NUMBER" "$REPO" "$PR_FILE"
done

# Count successful PR fetches
SUCCESSFUL_PRS=$(find "$TEMP_DIR" -type f -name "pr_*.json" -not -empty | wc -l)
echo "Successfully fetched details for $SUCCESSFUL_PRS PRs"

# Output in requested format
case "$OUTPUT_FORMAT" in
  md)
    OUTPUT_FILE="pr_list_$(date +%Y%m%d).md"
    {
      echo "# PRs merged after commit $COMMIT_HASH on branch $BRANCH"
      echo ""
      echo "Generated on $(date '+%Y-%m-%d %H:%M:%S')"
      echo ""
      
      # Find all PRs sorted by merge date
      find "$TEMP_DIR" -type f -name "pr_*.json" -not -empty | while read PR_FILE; do
        PR_DATA=$(cat "$PR_FILE")
        PR_TITLE=$(echo "$PR_DATA" | jq -r '.title')
        PR_URL=$(echo "$PR_DATA" | jq -r '.url')
        PR_NUMBER=$(echo "$PR_DATA" | jq -r '.number')
        PR_BODY=$(echo "$PR_DATA" | jq -r '.body' | grep -v '<!--' | sed '/^$/d' | head -n 3)
        PR_MERGED_AT=$(echo "$PR_DATA" | jq -r '.mergedAt')
        PR_AUTHOR=$(echo "$PR_DATA" | jq -r '.author.login')
        PR_MERGE_COMMIT=$(echo "$PR_DATA" | jq -r '.mergeCommit.oid')
        
        echo "## [PR #$PR_NUMBER]($PR_URL): $PR_TITLE"
        echo ""
        echo "**Author:** $PR_AUTHOR"
        echo "**Merged at:** $PR_MERGED_AT"
        echo "**Merge commit:** $PR_MERGE_COMMIT"
        echo ""
        echo "**Description:**"
        echo ""
        echo "$PR_BODY..."
        echo ""
        echo "---"
        echo ""
      done
    } > "$OUTPUT_FILE"
    echo "Output written to $OUTPUT_FILE"
    ;;
    
  html)
    OUTPUT_FILE="pr_list_$(date +%Y%m%d).html"
    {
      echo "<!DOCTYPE html>"
      echo "<html lang=\"en\">"
      echo "<head>"
      echo "    <meta charset=\"UTF-8\">"
      echo "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">"
      echo "    <title>PRs after $COMMIT_HASH on $BRANCH</title>"
      echo "    <style>"
      echo "        body { font-family: -apple-system, BlinkMacSystemFont, \"Segoe UI\", Roboto, Oxygen, Ubuntu, Cantarell, \"Open Sans\", \"Helvetica Neue\", sans-serif; line-height: 1.6; color: #333; max-width: 900px; margin: 0 auto; padding: 20px; }"
      echo "        h1, h2 { color: #1a73e8; }"
      echo "        h1 { border-bottom: 1px solid #eee; padding-bottom: 10px; }"
      echo "        a { color: #1a73e8; text-decoration: none; }"
      echo "        a:hover { text-decoration: underline; }"
      echo "        .pr-meta { color: #666; font-size: 0.9em; margin-bottom: 10px; }"
      echo "        .pr-body { margin-left: 20px; border-left: 3px solid #eee; padding-left: 15px; }"
      echo "        .divider { border-bottom: 1px solid #eee; margin: 30px 0; }"
      echo "    </style>"
      echo "</head>"
      echo "<body>"
      echo "    <h1>PRs merged after commit $COMMIT_HASH on branch $BRANCH</h1>"
      echo "    <p>Generated on $(date '+%Y-%m-%d %H:%M:%S')</p>"
      
      # Find all PRs sorted by merge date
      find "$TEMP_DIR" -type f -name "pr_*.json" -not -empty | while read PR_FILE; do
        PR_DATA=$(cat "$PR_FILE")
        PR_TITLE=$(echo "$PR_DATA" | jq -r '.title')
        PR_URL=$(echo "$PR_DATA" | jq -r '.url')
        PR_NUMBER=$(echo "$PR_DATA" | jq -r '.number')
        PR_BODY=$(echo "$PR_DATA" | jq -r '.body' | grep -v '<!--' | sed '/^$/d' | head -n 3)
        PR_MERGED_AT=$(echo "$PR_DATA" | jq -r '.mergedAt')
        PR_AUTHOR=$(echo "$PR_DATA" | jq -r '.author.login')
        PR_MERGE_COMMIT=$(echo "$PR_DATA" | jq -r '.mergeCommit.oid')
        
        echo "    <div class=\"pr-entry\">"
        echo "        <h2><a href=\"$PR_URL\">PR #$PR_NUMBER: $PR_TITLE</a></h2>"
        echo "        <div class=\"pr-meta\">"
        echo "            <strong>Author:</strong> $PR_AUTHOR<br>"
        echo "            <strong>Merged at:</strong> $PR_MERGED_AT<br>"
        echo "            <strong>Merge commit:</strong> $PR_MERGE_COMMIT"
        echo "        </div>"
        echo "        <div class=\"pr-body\">"
        echo "            <h3>Description:</h3>"
        echo "            <p>$PR_BODY...</p>"
        echo "        </div>"
        echo "        <div class=\"divider\"></div>"
        echo "    </div>"
      done
      
      echo "</body>"
      echo "</html>"
    } > "$OUTPUT_FILE"
    echo "Output written to $OUTPUT_FILE"
    ;;
    
  json)
    OUTPUT_FILE="pr_list_$(date +%Y%m%d).json"
    {
      echo "{"
      echo "  \"metadata\": {"
      echo "    \"commit\": \"$COMMIT_HASH\","
      echo "    \"branch\": \"$BRANCH\","
      echo "    \"repository\": \"$REPO\","
      echo "    \"generated_at\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\""
      echo "  },"
      echo "  \"pull_requests\": ["
      
      # Find all PR files
      PR_FILES=($(find "$TEMP_DIR" -type f -name "pr_*.json" -not -empty))
      PR_COUNT=${#PR_FILES[@]}
      
      for ((i=0; i<PR_COUNT; i++)); do
        PR_FILE="${PR_FILES[$i]}"
        PR_DATA=$(cat "$PR_FILE")
        
        # Output the PR data as JSON
        echo "$PR_DATA"
        
        # Add comma if not the last item
        if [ $i -lt $((PR_COUNT-1)) ]; then
          echo ","
        fi
      done
      
      echo "  ]"
      echo "}"
    } > "$OUTPUT_FILE"
    echo "Output written to $OUTPUT_FILE"
    ;;
esac

echo "Done!"