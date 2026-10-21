#!/usr/bin/env bash
# ==============================================================================
# Batch Branch Protection Enforcer for GitHub Repositories
# Enforces:
#  - Block Force Pushes (allow_force_pushes: false)
#  - Block Branch Deletion (allow_deletions: false)
#  - Dynamic default branch detection (main, master, etc.)
#  - Public vs Private plan compatibility handling
# ==============================================================================

set -o errexit
set -o pipefail
set -o nounset

BOLD="\033[1m"
GREEN="\033[38;5;82m"
RED="\033[38;5;196m"
YELLOW="\033[38;5;220m"
CYAN="\033[38;5;39m"
RESET="\033[0m"

DRY_RUN=false
TARGET_REPO=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --repo)
      TARGET_REPO="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [--dry-run] [--repo <repo-name>]"
      echo "  --dry-run          Preview changes without applying branch protection"
      echo "  --repo <name>      Target a specific repository only"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Check gh auth status
if ! gh auth status >/dev/null 2>&1; then
  echo -e "${RED}❌ You are not authenticated with GitHub CLI.${RESET}"
  echo -e "${YELLOW}Please run:${RESET} gh auth login"
  exit 1
fi

OWNER=$(gh api user -q .login)
echo -e "${BOLD}${CYAN}==============================================================================${RESET}"
echo -e "${BOLD}${CYAN}  🛡️  GitHub Branch Protection Enforcer | Account: @${OWNER}${RESET}"
if [ "$DRY_RUN" = true ]; then
  echo -e "${YELLOW}  [MODE: DRY-RUN (No changes will be written)]${RESET}"
fi
echo -e "${BOLD}${CYAN}==============================================================================${RESET}\n"

# Fetch repositories
echo -e "${CYAN}ℹ️  Fetching repositories...${RESET}"
if [ -n "$TARGET_REPO" ]; then
  REPOS_JSON=$(gh repo view "${OWNER}/${TARGET_REPO}" --json name,isPrivate,defaultBranchRef | jq '[.]')
else
  REPOS_JSON=$(gh repo list "${OWNER}" --limit 100 --json name,isPrivate,defaultBranchRef)
fi

TOTAL=$(echo "$REPOS_JSON" | jq 'length')
echo -e "${CYAN}Found ${TOTAL} repository(ies) to process.${RESET}\n"

SUCCESS_COUNT=0
SKIP_COUNT=0

while read -r name is_private default_branch; do
  if [ -z "$name" ] || [ "$name" = "null" ]; then
    continue
  fi

  if [ -z "$default_branch" ] || [ "$default_branch" = "null" ]; then
    default_branch="main"
  fi

  VISIBILITY="public"
  if [ "$is_private" = "true" ]; then
    VISIBILITY="private"
  fi

  echo -e "${BOLD}▶ [${name}]${RESET} (Visibility: ${VISIBILITY}, Default Branch: ${default_branch})"

  if [ "$DRY_RUN" = true ]; then
    echo -e "   ${YELLOW}🔍 [DRY-RUN] Would protect branch '${default_branch}' against force-pushes and deletions.${RESET}"
    ((SUCCESS_COUNT++))
    continue
  fi

  PAYLOAD='{
    "required_status_checks": null,
    "enforce_admins": false,
    "required_pull_request_reviews": null,
    "restrictions": null,
    "allow_force_pushes": false,
    "allow_deletions": false
  }'

  # Call GitHub API to apply protection
  RESPONSE=$(gh api -X PUT "repos/${OWNER}/${name}/branches/${default_branch}/protection" \
    --input - <<< "$PAYLOAD" 2>&1) && STATUS=0 || STATUS=$?

  if [ $STATUS -eq 0 ]; then
    echo -e "   ${GREEN}✅ Branch protection applied: force-pushes & deletions BLOCKED on '${default_branch}'${RESET}"
    ((SUCCESS_COUNT++))
  else
    if echo "$RESPONSE" | grep -q "Upgrade to GitHub Pro"; then
      echo -e "   ${YELLOW}⚠️  Skipped (Private repo on GitHub Free tier requires GitHub Pro/Team for branch protection rules).${RESET}"
    else
      echo -e "   ${RED}❌ Failed to apply protection: ${RESPONSE}${RESET}"
    fi
    ((SKIP_COUNT++))
  fi

done < <(echo "$REPOS_JSON" | jq -r '.[] | "\(.name) \(.isPrivate) \(.defaultBranchRef.name)"')

echo -e "\n${BOLD}${CYAN}==============================================================================${RESET}"
echo -e "${BOLD}  Summary: ${GREEN}${SUCCESS_COUNT} Protected${RESET} | ${YELLOW}${SKIP_COUNT} Skipped${RESET} | ${BOLD}Total: ${TOTAL}${RESET}"
echo -e "${BOLD}${CYAN}==============================================================================${RESET}"
