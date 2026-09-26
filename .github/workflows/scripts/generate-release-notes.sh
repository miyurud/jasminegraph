#!/usr/bin/env bash
# Builds RELEASE_NOTES.md for the current tag by grouping every PR merged into
# develop since the previous tag, using each PR's label, title, author and
# description. develop is used regardless of which branch the tag itself was
# cut from (master or develop), because individual PRs are always raised
# against develop - master only ever receives the bulk "merge develop into
# master" PR, which carries no per-change detail of its own.
# Requires: gh (authenticated via GH_TOKEN), jq, git history with tags
# (checkout must use fetch-depth: 0).
set -euo pipefail

CURRENT_TAG="${GITHUB_REF_NAME}"
REPO="${GITHUB_REPOSITORY}"
PR_BASE_BRANCH="develop"

PREV_TAG=$(git tag --sort=-creatordate | grep -Fxv "${CURRENT_TAG}" | head -n1 || true)

UNTIL=$(TZ=UTC git log -1 --date='format-local:%Y-%m-%dT%H:%M:%SZ' --format=%cd "${CURRENT_TAG}")
if [[ -n ${PREV_TAG} ]]; then
    SINCE=$(TZ=UTC git log -1 --date='format-local:%Y-%m-%dT%H:%M:%SZ' --format=%cd "${PREV_TAG}")
else
    SINCE="1970-01-01T00:00:00Z"
fi

PRS_JSON=$(gh pr list --repo "${REPO}" --state merged --base "${PR_BASE_BRANCH}" \
    --json number,title,body,author,labels,mergedAt,url --limit 300)

FILTERED=$(echo "${PRS_JSON}" | jq --arg since "${SINCE}" --arg until "${UNTIL}" \
    '[.[] | select(.mergedAt > $since and .mergedAt <= $until)]')

print_pr() {
    # $1 = single PR object (compact JSON)
    local pr_json num title author url body
    pr_json="$1"
    num=$(echo "${pr_json}" | jq -r '.number')
    title=$(echo "${pr_json}" | jq -r '.title')
    author=$(echo "${pr_json}" | jq -r '.author.login')
    url=$(echo "${pr_json}" | jq -r '.url')
    body=$(echo "${pr_json}" | jq -r '.body // ""' | tr -d '\r')

    echo "- **${title}** ([#${num}](${url})) by @${author}"
    if [[ -n "$(echo "${body}" | tr -d '[:space:]')" ]]; then
        echo "${body}" | head -c 500 | sed 's/^/  > /'
        echo
    fi
    echo
}

{
    echo "## What's Changed"
    echo

    CATEGORIZED_NUMBERS="[]"
    declare -A SECTION_TITLES=(
        [enhancement]="🚀 New Features"
        [bug]="🐛 Bug Fixes"
        [documentation]="📚 Documentation"
        [maintenance]="🧰 Maintenance"
        [dependencies]="⬆️ Dependencies"
    )
    CATEGORY_ORDER=(enhancement bug documentation maintenance dependencies)

    for label in "${CATEGORY_ORDER[@]}"; do
        SECTION_PRS=$(echo "${FILTERED}" | jq --arg label "${label}" \
            '[.[] | select([.labels[].name] | index($label))]')
        COUNT=$(echo "${SECTION_PRS}" | jq 'length')
        if [[ ${COUNT} -gt 0 ]]; then
            echo "### ${SECTION_TITLES[$label]}"
            echo
            while IFS= read -r pr; do
                print_pr "${pr}"
            done < <(echo "${SECTION_PRS}" | jq -c '.[]')
            NUMS=$(echo "${SECTION_PRS}" | jq '[.[].number]')
            CATEGORIZED_NUMBERS=$(jq -n --argjson a "${CATEGORIZED_NUMBERS}" --argjson b "${NUMS}" '$a + $b')
        fi
    done

    OTHER_PRS=$(echo "${FILTERED}" | jq --argjson used "${CATEGORIZED_NUMBERS}" \
        '[.[] | select(.number as $n | ($used | index($n)) | not)]')
    OTHER_COUNT=$(echo "${OTHER_PRS}" | jq 'length')
    if [[ ${OTHER_COUNT} -gt 0 ]]; then
        echo "### 🔧 Other Changes"
        echo
        while IFS= read -r pr; do
            print_pr "${pr}"
        done < <(echo "${OTHER_PRS}" | jq -c '.[]')
    fi

    TOTAL=$(echo "${FILTERED}" | jq 'length')
    if [[ ${TOTAL} -eq 0 ]]; then
        echo "_No merged pull requests found for this release._"
        echo
    fi

    echo "---"
    if [[ -n ${PREV_TAG} ]]; then
        echo "**Full Changelog**: https://github.com/${REPO}/compare/${PREV_TAG}...${CURRENT_TAG}"
    else
        echo "**Full Changelog**: https://github.com/${REPO}/commits/${CURRENT_TAG}"
    fi
} >RELEASE_NOTES.md

cat RELEASE_NOTES.md
