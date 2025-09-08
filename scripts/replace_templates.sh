#!/usr/bin/env bash

set -e

# Check if all arguments are provided
if [ "$#" -ne 5 ]; then
    echo "Usage: $0 <package_name> <repo_owner> <repo_name> <cli_bin> <env_prefix>"
    echo "Example: $0 my-cli myusername my-cli-repo mycli MYCLI"
    exit 1
fi

PACKAGE_NAME="$1"
REPO_OWNER="$2"
REPO_NAME="$3"
CLI_BIN="$4"
ENV_PREFIX="$5"

echo "Current directory: $(pwd)"
echo ""
echo "Template replacements to be made:"
echo "  __TEMPLATE_PACKAGE_NAME__ → $PACKAGE_NAME"
echo "  __TEMPLATE_REPO_OWNER__ → $REPO_OWNER"
echo "  __TEMPLATE_REPO__ → $REPO_NAME"
echo "  __TEMPLATE_CLI_BIN__ → $CLI_BIN"
echo "  __TEMPLATE_ENV_PREFIX__ → $ENV_PREFIX"
echo ""
read -p "Do you want to proceed with these replacements? (y/N) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi
echo ""
echo "Replacing template strings..."

# Find all files with template strings and replace them
find . -type f \
    -not -path "./.git/*" \
    -not -path "./target/*" \
    -exec grep -l "__TEMPLATE" {} \; | while read -r file; do

    echo "Processing: $file"

    # Use sed with different syntax for macOS vs Linux
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' \
            -e "s/__TEMPLATE_PACKAGE_NAME__/$PACKAGE_NAME/g" \
            -e "s/__TEMPLATE_REPO_OWNER__/$REPO_OWNER/g" \
            -e "s/__TEMPLATE_REPO__/$REPO_NAME/g" \
            -e "s/__TEMPLATE_CLI_BIN__/$CLI_BIN/g" \
            -e "s/__TEMPLATE_ENV_PREFIX__/$ENV_PREFIX/g" \
            "$file"
    else
        sed -i \
            -e "s/__TEMPLATE_PACKAGE_NAME__/$PACKAGE_NAME/g" \
            -e "s/__TEMPLATE_REPO_OWNER__/$REPO_OWNER/g" \
            -e "s/__TEMPLATE_REPO__/$REPO_NAME/g" \
            -e "s/__TEMPLATE_CLI_BIN__/$CLI_BIN/g" \
            -e "s/__TEMPLATE_ENV_PREFIX__/$ENV_PREFIX/g" \
            "$file"
    fi
done

echo ""
echo "Done. Run 'cargo build' to verify everything compiles."
