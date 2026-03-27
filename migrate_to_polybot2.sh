#!/usr/bin/env bash
# migrate_to_polybot2.sh – Create a fresh "polybot2" repo from the current files.
#
# Usage:
#   ./migrate_to_polybot2.sh                       # creates ../polybot2
#   ./migrate_to_polybot2.sh /path/to/target       # creates at custom location
#
# The script copies every tracked file (no git history) into a new directory,
# initializes a fresh git repo there, and makes an initial commit.
# The original PolyBot directory is left untouched.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TARGET="${1:-${SCRIPT_DIR}/../polybot2}"

if [ -d "$TARGET" ]; then
    echo "❌  Target directory already exists: $TARGET"
    echo "    Remove it first or choose a different path."
    exit 1
fi

echo "📦  Exporting tracked files from PolyBot …"
mkdir -p "$TARGET"

# Use git archive to get a clean snapshot (no history, respects .gitignore)
git -C "$SCRIPT_DIR" archive HEAD | tar -x -C "$TARGET"

echo "🔧  Initializing fresh git repository …"
cd "$TARGET"
git init -b main

# Ensure git has a user identity for the initial commit
if ! git config user.name >/dev/null 2>&1; then
    git config user.name "PolyBot Migration"
fi
if ! git config user.email >/dev/null 2>&1; then
    git config user.email "noreply@polybot.local"
fi

git add .
git commit -m "Initial commit – polybot2 (migrated from PolyBot)"

echo ""
echo "✅  Done!  Fresh repo created at: $TARGET"
echo ""
echo "Next steps:"
echo "  1. Create a new GitHub repo named 'polybot2':"
echo "       gh repo create polybot2 --private --source=\"$TARGET\" --push"
echo "     OR manually:"
echo "       cd $TARGET"
echo "       git remote add origin https://github.com/<YOUR_USERNAME>/polybot2.git"
echo "       git push -u origin main"
echo ""
echo "  2. Set your Railway / env vars on the new repo."
echo "  3. The original PolyBot directory is unchanged."
