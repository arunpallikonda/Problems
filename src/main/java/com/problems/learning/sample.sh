#!/bin/bash

# Usage: ./replace.sh --from dev-30 --to dev-07

# ─── Parse named parameters ───────────────────────────────────────
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --from) FROM="$2"; shift ;;
        --to)   TO="$2";   shift ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

# ─── Validate ─────────────────────────────────────────────────────
if [[ -z "$FROM" || -z "$TO" ]]; then
    echo "Usage: $0 --from <search_string> --to <replace_string>"
    exit 1
fi

# ─── Hardcoded list of directories ────────────────────────────────
# Add your target directories here
DIRS=(
    "/path/to/dir1"
    "/path/to/dir2"
    "/path/to/dir3"
)

# ─── Process each directory ───────────────────────────────────────
echo "Replacing '$FROM' → '$TO'"
echo "=================================================="

TOTAL=0

for DIR in "${DIRS[@]}"; do
    if [[ ! -d "$DIR" ]]; then
        echo "[SKIP] Directory not found: $DIR"
        continue
    fi

    echo ""
    echo "📁 Directory: $DIR"
    echo "--------------------------------------------------"

    FILES=$(grep -rl "$FROM" "$DIR"/*.properties 2>/dev/null)

    if [[ -z "$FILES" ]]; then
        echo "  No matches found."
        continue
    fi

    for FILE in $FILES; do
        while IFS= read -r MATCH; do
            LINENUM=$(echo "$MATCH" | cut -d: -f1)
            OLDLINE=$(echo "$MATCH" | cut -d: -f2-)
            NEWLINE=$(echo "$OLDLINE" | sed "s/$FROM/$TO/g")
            echo "  FILE : $FILE"
            echo "  LINE : $LINENUM"
            echo "  FROM : $OLDLINE"
            echo "  TO   : $NEWLINE"
            echo ""
            ((TOTAL++))
        done < <(grep -n "$FROM" "$FILE")

        sed -i "s/$FROM/$TO/g" "$FILE"
    done
done

echo "=================================================="
echo "✅ Done. $TOTAL line(s) changed across all directories."
