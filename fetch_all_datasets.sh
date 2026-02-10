#!/bin/bash
# Fetch all INSPIRE datasets from Austria

mkdir -p raw_data

BASE_URL="https://geometadatensuche.inspire.gv.at/metadatensuche/inspire/api/search/records/_search"
PAGE_SIZE=100
TOTAL=1917
PAGES=$((($TOTAL + $PAGE_SIZE - 1) / $PAGE_SIZE))

for ((page=0; page<PAGES; page++)); do
    FROM=$((page * PAGE_SIZE))
    echo "Fetching page $((page+1))/$PAGES (from=$FROM)..."
    
    curl -s "$BASE_URL" \
        -H "Content-Type: application/json" \
        -H "Accept: application/json" \
        -d "{\"query\":{\"match_all\":{}},\"from\":$FROM,\"size\":$PAGE_SIZE}" \
        > "raw_data/page_${page}.json"
    
    sleep 0.5  # Be nice to the server
done

echo "Done fetching all pages."
