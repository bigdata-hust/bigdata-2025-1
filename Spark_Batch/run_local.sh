#!/bin/bash
# =============================================================================
# Yelp Big Data Analysis - Quick Run Script
# =============================================================================

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "================================================================================"
echo "                  YELP BIG DATA ANALYSIS - BATCH MODE"
echo "================================================================================"

# Default paths
DATA_PATH="${DATA_PATH:-./data/}"
OUTPUT_PATH="${OUTPUT_PATH:-./output/}"
SHOW_ROWS="${SHOW_ROWS:-10}"
SAVE_FORMAT="${SAVE_FORMAT:-parquet}"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --data-path)
            DATA_PATH="$2"
            shift 2
            ;;
        --output-path)
            OUTPUT_PATH="$2"
            shift 2
            ;;
        --show-rows)
            SHOW_ROWS="$2"
            shift 2
            ;;
        --save-format)
            SAVE_FORMAT="$2"
            shift 2
            ;;
        --skip-save)
            SKIP_SAVE="--skip-save"
            shift
            ;;
        --help)
            echo "Usage: ./run_local.sh [options]"
            echo ""
            echo "Options:"
            echo "  --data-path PATH        Path to data directory (default: ./data/)"
            echo "  --output-path PATH      Path to output directory (default: ./output/)"
            echo "  --show-rows N           Number of rows to display (default: 10)"
            echo "  --save-format FORMAT    Output format: parquet, csv, json (default: parquet)"
            echo "  --skip-save             Skip saving results to disk"
            echo "  --help                  Show this help message"
            echo ""
            echo "Examples:"
            echo "  ./run_local.sh"
            echo "  ./run_local.sh --data-path /path/to/data/"
            echo "  ./run_local.sh --data-path ./data/ --show-rows 20 --save-format csv"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo "  Data path:    $DATA_PATH"
echo "  Output path:  $OUTPUT_PATH"
echo "  Show rows:    $SHOW_ROWS"
echo "  Save format:  $SAVE_FORMAT"
echo ""

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}✗ Python 3 is not installed${NC}"
    echo "Please install Python 3.8 or higher"
    exit 1
fi

echo -e "${GREEN}✓ Python 3 found:${NC} $(python3 --version)"

# Check if Java is installed (required for PySpark)
if ! command -v java &> /dev/null; then
    echo -e "${RED}✗ Java is not installed${NC}"
    echo "Please install Java 11 or higher"
    echo "Download from: https://adoptium.net/"
    exit 1
fi

echo -e "${GREEN}✓ Java found:${NC} $(java -version 2>&1 | head -n 1)"

# Check if PySpark is installed
if ! python3 -c "import pyspark" 2>/dev/null; then
    echo -e "${YELLOW}⚠ PySpark not found. Installing dependencies...${NC}"
    pip install pyspark==4.0.1 requests
fi

echo -e "${GREEN}✓ PySpark installed${NC}"

# Check if data directory exists
if [ ! -d "$DATA_PATH" ]; then
    echo -e "${RED}✗ Data directory not found: $DATA_PATH${NC}"
    echo ""
    echo "Please create the data directory and add your Yelp data files:"
    echo "  mkdir -p $DATA_PATH"
    echo "  cp business.json $DATA_PATH/"
    echo "  cp review.json $DATA_PATH/"
    exit 1
fi

echo -e "${GREEN}✓ Data directory found${NC}"

# Check if required data files exist
MISSING_FILES=0
for file in business.json review.json; do
    if [ ! -f "$DATA_PATH/$file" ]; then
        echo -e "${RED}✗ Missing data file: $DATA_PATH/$file${NC}"
        MISSING_FILES=1
    else
        SIZE=$(du -h "$DATA_PATH/$file" | cut -f1)
        echo -e "${GREEN}✓ Found $file${NC} (Size: $SIZE)"
    fi
done

if [ $MISSING_FILES -eq 1 ]; then
    echo ""
    echo "Please ensure the following files exist in $DATA_PATH:"
    echo "  - business.json"
    echo "  - review.json"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_PATH"

echo ""
echo "================================================================================"
echo "                         RUNNING ANALYSIS PIPELINE"
echo "================================================================================"
echo ""

# Run the pipeline
python3 batch_main.py \
    --data-path "$DATA_PATH" \
    --output-path "$OUTPUT_PATH" \
    --show-rows "$SHOW_ROWS" \
    --save-format "$SAVE_FORMAT" \
    $SKIP_SAVE

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo -e "${GREEN}================================================================================${NC}"
    echo -e "${GREEN}                    ✓ PIPELINE COMPLETED SUCCESSFULLY${NC}"
    echo -e "${GREEN}================================================================================${NC}"
    echo ""
    if [ -z "$SKIP_SAVE" ]; then
        echo "Results saved to: $OUTPUT_PATH"
        echo ""
        echo "To view results:"
        echo "  ls -lh $OUTPUT_PATH"
        echo ""
    fi
    echo "Spark UI was available at: http://localhost:4040 (during execution)"
    echo ""
else
    echo ""
    echo -e "${RED}================================================================================${NC}"
    echo -e "${RED}                        ✗ PIPELINE FAILED${NC}"
    echo -e "${RED}================================================================================${NC}"
    echo ""
    echo "Please check the error messages above and ensure:"
    echo "  1. Data files are in correct format (JSON)"
    echo "  2. Data files are not corrupted"
    echo "  3. You have enough memory (recommended: 8GB+ RAM)"
    echo ""
fi

exit $EXIT_CODE
