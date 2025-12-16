#!/bin/bash

###############################################################################
# Yelp Big Data Analysis - Kibana Integration Setup Script
# Automates deployment of Elasticsearch + Kibana + Batch Analytics
###############################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
ES_HOST="localhost"
ES_PORT=9200
KIBANA_PORT=5601
DATA_PATH="./data"

###############################################################################
# Helper Functions
###############################################################################

print_header() {
    echo -e "${BLUE}"
    echo "================================================================================"
    echo "  $1"
    echo "================================================================================"
    echo -e "${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

check_command() {
    if ! command -v $1 &> /dev/null; then
        print_error "$1 is not installed. Please install it first."
        exit 1
    fi
}

wait_for_service() {
    local url=$1
    local service_name=$2
    local max_attempts=30
    local attempt=1

    print_info "Waiting for $service_name to start..."

    while [ $attempt -le $max_attempts ]; do
        if curl -sf "$url" > /dev/null 2>&1; then
            print_success "$service_name is ready!"
            return 0
        fi
        echo -n "."
        sleep 2
        ((attempt++))
    done

    print_error "$service_name failed to start after $max_attempts attempts"
    return 1
}

###############################################################################
# Main Functions
###############################################################################

check_prerequisites() {
    print_header "Checking Prerequisites"

    check_command "docker"
    check_command "docker-compose"
    check_command "python3"
    check_command "curl"

    # Check Python packages
    if ! python3 -c "import pyspark" 2>/dev/null; then
        print_warning "PySpark not installed. Installing..."
        pip3 install pyspark
    fi

    print_success "All prerequisites satisfied"
}

start_services() {
    print_header "Starting Elasticsearch + Kibana"

    if [ ! -f "docker-compose-kibana.yml" ]; then
        print_error "docker-compose-kibana.yml not found"
        exit 1
    fi

    # Stop existing services
    print_info "Stopping any existing services..."
    docker-compose -f docker-compose-kibana.yml down > /dev/null 2>&1 || true

    # Start services
    print_info "Starting Docker services..."
    docker-compose -f docker-compose-kibana.yml up -d

    # Wait for services
    wait_for_service "http://$ES_HOST:$ES_PORT" "Elasticsearch"
    wait_for_service "http://$ES_HOST:$KIBANA_PORT/api/status" "Kibana"

    print_success "All services started successfully"
}

initialize_indices() {
    print_header "Initializing Elasticsearch Indices"

    cd Spark_Batch

    print_info "Creating indices with mappings..."
    python3 -c "from save_elasticsearch import initialize_all_indices; initialize_all_indices('$ES_HOST', $ES_PORT)"

    cd ..

    print_success "Indices initialized"
}

run_pipeline() {
    print_header "Running Batch Analytics Pipeline"

    if [ ! -d "$DATA_PATH" ]; then
        print_error "Data directory not found: $DATA_PATH"
        print_info "Please ensure data files exist at: $DATA_PATH"
        exit 1
    fi

    cd Spark_Batch

    print_info "Running pipeline (this may take 5-10 minutes)..."
    python3 batch_main_elasticsearch.py \
        --data-path "$DATA_PATH" \
        --es-host "$ES_HOST" \
        --es-port $ES_PORT

    cd ..

    print_success "Pipeline completed"
}

verify_deployment() {
    print_header "Verifying Deployment"

    # Check Elasticsearch indices
    print_info "Checking Elasticsearch indices..."
    indices=$(curl -s "http://$ES_HOST:$ES_PORT/_cat/indices?v" | grep "yelp-analysis" | wc -l)
    print_success "Found $indices indices"

    # Check document counts
    print_info "Document counts per index:"
    for i in {1..9}; do
        case $i in
            1) index="yelp-analysis-1-top-selling" ;;
            2) index="yelp-analysis-2-user-patterns" ;;
            3) index="yelp-analysis-3-top-users" ;;
            4) index="yelp-analysis-4-category-trends" ;;
            5) index="yelp-analysis-5-high-rating-low-review" ;;
            6) index="yelp-analysis-6-geographic" ;;
            7) index="yelp-analysis-7-seasonal" ;;
            8) index="yelp-analysis-8-trending" ;;
            9) index="yelp-analysis-9-performance-matrix" ;;
        esac

        count=$(curl -s "http://$ES_HOST:$ES_PORT/$index/_count" | python3 -c "import sys, json; print(json.load(sys.stdin)['count'])" 2>/dev/null || echo "0")
        echo "  Analysis $i: $count documents"
    done

    print_success "Verification complete"
}

show_summary() {
    print_header "Setup Complete!"

    echo ""
    echo -e "${GREEN}✓ Elasticsearch:${NC} http://$ES_HOST:$ES_PORT"
    echo -e "${GREEN}✓ Kibana:${NC} http://$ES_HOST:$KIBANA_PORT"
    echo ""
    echo -e "${BLUE}Next Steps:${NC}"
    echo "  1. Open Kibana: http://localhost:$KIBANA_PORT"
    echo "  2. Create Data Views (Index Patterns) for 9 analyses"
    echo "  3. Build visualizations and dashboards"
    echo ""
    echo -e "${BLUE}Documentation:${NC}"
    echo "  - Kibana Setup Guide: kibana_dashboards/KIBANA_SETUP_GUIDE.md"
    echo "  - Integration README: KIBANA_INTEGRATION_README.md"
    echo "  - Code Structure: Spark_Batch/PROJECT_STRUCTURE.md"
    echo ""
    echo -e "${BLUE}Useful Commands:${NC}"
    echo "  # View logs"
    echo "  docker-compose -f docker-compose-kibana.yml logs -f"
    echo ""
    echo "  # Stop services"
    echo "  docker-compose -f docker-compose-kibana.yml down"
    echo ""
    echo "  # Re-run pipeline"
    echo "  cd Spark_Batch && python3 batch_main_elasticsearch.py --data-path $DATA_PATH"
    echo ""
}

clean_deployment() {
    print_header "Cleaning Deployment"

    print_warning "This will remove all Docker containers and volumes!"
    read -p "Are you sure? (yes/no): " confirm

    if [ "$confirm" = "yes" ]; then
        docker-compose -f docker-compose-kibana.yml down -v
        print_success "Deployment cleaned"
    else
        print_info "Cancelled"
    fi
}

###############################################################################
# Main Script
###############################################################################

show_help() {
    echo "Yelp Big Data Analysis - Kibana Integration Setup"
    echo ""
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Options:"
    echo "  start       Start Elasticsearch + Kibana services"
    echo "  init        Initialize Elasticsearch indices"
    echo "  run         Run batch analytics pipeline"
    echo "  verify      Verify deployment"
    echo "  full        Full setup (start + init + run + verify)"
    echo "  clean       Clean deployment (remove containers and volumes)"
    echo "  help        Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 full              # Complete setup from scratch"
    echo "  $0 start             # Only start services"
    echo "  $0 run               # Only run pipeline (services must be running)"
    echo ""
}

main() {
    case "${1:-help}" in
        start)
            check_prerequisites
            start_services
            ;;
        init)
            initialize_indices
            ;;
        run)
            run_pipeline
            ;;
        verify)
            verify_deployment
            ;;
        full)
            check_prerequisites
            start_services
            initialize_indices
            run_pipeline
            verify_deployment
            show_summary
            ;;
        clean)
            clean_deployment
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            print_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
