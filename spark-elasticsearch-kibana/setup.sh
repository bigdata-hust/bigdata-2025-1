#!/bin/bash
# Setup script cho Spark-Elasticsearch-Kibana Pipeline

echo "======================================================================"
echo "        SPARK-ELASTICSEARCH-KIBANA SETUP SCRIPT"
echo "======================================================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_info() {
    echo "ℹ $1"
}

# Step 1: Check Python
echo "Step 1: Kiểm tra Python..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
    print_success "Python $PYTHON_VERSION đã được cài đặt"
else
    print_error "Python3 chưa được cài đặt"
    echo "Hãy cài đặt Python 3.8+ từ: https://www.python.org/downloads/"
    exit 1
fi

# Step 2: Check pip
echo ""
echo "Step 2: Kiểm tra pip..."
if command -v pip3 &> /dev/null; then
    print_success "pip3 đã sẵn sàng"
else
    print_error "pip3 chưa được cài đặt"
    exit 1
fi

# Step 3: Check Docker
echo ""
echo "Step 3: Kiểm tra Docker..."
if command -v docker &> /dev/null; then
    DOCKER_VERSION=$(docker --version | cut -d' ' -f3 | cut -d',' -f1)
    print_success "Docker $DOCKER_VERSION đã được cài đặt"
else
    print_error "Docker chưa được cài đặt"
    echo "Hãy cài đặt Docker từ: https://docs.docker.com/get-docker/"
    exit 1
fi

# Step 4: Check Docker Compose
echo ""
echo "Step 4: Kiểm tra Docker Compose..."
if command -v docker-compose &> /dev/null; then
    COMPOSE_VERSION=$(docker-compose --version | cut -d' ' -f3 | cut -d',' -f1)
    print_success "Docker Compose $COMPOSE_VERSION đã được cài đặt"
else
    print_error "Docker Compose chưa được cài đặt"
    echo "Hãy cài đặt Docker Compose từ: https://docs.docker.com/compose/install/"
    exit 1
fi

# Step 5: Check data directory
echo ""
echo "Step 5: Kiểm tra thư mục dữ liệu..."
if [ -d "../processed_data" ]; then
    print_success "Thư mục processed_data tồn tại"
    
    # Check for CSV files
    if [ -f "../processed_data/business.csv" ]; then
        print_success "business.csv tìm thấy"
    else
        print_warning "business.csv không tìm thấy"
    fi
    
    if [ -f "../processed_data/review_combined_1.csv" ]; then
        print_success "review_combined_1.csv tìm thấy"
    else
        print_warning "review_combined_1.csv không tìm thấy"
    fi
    
    if [ -f "../processed_data/user.csv" ]; then
        print_success "user.csv tìm thấy"
    else
        print_warning "user.csv không tìm thấy (optional)"
    fi
else
    print_error "Thư mục ../processed_data không tồn tại"
    echo ""
    print_info "Cấu trúc thư mục yêu cầu:"
    echo "  your-project/"
    echo "  ├── processed_data/"
    echo "  │   ├── business.csv"
    echo "  │   ├── user.csv"
    echo "  │   └── review_combined_1.csv"
    echo "  └── spark-elasticsearch-kibana/"
    echo ""
    exit 1
fi

# Step 6: Create virtual environment (optional)
echo ""
echo "Step 6: Tạo virtual environment..."
read -p "Bạn có muốn tạo virtual environment? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    if [ ! -d "venv" ]; then
        python3 -m venv venv
        print_success "Virtual environment đã được tạo"
        
        # Activate virtual environment
        source venv/bin/activate 2>/dev/null || . venv/Scripts/activate 2>/dev/null
        print_success "Virtual environment đã được kích hoạt"
    else
        print_info "Virtual environment đã tồn tại"
    fi
fi

# Step 7: Install Python packages
echo ""
echo "Step 7: Cài đặt Python packages..."
echo "Đang cài đặt dependencies từ requirements.txt..."
pip3 install -r requirements.txt
if [ $? -eq 0 ]; then
    print_success "Python packages đã được cài đặt"
else
    print_error "Lỗi khi cài đặt Python packages"
    exit 1
fi

# Step 8: Setup environment variables
echo ""
echo "Step 8: Setup environment variables..."
if [ ! -f ".env" ]; then
    cp env.example .env
    print_success "File .env đã được tạo từ env.example"
    print_info "Bạn có thể edit .env để tùy chỉnh cấu hình"
else
    print_info "File .env đã tồn tại"
fi

# Step 9: Start Elasticsearch & Kibana
echo ""
echo "Step 9: Khởi động Elasticsearch & Kibana..."
read -p "Bạn có muốn khởi động Docker containers ngay? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Đang khởi động Docker containers..."
    docker-compose up -d
    
    if [ $? -eq 0 ]; then
        print_success "Docker containers đã được khởi động"
        echo ""
        print_info "Đang đợi services khởi động (30 giây)..."
        sleep 30
        
        # Check Elasticsearch
        if curl -s http://localhost:9200 > /dev/null; then
            print_success "Elasticsearch đang chạy tại http://localhost:9200"
        else
            print_warning "Elasticsearch chưa sẵn sàng, hãy đợi thêm"
        fi
        
        # Check Kibana
        if curl -s http://localhost:5601 > /dev/null; then
            print_success "Kibana đang chạy tại http://localhost:5601"
        else
            print_warning "Kibana chưa sẵn sàng, hãy đợi thêm"
        fi
    else
        print_error "Lỗi khi khởi động Docker containers"
        exit 1
    fi
fi

# Final summary
echo ""
echo "======================================================================"
echo "                    SETUP HOÀN TẤT!"
echo "======================================================================"
echo ""
print_success "Môi trường đã được setup thành công!"
echo ""
echo "Bước tiếp theo:"
echo "  1. Đảm bảo Elasticsearch & Kibana đang chạy:"
echo "     $ docker-compose ps"
echo ""
echo "  2. Chạy pipeline:"
echo "     $ python main.py"
echo ""
echo "  3. Mở Kibana trong browser:"
echo "     http://localhost:5601"
echo ""
echo "Xem README.md để biết thêm chi tiết."
echo ""
echo "======================================================================"
