# 1. Đổi base image từ 3.12 sang 3.8
FROM python:3.8-slim-bookworm 

# Cài đặt Java và procps
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Cấu hình Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Cấu hình Python cho PySpark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /app

# 2. CÀI ĐẶT THƯ VIỆN
# Đảm bảo file requirements.txt của bạn để pyspark==3.5.0
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy code
COPY . .

EXPOSE 4040 7078 7079

CMD ["python3", "main.py"]