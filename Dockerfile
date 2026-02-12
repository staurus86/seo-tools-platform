# Multi-stage build for SEO Tools Platform
FROM python:3.11-bookworm as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /build

# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Production stage
FROM python:3.11-bookworm

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libxml2 \
    libxslt1.1 \
    redis-tools \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Copy Python packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application files
COPY app/ /app/app/
COPY *.py /app/
COPY *.sh /app/
COPY *.txt /app/
COPY *.toml /app/
COPY *.md /app/

# Make entrypoint executable and create reports dir
RUN chmod +x entrypoint.sh && mkdir -p reports_output

# Debug: list files
RUN echo "=== /app contents ===" && ls -la /app && echo "=== /app/app contents ===" && ls -la /app/app/

# Expose port
EXPOSE 8000

# Run
CMD ["./entrypoint.sh"]
