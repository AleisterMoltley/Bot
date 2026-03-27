# =============================================================================
# PolyBot Dockerfile - Multi-stage build for Railway Deployment
# =============================================================================
# Version: 26 (2026-03-18)
# Features:
#   - Multi-stage build for smaller final image (~200MB savings)
#   - OCI standard labels for container metadata
#   - Optimized layer caching (dependencies before code)
#   - Non-root user with minimal privileges
#   - Health check with startup grace period
#   - Pure FastAPI server (no Telegram dependency)
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Builder - Install dependencies and build the package
# -----------------------------------------------------------------------------
FROM python:3.11-slim AS builder

# Build-time environment
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# Install build dependencies (gcc for native extensions)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first (better layer caching)
COPY requirements.txt pyproject.toml ./
COPY src/ ./src/

# Install dependencies and the package
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir .

# -----------------------------------------------------------------------------
# Stage 2: Runtime - Minimal image with only runtime dependencies
# -----------------------------------------------------------------------------
FROM python:3.11-slim AS runtime

# OCI Standard Labels (https://github.com/opencontainers/image-spec/blob/main/annotations.md)
LABEL org.opencontainers.image.title="PolyBot" \
      org.opencontainers.image.description="Polymarket Trading Bot - Pure FastAPI backend" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.vendor="PolyBot" \
      org.opencontainers.image.source="https://github.com/AleisterMoltley/PolyBot" \
      org.opencontainers.image.licenses="MIT"

# Runtime environment
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # Structured logging in production
    LOG_FORMAT=json \
    # Python optimizations
    PYTHONOPTIMIZE=1 \
    # Default port for FastAPI
    PORT=8000 \
    # Ensure /app is on the Python path so src.polybot is importable
    PYTHONPATH=/app

WORKDIR /app

# Copy installed packages from builder stage
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY --from=builder /build/src ./src
COPY --from=builder /build/pyproject.toml ./

# Copy static assets (dashboard.html and other static files served by FastAPI)
COPY static/ ./static/

# Create non-root user with minimal privileges
RUN useradd --create-home --shell /bin/false --uid 1000 polybot && \
    chown -R polybot:polybot /app && \
    # Create data directory for SQLite and configs
    mkdir -p /app/data && \
    chown polybot:polybot /app/data

# Switch to non-root user
USER polybot

# Expose the FastAPI port
EXPOSE 8000

# Default command: Run FastAPI server
# NOTE: Railway's startCommand in railway.json may override this
CMD ["uvicorn", "src.polybot.main_fastapi:app", "--host", "0.0.0.0", "--port", "8000"]
