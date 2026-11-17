# Single image for the Boss and all four Workers; the Compose command chooses the role.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app:/app/proto

WORKDIR /app

# Install pinned versions compatible with the checked-in generated gRPC modules.
COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY boss/ ./boss/
COPY worker/ ./worker/
COPY proto/ ./proto/

EXPOSE 50050 50051
