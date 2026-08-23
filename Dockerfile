FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir . && pip install --no-cache-dir mcp || true

# run as unprivileged user; /data is the only writable volume
RUN useradd -m -u 10001 memd && mkdir -p /data && chown memd:memd /data
USER memd

ENV MEMD_DATA=/data \
    MEMD_HOST=0.0.0.0 \
    MEMD_PORT=8700

VOLUME ["/data"]
EXPOSE 8700

HEALTHCHECK --interval=30s --timeout=3s CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8700/health')"

ENTRYPOINT ["memd"]
CMD ["serve", "--http"]
