FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml LICENSE ./
COPY src ./src
# Shell precedence matters here: `A && B || true` is `(A && B) || true`, so
# the trailing `|| true` swallowed a failure of the CORE install too - the
# image built successfully with memd itself absent and failed at runtime.
# Only the OPTIONAL extra is allowed to fail.
RUN pip install --no-cache-dir . \
 && python -c "import memd; print('memd', memd.__file__)" \
 && { pip install --no-cache-dir mcp || echo "optional extra 'mcp' unavailable; continuing"; }

# Hosted billing (memd serve --http --hosted) needs the Stripe SDK, ~26 MB
# installed. The default image leaves it out: self-hosted memd never uses
# it. Build the hosted variant with:
#   docker build --build-arg MEMD_BILLING=1 -t memd/memd:hosted .
# Unlike mcp above, a requested billing install must NOT fail silently.
ARG MEMD_BILLING=0
RUN if [ "$MEMD_BILLING" = "1" ]; then \
      pip install --no-cache-dir ".[billing]" \
      && python -c "import stripe, memd.hosted.billing; print('stripe', stripe.VERSION)"; \
    fi

# Object storage + AWS KMS (s3:// data roots, the aws-kms key provider,
# multi-node serving - see docker-compose.yml): boto3, ~90 MB installed.
#   docker build --build-arg MEMD_S3=1 -t memd/memd:cluster .
ARG MEMD_S3=0
RUN if [ "$MEMD_S3" = "1" ]; then \
      pip install --no-cache-dir ".[s3]" \
      && python -c "import boto3, memd.storage.s3store; print('boto3', boto3.__version__)"; \
    fi

# run as unprivileged user; /data (node-local) and /state (shared by the
# nodes of a cluster: API keys, hosted admin db) are the writable volumes
RUN useradd -m -u 10001 memd && mkdir -p /data /state && chown memd:memd /data /state
USER memd

ENV MEMD_DATA=/data \
    MEMD_HOST=0.0.0.0 \
    MEMD_PORT=8700

VOLUME ["/data"]
EXPOSE 8700

HEALTHCHECK --interval=30s --timeout=3s CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8700/health')"

ENTRYPOINT ["memd"]
CMD ["serve", "--http"]
