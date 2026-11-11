FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MCS_CONFIG=/config/config.yaml \
    MCS_DATABASE__SQLITE_PATH=/data/meshcore.db

WORKDIR /app

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install ".[mysql]" \
    && groupadd --gid 1000 meshcore \
    && useradd --uid 1000 --gid 1000 --home /data --shell /usr/sbin/nologin meshcore \
    && mkdir -p /data /config \
    && chown meshcore:meshcore /data

USER meshcore
VOLUME ["/data"]
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=4).status == 200 else 1)"

ENTRYPOINT ["meshcore-server"]
CMD ["run"]
