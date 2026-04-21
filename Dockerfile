FROM python:3.12

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bwa samtools awscli ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN bwa 2>&1 | head -3 || true
RUN samtools --version | head -1
RUN aws --version
