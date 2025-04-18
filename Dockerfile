# syntax=docker/dockerfile:1

FROM ubuntu:20.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && \
    apt install -y --no-install-recommends \
    curl \
    ca-certificates \
    git \
    make \
    gcc \
    libc6-dev \
    libsqlite3-dev \
    libpcre3-dev && \
    rm -rf /var/lib/apt/lists/*

RUN curl -sL https://golang.org/dl/go1.23.8.linux-amd64.tar.gz | tar -xz -C /usr/local
RUN curl -sL https://github.com/goreleaser/goreleaser/releases/download/v2.8.2/goreleaser_Linux_x86_64.tar.gz | tar -xz -C /usr/local/bin
ENV PATH="/usr/local/go/bin:$PATH"

# Build the package
WORKDIR /app
COPY . .
ARG SNAPSHOT=true
RUN if [ "$SNAPSHOT" = "true" ]; then \
    goreleaser release --snapshot --clean; \
    else \
    goreleaser release --clean; \
    fi


FROM scratch AS final
COPY --from=builder /app/dist /
