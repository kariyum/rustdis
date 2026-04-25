# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.92.0
ARG APP_NAME=rustdis

FROM rust:${RUST_VERSION}-alpine AS build
ARG APP_NAME
WORKDIR /app

RUN apk add --no-cache clang lld musl-dev git

COPY . .
RUN cargo build --locked --release && \
    cp ./target/release/$APP_NAME /usr/local/bin/$APP_NAME

FROM alpine:3.18 AS final

RUN apk add --no-cache openjdk17-jre-headless bash graphviz git gnuplot

WORKDIR /maelstrom

COPY maelstrom/ .

COPY --from=build /usr/local/bin/rustdis /usr/local/bin/rustdis

RUN chmod +x maelstrom

VOLUME /maelstrom/store

ENTRYPOINT ["./maelstrom"]
