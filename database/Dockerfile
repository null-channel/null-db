FROM rust:1.74.1 as builder
#! /bin/bash
RUN apt update
RUN apt install -y protobuf-compiler
WORKDIR /project
COPY . .
RUN cargo build

FROM rust:1.74.1
COPY --from=builder /project/target/debug/ /usr/local/bin/
WORKDIR /db/
CMD ["null-db"]

