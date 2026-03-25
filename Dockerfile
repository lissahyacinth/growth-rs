FROM rustlang/rust:nightly AS build

WORKDIR /usr/src/growthrs
COPY . .
RUN cargo build --manifest-path growthrs/Cargo.toml --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/src/growthrs/growthrs/target/release/growthrs /usr/local/bin/
CMD ["growthrs"]
