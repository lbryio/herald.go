FROM debian:10-slim

EXPOSE 50051
COPY ./hub /hub
ENTRYPOINT ["/hub", "serve", "--dev"]
