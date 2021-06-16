FROM debian:10-slim

EXPOSE 50051
RUN apt-get update && apt-get install curl -y
RUN curl -L -o /hub https://github.com/lbryio/hub/releases/download/v0.2021.06.14-beta/hub && chmod +x /hub
ENTRYPOINT ["/hub", "serve", "--dev"]
