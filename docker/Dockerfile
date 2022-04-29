FROM golang:alpine as stage1

EXPOSE 50051
RUN mkdir /app
WORKDIR /app
COPY . /app
RUN go build
FROM alpine:latest
COPY --from=stage1 /app/hub /hub
ENTRYPOINT ["/hub", "serve"]
