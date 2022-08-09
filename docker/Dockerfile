FROM golang:alpine as stage1

EXPOSE 50051
RUN mkdir /app
WORKDIR /app
COPY . /app
RUN go build -o herald .
FROM alpine:latest
COPY --from=stage1 /app/herald /herald
ENTRYPOINT ["/herald", "serve"]
