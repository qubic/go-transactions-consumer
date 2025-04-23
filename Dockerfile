FROM golang:1.24 AS builder
ENV CGO_ENABLED=0

WORKDIR /src/go-transactions-consumer
COPY . /src/go-transactions-consumer

RUN go mod tidy
WORKDIR /src/go-transactions-consumer
RUN go build

FROM alpine:latest
LABEL authors="mio@qubic.org"

# copy executable from build stage
COPY --from=builder /src/go-transactions-consumer/go-transactions-consumer /app/go-transactions-consumer

RUN chmod +x /app/go-transactions-consumer

WORKDIR /app

ENTRYPOINT ["./go-transactions-consumer"]