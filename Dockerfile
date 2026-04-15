FROM golang:1.25-alpine AS builder

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/majordomo-steward ./cmd/majordomo-steward

FROM alpine:3.19

WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /app/majordomo-steward /app/majordomo-steward
COPY pricing.json /app/pricing.json
COPY model_aliases.json /app/model_aliases.json
COPY migrations/ /app/migrations/

RUN adduser -D -u 1000 majordomo
USER majordomo

EXPOSE 7680

ENTRYPOINT ["/app/majordomo-steward"]
