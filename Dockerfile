FROM golang:1.15.0-alpine3.12 as builder

COPY . /app/
WORKDIR /app
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o google-pubsub-stream .

FROM scratch
COPY --from=builder /app/google-pubsub-stream /google-pubsub-stream
ENTRYPOINT [ "/google-pubsub-stream" ]
