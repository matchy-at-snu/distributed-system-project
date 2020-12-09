FROM golang:1.15-alpine as builder
WORKDIR /usr/src/app
COPY ./go.mod ./
COPY ./go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o mapper-wordcount

FROM scratch as runner
COPY --from=builder /usr/src/app/mapper-wordcount /opt/mapper-wordcount
EXPOSE 8080
CMD ["/opt/mapper-wordcount"]