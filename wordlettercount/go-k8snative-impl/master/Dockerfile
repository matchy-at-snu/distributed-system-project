FROM golang:1.15-alpine as builder
LABEL stage=builder
WORKDIR /usr/src/app
COPY ./go.mod ./
COPY ./go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o master-wordcount

FROM google/cloud-sdk:alpine as runner
COPY --from=builder /usr/src/app/master-wordcount /opt/master-wordcount
EXPOSE 8080
ENTRYPOINT ["/opt/master-wordcount"]