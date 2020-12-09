FROM golang:1.15-alpine as builder
WORKDIR /usr/src/app
COPY ./go.mod ./
COPY ./go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o master-lettercount

FROM google/cloud-sdk:alpine as runner
COPY --from=builder /usr/src/app/master-lettercount /opt/master-lettercount
EXPOSE 8080
ENTRYPOINT ["/opt/master-lettercount"]