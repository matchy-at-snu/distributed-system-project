FROM golang:1.15-alpine as builder
LABEL stage=builder
WORKDIR /usr/src/app
COPY ./go.mod ./
COPY ./go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o reducer

FROM scratch as runner
COPY --from=builder /usr/src/app/reducer /opt/reducer
EXPOSE 8080
CMD ["/opt/reducer"]