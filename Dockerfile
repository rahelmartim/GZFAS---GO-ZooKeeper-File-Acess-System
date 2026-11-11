# Builds gzfas-server into a small image without a shell.
FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /gzfas-server ./cmd/gzfas-server
RUN mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /gzfas-server /gzfas-server
# The server runs as the nonroot user, so it must own the data folder.
COPY --from=build --chown=nonroot:nonroot /data /data
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/gzfas-server", "-addr", "0.0.0.0:8080", "-data", "/data"]
CMD ["-zk", "zookeeper:2181"]
