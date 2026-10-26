FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /upload-server . && mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /upload-server /upload-server
COPY --from=build --chown=65532:65532 /data /data
ENV DATA_DIR=/data GOMEMLIMIT=512MiB GOMAXPROCS=2
EXPOSE 8090
ENTRYPOINT ["/upload-server"]
