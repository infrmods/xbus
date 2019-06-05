FROM golang
WORKDIR $GOPATH/src/github.com/infrmods/xbus
COPY . .
RUN CGO_ENABLED=0 go build
RUN cp xbus /xbus

FROM alpine
COPY --from=0 /xbus /usr/bin/xbus
WORKDIR /xbus
CMD ["/usr/bin/xbus", "-config", "/xbus/config.yaml", "run"]
