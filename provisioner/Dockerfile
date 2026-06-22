FROM alpine:3.20
RUN apk add --no-cache bash curl jq
COPY bootstrap.sh /bootstrap.sh
RUN chmod +x /bootstrap.sh
ENTRYPOINT ["/bootstrap.sh"]
