FROM golang:1.8

EXPOSE 3000

WORKDIR /go/src/app
COPY . .

RUN go-wrapper download
RUN go-wrapper install

CMD ["go-wrapper", "run"] # ["app"]