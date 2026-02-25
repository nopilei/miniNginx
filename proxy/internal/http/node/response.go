package node

import (
	"bytes"
	"strconv"
)


type HTTPResponse struct{
	version []byte
	status []byte
	reason []byte
	headers map [string] []byte
	body []byte
}

func (r HTTPResponse) Full() []byte {
	crlf := []byte("\r\n")
	requestLine := bytes.Join([][]byte{r.version, r.status, r.reason}, []byte(" "))
	headers := [][]byte{}
	for k, v := range r.headers {
		headers = append(headers, bytes.Join([][]byte{[]byte(k), v}, []byte(": ")))
	}
	rawHeaders := bytes.Join(headers, crlf)
	rawResponse := []byte{}
	rawResponse = append(requestLine, crlf...)
	rawResponse = append(rawResponse, rawHeaders...)
	rawResponse = append(rawResponse, []byte("\r\n\r\n")...)
	rawResponse = append(rawResponse, r.body...)

	return rawResponse
}

func GetErrorResponse(status int, reason string, body string) []byte {

	return HTTPResponse{
		version: []byte("HTTP/1.1"),
		status: []byte(strconv.Itoa(status)),
		reason: []byte(reason),
		body: []byte(body),
		headers: map[string][]byte{
			"Content-Length": []byte(strconv.Itoa(len(body))),
		},
	}.Full()
}