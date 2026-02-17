package httputils

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"iter"
	"net"
	"net/http"
	"strconv"
	"strings"
)

type ParseError struct {
	Err error
}

func (e ParseError) Error() string {
	return e.Err.Error()
}

// Reader читает HTTP-сообщения из bufio.Reader, валидируя их и возвращая его по частям через итератор.
type Reader struct {
	reader    *bufio.Reader
	validator Validator
}

// Валидирует структуру HTTP
type Validator interface {
	ValidateStartLine(startLine []byte) error
}
type RequestValidator struct{}
type ResponseValidator struct{}

func (v RequestValidator) ValidateStartLine(startLine []byte) error {
	splitedLine := bytes.Split(startLine[:len(startLine)-2], []byte(" "))
	if len(splitedLine) < 3 {
		return ParseError{Err: errors.New("Not enough params")}
	}
	method, path, version := splitedLine[0], splitedLine[1], splitedLine[2]

	switch string(method) {
	case http.MethodGet,
		http.MethodPost,
		http.MethodPut,
		http.MethodPatch,
		http.MethodDelete,
		http.MethodConnect,
		http.MethodHead,
		http.MethodOptions,
		http.MethodTrace:
	default:
		return ParseError{Err: fmt.Errorf("Wrong method: %s", method)}
	}
	// # logger.info(f"Getting request. {method} {path} {version}")

	if len(path) == 0 {
		return ParseError{Err: errors.New("Empty path")}
	}
	if !bytes.HasPrefix(version, []byte("HTTP/")) && string(version) < "HTTP/1.1" {
		return ParseError{Err: fmt.Errorf("Invalid version: %s", version)}
	}
	return nil
}
func (v ResponseValidator) ValidateStartLine(startLine []byte) error {
	splitedLine := bytes.Split(startLine[:len(startLine)-2], []byte(" "))
	if len(splitedLine) < 3 {
		return ParseError{Err: errors.New("Not enough params")}
	}
	version, status, _ := splitedLine[0], splitedLine[1], splitedLine[2]
	if _, err := strconv.Atoi(string(status)); err != nil {
		return ParseError{Err: fmt.Errorf("Wrong status code: %s", status)}
	}
	if !bytes.HasPrefix(version, []byte("HTTP/")) && string(version) < "HTTP/1.1" {
		return ParseError{Err: fmt.Errorf("Invalid version: %s", version)}
	}
	return nil

}

type Chunk struct {
	Chunk           []byte
	IsMessageStart bool
	IsMessageEnd   bool
}

func (r Reader) All() iter.Seq2[Chunk, error] {
	return func(yield func(Chunk, error) bool) {
		for {
			// Читаем стартовую строку
			startLine, err := r.getStartLine()
			if err != nil {
				yield(Chunk{}, err)
				return
			}
			err = r.validator.ValidateStartLine(startLine)
			if err != nil {
				yield(Chunk{}, err)
				return
			}
			chunk := Chunk{
				Chunk:           startLine,
				IsMessageStart: true,
				IsMessageEnd:   false,
			}
			if !yield(chunk, nil) {
				return
			}

			// Читаем заголовки
			headers, err := r.getHeaders()
			if err != nil {
				yield(Chunk{}, err)
				return
			}
			chunk = Chunk{
				Chunk:           headers,
				IsMessageStart: false,
				IsMessageEnd:   false,
			}
			if !yield(chunk, nil) {
				return
			}

			// Читаем тело через итератор
			parsedHeaders := r.getParsedHeaders(headers)
			for chunk, err := range r.getBodyIterator(parsedHeaders) {
				if !yield(chunk, err) {
					return
				}
			}
		}

	}
}

func (r Reader) getStartLine() ([]byte, error) {
	return r.readUntil([]byte("\r\n"))
}

func (r Reader) getHeaders() ([]byte, error) {
	return r.readUntil([]byte("\r\n\r\n"))
}

func (r Reader) getBodyIterator(headers map[string][]byte) iter.Seq2[Chunk, error] {
	return func(yield func(Chunk, error) bool) {
		contentLength := 0
		if contentLengthBytes, ok := headers["content-length"]; ok {
			contentLength, _ = strconv.Atoi(string(contentLengthBytes))
		}
		if contentLength == 0 {
			chunk := Chunk{
				Chunk:           []byte{},
				IsMessageStart: false,
				IsMessageEnd:   true,
			}
			yield(chunk, nil)
			return
		}
		chunkSize := 512
		bytesRead := 0

		for bytesRead < contentLength {
			toRead := min(chunkSize, contentLength-bytesRead)
			isMessageEnd := contentLength-bytesRead <= chunkSize
			buf := make([]byte, toRead)
			_, err := io.ReadFull(r.reader, buf)
			if err != nil {
				yield(Chunk{}, err)
				return
			}
			chunk := Chunk{
				Chunk:           buf,
				IsMessageStart: false,
				IsMessageEnd:   isMessageEnd,
			}
			if !yield(chunk, nil) {
				return
			}
			bytesRead += toRead
		}

	}
}

func (r Reader) readUntil(delim []byte) ([]byte, error) {
	var buf []byte

	for {
		chunk, err := r.reader.ReadBytes(delim[len(delim)-1])
		buf = append(buf, chunk...)

		if err != nil {
			return buf, err
		}

		if bytes.Contains(buf, delim) {
			return buf, nil
		}
	}
}

func (r Reader) getParsedHeaders(rawHeaders []byte) map[string][]byte {
	headers := make(map[string][]byte)

	for _, headerLine := range bytes.Split(rawHeaders[:len(rawHeaders)-4], []byte("\r\n")) {
		if len(headerLine) == 0 {
			continue
		}
		parts := bytes.SplitN(headerLine, []byte(":"), 2)
		if len(parts) != 2 {
			continue
		}
		name := string(bytes.TrimSpace(parts[0]))
		value := bytes.TrimSpace(parts[1])
		headers[strings.ToLower(name)] = value
	}

	return headers
}

func NewRequestReader(conn net.Conn) Reader {
    reader := bufio.NewReader(conn)
    return Reader{
        reader:    reader,
        validator: RequestValidator{},
    }
}
func NewResponseReader(conn net.Conn) Reader {
    reader := bufio.NewReader(conn)
    return Reader{
        reader:    reader,
        validator: ResponseValidator{},
    }
}