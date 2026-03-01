package stream

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"iter"
	"net"
	"strconv"
	"strings"
)

type Chunk struct {
	Chunk          []byte
	IsMessageStart bool
	IsMessageEnd   bool
}

// Reader читает HTTP-сообщения из bufio.Reader, валидируя их и возвращая его по частям через итератор.
type Reader struct {
	reader    *bufio.Reader
	validator Validator
}

func NewRequestReader(conn net.Conn) *Reader {
	reader := bufio.NewReader(conn)
	return &Reader{
		reader:    reader,
		validator: RequestValidator{},
	}
}
func NewResponseReader(conn net.Conn) *Reader {
	reader := bufio.NewReader(conn)
	return &Reader{
		reader:    reader,
		validator: ResponseValidator{},
	}
}

func (r *Reader) All(ctx context.Context) iter.Seq2[Chunk, error] {
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
				Chunk:          startLine,
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
				Chunk:          headers,
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

func (r *Reader) getStartLine() ([]byte, error) {
	return r.readUntil([]byte("\r\n"))
}

func (r *Reader) getHeaders() ([]byte, error) {
	return r.readUntil([]byte("\r\n\r\n"))
}

func (r *Reader) getBodyIterator(headers map[string][]byte) iter.Seq2[Chunk, error] {
	return func(yield func(Chunk, error) bool) {
		contentLength := 0
		if contentLengthBytes, ok := headers["content-length"]; ok {
			contentLength, _ = strconv.Atoi(string(contentLengthBytes))
		}
		if contentLength == 0 {
			chunk := Chunk{
				Chunk:          []byte{},
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
				Chunk:          buf,
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

func (r *Reader) readUntil(delim []byte) ([]byte, error) {
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

func (r *Reader) getParsedHeaders(rawHeaders []byte) map[string][]byte {
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
