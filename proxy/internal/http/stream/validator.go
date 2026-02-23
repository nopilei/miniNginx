package stream

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"strconv"
)

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
