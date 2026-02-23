package stream

type ParseError struct {
	Err error
}

func (e ParseError) Error() string {
	return e.Err.Error()
}
