package upstream

type PoolConnectionError struct {
	Err string
}

func (e PoolConnectionError) Error() string {
	return e.Err
}
