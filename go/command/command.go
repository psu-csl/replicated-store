package command

type Type int64

const (
	Get Type = iota
	Put
	Del
)

type Command struct {
	Key   string
	Value string
	Type  Type
}

type Result struct {
	Ok    bool
	Value string
}