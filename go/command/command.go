package command

type Type int

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