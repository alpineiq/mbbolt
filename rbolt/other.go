package rbolt

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[opGet-1]
	_ = x[opPut-2]
	_ = x[opDel-3]
	_ = x[opSeq-4]
	_ = x[opSetSeq-5]
	_ = x[opForEach-6]
}

type op uint8

const (
	opGet op = iota + 1
	opPut
	opDel
	opSeq
	opSetSeq
	opForEach
)

const _op_name = "GetPutDelSeqSetSeqForEach"

var _op_index = [...]uint8{0, 3, 6, 9, 12, 18, 25}

func (i op) String() string {
	i -= 1
	if i >= op(len(_op_index)-1) {
		return "op(" + strconv.FormatInt(int64(i+1), 10) + ")"
	}
	return _op_name[_op_index[i]:_op_index[i+1]]
}

type srvReq struct {
	Op     op     `json:"op"`
	Bucket string `json:"b"`
	Key    string `json:"k"`
	Value  any    `json:"v"`
}
