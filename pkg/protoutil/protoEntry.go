package protoutil

import (
	"encoding/binary"
	"io"
)

type ProtoEntry struct {
	Data    []byte
	DataLen uint32
}

func (p ProtoEntry) MarshalBinary() ([]byte, error) {

	buf := make([]byte, p.DataLen+4)
	binary.LittleEndian.PutUint32(buf[0:4], p.DataLen)
	buf = append(buf[4:], p.Data...)

	return buf, nil
}

func (p *ProtoEntry) ReadFrom(r io.Reader) (int64, error) {

	buf := make([]byte, 4)
	n := 0
	var err error
	if n, err = r.Read(buf); err != nil {
		return int64(n), err
	}

	p.DataLen = binary.LittleEndian.Uint32(buf)
	p.Data = make([]byte, p.DataLen)

	n2, err := r.Read(p.Data)
	if err != nil {
		return int64(n + n2), err
	}

	return int64(n + n2), nil
}
