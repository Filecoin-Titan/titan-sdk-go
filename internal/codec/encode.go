package codec

import (
	"bytes"
	"encoding/gob"
)

func Encode(input interface{}) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(input)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func Decode(data []byte, out interface{}) error {
	buffer := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buffer)
	err := dec.Decode(out)
	if err != nil {
		return err
	}
	return nil
}
