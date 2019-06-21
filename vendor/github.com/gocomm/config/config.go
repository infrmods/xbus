package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type Unmarshaler interface {
	Unmarshal(data []byte, v interface{}) error
}

type UnmarshalFunc func(data []byte, v interface{}) error

func (f UnmarshalFunc) Unmarshal(data []byte, v interface{}) error {
	return f(data, v)
}

func Load(data []byte, v interface{}, unmarshaler Unmarshaler) error {
	if err := DefaultConfig(v); err != nil {
		return err
	}
	return unmarshaler.Unmarshal(data, v)
}

func LoadF(data []byte, v interface{}, unmarshaler UnmarshalFunc) error {
	return Load(data, v, unmarshaler)
}

func LoadJson(data []byte, v interface{}) error {
	return LoadF(data, v, json.Unmarshal)
}

func LoadFromFile(path string, v interface{}, unmarshaler Unmarshaler) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	return Load(data, v, unmarshaler)
}

func LoadFromFileF(path string, v interface{}, unmarshaler UnmarshalFunc) error {
	return LoadFromFile(path, v, unmarshaler)
}

func LoadJsonFromFile(path string, v interface{}) error {
	return LoadFromFileF(path, v, json.Unmarshal)
}
