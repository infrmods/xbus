package dbutil

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode"
)

func ToSnake(in string) string {
	runes := []rune(in)
	length := len(runes)

	var out []rune
	for i := 0; i < length; i++ {
		if i > 0 && unicode.IsUpper(runes[i]) &&
			((i+1 < length && unicode.IsLower(runes[i+1])) || unicode.IsLower(runes[i-1])) {
			out = append(out, '_')
		}
		out = append(out, unicode.ToLower(runes[i]))
	}

	return string(out)
}

type TableMeta struct {
	Type        reflect.Type
	fieldValues map[string]func(reflect.Value) reflect.Value
}

func (m *TableMeta) GetScanFields(names []string, v interface{}) (fields []interface{}, err error) {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		if val.Type().Elem() != m.Type {
			return nil, fmt.Errorf("mismatched type: %v, meta type: %v", val.Type(), m.Type)
		}
		val = val.Elem()
	} else {
		return nil, fmt.Errorf("mismatched type: %v, meta type: %v", val.Type(), m.Type)
	}

	for _, name := range names {
		f := m.fieldValues[name]
		if f == nil {
			fields = append(fields, new(interface{}))
		} else {
			fields = append(fields, f(val).Addr().Interface())
		}
	}
	return
}

type PreparedScan struct {
	typ reflect.Type
	fs  []func(reflect.Value) reflect.Value
}

func (p *PreparedScan) GetScanFields(v interface{}) (fields []interface{}, err error) {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		if val.Type().Elem() != p.typ {
			return nil, fmt.Errorf("mismatched type: %v, meta type: %v", val.Type(), p.typ)
		}
		val = val.Elem()
	} else {
		return nil, fmt.Errorf("mismatched type: %v, meta type: %v", val.Type(), p.typ)
	}

	for _, f := range p.fs {
		if f == nil {
			fields = append(fields, new(interface{}))
		} else {
			fields = append(fields, f(val).Addr().Interface())
		}
	}
	return
}

func (m *TableMeta) PrepareScan(names []string) (*PreparedScan, error) {
	p := &PreparedScan{typ: m.Type, fs: make([]func(reflect.Value) reflect.Value, 0)}
	for _, name := range names {
		f := m.fieldValues[name]
		if f == nil {
			p.fs = append(p.fs, nil)
		} else {
			p.fs = append(p.fs, f)
		}
	}
	return p, nil
}

var tableLk = &sync.RWMutex{}
var tableMetas = make(map[reflect.Type]*TableMeta)

func getTableFields(typ reflect.Type, preindex []int,
	fields map[string]func(reflect.Value) reflect.Value) {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.Anonymous {
			p := append(preindex, field.Index...)
			getTableFields(field.Type, p, fields)
			continue
		}
		if field.PkgPath != "" {
			continue
		}

		fieldName := ToSnake(field.Name)
		if dbTag := field.Tag.Get("db"); dbTag != "" {
			parts := strings.Split(dbTag, ",")
			if len(parts) > 0 && parts[0] != "" {
				fieldName = parts[0]
			}
		}

		idx := append(preindex, field.Index...)
		fields[fieldName] = func(v reflect.Value) reflect.Value {
			return v.FieldByIndex(idx)
		}
	}
}

func GetTableMeta(typ reflect.Type) *TableMeta {
	if typ.Kind() != reflect.Struct {
		panic("GetTableMeta only accept struct type")
	}

	tableLk.RLock()
	meta, exists := tableMetas[typ]
	tableLk.RUnlock()

	if exists {
		return meta
	} else {
		meta = &TableMeta{Type: typ,
			fieldValues: make(map[string]func(reflect.Value) reflect.Value)}
		getTableFields(typ, nil, meta.fieldValues)
		tableLk.Lock()
		if m, exists := tableMetas[typ]; !exists {
			tableMetas[typ] = meta
		} else {
			meta = m
		}
		tableLk.Unlock()
		return meta
	}
}
