package config

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"time"
)

func DefaultConfig(v interface{}) error {
	typ := reflect.TypeOf(v)
	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Struct {
		return fmt.Errorf(
			"InitConfig fail, invalid type(must be a pointer of struct): %v",
			reflect.TypeOf(v))
	}

	val := reflect.ValueOf(v).Elem()
	if !val.IsValid() {
		val = reflect.New(typ.Elem())
		reflect.ValueOf(v).Set(val)
		val = val.Elem()
	}
	_, err := setDefaultConfig(val)
	return err
}

func setDefaultConfig(v reflect.Value) (bool, error) {
	if v.Kind() == reflect.Ptr {
		val := v.Elem()
		new_val := false
		if !val.IsValid() {
			val = reflect.New(v.Type().Elem())
			v.Set(val)
			val = val.Elem()
			new_val = true
		}
		hasDefaults, err := setDefaultConfig(val)
		if err != nil {
			return false, err
		}
		if !hasDefaults && new_val {
			v.Set(reflect.Zero(v.Type()))
		}
		return hasDefaults, nil
	}
	return setStructValue(v)
}

func setStructValue(val reflect.Value) (bool, error) {
	name := val.Type().Name()
	if val.Kind() != reflect.Struct {
		panic(fmt.Sprintf("invalid struct: %s, %v", name, val.Type()))
	}
	typ := val.Type()
	hasDefaults := false
	for i := 0; i < typ.NumField(); i++ {
		var has bool
		var err error
		field := typ.Field(i)
		if field.Tag.Get("default") == "-" {
			continue
		}

		if field.Anonymous {
			has, err = setDefaultConfig(val.Field(i))
		} else if field.PkgPath == "" {
			has, err = setFieldValue(name+"."+field.Name, field.Tag, val.Field(i))
		}

		if err != nil {
			return false, err
		} else if has {
			hasDefaults = true
		}
	}
	return hasDefaults, nil
}

func setFieldValue(name string, tag reflect.StructTag, v reflect.Value) (bool, error) {
	switch v.Type().Kind() {
	case reflect.Ptr:
		var val reflect.Value
		new_val := false
		if v.IsNil() {
			val = reflect.New(v.Type().Elem())
			v.Set(val)
			val = val.Elem()
			new_val = true
		} else {
			val = v.Elem()
		}
		if hasDefaults, err := setFieldValue(name, tag, val); err != nil {
			return false, err
		} else {
			if !hasDefaults && new_val {
				v.Set(reflect.Zero(v.Type()))
			}
			return hasDefaults, nil
		}
	case reflect.Struct:
		return setStructValue(v)
	}

	defval := tag.Get("default")
	if defval == "" {
		return false, nil
	}
	defval = os.ExpandEnv(defval)
	return true, setPrimaryValue(name, defval, v)
}

var DurationType = reflect.TypeOf((time.Duration)(1))

func setPrimaryValue(name, valstr string, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Bool:
		switch valstr {
		case "true", "True", "TRUE":
			v.SetBool(true)
		case "false", "False", "FALSE":
			v.SetBool(false)
		default:
			return fmt.Errorf("invalid bool value(%s): %s", name, valstr)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return parseNum(name, valstr, v.Type().Bits(), false, v)
	case reflect.Int64:
		if v.Type() == DurationType {
			if d, err := time.ParseDuration(valstr); err == nil {
				v.SetInt(int64(d))
			} else {
				return fmt.Errorf("invalid duration(%s): %s(%v)", name, valstr, err)
			}
		} else {
			return parseNum(name, valstr, v.Type().Bits(), false, v)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return parseNum(name, valstr, v.Type().Bits(), true, v)
	case reflect.Float32, reflect.Float64:
		if n, err := strconv.ParseFloat(valstr, v.Type().Bits()); err == nil {
			v.SetFloat(n)
		} else {
			return fmt.Errorf("invalid float(%s): %s(%v)", name, valstr, err)
		}
	case reflect.String:
		v.SetString(valstr)
	case reflect.Array, reflect.Slice, reflect.Map:
		if err := json.Unmarshal([]byte(valstr), v.Addr().Interface()); err != nil {
			return fmt.Errorf("invalid array/slice/map(%s): %s(%v)", name, valstr, err)
		}
	default:
		return fmt.Errorf("unsupported default type(%s): %v", name, v.Type())
	}
	return nil
}

var numR = regexp.MustCompile(`^(-)?(0[xob]?)?([0-9a-fA-F]+)$`)

func parseNum(name, valstr string, bitsize int, unsigned bool, val reflect.Value) error {
	parts := numR.FindStringSubmatch(valstr)
	if len(parts) == 0 {
		return fmt.Errorf("invalid num value(%s): %s", name, valstr)
	}

	num := parts[3]
	if parts[1] == "-" {
		num = "-" + num
	}

	base := 10
	switch len(parts[2]) {
	case 1:
		base = 8
	case 2:
		switch parts[2][1] {
		case 'x':
			base = 16
		case 'o':
			base = 8
		case 'b':
			base = 2
		}
	}

	if unsigned {
		if n, err := strconv.ParseUint(num, base, bitsize); err != nil {
			return fmt.Errorf("invalid uint* value(%s): %s(%v)", name, valstr, err)
		} else {
			val.SetUint(n)
			return nil
		}
	} else {
		if n, err := strconv.ParseInt(num, base, bitsize); err != nil {
			return fmt.Errorf("invalid int* value(%s): %s(%v)", name, valstr, err)
		} else {
			val.SetInt(n)
			return nil
		}
	}
}
