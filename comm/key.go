package comm

func RangeEndKey(key string) string {
	data := []byte(key)
	data[len(data)-1] += 1
	return string(data)
}

func NextRangeFromKey(key string) string {
	return key + "!"
}
