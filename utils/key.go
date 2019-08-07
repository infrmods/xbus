package utils

// RangeEndKey range end key
func RangeEndKey(key string) string {
	data := []byte(key)
	data[len(data)-1]++
	return string(data)
}

// NextRangeFromKey next range from key
func NextRangeFromKey(key string) string {
	return key + "!"
}
