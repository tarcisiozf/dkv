package env

import "os"

func Env(key string, fallback ...string) string {
	if len(fallback) > 1 {
		panic("only one fallback value is allowed")
	}
	value := os.Getenv(key)
	if value == "" && len(fallback) > 0 {
		return fallback[0]
	}
	return value
}
