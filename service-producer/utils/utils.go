package utils

import "os"

func GetHost() string {
	host := os.Getenv("HOST")
	if host == "" {
		host = "http://localhost:2566"
	}
	return host
}
