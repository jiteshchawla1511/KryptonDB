package test

import (
	"math/rand"
	"net"
	"testing"
	"time"
)

const Threshold = 1000000

var key = make([]string, Threshold)
var value = make([]string, Threshold)

func BenchmarkPutTest(t *testing.B) {
	conn, err := net.Dial("tcp", "localhost:8080")

	if err != nil {
		t.Error(err)
	}

	defer conn.Close()

	for i := 0; i < Threshold; i++ {
		key[i] = getRandomString(10)
		value[i] = getRandomString(10)
	}

	for i := 0; i < Threshold; i++ {
		_, err := conn.Write([]byte("PUT " + key[i] + " " + value[i] + "\n"))

		if err != nil {
			t.Error(err)
		}
	}
}

func BenchmarkGetTest(t *testing.B) {
	conn, err := net.Dial("tcp", "localhost:8080")

	if err != nil {
		t.Error(err)
	}

	defer conn.Close()

	for i := 0; i < Threshold; i++ {
		key[i] = getRandomString(10)
		value[i] = getRandomString(10)
	}

	for i := 0; i < Threshold; i++ {
		_, err := conn.Write([]byte("GET " + key[i] + "\n"))

		if err != nil {
			t.Error(err)
		}

		buf := make([]byte, 1024)
		_, err = conn.Read(buf)

		if err != nil {
			t.Error(err)
		}
	}
}

func BenchmarkGetUDPTest(t *testing.B) {
	conn, err := net.Dial("udp", "localhost:1053")

	errorRate := 0

	if err != nil {
		t.Error(err)
	}

	defer conn.Close()

	for i := 0; i < Threshold; i++ {
		key[i] = getRandomString(10)
		value[i] = getRandomString(10)
	}

	for i := 0; i < Threshold; i++ {
		_, err := conn.Write([]byte("GET " + key[i] + "\n"))

		if err != nil {
			t.Error(err)
		}

		buf := make([]byte, 1024)
		_, err = conn.Read(buf)

		if err != nil {
			t.Error(err)
		}

		if string(buf) == "NOT FOUND" {
			errorRate++
		}
	}
}

func getRandomString(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	rand.Seed(time.Now().UnixNano())

	result := make([]byte, length)

	for i := 0; i < length; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}
