package server

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io/ioutil"
	"math"
	"unicode/utf16"
)

// TODO - collapse the below
func decodeAndTestFilter(encodedFilter []byte, identifier string) (bool, error) {
	buckets, m, k, err := decodeFilter(encodedFilter)
	if err != nil {
		return false, err
	}

	return test(identifier, buckets, m, k), nil
}

func decodeFilter(encodedFilter []byte) ([]int, int, int, error) {
	payload := struct {
		Buckets          []int `json:"buckets"`
		NumHashFunctions int   `json:"numHashFunctions"`
	}{}

	r, err := gzip.NewReader(bytes.NewReader(encodedFilter))
	if err != nil {
		return nil, 0, 0, err
	}

	f, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, 0, 0, err
	}

	if err := json.Unmarshal(f, &payload); err != nil {
		return nil, 0, 0, err
	}

	m := int(math.Ceil(float64(len(payload.Buckets)*32)/32) * 32)
	return payload.Buckets, m, payload.NumHashFunctions, nil
}

// TODO - rename
func test(v string, buckets []int, m, k int) bool {
	for _, b := range locations(v, m, k) {
		if (buckets[int(math.Floor(float64(b)/32))] & (1 << (b % 32))) == 0 {
			return false
		}
	}
	return true
}

// See http://willwhim.wpengine.com/2011/09/03/producing-n-hash-functions-by-hashing-only-once/
func locations(v string, m, k int) []int32 {
	a := fnv_1a(v, 0)
	b := fnv_1a(v, 1576284489) // The seed value is chosen randomly

	x := a % int32(m)
	r := make([]int32, k)
	for i := 0; i < k; i++ {
		if x < 0 {
			r[i] = x + int32(m)
		} else {
			r[i] = x
		}
		x = (x + b) % int32(m)
	}

	return r
}

// Fowler/Noll/Vo hashing.
// Nonstandard variation: this function optionally takes a seed value that is incorporated
// into the offset basis. According to http://www.isthe.com/chongo/tech/comp/fnv/index.html
// "almost any offset_basis will serve so long as it is non-zero".
func fnv_1a(v string, seed int32) int32 {
	q := 2166136261
	a := int64(int32(q) ^ seed)

	for _, r := range utf16Runes(v) {
		c := int64(int32(r))
		if d := c & 0xff00; d != 0 {
			a = (fnv_multiply(int32(a ^ int64(d>>8))))
		}
		a = fnv_multiply(int32(a) ^ int32(c&0xff))
	}

	return fnv_mix(int32(a))
}

// a * 16777619 mod 2**32
func fnv_multiply(a int32) int64 {
	return (int64(a) + int64(a<<1) + int64(a<<4) + int64(a<<7) + int64(a<<8) + int64(a<<24))
}

// See https://web.archive.org/web/20131019013225/http://home.comcast.net/~bretm/hash/6.html
func fnv_mix(a int32) int32 {
	a += a << 13
	a ^= int32(uint32(a) >> 7)
	a += a << 3
	a ^= int32(uint32(a) >> 17)
	a += a << 5
	return a // int32(int(a) & 0xffffffff)
}

func utf16Runes(v string) []rune {
	var runes []rune
	for _, r := range v {
		if a, b := utf16.EncodeRune(r); a == '\uFFFD' && b == '\uFFFD' {
			runes = append(runes, r)
		} else {
			runes = append(runes, a, b)
		}
	}
	return runes
}
