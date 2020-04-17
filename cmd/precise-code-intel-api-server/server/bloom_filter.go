package server

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io/ioutil"
	"math"
	"unicode"
	"unicode/utf16"
)

// TODO - collapse the below
func decodeAndTestFilter(encodedFilter []byte, identifier string) (bool, error) {
	payload := struct {
		Buckets          []int `json:"buckets"`
		NumHashFunctions int32 `json:"numHashFunctions"`
	}{}

	r, err := gzip.NewReader(bytes.NewReader(encodedFilter))
	if err != nil {
		return false, err
	}

	f, err := ioutil.ReadAll(r)
	if err != nil {
		return false, err
	}

	if err := json.Unmarshal(f, &payload); err != nil {
		return false, err
	}

	buckets := payload.Buckets
	m := int32(math.Ceil(float64(len(payload.Buckets)*32)/32) * 32) // TODO - sad dog
	k := payload.NumHashFunctions

	for _, b := range hashLocations(identifier, m, k) {
		if (buckets[int(math.Floor(float64(b)/32))] & (1 << (b % 32))) == 0 {
			return false, nil
		}
	}
	return true, nil
}

// See http://willwhim.wpengine.com/2011/09/03/producing-n-hash-functions-by-hashing-only-once/
func hashLocations(v string, m, k int32) []int32 {
	a := fowlerNollVo1a(v, 0)
	b := fowlerNollVo1a(v, 1576284489) // The seed value is chosen randomly

	x := a % int32(m)
	r := make([]int32, k)
	for i := int32(0); i < k; i++ {
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
func fowlerNollVo1a(v string, seed int32) int32 {
	q := 2166136261
	a := int64(int32(q) ^ seed)

	for _, r := range utf16Runes(v) {
		c := int64(r)
		if d := c & 0xff00; d != 0 {
			a = (fowlerNollVoMultiply(int32(a ^ int64(d>>8))))
		}
		a = fowlerNollVoMultiply(int32(a) ^ int32(c&0xff))
	}

	return fowlerNollVoMix(int32(a))
}

func fowlerNollVoMultiply(a int32) int64 {
	// a * 16777619 mod 2**32
	return (int64(a) + int64(a<<1) + int64(a<<4) + int64(a<<7) + int64(a<<8) + int64(a<<24))
}

// See https://web.archive.org/web/20131019013225/http://home.comcast.net/~bretm/hash/6.html
func fowlerNollVoMix(a int32) int32 {
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
		if a, b := utf16.EncodeRune(r); a == unicode.ReplacementChar && b == unicode.ReplacementChar {
			runes = append(runes, r)
		} else {
			runes = append(runes, a, b)
		}
	}

	return runes
}
