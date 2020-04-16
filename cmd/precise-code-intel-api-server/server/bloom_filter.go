package server

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io/ioutil"
	"math"
	"unicode/utf16"

	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-api-server/server/db"
)

func applyBloomFilter(refs []db.Reference, identifier string, limit int) ([]db.Reference, int) {
	var filtered []db.Reference
	for i, ref := range refs {
		buckets, m, k, err := decodeFilter([]byte(ref.Filter)) // TODO - should be bytes in db?
		if err != nil {
			continue
		}

		if !test(identifier, buckets, m, k) {
			continue
		}

		filtered = append(filtered, ref)

		if len(filtered) >= limit {
			return filtered, i + 1
		}
	}

	return filtered, len(refs)
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

	return payload.Buckets, makeBloomFilter(payload.Buckets), payload.NumHashFunctions, nil
}

//
//
//

// // Creates a new bloom filter.  If *m* is an array-like object, with a length
// // property, then the bloom filter is loaded with data from the array, where
// // each element is a 32-bit integer.  Otherwise, *m* should specify the
// // number of bits.  Note that *m* is rounded up to the nearest multiple of
// // 32.  *k* specifies the number of hashing functions.
// function BloomFilter(m, k) {
//     var a = m
//     m = a.length * 32

//     var n = Math.ceil(m / 32)
//     var i = -1
//     this.m = m = n * 32
//     this.k = k

//     var kbytes = 1 << Math.ceil(Math.log(Math.ceil(Math.log(m) / Math.LN2 / 8)) / Math.LN2)
//     var array = kbytes === 1 ? Uint8Array : kbytes === 2 ? Uint16Array : Uint32Array
//     var kbuffer = new ArrayBuffer(kbytes * k)
//     var buckets = (this.buckets = new Int32Array(n))
//     while (++i < n) {
//         buckets[i] = a[i]
//     }
//     this._locations = new array(kbuffer)
// }

// TODO - hate this, don't understand why m is chosen
func makeBloomFilter(a []int) int {
	return int(math.Ceil(float64(len(a)*32)/32) * 32)
}

// BloomFilter.prototype.test = function (v) {
//     var l = this.locations(v + ''),
//         k = this.k,
//         buckets = this.buckets
//     for (var i = 0; i < k; ++i) {
//         var b = l[i]
//         if ((buckets[Math.floor(b / 32)] & (1 << b % 32)) === 0) {
//             return false
//         }
//     }
//     return true
// }

// TODO - rename
func test(v string, buckets []int, m, k int) bool {
	for _, b := range locations(v, m, k) {
		if (buckets[int(math.Floor(float64(b)/32))] & (1 << (b % 32))) == 0 {
			return false
		}
	}
	return true
}

// BloomFilter.prototype.locations = function (v) {
//     var k = this.k
//     var m = this.m
//     var r = this._locations
//     var a = fnv_1a(v)
//     var b = fnv_1a(v, 1576284489) // The seed value is chosen randomly
//     var x = a % m
//     for (var i = 0; i < k; ++i) {
//         r[i] = x < 0 ? x + m : x
//         x = (x + b) % m
//     }
//     return r
// }

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

//
//
//

// Fowler/Noll/Vo hashing.
// Nonstandard variation: this function optionally takes a seed value that is incorporated
// into the offset basis. According to http://www.isthe.com/chongo/tech/comp/fnv/index.html
// "almost any offset_basis will serve so long as it is non-zero".
func fnv_1a(v string, seed int32) int32 {
	q := 2166136261
	a := int64(int32(q) ^ seed)

	var runes []rune
	for _, r := range v {
		a, b := utf16.EncodeRune(r)
		if a == '\uFFFD' && b == '\uFFFD' {
			runes = append(runes, r)
		} else {
			runes = append(runes, a, b)
		}
	}

	for _, r := range runes {
		c := int64(int32(r))
		d := c & 0xff00
		ds := int64(d >> 8)
		orzo := a ^ ds
		// fmt.Printf("%d %d\n", c, d)
		if d != 0 {
			// TODO - this condition is never hit we should do some weirder characters
			a = (fnv_multiply(int32(orzo)))
		}
		a = fnv_multiply(int32(a) ^ int32(c&0xff))
	}

	return fnv_mix(int32(a))
}

// function fnv_1a(v, seed) {
//     var a = 2166136261 ^ (seed || 0)
//     for (var i = 0, n = v.length; i < n; ++i) {
//         var c = v.charCodeAt(i)
//         var d = c & 0xff00
//         if (d) {
//             a = fnv_multiply(a ^ (d >> 8))
//         }
//         a = fnv_multiply(a ^ (c & 0xff))
//     }
//     return fnv_mix(a)
// }

// a * 16777619 mod 2**32
func fnv_multiply(a int32) int64 {
	// fmt.Printf(
	// 	"%d %d %d %d %d %d\n",
	// 	a,
	// 	(a << 1),
	// 	(a << 4),
	// 	(a << 7),
	// 	(a << 8),
	// 	(a << 24))

	r := (int64(a) + int64(a<<1) + int64(a<<4) + int64(a<<7) + int64(a<<8) + int64(a<<24))
	// fmt.Printf("\n%d + %d + %d + %d + %d + %d = %d\n\n", (a), (a << 1), (a << 4), (a << 7), (a << 8), (a << 24), r)
	// fmt.Printf("A a, (a << 1), (a << 4), (a << 7), (a << 8), (a << 24) => %d + %d + %d + %d + %d + %d\n", a, (a << 1), (a << 4), (a << 7), (a << 8), (a << 24))
	// fmt.Printf("B a+(a<<1) => %d\n", a+(a<<1))
	// fmt.Printf("C a+(a<<1)+(a<<4) => %d\n", a+(a<<1)+(a<<4))
	// fmt.Printf("D a+(a<<1)+(a<<4)+(a<<7) => %d\n", a+(a<<1)+(a<<4)+(a<<7))
	// fmt.Printf("E a+(a<<1)+(a<<4)+(a<<7)+(a<<8) => %d\n", a+(a<<1)+(a<<4)+(a<<7)+(a<<8))
	// fmt.Printf("F a+(a<<1)+(a<<4)+(a<<7)+(a<<8)+(a<<24) => %d\n", a+(a<<1)+(a<<4)+(a<<7)+(a<<8)+(a<<24))
	// fmt.Printf("G %d %d\n\n\n\n", a, r)
	// fmt.Printf("%d + %d + %d + %d + %d + %d => %d\n", a, (a << 1), (a << 4), (a << 7), (a << 8), (a << 24), r)
	return (r)
}

// function fnv_multiply(a) {
//     return a + (a << 1) + (a << 4) + (a << 7) + (a << 8) + (a << 24)
// }

// See https://web.archive.org/web/20131019013225/http://home.comcast.net/~bretm/hash/6.html
func fnv_mix(a int32) int32 {
	a += a << 13
	a ^= int32(uint32(a) >> 7)
	a += a << 3
	a ^= int32(uint32(a) >> 17)
	a += a << 5
	return int32(int(a) & 0xffffffff)
}

// function fnv_mix(a) {
//     a += a << 13
//     a ^= a >>> 7
//     a += a << 3
//     a ^= a >>> 17
//     a += a << 5
//     return a & 0xffffffff
// }
