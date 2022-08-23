package stats

import (
	"fmt"
	"log"
	"math"
	"sort"
)

type StatCollector interface {
	Push(value any)
	Info() string
	Count() int
	Freq(n int, least bool) ([]string, []int)

}

// Stat collector for numerical types
// based on https://www.johndcook.com/blog/standard_deviation/
type RunningStats struct {
	m_n, m_M, m_S, min, max float64
	cnt int
	nullCnt int
}
// TODO: add counting nulls etc.
/*
		if !fc.notNull[i] {comment += "ALL_NULL "}
		if fc.negative[i] {comment += "NEGATIVE "}
		if fc.emptyString[i] {comment += "EMPTY_STRINGS "}
		if fc.nullString[i] {comment += "NULL_LITERALS "}
*/

/*
Considered using reflection but type switch seems way faster:
cpu: Intel(R) Core(TM) i7-3520M CPU @ 2.90GHz
BenchmarkReflect-4      	18460486	        65.82 ns/op	       8 B/op	       1 allocs/op
BenchmarkTypeSwitch-4   	214656763	         5.600 ns/op	       0 B/op	       0 allocs/op

var F64 reflect.Type = reflect.TypeOf(float64(0))
func conv2(value any) float64 {
	var y float64
	if reflect.ValueOf(value).CanConvert(F64) {
		y = reflect.ValueOf(value).Convert(F64).Float()
	} else {
		log.Panic(fmt.Printf("unexpected type %T", value))
	}
	return y
}
*/
// Pushes new value to RunningStats and updates the stats
// BenchmarkRunningStatsPush-4   	143189905	         8.039 ns/op	       0 B/op	       0 allocs/op
func (rs *RunningStats) Push(value any) {
	rs.cnt++
	if value == nil {
		rs.nullCnt++
		return
	}
	var x float64
	// Looks bad but it's faster than reflection
	switch v := value.(type) {
	case uint8:
		x = float64(v)
	case uint16:
		x = float64(v)
	case uint32:
		x = float64(v)
	case uint64:
		x = float64(v)
	case uint:
		x = float64(v)
	case int8:
		x = float64(v)
	case int16:
		x = float64(v)
	case int32:
		x = float64(v)
	case int64:
		x = float64(v)
	case int:
		x = float64(v)
	case float32:
		x = float64(v)
	case float64:
		x = float64(v)
	default:
		log.Panic(fmt.Printf("unexpected type: %T is not numeric\n", v))
	}
	rs.m_n++
	if rs.m_n == 1.0 {
		rs.m_M = x
		rs.min = x
		rs.max = x
	} else {
		t := rs.m_M + (x-rs.m_M)/rs.m_n
		rs.m_S = rs.m_S + (x-rs.m_M)*(x-t)
		rs.m_M = t
		if x < rs.min {
			rs.min = x
		}
		if x > rs.max {
			rs.max = x
		}
	}
}
func (rs *RunningStats) Count() int {
	return int(rs.m_n)
}

func (rs *RunningStats) Mean() float64 {
	return rs.m_M
}
func (rs *RunningStats) Variance() float64 {
	if rs.m_n <= 1.0 {
		return 0.0
	}
	return rs.m_S / (rs.m_n - 1)
}
func (rs *RunningStats) StdDev() float64 {
	return math.Sqrt(rs.Variance())
}
func (rs *RunningStats) Freq(n int, least bool) ([]string, []int) {
	log.Fatal("not implemented")
	return nil, nil
}
func (rs *RunningStats) Info() string {
	var ret string
	if rs.nullCnt > 0 {
		if rs.cnt == rs.nullCnt {
			return "ALL NULL"
		} else {
			ret = fmt.Sprintf("%d NULL ", rs.nullCnt)
		}
	}
	ret += fmt.Sprintf("min: %.3g, max: %.3g, mean: %.3g, std: %.3g", rs.min, rs.max, rs.m_M, rs.StdDev())
	return ret
}

//
// Stats collector for categorical values (casted to string)
//
type StringFreq struct {
	counts map[string]int
	n      int // non null or empty
	minl   int
	maxl   int
	cnt    int // all including nulls and empty
	nullCnt int
}
func NewStringFreq() *StringFreq {
	return &StringFreq{map[string]int{}, 0, 1<<32, -1, 0, 0}
}
func (sf *StringFreq) Push(value any) {
	// Looks bad but it's faster than reflection
	sf.cnt++
	if value == nil {
		sf.nullCnt++
		return
	}
	var s string
	switch v := value.(type) {
	case string:
		s = string(v)
	default:
		s = fmt.Sprintf("%v", v)
	}
	l := len(s)
	if l > 0 {
		sf.counts[s]++
		sf.n++
		if l > sf.maxl {
			sf.maxl = l
		} else if l < sf.minl {
			sf.minl = l
		}
	}
}
func (sf *StringFreq) Count() int {
	return sf.n
}
func (sf *StringFreq) Freq(n int, least bool) ([]string, []int) {
	keys := make([]string, len(sf.counts))
	var i int
	for k := range sf.counts {
		keys[i] = k
		i++
	}
	sort.Slice(keys, func(i, j int) bool { return sf.counts[keys[i]] > sf.counts[keys[j]] })

	var fkeys []string
	if least {
		fkeys = keys[Max(0, len(keys)-n):]
	} else {
		fkeys = keys[:Min(n, len(keys))]
	}
	fvals := make([]int, len(fkeys))
	for i, k := range fkeys {
		fvals[i] = sf.counts[k]
	}
	return fkeys, fvals
}
func (sf *StringFreq) Info() string {
	var ret string
	if sf.nullCnt > 0 {
		if sf.cnt == sf.nullCnt {
			return "ALL NULL"
		} else {
			ret = fmt.Sprintf("%d NULL ", sf.nullCnt)
		}
	}
	if sf.n > 0 {
		return ret + fmt.Sprintf("length min: %d, max: %d", sf.minl, sf.maxl)
	} 
	return ret + "EMPTY"
}

// helper functions
func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}