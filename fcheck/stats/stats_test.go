package stats

import (
	"testing"
)

func assert[T comparable](t *testing.T, act T, exp T, msg string) {
	if act != exp {
		t.Fatalf("%s expected: %v, got: %v", msg, exp, act)
	}
}
func TestRunningStats(t *testing.T) {
	s := RunningStats{}
	for i:=0; i<100; i++ {
		s.Push(i)
	}
	assert(t, s.Count(), 100, "Count")
	assert(t, s.Mean(), 49.5, "Mean")
	assert(t, s.StdDev(), 29.011491975882016, "StdDev")
}


func BenchmarkRunningStatsPush(b *testing.B) {
	var x any = float32(1.123)
	s := RunningStats{}
	for i:=0; i<b.N; i++ {
		s.Push(x)
	}
}



