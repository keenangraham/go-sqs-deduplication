package queue


import "testing"


func TestTest(t *testing.T) {
	if 1 != 1 {
        expected := 1
        actual := 1
        if expected != actual {
            t.Errorf("Expected not actual")
        }
	}
}
