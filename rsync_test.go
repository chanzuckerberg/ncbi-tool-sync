package main

func FakeRsync(cmd string) (string, string, error) {
	stdout := "1 apple\n1 banana\n1 cherry/cranberry\n1 date/dragonfruit\n1 elderberry\n1 fig\n1 grape\n1 huckleberry"
	return stdout, "banana", nil
}
