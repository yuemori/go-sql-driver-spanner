package internal

import (
	"fmt"
	"regexp"
)

var namedValueParamNameRegex = regexp.MustCompile("@(\\w+)")

func NamedValueParamNames(q string, n int) ([]string, error) {
	var names []string
	matches := namedValueParamNameRegex.FindAllStringSubmatch(q, n)
	if m := len(matches); n != -1 && m < n {
		return nil, fmt.Errorf("query has %d placeholders but %d arguments are provided", m, n)
	}
	for _, m := range matches {
		names = append(names, m[1])
	}
	return names, nil
}
