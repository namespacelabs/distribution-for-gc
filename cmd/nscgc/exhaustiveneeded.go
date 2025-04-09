package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"

	"github.com/distribution/distribution/v3/registry/storage"
	"github.com/opencontainers/go-digest"
)

func parseExhaustiveNeeded(repoPattern string, neededDigestsFile string) (storage.ExhaustiveNeededImages, error) {
	res := storage.ExhaustiveNeededImages{}

	err := parsePatternBased(repoPattern, neededDigestsFile, &res)
	if err != nil {
		return res, err
	}

	return res, nil
}

func dumpExhaustiveNeeded(en *storage.ExhaustiveNeededImages) {
	fmt.Printf("got pattern-based exhaustive needed for '%s':\n", en.Pattern.String())
	for n := range en.Digests {
		fmt.Printf("   %s\n", n)
	}

	fmt.Print("\n")
}

func parsePatternBased(patternIn string, path string, res *storage.ExhaustiveNeededImages) error {
	pattern, err := regexp.Compile(patternIn)
	if err != nil {
		return fmt.Errorf("can't parse pattern: %v", err)
	}
	res.Pattern = pattern

	res.Digests = storage.NeededDigests{}

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		if !digestRegex.MatchString(line) {
			return fmt.Errorf("exhaustive needed parsing: expected digest but got %s", line)
		}

		d := digest.NewDigestFromEncoded(digest.SHA256, line)
		res.Digests[d] = struct{}{}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

var (
	// sha256
	digestRegex = regexp.MustCompile("[A-Fa-f0-9]{64}")
)
