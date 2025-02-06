package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"

	"github.com/distribution/distribution/v3/registry/storage"
	"github.com/opencontainers/go-digest"
)

func parseExhaustiveNeeded(perRepoPath string, patternIn string, patternPath string) (storage.ExhaustiveNeededImages, error) {
	res := storage.ExhaustiveNeededImages{}

	if perRepoPath != "" {
		err := parsePerRepo(perRepoPath, res.PerRepo)
		if err != nil {
			return res, err
		}
	}

	if patternPath != "" {
		if patternIn == "" {
			return res, fmt.Errorf("for exhaustive needed v2, need a pattern too")
		}

		err := parsePatternBased(patternIn, patternPath, &res)
		if err != nil {
			return res, err
		}
	}

	return res, nil
}

func parsePerRepo(path string, res map[string]storage.NeededDigests) error {
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

		ref, err := ParseImageRef(line)
		if err != nil {
			return fmt.Errorf("could not parse %s: %v", line, err)
		}

		if ref.Digest == "" {
			return fmt.Errorf("currently only supports images refrenced by digest, got something else for %s", ref.Repository)
		}

		m := res[ref.Repository]
		if m == nil {
			m = storage.NeededDigests{}
			res[ref.Repository] = m
		}

		m[digest.Digest(ref.Digest)] = struct{}{}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil

}

func dumpExhaustiveNeeded(en *storage.ExhaustiveNeededImages) {
	if len(en.Digests) != 0 {
		fmt.Println("got pattern-based exhaustive needed for '%s':\n", en.Pattern.String())
		for n := range en.Digests {
			fmt.Printf("   %s\n", n)
		}
	}

	for repo, needed := range en.PerRepo {
		fmt.Printf("got per-repo exhaustive needed set for %s:\n", repo)
		for n := range needed {
			fmt.Printf("   %s\n", n)
		}
	}
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
