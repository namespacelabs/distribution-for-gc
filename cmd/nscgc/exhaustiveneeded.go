package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"

	"github.com/distribution/distribution/v3/registry/storage"
	"github.com/opencontainers/go-digest"
)

func parseExhaustiveNeeded(path string) (storage.ExhaustiveNeededImages, error) {
	res := storage.ExhaustiveNeededImages{}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
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
			return nil, fmt.Errorf("could not parse %s: %v", line, err)
		}

		if ref.Digest == "" {
			return nil, fmt.Errorf("currently only supports images refrenced by digest, got something else for %s", ref.Repository)
		}

		m := res[ref.Repository]
		if m == nil {
			m = &storage.NeededImages{}
			res[ref.Repository] = m
		}

		(*m)[digest.Digest(ref.Digest)] = struct{}{}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func dumpExhaustiveNeeded(en storage.ExhaustiveNeededImages) {
	if len(en) == 0 {
		fmt.Println("exhaustive needed map empty")
		return
	}

	for repo, needed := range en {
		fmt.Printf("got exhaustive needed set for %s:\n", repo)
		for n := range *needed {
			fmt.Printf("   %s\n", n)
		}
	}
}

func parseExhaustiveNeededV2(path string) (storage.ExhaustiveNeededImages, error) {
	res := storage.ExhaustiveNeededImages{}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	digests := false

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		if !digests && digestRegex.MatchString(line) {
			digests = true
		}

		if digests && !digestRegex.MatchString(line) {
			return nil, fmt.Errorf("didn't expect non-digest %s", line)
		}

		if !digests {
			m := res[line]
			if m == nil {
				m = &storage.NeededImages{}
				res[line] = m
			}
		}
		if digests {
			d := digest.NewDigestFromEncoded(digest.SHA256, line)
			for _, repo := range res {
				(*repo)[d] = struct{}{}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

var (
	// sha256
	digestRegex = regexp.MustCompile("[A-Fa-f0-9]{64}")
)
