package main

import (
	"bufio"
	"fmt"
	"os"

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
