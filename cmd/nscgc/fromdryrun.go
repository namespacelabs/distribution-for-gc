package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/distribution/distribution/v3/registry/storage"
	"github.com/opencontainers/go-digest"
)

func parseFromDryRun(path string) (storage.FromDryRun, error) {
	res := storage.FromDryRun{
		BlobsToDelete:  make(map[string]struct{}),
		LayersToDelete: make(map[string]map[digest.Digest]struct{}),
	}

	file, err := os.Open(path)
	if err != nil {
		return res, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "PMDELETEBLOB ") {
			dgst := strings.TrimPrefix(line, "PMDELETEBLOB ")
			res.BlobsToDelete[dgst] = struct{}{}
			continue
		}

		if strings.HasPrefix(line, "PMDELETELAYER ") {
			parts := strings.Split(line, " ")
			if len(parts) != 3 {
				return res, fmt.Errorf(" could not parse %s", line)
			}
			repo := parts[1]
			dgst := digest.Digest(parts[2])
			if _, hasMap := res.LayersToDelete[repo]; !hasMap {
				res.LayersToDelete[repo] = make(map[digest.Digest]struct{})
			}
			res.LayersToDelete[repo][dgst] = struct{}{}
			continue
		}

		continue
	}

	if err := scanner.Err(); err != nil {
		return res, err
	}

	return res, nil
}
