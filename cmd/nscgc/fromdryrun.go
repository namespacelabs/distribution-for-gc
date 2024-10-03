package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/distribution/distribution/v3/registry/storage"
	"github.com/opencontainers/go-digest"
)

func parseFromDryRun(path string) (storage.ToDelete, error) {
	res := storage.ToDelete{
		BlobsToDelete:     make(map[digest.Digest]struct{}),
		LayersToDelete:    make(map[string][]digest.Digest),
		ManifestsToDelete: []storage.ManifestDel{},
	}

	file, err := os.Open(path)
	if err != nil {
		return res, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "PMDELETEBLOB ") {
			dgst := digest.Digest(strings.TrimPrefix(line, "PMDELETEBLOB "))
			res.BlobsToDelete[dgst] = struct{}{}
			continue
		}

		if strings.HasPrefix(line, "PMDELETELAYER ") {
			params := strings.TrimPrefix(line, "PMDELETELAYER ")
			parts := strings.Split(params, "|")
			if len(parts) != 2 {
				return res, fmt.Errorf(" could not parse %s", line)
			}
			repo := parts[0]
			dgst := digest.Digest(parts[1])
			res.LayersToDelete[repo] = append(res.LayersToDelete[repo], dgst)
			continue
		}

		if strings.HasPrefix(line, "PMDELETEMANIFEST ") {
			params := strings.TrimPrefix(line, "PMDELETEMANIFEST ")
			parts := strings.Split(params, "|")
			if len(parts) != 3 {
				return res, fmt.Errorf(" could not parse %s", line)
			}
			name := parts[0]
			digest := digest.Digest(parts[1])
			tags := strings.Split(parts[2], ",")
			res.ManifestsToDelete = append(res.ManifestsToDelete, storage.ManifestDel{Name: name, Digest: digest, Tags: tags})
			continue
		}
	}

	if err := scanner.Err(); err != nil {
		return res, err
	}

	return res, nil
}
