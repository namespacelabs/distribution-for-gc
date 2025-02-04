package storage

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"sync"

	"github.com/opencontainers/go-digest"
	"golang.org/x/sync/errgroup"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/registry/storage/driver"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
)

var _ distribution.TagService = &tagStore{}

// tagStore provides methods to manage manifest tags in a backend storage driver.
// This implementation uses the same on-disk layout as the (now deleted) tag
// store.  This provides backward compatibility with current registry deployments
// which only makes use of the Digest field of the returned distribution.Descriptor
// but does not enable full roundtripping of Descriptor objects
type tagStore struct {
	repository       *repository
	blobStore        *blobStore
	concurrencyLimit int
	digestToCurrent  map[digest.Digest][]string
	digestToHist     map[digest.Digest][]string
}

// All returns all tags
func (ts *tagStore) All(ctx context.Context) ([]string, error) {
	pathSpec, err := pathFor(manifestTagsPathSpec{
		name: ts.repository.Named().Name(),
	})
	if err != nil {
		return nil, err
	}

	entries, err := ts.blobStore.driver.List(ctx, pathSpec)
	if err != nil {
		switch err := err.(type) {
		case storagedriver.PathNotFoundError:
			return nil, distribution.ErrRepositoryUnknown{Name: ts.repository.Named().Name()}
		default:
			return nil, err
		}
	}

	tags := make([]string, 0, len(entries))
	for _, entry := range entries {
		_, filename := path.Split(entry)
		tags = append(tags, filename)
	}

	// there is no guarantee for the order,
	// therefore sort before return.
	sort.Strings(tags)

	return tags, nil
}

// Tag tags the digest with the given tag, updating the store to point at
// the current tag. The digest must point to a manifest.
func (ts *tagStore) Tag(ctx context.Context, tag string, desc distribution.Descriptor) error {
	currentPath, err := pathFor(manifestTagCurrentPathSpec{
		name: ts.repository.Named().Name(),
		tag:  tag,
	})
	if err != nil {
		return err
	}

	lbs := ts.linkedBlobStore(ctx, tag)

	// Link into the index
	if err := lbs.linkBlob(ctx, desc); err != nil {
		return err
	}

	// Overwrite the current link
	return ts.blobStore.link(ctx, currentPath, desc.Digest)
}

// resolve the current revision for name and tag.
func (ts *tagStore) Get(ctx context.Context, tag string) (distribution.Descriptor, error) {
	currentPath, err := pathFor(manifestTagCurrentPathSpec{
		name: ts.repository.Named().Name(),
		tag:  tag,
	})
	if err != nil {
		return distribution.Descriptor{}, err
	}

	revision, err := ts.blobStore.readlink(ctx, currentPath)
	if err != nil {
		switch err.(type) {
		case storagedriver.PathNotFoundError:
			return distribution.Descriptor{}, distribution.ErrTagUnknown{Tag: tag}
		}

		return distribution.Descriptor{}, err
	}

	return distribution.Descriptor{Digest: revision}, nil
}

// Untag removes the tag association
func (ts *tagStore) Untag(ctx context.Context, tag string) error {
	tagPath, err := pathFor(manifestTagPathSpec{
		name: ts.repository.Named().Name(),
		tag:  tag,
	})
	if err != nil {
		return err
	}

	return ts.blobStore.driver.Delete(ctx, tagPath)
}

// linkedBlobStore returns the linkedBlobStore for the named tag, allowing one
// to index manifest blobs by tag name. While the tag store doesn't map
// precisely to the linked blob store, using this ensures the links are
// managed via the same code path.
func (ts *tagStore) linkedBlobStore(ctx context.Context, tag string) *linkedBlobStore {
	return &linkedBlobStore{
		blobStore:  ts.blobStore,
		repository: ts.repository,
		ctx:        ctx,
		linkPath: func(name string, dgst digest.Digest) (string, error) {
			return pathFor(manifestTagIndexEntryLinkPathSpec{
				name:     name,
				tag:      tag,
				revision: dgst,
			})
		},
	}
}

// Lookup recovers a list of tags which refer to this digest.  When a manifest is deleted by
// digest, tag entries which point to it need to be recovered to avoid dangling tags.
func (ts *tagStore) Lookup(ctx context.Context, desc distribution.Descriptor) ([]string, error) {
	allTags, err := ts.All(ctx)
	switch err.(type) {
	case distribution.ErrRepositoryUnknown:
		// This tag store has been initialized but not yet populated
		break
	case nil:
		break
	default:
		return nil, err
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(ts.concurrencyLimit)

	var (
		tags []string
		mu   sync.Mutex
	)
	for _, tag := range allTags {
		if ctx.Err() != nil {
			break
		}
		tag := tag

		g.Go(func() error {
			tagLinkPathSpec := manifestTagCurrentPathSpec{
				name: ts.repository.Named().Name(),
				tag:  tag,
			}

			tagLinkPath, _ := pathFor(tagLinkPathSpec)
			tagDigest, err := ts.blobStore.readlink(ctx, tagLinkPath)
			if err != nil {
				switch err.(type) {
				case storagedriver.PathNotFoundError:
					return nil
				}
				return err
			}

			if tagDigest == desc.Digest {
				mu.Lock()
				tags = append(tags, tag)
				mu.Unlock()
			}

			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		return nil, err
	}

	return tags, nil
}

func (ts *tagStore) ManifestDigests(ctx context.Context, tag string) ([]digest.Digest, error) {
	tagLinkPath := func(name string, dgst digest.Digest) (string, error) {
		return pathFor(manifestTagIndexEntryLinkPathSpec{
			name:     name,
			tag:      tag,
			revision: dgst,
		})
	}
	lbs := &linkedBlobStore{
		blobStore: ts.blobStore,
		blobAccessController: &linkedBlobStatter{
			blobStore:  ts.blobStore,
			repository: ts.repository,
			linkPath:   manifestRevisionLinkPath,
		},
		repository: ts.repository,
		ctx:        ctx,
		linkPath:   tagLinkPath,
		linkDirectoryPathSpec: manifestTagIndexPathSpec{
			name: ts.repository.Named().Name(),
			tag:  tag,
		},
	}
	var dgsts []digest.Digest
	err := lbs.Enumerate(ctx, func(dgst digest.Digest) error {
		dgsts = append(dgsts, dgst)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dgsts, nil
}

func (ts *tagStore) Lookup2(ctx context.Context, desc distribution.Descriptor) ([]string, []string, error) {
	if ts.digestToCurrent == nil || ts.digestToHist == nil {
		fmt.Printf("building tag map for %s\n", ts.repository.Named().String())
		err := ts.buildMap(ctx)
		if err != nil {
			return nil, nil, err
		}
		fmt.Printf("  done building tag map for %s\n", ts.repository.Named().String())
	}

	return ts.digestToCurrent[desc.Digest], ts.digestToHist[desc.Digest], nil
}

func (ts *tagStore) buildMap(ctx context.Context) error {
	allTags, err := ts.All(ctx)
	switch err.(type) {
	case distribution.ErrRepositoryUnknown:
		// This tag store has been initialized but not yet populated
		break
	case nil:
		break
	default:
		return err
	}

	ts.digestToCurrent = make(map[digest.Digest][]string)
	ts.digestToHist = make(map[digest.Digest][]string)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(ts.concurrencyLimit)

	var (
		mu sync.Mutex
	)
	for _, tag := range allTags {
		if ctx.Err() != nil {
			break
		}
		tag := tag

		g.Go(func() error {
			tagLinkPathSpec := manifestTagCurrentPathSpec{
				name: ts.repository.Named().Name(),
				tag:  tag,
			}

			tagLinkPath, _ := pathFor(tagLinkPathSpec)
			tagDigest, err := ts.blobStore.readlink(ctx, tagLinkPath)
			if err != nil {
				switch err.(type) {
				case storagedriver.PathNotFoundError:
					return nil
				}
				return err
			}

			//	manifestTagIndexPathSpec:              <root>/v2/repositories/<name>/_manifests/tags/<tag>/index/
			//	manifestTagIndexEntryPathSpec:         <root>/v2/repositories/<name>/_manifests/tags/<tag>/index/<algorithm>/<hex digest>/
			//	manifestTagIndexEntryLinkPathSpec:     <root>/v2/repositories/<name>/_manifests/tags/<tag>/index/<algorithm>/<hex digest>/link
			indexPath, err := pathFor(manifestTagIndexPathSpec{name: ts.repository.Named().Name(), tag: tag})
			if err != nil {
				return err
			}

			digestToHist := make(map[digest.Digest][]string)
			ts.blobStore.driver.Walk(ctx, indexPath, func(fileInfo driver.FileInfo) error {
				if fileInfo.IsDir() {
					return nil
				}

				dir, fileName := path.Split(fileInfo.Path())
				if fileName != "link" {
					return nil
				}

				dgst, err := ts.blobStore.readlink(ctx, fileInfo.Path())
				if err != nil {
					return fmt.Errorf("can not read link %s: %v", fileInfo.Path(), err)
				}

				dirRest, cont := path.Split(filepath.Clean(dir))
				_, algorithm := path.Split(filepath.Clean(dirRest))
				dgst2, err := digest.Parse(algorithm + ":" + cont)
				if err != nil {
					return fmt.Errorf("can not parse diegst from %s: %v", dir, err)
				}

				if dgst != dgst2 {
					return fmt.Errorf("digest in %s does not match dir-derived digest", fileInfo.Path())
				}

				digestToHist[dgst] = append(digestToHist[dgst], tag)
				return nil
			})

			mu.Lock()
			ts.digestToCurrent[tagDigest] = append(ts.digestToCurrent[tagDigest], tag)
			for digest, entries := range digestToHist {
				ts.digestToHist[digest] = append(ts.digestToHist[digest], entries...)
			}
			mu.Unlock()

			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		return err
	}

	return nil
}
