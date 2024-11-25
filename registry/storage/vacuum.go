package storage

import (
	"context"
	"fmt"
	"path"

	"github.com/distribution/distribution/v3/internal/dcontext"
	"github.com/distribution/distribution/v3/registry/storage/driver"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/opencontainers/go-digest"
)

// vacuum contains functions for cleaning up repositories and blobs
// These functions will only reliably work on strongly consistent
// storage systems.
// https://en.wikipedia.org/wiki/Consistency_model

// NewVacuum creates a new Vacuum
func NewVacuum(ctx context.Context, driver driver.StorageDriver) Vacuum {
	return Vacuum{
		ctx:    ctx,
		driver: driver,
	}
}

// Vacuum removes content from the filesystem
type Vacuum struct {
	driver driver.StorageDriver
	ctx    context.Context
}

// RemoveBlob removes a blob from the filesystem
func (v Vacuum) RemoveBlob(dgst string) error {
	d, err := digest.Parse(dgst)
	if err != nil {
		return err
	}

	blobPath, err := pathFor(blobPathSpec{digest: d})
	if err != nil {
		return err
	}

	dcontext.GetLogger(v.ctx).Infof("Deleting blob: %s", blobPath)

	err = v.driver.Delete(v.ctx, blobPath)
	if err != nil {
		return err
	}

	return nil
}

// RemoveManifest removes a manifest from the filesystem
func (v Vacuum) RemoveManifest(name string, dgst digest.Digest, currentTags []string, histTags []string) error {
	for _, tag := range histTags {
		//	manifestTagIndexPathSpec:              <root>/v2/repositories/<name>/_manifests/tags/<tag>/index/
		//	manifestTagIndexEntryPathSpec:         <root>/v2/repositories/<name>/_manifests/tags/<tag>/index/<algorithm>/<hex digest>/
		//	manifestTagIndexEntryLinkPathSpec:     <root>/v2/repositories/<name>/_manifests/tags/<tag>/index/<algorithm>/<hex digest>/link
		indexEntry, err := pathFor(manifestTagIndexEntryPathSpec{name: name, revision: dgst, tag: tag})
		if err != nil {
			return err
		}

		_, err = v.driver.Stat(v.ctx, indexEntry)
		if err != nil {
			switch err := err.(type) {
			case driver.PathNotFoundError:
				dcontext.GetLogger(v.ctx).Infof("outdated manifest hist tag reference: %s -> not deleting", indexEntry)
				continue
			default:
				return err
			}
		}
		dcontext.GetLogger(v.ctx).Infof("deleting manifest hist tag reference: %s", indexEntry)
		err = v.driver.Delete(v.ctx, indexEntry)
		if err != nil {
			return err
		}
	}

	for _, tag := range currentTags {
		tagPath, err := pathFor(manifestTagPathSpec{
			name: name,
			tag:  tag,
		})
		if err != nil {
			return err
		}

		revision, err := readLink(v.ctx, v.driver, tagPath)
		if err != nil {
			switch err.(type) {
			case storagedriver.PathNotFoundError:
				dcontext.GetLogger(v.ctx).Infof("tag %s not known anymore -> skipping", tag)
				continue
			}

			return err
		}

		if revision == dgst {
			dcontext.GetLogger(v.ctx).Infof("tag %s now points to %v and not to %v anymore -> skipping", tag, revision, dgst)
			continue

		}
		dcontext.GetLogger(v.ctx).Infof("deleting manifest current tag reference: %s", tagPath)
		err = v.driver.Delete(v.ctx, tagPath)
		if err != nil {
			return fmt.Errorf("could not delete %s: %v", tagPath, err)
		}
	}

	manifestPath, err := pathFor(manifestRevisionPathSpec{name: name, revision: dgst})
	if err != nil {
		return err
	}
	dcontext.GetLogger(v.ctx).Infof("deleting manifest: %s", manifestPath)
	return v.driver.Delete(v.ctx, manifestPath)
}

// RemoveRepository removes a repository directory from the
// filesystem
func (v Vacuum) RemoveRepository(repoName string) error {
	rootForRepository, err := pathFor(repositoriesRootPathSpec{})
	if err != nil {
		return err
	}
	repoDir := path.Join(rootForRepository, repoName)
	dcontext.GetLogger(v.ctx).Infof("Deleting repo: %s", repoDir)
	err = v.driver.Delete(v.ctx, repoDir)
	if err != nil {
		return err
	}

	return nil
}

// RemoveLayer removes a layer link path from the storage
func (v Vacuum) RemoveLayer(repoName string, dgst digest.Digest) error {
	layerLinkPath, err := pathFor(layerLinkPathSpec{name: repoName, digest: dgst})
	if err != nil {
		return err
	}
	dcontext.GetLogger(v.ctx).Infof("Deleting layer link path: %s", layerLinkPath)
	err = v.driver.Delete(v.ctx, layerLinkPath)
	if err != nil {
		return err
	}

	return nil
}

func readLink(ctx context.Context, driver driver.StorageDriver, path string) (digest.Digest, error) {
	content, err := driver.GetContent(ctx, path)
	if err != nil {
		return "", err
	}

	linked, err := digest.Parse(string(content))
	if err != nil {
		return "", err
	}

	return linked, nil
}
