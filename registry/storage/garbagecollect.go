package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
)

func emit(format string, a ...interface{}) {
	fmt.Printf(format+"\n", a...)
}

type NeededImages map[string]struct{}
type ExhaustiveNeededImages map[string]*NeededImages

// GCOpts contains options for garbage collector
type GCOpts struct {
	DryRun           bool
	RemoveUntagged   bool
	OlderThan        time.Time
	ExhaustiveNeeded ExhaustiveNeededImages
}

type FromDryRun struct {
	BlobsToDelete  map[string]struct{}
	LayersToDelete map[string]map[digest.Digest]struct{}
	// TODO support for manifests
}

// ManifestDel contains manifest structure which will be deleted
type ManifestDel struct {
	Name   string
	Digest digest.Digest
	Tags   []string
}

// MarkAndSweep performs a mark and sweep of registry data
func MarkAndSweep(ctx context.Context, storageDriver driver.StorageDriver, registry distribution.Namespace, opts GCOpts) error {
	repositoryEnumerator, ok := registry.(distribution.RepositoryEnumerator)
	if !ok {
		return fmt.Errorf("unable to convert Namespace to RepositoryEnumerator")
	}

	repos := make(map[string]struct{})
	err := repositoryEnumerator.Enumerate(ctx, func(repoName string) error {
		emit("will look at repo %s", repoName)
		repos[repoName] = struct{}{}
		return nil
	})
	if err != nil {
		return err
	}

	blobService := registry.Blobs()
	maybeDeleteBlobs := make(map[digest.Digest]struct{})
	err = blobService.Enumerate(ctx, func(dgst digest.Digest, modTime time.Time) error {
		if !opts.OlderThan.IsZero() && modTime.After(opts.OlderThan) {
			return nil
		}

		maybeDeleteBlobs[dgst] = struct{}{}
		return nil
	})
	if err != nil {
		return fmt.Errorf("error enumerating blobs: %v", err)
	}

	// mark all blobs referenced by manifests as not to be deleted
	markSet := make(map[digest.Digest]struct{})
	deleteLayerSet := make(map[string][]digest.Digest)
	manifestArr := make([]ManifestDel, 0)
	for repoName := range repos {
		emit("Looking at repo %s", repoName)
		exhaustiveNeeded := opts.ExhaustiveNeeded[repoName]

		var err error
		named, err := reference.WithName(repoName)
		if err != nil {
			return fmt.Errorf("failed to parse repo name %s: %v", repoName, err)
		}
		repository, err := registry.Repository(ctx, named)
		if err != nil {
			return fmt.Errorf("failed to construct repository: %v", err)
		}

		manifestService, err := repository.Manifests(ctx)
		if err != nil {
			return fmt.Errorf("failed to construct manifest service: %v", err)
		}

		manifestEnumerator, ok := manifestService.(distribution.ManifestEnumerator)
		if !ok {
			return fmt.Errorf("unable to convert ManifestService into ManifestEnumerator")
		}

		err = manifestEnumerator.Enumerate(ctx, func(dgst digest.Digest) error {
			removeDueToUntagged := false
			removeDueToNotNeeded := false

			if exhaustiveNeeded != nil && len(*exhaustiveNeeded) > 0 {
				// For this repo we know exhaustively which images are needed.
				// If this one is not part of that, get rid of it :-)
				if _, needed := (*exhaustiveNeeded)[string(dgst)]; !needed {
					removeDueToNotNeeded = true
					emit("remove manifest %s: not needed", dgst)
				}
			}

			if !removeDueToNotNeeded && opts.RemoveUntagged {
				// fetch all tags where this manifest is the latest one
				tags, err := repository.Tags(ctx).Lookup(ctx, distribution.Descriptor{Digest: dgst})
				if err != nil {
					return fmt.Errorf("failed to retrieve tags for digest %v: %v", dgst, err)
				}
				if len(tags) == 0 {
					removeDueToUntagged = true
					emit("remove manifest %s: untagged", dgst)
				}
			}

			if removeDueToUntagged || removeDueToNotNeeded {
				// fetch all tags from repository
				// all of these tags could contain manifest in history
				// which means that we need check (and delete) those references when deleting manifest
				allTags, err := repository.Tags(ctx).All(ctx)
				if err != nil {
					if _, ok := err.(distribution.ErrRepositoryUnknown); ok {
						emit("manifest tags path of repository %s does not exist", repoName)
						return nil
					}
					return fmt.Errorf("failed to retrieve tags %v", err)
				}
				manifestArr = append(manifestArr, ManifestDel{Name: repoName, Digest: dgst, Tags: allTags})
				return nil
			}

			// Mark the manifest's blob
			emit("%s: marking manifest %s ", repoName, dgst)
			delete(maybeDeleteBlobs, dgst)
			markSet[dgst] = struct{}{}

			return markManifestReferences(dgst, manifestService, ctx, func(d digest.Digest) bool {
				delete(maybeDeleteBlobs, d)
				_, marked := markSet[d]
				if !marked {
					markSet[d] = struct{}{}
					emit("%s: marking blob %s", repoName, d)
				}
				return marked
			})
		})

		if err != nil {
			// In certain situations such as unfinished uploads, deleting all
			// tags in S3 or removing the _manifests folder manually, this
			// error may be of type PathNotFound.
			//
			// In these cases we can continue marking other manifests safely.
			if _, ok := err.(driver.PathNotFoundError); !ok {
				return err
			}
		}
		blobService := repository.Blobs(ctx)
		layerEnumerator, ok := blobService.(distribution.ManifestEnumerator)
		if !ok {
			return errors.New("unable to convert BlobService into ManifestEnumerator")
		}

		var deleteLayers []digest.Digest
		err = layerEnumerator.Enumerate(ctx, func(dgst digest.Digest) error {
			if _, shouldBeDeleted := maybeDeleteBlobs[dgst]; shouldBeDeleted {
				emit("mark layer %s %s for delete", repoName, dgst)
				deleteLayers = append(deleteLayers, dgst)
			} else {
				emit("mark layer %s %s for not_delete", repoName, dgst)
			}
			return nil
		})
		if len(deleteLayers) > 0 {
			deleteLayerSet[repoName] = deleteLayers
		}
		if err != nil {
			return err
		}
	}

	manifestArr = unmarkReferencedManifest(manifestArr, markSet)

	// sweep
	vacuum := NewVacuum(ctx, storageDriver)
	if !opts.DryRun {
		for _, obj := range manifestArr {
			err = vacuum.RemoveManifest(obj.Name, obj.Digest, obj.Tags)
			if err != nil {
				return fmt.Errorf("failed to delete manifest %s: %v", obj.Digest, err)
			}
		}
	}
	emit("\n%d blobs marked, %d blobs and %d manifests eligible for deletion", len(markSet), len(maybeDeleteBlobs), len(manifestArr))
	for dgst := range maybeDeleteBlobs {
		emit("PMDELETEBLOB %s", dgst)
		if opts.DryRun {
			continue
		}
		err = vacuum.RemoveBlob(string(dgst))
		if err != nil {
			return fmt.Errorf("failed to delete blob %s: %v", dgst, err)
		}
	}

	for repo, dgsts := range deleteLayerSet {
		for _, dgst := range dgsts {
			emit("PMDELETELAYER %s %s", repo, dgst)
			if opts.DryRun {
				continue
			}
			err = vacuum.RemoveLayer(repo, dgst)
			if err != nil {
				return fmt.Errorf("failed to delete layer link %s of repo %s: %v", dgst, repo, err)
			}
		}
	}

	return err
}

// TODO deduplicate with the above
// TODO add support for remoing manifests
func Sweep(ctx context.Context, storageDriver driver.StorageDriver, dryRun bool, what FromDryRun) error {
	vacuum := NewVacuum(ctx, storageDriver)

	for dgst := range what.BlobsToDelete {
		emit("PMDELETEBLOB %s", dgst)
		if dryRun {
			continue
		}
		err := vacuum.RemoveBlob(string(dgst))
		if err != nil {
			return fmt.Errorf("failed to delete blob %s: %v", dgst, err)
		}
	}

	for repo, dgsts := range what.LayersToDelete {
		for dgst := range dgsts {
			emit("PMDELETELAYER %s %s", repo, dgst)
			if dryRun {
				continue
			}
			err := vacuum.RemoveLayer(repo, dgst)
			if err != nil {
				return fmt.Errorf("failed to delete layer link %s of repo %s: %v", dgst, repo, err)
			}
		}
	}

	return nil
}

// unmarkReferencedManifest filters out manifest present in markSet
func unmarkReferencedManifest(manifestArr []ManifestDel, markSet map[digest.Digest]struct{}) []ManifestDel {
	filtered := make([]ManifestDel, 0)
	for _, obj := range manifestArr {
		if _, ok := markSet[obj.Digest]; !ok {
			emit("manifest eligible for deletion: %s", obj)
			filtered = append(filtered, obj)
		}
	}
	return filtered
}

// markManifestReferences marks the manifest references
func markManifestReferences(dgst digest.Digest, manifestService distribution.ManifestService, ctx context.Context, ingester func(digest.Digest) bool) error {
	manifest, err := manifestService.Get(ctx, dgst)
	if err != nil {
		return fmt.Errorf("failed to retrieve manifest for digest %v: %v", dgst, err)
	}

	descriptors := manifest.References()
	for _, descriptor := range descriptors {

		// do not visit references if already marked
		if ingester(descriptor.Digest) {
			continue
		}

		if ok, _ := manifestService.Exists(ctx, descriptor.Digest); ok {
			err := markManifestReferences(descriptor.Digest, manifestService, ctx, ingester)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
