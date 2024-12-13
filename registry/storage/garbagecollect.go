package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
)

func emit(format string, a ...interface{}) {
	fmt.Printf(format+"\n", a...)
}

type NeededImages map[digest.Digest]struct{}
type ExhaustiveNeededImages map[string]*NeededImages

// GCOpts contains options for garbage collector
type GCOpts struct {
	DryRun           bool
	RemoveUntagged   bool
	OlderThan        time.Time
	ExhaustiveNeeded ExhaustiveNeededImages
}

type ToDelete struct {
	BlobsToDelete     map[digest.Digest]struct{}
	LayersToDelete    map[string][]digest.Digest
	ManifestsToDelete []ManifestDel
}

// ManifestDel contains manifest structure which will be deleted
type ManifestDel struct {
	Name        string
	Digest      digest.Digest
	CurrentTags []string
	HistTags    []string
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
	allBlobs := make(map[digest.Digest]int64)
	maybeDeleteBlobs := make(map[digest.Digest]struct{})
	err = blobService.Enumerate(ctx, func(dgst digest.Digest, modTime time.Time, size int64) error {
		if !opts.OlderThan.IsZero() && modTime.After(opts.OlderThan) {
			return nil
		}

		allBlobs[dgst] = size
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
		repoUsedBlobs := make(map[digest.Digest]int64)

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

		tagsService := repository.Tags(ctx)

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
				if _, needed := (*exhaustiveNeeded)[dgst]; !needed {
					removeDueToNotNeeded = true
					emit("remove manifest %s: not needed", dgst)
				}
			}

			if !removeDueToNotNeeded && opts.RemoveUntagged {
				// fetch all tags where this manifest is the latest one
				tags, _, err := tagsService.Lookup2(ctx, distribution.Descriptor{Digest: dgst})
				if err != nil {
					return fmt.Errorf("failed to retrieve tags for digest %v: %v", dgst, err)
				}
				if len(tags) == 0 {
					removeDueToUntagged = true
					emit("remove manifest %s: untagged", dgst)
				}
			}

			if removeDueToUntagged || removeDueToNotNeeded {
				// Get tags that currently refer to this manifest and those that used to refer to it.
				currentTags, histTags, err := tagsService.Lookup2(ctx, distribution.Descriptor{Digest: dgst})
				if err != nil {
					return fmt.Errorf("failed to retrieve current tags for digest %v: %v", dgst, err)
				}
				manifestArr = append(manifestArr, ManifestDel{Name: repoName, Digest: dgst, CurrentTags: currentTags, HistTags: histTags})
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
					emit("%s: marking blob %s for not_delete", repoName, d)
				}
				repoUsedBlobs[dgst] = allBlobs[dgst]
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
				repoUsedBlobs[dgst] = allBlobs[dgst]
			}
			return nil
		})
		if len(deleteLayers) > 0 {
			deleteLayerSet[repoName] = deleteLayers
		}
		if err != nil {
			return err
		}

		var totalBlobSize int64
		for _, size := range repoUsedBlobs {
			totalBlobSize += size
		}

		emit("Repo blob size %s: %d MB", repoName, totalBlobSize/1024/1024)
	}

	manifestArr = unmarkReferencedManifest(manifestArr, markSet)

	// sweep
	emit("\n%d blobs marked, %d blobs and %d manifests eligible for deletion", len(markSet), len(maybeDeleteBlobs), len(manifestArr))

	return Sweep(ctx, storageDriver, opts.DryRun, ToDelete{
		ManifestsToDelete: manifestArr,
		BlobsToDelete:     maybeDeleteBlobs,
		LayersToDelete:    deleteLayerSet,
	})
}

func Sweep(ctx context.Context, storageDriver driver.StorageDriver, dryRun bool, what ToDelete) error {
	vacuum := NewVacuum(ctx, storageDriver)

	for _, obj := range what.ManifestsToDelete {
		emit("PMDELETEMANIFEST %s|%s|%s|%s", obj.Name, obj.Digest, strings.Join(obj.CurrentTags, ","), strings.Join(obj.HistTags, ","))
		if dryRun {
			continue
		}

		err := vacuum.RemoveManifest(obj.Name, obj.Digest, obj.CurrentTags, obj.HistTags)
		if err != nil {
			return fmt.Errorf("failed to delete manifest %s: %v", obj.Digest, err)
		}
	}

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
		for _, dgst := range dgsts {
			emit("PMDELETELAYER %s|%s", repo, dgst)
			if dryRun {
				continue
			}

			err := vacuum.RemoveLayer(repo, dgst)
			if err != nil {
				return fmt.Errorf("failed to delete layer link %s of repo %s: %v", dgst, repo, err)
			}
			time.Sleep(500 * time.Millisecond)
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
