package storage

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/manifest/ocischema"
	"github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
)

func emit(format string, a ...interface{}) {
	fmt.Printf(format+"\n", a...)
}

type NeededDigests map[digest.Digest]struct{}

type ExhaustiveNeededImages struct {
	Pattern *regexp.Regexp
	Digests NeededDigests
}

// GCOpts contains options for garbage collector
type GCOpts struct {
	DryRun           bool
	RemoveUntagged   bool
	OlderThan        time.Time
	ExhaustiveNeeded *ExhaustiveNeededImages // If non-nil, manifests older than OlderThan which are not matched by this will get removed
	DeleteManifests  bool                    // If true, all manifests older than OlderThan will be removed
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
	if opts.DeleteManifests {
		emit("Deleting manifests")
	} else {
		emit("Not deleting manifests")
	}

	emit("Considering all objects older than %s for deletion\n", opts.OlderThan.Format(time.RFC3339))

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
	allBlobs := make(map[digest.Digest]int64)        // all blobs and their size
	blobModTime := make(map[digest.Digest]time.Time) // all blobs and their mod time
	maybeDeleteBlobs := make(map[digest.Digest]struct{})
	err = blobService.Enumerate(ctx, func(dgst digest.Digest, modTime time.Time, size int64) error {
		blobModTime[dgst] = modTime
		allBlobs[dgst] = size

		if modTime.Before(opts.OlderThan) {
			maybeDeleteBlobs[dgst] = struct{}{}
		}

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

		neededDigests := neededDigestsForRepo(opts.ExhaustiveNeeded, repoName)
		emit("Looking at repo %s (exhaustive needed: %d)", repoName, len(neededDigests))

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

		// Note that if there's no needed image for a repo it currently means "keep all manifests in this repo".
		if len(neededDigests) != 0 {
			additionalNeeded, err := resolveManifestIndices(ctx, manifestService, neededDigests)
			if err != nil {
				return err
			}

			for img := range additionalNeeded {
				neededDigests[img] = struct{}{}
			}
		}

		manifestEnumerator, ok := manifestService.(distribution.ManifestEnumerator)
		if !ok {
			return fmt.Errorf("unable to convert ManifestService into ManifestEnumerator")
		}

		err = manifestEnumerator.Enumerate(ctx, func(dgst digest.Digest) error {
			modTime, hasModTime := blobModTime[dgst]

			if !hasModTime {
				// in this case it was probably uploaded whilst the run started -> we will skip deleting it
				emit("manifest %s does not have a mod time", dgst)
			}

			removeDueToExpired := false
			removeDueToUntagged := false
			removeDueToNotNeeded := false

			if opts.DeleteManifests && hasModTime && modTime.Before(opts.OlderThan) {
				removeDueToExpired = true
			}

			if !removeDueToExpired && len(neededDigests) > 0 {
				// For this repo we know exhaustively which images are needed.
				// If this one is not part of that, get rid of it :-)
				needed, err := manifestNeeded(ctx, tagsService, neededDigests, dgst)
				if err != nil {
					return err
				}

				if !needed {
					if hasModTime && modTime.After(opts.OlderThan) {
						emit("keeping manifest %s due to recent mod time %s even though it's not in exhaustive needed set", dgst, modTime.Format(time.RFC3339))
					} else {
						removeDueToNotNeeded = true
						emit("remove manifest %s: not needed", dgst)
					}
				}
			}

			if !removeDueToExpired && !removeDueToNotNeeded && opts.RemoveUntagged {
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

			if removeDueToExpired || removeDueToUntagged || removeDueToNotNeeded {
				// Get tags that currently refer to this manifest and those that used to refer to it.
				currentTags, histTags, err := tagsService.Lookup2(ctx, distribution.Descriptor{Digest: dgst})
				if err != nil {
					return fmt.Errorf("failed to retrieve current tags for digest %v: %v", dgst, err)
				}
				manifestArr = append(manifestArr, ManifestDel{Name: repoName, Digest: dgst, CurrentTags: currentTags, HistTags: histTags})
				return nil
			}

			// Mark the manifest's blob
			emit("%s: marking manifest %s for not_delete", repoName, dgst)
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
			mt := blobModTime[dgst]
			if _, shouldBeDeleted := maybeDeleteBlobs[dgst]; shouldBeDeleted {
				emit("mark layer %s %s for delete, %s", repoName, dgst, mt.Format(time.RFC3339))
				deleteLayers = append(deleteLayers, dgst)
			} else {
				emit("mark layer %s %s for not_delete, %s", repoName, dgst, mt.Format(time.RFC3339))
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

	manifestArr = unmarkReferencedManifest(manifestArr, markSet, blobModTime)

	var totalBlobSize int64
	for dgst := range maybeDeleteBlobs {
		totalBlobSize += allBlobs[dgst]
	}

	// sweep
	emit("\n%d blobs marked, %d blobs and %d manifests eligible for deletion", len(markSet), len(maybeDeleteBlobs), len(manifestArr))
	emit("will free %d MB in total", totalBlobSize/1024/1024)

	return Sweep(ctx, storageDriver, opts.DryRun, ToDelete{
		ManifestsToDelete: manifestArr,
		BlobsToDelete:     maybeDeleteBlobs,
		LayersToDelete:    deleteLayerSet,
	})
}

// If this returns something with len() == 0 it currently means "keep all digests"
func neededDigestsForRepo(en *ExhaustiveNeededImages, repoName string) NeededDigests {
	if en != nil && en.Pattern != nil && en.Pattern.MatchString(repoName) {
		return en.Digests
	}

	return nil
}

func resolveManifestIndices(ctx context.Context, manifestService distribution.ManifestService, needed NeededDigests) (NeededDigests, error) {
	res := NeededDigests{}
	emit("resolving manifest indices ..")
	for img := range needed {
		manifest, err := manifestService.Get(ctx, img)
		if _, unknown := err.(*distribution.ErrManifestUnknown); unknown {
			continue
		}

		index, ok := manifest.(*ocischema.DeserializedImageIndex)
		if !ok {
			continue
		}
		for _, man := range index.Manifests {
			res[man.Digest] = struct{}{}
			emit("for index %s discovered %s", img.Encoded(), man.Digest.Encoded())
		}
	}
	emit("  resolving manifest indices done")

	return res, nil
}

// Checks if `dgst` is needed according to `needed`.
// Needed case 1: `dgst` may be needed directly, i.e. `needed` contains it.
// Needed case 2: If there is a tag t1 that points to `dgst`, and if t1's name happens to contain a digest (as a sha256 hash), and if *that* digest is needed, `dgst` is also needed.
// This is checked transitively up to a certain depth.`
func manifestNeeded(ctx context.Context, tagsService distribution.TagService, needed NeededDigests, dgst digest.Digest) (bool, error) {
	return manifestNeededInner(ctx, tagsService, needed, []digest.Digest{dgst})
}

func manifestNeededInner(ctx context.Context, tagsService distribution.TagService, needed NeededDigests, path []digest.Digest) (bool, error) {
	if len(path) == 0 {
		return false, fmt.Errorf("internal error: manifestNeededInner called without a path - needs at least one element")
	}
	if len(path) >= 3 {
		return false, fmt.Errorf("too long manifest tag reference chain %s", formatManifestChain(path))
	}
	root := path[0]
	leaf := path[len(path)-1]
	if _, leafNeeded := needed[leaf]; leafNeeded {
		emit("manifest %s: needed: %s matched exhaustive needed set", root, formatManifestChain(path))
		return true, nil
	}

	// fetch all tags where this manifest is the latest one
	tags, _, err := tagsService.Lookup2(ctx, distribution.Descriptor{Digest: leaf})
	if err != nil {
		return false, fmt.Errorf("failed to fetch tags of %s: %v", leaf.Encoded(), err)
	}

	for _, tag := range tags {
		referencedDigest := findDigest(tag)
		if referencedDigest == "" {
			continue
		}

		innerDgst := digest.NewDigestFromEncoded(digest.SHA256, referencedDigest)

		// Don't follow circles (e.g. a manifest that has a tag that mentions its own digest)
		alreadySeen := false
		for _, part := range path {
			if innerDgst == part {
				alreadySeen = true
				break
			}
		}

		if alreadySeen {
			continue
		}

		innerNeeded, err := manifestNeededInner(ctx, tagsService, needed, append(path, innerDgst))
		if err != nil {
			return false, err
		}

		if innerNeeded {
			return true, nil
		}
	}

	return false, nil
}

func formatManifestChain(m []digest.Digest) string {
	if len(m) == 0 {
		return "(empty path)"
	}

	res := m[0].Encoded()

	for i := 1; i < len(m); i++ {
		res += "->" + m[i].Encoded()
	}

	return res
}

var (
	// sha256
	digestRegex = regexp.MustCompile("[A-Fa-f0-9]{64}")
)

func findDigest(in string) string {
	return digestRegex.FindString(in)
}

func Sweep(ctx context.Context, storageDriver driver.StorageDriver, dryRun bool, what ToDelete) error {
	vacuum := NewVacuum(ctx, storageDriver)

	for _, obj := range what.ManifestsToDelete {
		emit("DEL_MANIFEST %s|%s|%s|%s", obj.Name, obj.Digest, strings.Join(obj.CurrentTags, ","), strings.Join(obj.HistTags, ","))
		if dryRun {
			continue
		}

		err := vacuum.RemoveManifest(obj.Name, obj.Digest, obj.CurrentTags, obj.HistTags)
		if err != nil {
			return fmt.Errorf("failed to delete manifest %s: %v", obj.Digest, err)
		}
	}

	for dgst := range what.BlobsToDelete {
		emit("DEL_BLOB %s", dgst)
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
			emit("DEL_LAYER %s|%s", repo, dgst)
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
func unmarkReferencedManifest(manifestArr []ManifestDel, markSet map[digest.Digest]struct{}, blobs map[digest.Digest]time.Time) []ManifestDel {
	filtered := make([]ManifestDel, 0)
	for _, obj := range manifestArr {
		if _, ok := markSet[obj.Digest]; !ok {
			emit("manifest eligible for deletion: %s, %s", obj, blobs[obj.Digest].Format(time.RFC3339))

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
