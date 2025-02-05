package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/distribution/distribution/v3/configuration"
	"github.com/distribution/distribution/v3/registry/storage"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
	_ "github.com/distribution/distribution/v3/registry/storage/driver/s3-aws"
)

var (
	dryRun                   = flag.Bool("dry_run", true, "do not actually remove the blobs")
	removeUntagged           = flag.Bool("remove_untagged", false, "delete manifests that are not currently referenced via tag")
	exhaustiveNeededImages   = flag.String("exhaustive_needed", "", "file that contains image manifests that are needed")
	exhaustiveNeededImagesV2 = flag.String("exhaustive_needed_v2", "", "file that contains image manifests that are needed, v2")
	realRunFromDryRun        = flag.String("real_run_from_dry_run", "", "pass dry run log, will delete what dry run marked")
	dropManifestsOlderThan   = flag.String("drop_manifests_older_than", "", "if passed, manifests older than this age will be dropped")
)

func main() {
	flag.Parse()

	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	var exhaustiveNeeded storage.ExhaustiveNeededImages
	if *exhaustiveNeededImages != "" {
		if en, err := parseExhaustiveNeeded(*exhaustiveNeededImages); err != nil {
			return err
		} else {
			exhaustiveNeeded = en
		}

		dumpExhaustiveNeeded(exhaustiveNeeded)
	}

	if *exhaustiveNeededImagesV2 != "" {
		if en, err := parseExhaustiveNeededV2(*exhaustiveNeededImagesV2); err != nil {
			return err
		} else {
			exhaustiveNeeded = en
		}

		dumpExhaustiveNeeded(exhaustiveNeeded)
	}

	var fromDryRun storage.ToDelete
	if *realRunFromDryRun != "" {
		if fd, err := parseFromDryRun(*realRunFromDryRun); err != nil {
			return err
		} else {
			fromDryRun = fd
		}
	}

	nsConfig := Config{
		S3Storage: &S3Storage{
			Bucket:             os.Getenv("S3_BUCKET_NAME"),
			AccessKey:          os.Getenv("S3_ACCESS_KEY"),
			SecretKey:          os.Getenv("S3_SECRET_ACCESS_KEY"),
			Region:             "us-east-1", // We use minio so this parameter doesn't matter.
			RegionEndpoint:     os.Getenv("S3_ENDPOINT"),
			Accelerate:         false,
			InsecureSkipVerify: true,
		},
	}

	config := MakeDistributionConfig(nsConfig)

	driver, err := factory.Create(ctx, config.Storage.Type(), config.Storage.Parameters())
	if err != nil {
		return fmt.Errorf("failed to make storage: %w", err)
	}

	if len(fromDryRun.BlobsToDelete) != 0 || len(fromDryRun.LayersToDelete) != 0 {
		if err := storage.Sweep(ctx, driver, *dryRun, fromDryRun); err != nil {
			return fmt.Errorf("failed to sweep: %v", err)
		}
		return nil
	}

	registry, err := storage.NewRegistry(ctx, driver)
	if err != nil {
		return fmt.Errorf("failed to make registry: %w", err)
	}

	olderThan := time.Now().UTC().Add(-7 * 24 * time.Hour)
	deleteManifests := false

	if dropManifestsOlderThan != nil && *dropManifestsOlderThan != "" {
		since, err := time.ParseDuration(*dropManifestsOlderThan)
		if err != nil {
			return err
		}

		olderThan = time.Now().UTC().Add(-since)
		deleteManifests = true
	}

	if err := storage.MarkAndSweep(ctx, driver, registry, storage.GCOpts{
		DryRun:           *dryRun,
		RemoveUntagged:   *removeUntagged,
		OlderThan:        olderThan,
		ExhaustiveNeeded: exhaustiveNeeded,
		DeleteManifests:  deleteManifests,
	}); err != nil {
		return fmt.Errorf("failed to garbage collect: %v", err)
	}

	return nil
}

type Config struct {
	DataDir                string
	S3Storage              *S3Storage
	BaseDomain             string
	AccessController       string
	TenantsInPath          bool
	ProxyURL               string
	DisableHealthChecks    bool
	DisableStorageRedirect bool
	MetricsAddr            string
}

type S3Storage struct {
	AccessKey                   string
	SecretKey                   string
	Region                      string
	RegionEndpoint              string
	Bucket                      string
	Accelerate                  bool
	Insecure                    bool
	InsecureSkipVerify          bool
	ChunkSize                   int
	MultipartCopyChunkSize      int
	MultipartCopyMaxConcurrency int
	MultipartCopyThresholdsize  int
}

func MakeDistributionConfig(cfg Config) *configuration.Configuration {
	config := &configuration.Configuration{
		Version: "0.1",
		Storage: configuration.Storage{
			"cache": configuration.Parameters{
				"blobdescriptor": "inmemory",
			},
		},
	}

	if !cfg.TenantsInPath && cfg.BaseDomain != "" {
		config.Middleware = map[string][]configuration.Middleware{
			"registry": {
				{Name: "namespace"},
			},
		}
	}

	if cfg.DataDir != "" {
		config.Storage["filesystem"] = configuration.Parameters{
			"rootdirectory": cfg.DataDir,
		}
	}

	if cfg.S3Storage != nil {
		sssParams := configuration.Parameters{
			"accesskey":      cfg.S3Storage.AccessKey,
			"secretkey":      cfg.S3Storage.SecretKey,
			"region":         cfg.S3Storage.Region,
			"bucket":         cfg.S3Storage.Bucket,
			"skipverify":     cfg.S3Storage.InsecureSkipVerify,
			"accelerate":     cfg.S3Storage.Accelerate,
			"forcepathstyle": true,
		}

		if cfg.S3Storage.Insecure {
			sssParams["secure"] = false
		}

		if cfg.S3Storage.RegionEndpoint != "" {
			sssParams["regionendpoint"] = cfg.S3Storage.RegionEndpoint
		}

		if cfg.S3Storage.ChunkSize != 0 {
			sssParams["chunksize"] = cfg.S3Storage.ChunkSize
		}
		if cfg.S3Storage.MultipartCopyChunkSize != 0 {
			sssParams["multipartcopychunksize"] = cfg.S3Storage.MultipartCopyChunkSize
		}
		if cfg.S3Storage.MultipartCopyMaxConcurrency != 0 {
			sssParams["multipartcopymaxconcurrency"] = cfg.S3Storage.MultipartCopyMaxConcurrency
		}
		if cfg.S3Storage.MultipartCopyThresholdsize != 0 {
			sssParams["multipartcopythresholdsize"] = cfg.S3Storage.MultipartCopyThresholdsize
		}

		config.Storage["s3"] = sssParams
	}

	if cfg.DisableStorageRedirect {
		config.Storage["redirect"] = configuration.Parameters{
			"disable": true,
		}
	}

	if cfg.ProxyURL != "" {
		config.Proxy = configuration.Proxy{
			RemoteURL: cfg.ProxyURL,
		}
	}

	config.HTTP.Secret = os.Getenv("REGISTRY_HTTP_SECRET")

	config.Health.StorageDriver.Enabled = true
	// Cargo-culted.
	config.Health.StorageDriver.Interval = 10 * time.Second
	config.Health.StorageDriver.Threshold = 3

	return config
}
