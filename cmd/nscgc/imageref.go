package main

import (
	"fmt"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
)

type Ref struct {
	Registry   string
	Repository string
	Tag        string
	Digest     string
}

func ParseImageRef(imageRef string) (Ref, error) {
	parts := strings.Split(imageRef, "@")
	if len(parts) > 2 {
		return Ref{}, fmt.Errorf("invalid image reference, must include at most one '@' separator, saw %q", imageRef)
	}

	t, err := name.NewTag(parts[0])
	if err != nil {
		return Ref{}, err
	}

	ref := Ref{}
	if len(parts) == 2 {
		ref.Digest = parts[1]
	}

	ref.Registry = t.RegistryStr()
	ref.Repository = t.RepositoryStr()
	ref.Tag = t.TagStr()

	return ref, nil
}
