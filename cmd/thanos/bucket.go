package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"text/template"
	"time"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

func registerBucket(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "inspect metric data in an object storage bucket")

	gcsBucket := cmd.Flag("gcs-bucket", "Google Cloud Storage bucket name for stored blocks.").
		PlaceHolder("<bucket>").Required().String()

	ls := cmd.Command("ls", "list all blocks in the bucket")

	lsFmt := ls.Flag("ouput", "format in which to print each block's information; may be 'json' or custom template").
		Short('o').Default("").String()

	m[name+" ls"] = func(g *run.Group, logger log.Logger, _ *prometheus.Registry, _ opentracing.Tracer) error {
		// Dummy actor to immediately kill the group after the run function returns.
		g.Add(func() error { return nil }, func(error) {})

		return runBucketList(logger, *gcsBucket, *lsFmt)
	}
}

func runBucketList(logger log.Logger, gcsBucket, format string) error {
	gcsClient, err := storage.NewClient(context.Background())
	if err != nil {
		return errors.Wrap(err, "create GCS client")
	}

	var bkt objstore.Bucket
	bkt = gcs.NewBucket(gcsBucket, gcsClient.Bucket(gcsBucket), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var printBlock func(name string) error

	switch format {
	case "":
		printBlock = func(name string) error {
			fmt.Fprintln(os.Stdout, name[:len(name)-1])
			return nil
		}
	case "json":
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "\t")

		printBlock = func(name string) error {
			rc, err := bkt.Get(ctx, path.Join(name, "meta.json"))
			if err != nil {
				return errors.Wrap(err, "get reader for meta.json")
			}
			defer rc.Close()

			// Do a full decode/encode cycle to ensure we only print valid JSON.
			var m block.Meta

			if err := json.NewDecoder(rc).Decode(&m); err != nil {
				return errors.Wrap(err, "deocde meta.json")
			}
			return enc.Encode(&m)
		}
	default:
		tmpl, err := template.New("").Parse(format)
		if err != nil {
			return errors.Wrap(err, "invalid template")
		}
		printBlock = func(name string) error {
			rc, err := bkt.Get(ctx, path.Join(name, "meta.json"))
			if err != nil {
				return errors.Wrap(err, "get reader for meta.json")
			}
			defer rc.Close()

			// Do a full decode/encode cycle to ensure we only print valid JSON.
			var m block.Meta

			if err := json.NewDecoder(rc).Decode(&m); err != nil {
				return errors.Wrap(err, "deocde meta.json")
			}

			if err := tmpl.Execute(os.Stdout, &m); err != nil {
				return errors.Wrap(err, "execute template")
			}
			fmt.Fprintln(os.Stdout, "")
			return nil
		}
	}
	return bkt.Iter(ctx, "", printBlock)
}
