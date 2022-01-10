package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"cloud.google.com/go/bigtable"
)

var (
	project     = flag.String("project", "directpath-prod-manual-testing", "GCP project to use")
	cbtInstance = flag.String("cbt_instance", "blackbox-us-central1-b", "Cloud Bigtable instance to use")
)

func main() {
	flag.Parse()

	ctx := context.Background()
	client, err := bigtable.NewAdminClient(ctx, *project, *cbtInstance)
	if err != nil {
		log.Fatalf("Bigtable admin client creation failed: %v", err)
	}
	defer client.Close()

	tables, err := client.Tables(ctx)
	if err != nil {
		log.Fatalf("Failed to read tables using the Bigtable admin client: %v", err)
	}
	fmt.Println(tables)
}
