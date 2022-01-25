package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/bigtable"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/google"
	"google.golang.org/grpc/status"

	_ "google.golang.org/grpc/balancer/rls" // Register the RLS LB policy
)

var (
	projectID           = flag.String("project_id", "directpath-prod-manual-testing", "GCP project to use")
	instanceID          = flag.String("instance_id", "blackbox-us-central1-b", "Cloud Bigtable instance to use")
	tableID             = flag.String("table_id", "easwars-test-table", "Cloud Bigtable table to use")
	columnFamily        = flag.String("column_family", "cf1", "Cloud Bigtable column family to use")
	columnQualifier     = flag.String("column_qualifier", "greeting", "Cloud Bigtable column qualifier to use")
	rowKeyPrefix        = flag.String("row_key_prefix", "row_key_", "Cloud Bigtable row key to use")
	appProfile          = flag.String("app_profile", "", "Cloud Bigtable application profile to use. If unspecified, the default app profile will be used")
	enableDefaultTarget = flag.Bool("enable_default_target", false, "Whether to set a default target in the service config")
	skipTableDeletion   = flag.Bool("skip_table_deletion", false, "Whether to skip table deletion at the end")
)

const (
	cbtAdminTestEndpoint = "dns:///test-bigtableadmin.sandbox.googleapis.com"
	cbtDataTestEndpoint  = "dns:///test-bigtable.sandbox.googleapis.com"
	cbtRLSTestEndpoint   = "dns:///test-bigtablerls.sandbox.googleapis.com"
	rlsDefaultTarget     = "dns:///test-bigtable.sandbox.googleapis.com"

	// Service config for the RLS LB policy.
	//
	// `lookupService` and `defaultTarget` are to be filled in with the value of
	// the RLS server and the default target respectively.
	//
	// Also contains service config for the gRPC channel to the RLS server. This
	// is required since the CBT RLS server implementation is only available via
	// directpath.
	serviceConfigTmpl = `
{
  "loadBalancingConfig": [
    {
      "rls_experimental": {
        "routeLookupConfig": {
          "grpcKeybuilders": [
            {
              "names": [
                {
                  "service": "google.bigtable.v2.Bigtable"
                }
              ],
              "headers": [
                {
                  "key": "x-goog-request-params",
                  "names": ["x-goog-request-params"]
                },
                {
                  "key": "google-cloud-resource-prefix",
                  "names": ["google-cloud-resource-prefix"]
                }
              ],
              "extraKeys": {
                "host": "server",
                "service": "service",
                "method": "method"
              }
            }
          ],
          "lookupService": "%s",
          "lookupServiceTimeout" : "10s",
          "maxAge": "300s",
          "staleAge" : "240s",
          "cacheSizeBytes": 1000,
          "defaultTarget": "%s"
        },
        "routeLookupChannelServiceConfig": {
          "loadBalancingConfig": [{"grpclb": {"childPolicy": [{"pick_first": {} }] }}]
        },
        "childPolicy": [
          {
            "grpclb": {
              "childPolicy": [
                {
                  "pick_first": {}
                }
              ]
            }
          }
        ],
        "childPolicyConfigTargetFieldName": "serviceName"
      }
    }
  ]
}`
)

func main() {
	flag.Parse()
	log.Printf("Running CBT RLS Test on project %q and instance %q...", *projectID, *instanceID)

	ctx := context.Background()
	adminClient, err := bigtable.NewAdminClient(ctx, *projectID, *instanceID, option.WithEndpoint(cbtAdminTestEndpoint))
	if err != nil {
		log.Fatalf("Bigtable admin client creation failed: %v", err)
	}
	defer adminClient.Close()

	dataClient, err := createDataClient(ctx, *projectID, *instanceID, cbtDataTestEndpoint, *appProfile, *enableDefaultTarget)
	if err != nil {
		log.Fatalf("Bigtable data client creation failed: %v", err)
	}
	defer dataClient.Close()

	log.Printf("Attempting to create table %q with columnFamily %q...\n", *tableID, *columnFamily)
	if err := createTable(ctx, adminClient, *tableID, *columnFamily); err != nil {
		log.Fatalf("Table creation using admin client failed: %v", err)
	}
	log.Printf("Table %q with columnFamily %q created\n", *tableID, *columnFamily)

	const tableCreationWaitDuration = 15 * time.Second
	log.Printf("Waiting %v for table creation to take effect...", tableCreationWaitDuration)
	time.Sleep(tableCreationWaitDuration)

	log.Printf("Attempting to write some greetings to table %q...\n", *tableID)
	if err := writeToTable(ctx, dataClient, *tableID, *columnFamily, *columnQualifier, *rowKeyPrefix); err != nil {
		log.Fatalf("Writing to table using data client failed: %v", err)
	}

	log.Printf("Attempting to read a single row from table %q...\n", *tableID)
	if err := readSingleRowFromTable(ctx, dataClient, *tableID, *rowKeyPrefix); err != nil {
		log.Fatalf("Reading single row from table using data client failed: %v", err)
	}

	log.Printf("Attempting to read the entire table %q...\n", *tableID)
	if err := readEntireTable(ctx, dataClient, *tableID, *rowKeyPrefix); err != nil {
		log.Fatalf("Reading entrire table using data client failed: %v", err)
	}

	if *skipTableDeletion {
		return
	}

	log.Printf("Attempting to delete table %q...\n", *tableID)
	if err := adminClient.DeleteTable(ctx, *tableID); err != nil {
		log.Fatalf("Failed to delete table %q: %v", *tableID, err)
	}
	log.Printf("Table %q deleted\n", *tableID)
}

func createDataClient(ctx context.Context, project, instance, endpoint, appProfile string, enableDefaultTarget bool) (*bigtable.Client, error) {
	defaultTarget := ""
	if enableDefaultTarget {
		defaultTarget = rlsDefaultTarget
	}
	serviceConfig := fmt.Sprintf(serviceConfigTmpl, cbtRLSTestEndpoint, defaultTarget)
	cc, err := grpc.Dial(endpoint,
		grpc.WithDisableServiceConfig(),
		grpc.WithDefaultServiceConfig(serviceConfig),
		grpc.WithCredentialsBundle(google.NewDefaultCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(1<<28), grpc.MaxCallRecvMsgSize(1<<28)),
	)
	if err != nil {
		return nil, err
	}

	opts := []option.ClientOption{option.WithGRPCConn(cc)}
	if appProfile != "" {
		return bigtable.NewClientWithConfig(ctx, project, instance, bigtable.ClientConfig{AppProfile: appProfile}, opts...)
	}
	return bigtable.NewClient(ctx, project, instance, opts...)
}

func createTable(ctx context.Context, adminClient *bigtable.AdminClient, tableID, columnFamily string) error {
	_, err := adminClient.TableInfo(ctx, tableID)
	if err == nil {
		// Table exists, return early.
		return nil
	}

	s, ok := status.FromError(err)
	if !ok { // Non-status error
		return fmt.Errorf("failed to read table %q: %v", tableID, err)
	}
	// NotFound errors are fine. We create a new table if that is the case.
	if s.Code() != codes.NotFound {
		return fmt.Errorf("failed to read rable %q: %s", tableID, s.Message())
	}
	if err := adminClient.CreateTable(ctx, tableID); err != nil {
		return fmt.Errorf("table creation using admin client failed: %v", err)
	}
	if err := adminClient.CreateColumnFamily(ctx, tableID, columnFamily); err != nil {
		return fmt.Errorf("column family creation using admin client failed: %v", err)
	}
	return nil
}

func writeToTable(ctx context.Context, client *bigtable.Client, tableID, columnFamily, columnQualifier, rowKeyPrefix string) error {
	greetings := []string{"Hello World!", "Hello Bigtable!", "Hello Golang!"}
	table := client.Open(tableID)
	if table == nil {
		return fmt.Errorf("failed to open table %q", tableID)
	}

	for i := 0; i < len(greetings); i++ {
		mutation := bigtable.NewMutation()
		mutation.Set(columnFamily, columnQualifier, bigtable.Now(), []byte(greetings[i]))
		if err := table.Apply(ctx, fmt.Sprintf("%s%d", rowKeyPrefix, i), mutation); err != nil {
			return err
		}
		log.Printf("Wrote greeting %q to table\n", greetings[i])
	}

	return nil
}

func readSingleRowFromTable(ctx context.Context, client *bigtable.Client, tableID, rowKeyPrefix string) error {
	table := client.Open(tableID)
	if table == nil {
		return fmt.Errorf("failed to open table %q", tableID)
	}

	rowKey := fmt.Sprintf("%s%d", rowKeyPrefix, 0)
	row, err := table.ReadRow(ctx, rowKey)
	if err != nil {
		return fmt.Errorf("failed to read row %q", rowKey)
	}
	for cf, ris := range row {
		for _, ri := range ris {
			log.Printf("Read row with ColumnFamily: %q, RowKey: %q, Column: %q, Timestamp: %v, Value: %q", cf, ri.Row, ri.Column, ri.Timestamp.Time(), string(ri.Value))
		}
	}

	return nil
}

func readEntireTable(ctx context.Context, client *bigtable.Client, tableID, rowKeyPrefix string) error {
	table := client.Open(tableID)
	if table == nil {
		return fmt.Errorf("failed to open table %q", tableID)
	}

	printRow := func(row bigtable.Row) bool {
		for cf, ris := range row {
			for _, ri := range ris {
				log.Printf("Read row with ColumnFamily: %q, RowKey: %q, Column: %q, Timestamp: %v, Value: %q", cf, ri.Row, ri.Column, ri.Timestamp.Time(), string(ri.Value))
			}
		}
		return true
	}
	if err := table.ReadRows(ctx, bigtable.PrefixRange(rowKeyPrefix), printRow); err != nil {
		return fmt.Errorf("failed to read rows from table %q: %v", tableID, err)
	}

	return nil
}
