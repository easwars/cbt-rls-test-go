package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/bigtable"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	projectID       = flag.String("project_id", "directpath-prod-manual-testing", "GCP project to use")
	instanceID      = flag.String("instance_id", "blackbox-us-central1-b", "Cloud Bigtable instance to use")
	tableID         = flag.String("table_id", "easwars-test-table", "Cloud Bigtable table to use")
	columnFamily    = flag.String("column_family", "cf1", "Cloud Bigtable column family to use")
	columnQualifier = flag.String("column_qualifier", "greeting", "Cloud Bigtable column qualifier to use")
	rowKeyPrefix    = flag.String("row_key_prefix", "row_key_", "Cloud Bigtable row key to use")
)

const (
	cbtAdminTestEndpoint = "test-bigtableadmin.sandbox.googleapis.com:443"
	cbtDataTestEndpoint  = "test-bigtable.sandbox.googleapis.com:443"
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

	// TODO: Create gRPC clientConn with RLS config, pointing to the CBT dataTestEndpoint, and pass it to bigtable.NewClient() with a WithGRPCConn() ClientOption.
	// cc, err := grpc.Dial(cbtDataTestEndpoint, grpc.WithDisableServiceConfig(), grpc.WithDefaultServiceConfig())
	dataClient, err := bigtable.NewClient(ctx, *projectID, *instanceID, option.WithEndpoint(cbtDataTestEndpoint))
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

	log.Printf("Attempting to delete table %q...\n", *tableID)
	if err := adminClient.DeleteTable(ctx, *tableID); err != nil {
		log.Fatalf("Failed to delete table %q: %v", *tableID, err)
	}
	log.Printf("Table %q deleted\n", *tableID)
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
