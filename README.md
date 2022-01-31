# cbt-rls-test-go

This example performs the following
- Creates a `bigtable.AdminClient` to the CBT test admin endpoint
  - Creates a table in the GCP project and bigtable instance, configured via command-line arguments
- Creates a `bigtable.Client` for data operations
  - Creates a `grpc.ClientConn` to the CBT test data endpoint
    - This gRPC channel is injected with a service config which configures the use of RLS LB policy
  - Configures the `bigtable.Client` to use the above `grpc.ClientConn`
  - Configures the `bigtable.Client` to use an appProfile, if specified via command-line arguments
- Writes some data to the table
- Reads the data back (as single rows, and as entire table)
- Deletes the table