# Caterpillar

Caterpillar is a batch metrics processing system for turning AV event history into display/input
usage metrics that are stored in a SQL database.

## Production Code

The code currently used in production lives in `v2/`.

Treat the root-level Go code and older packages as legacy unless you are
intentionally investigating historical behavior. This includes the root
`server.go`, root `makefile`, root `go.mod`, `caterpillar/`, `hatchery/`,
`nydus/`, `config/`, `config.json`, and `service-config.json`.

For production work, start here:

- `v2/server.go` - command entrypoint.
- `v2/displayinputcaterpillar/` - main display/input metric processing logic.
- `v2/elkquery/` - Elasticsearch/ELK query client.
- `v2/caterpillarmssql/` - SQL Server connection helpers.
- `v2/makefile` - build and test targets for the production module.

## What `v2` Does

`v2` processes one building at a time. The command takes a building code, finds
matching display/input events in ELK, enriches the resulting time slices with
class schedule data, and writes the metric rows into SQL Server.

At a high level, it:

1. Connects to the metrics SQL Server database.
2. Reads state from `LastKnownStates`.
3. Queries `av-delta-events*` for display event keys:
   `input`, `power`, `active-signal`, and `blanked`.
4. Processes each matching device in the requested building.
5. Rebuilds affected `DisplayInputMetrics` rows for each device.
6. Slices metric intervals by hour and class schedule boundaries.
7. Bulk inserts new `DisplayInputMetrics` rows.
8. Updates the device state with the `UpdateLastKnownState` stored procedure.

Because this process deletes and recreates SQL metric rows from the last known
state forward, run it only against the intended metrics database.

## Required Configuration

Set these environment variables before running `v2`:

| Variable | Purpose |
| --- | --- |
| `METRICS_SQL_CONNECTION_STRING` | SQL Server connection string for metrics reads and writes. |
| `ELK_DIRECT_ADDRESS` | Base URL for direct ELK/Elasticsearch requests. |
| `ELK_SA_USERNAME` | ELK service account username. |
| `ELK_SA_PASSWORD` | ELK service account password. |
| `CLIENT_KEY` | WSO2 client key used for class schedule lookups. |
| `CLIENT_SECRET` | WSO2 client secret used for class schedule lookups. |
| `TOKEN_REFRESH_URL` | WSO2 token endpoint. |

## Running

Run commands from `v2/`.

```sh
cd v2
go run server.go HCEB
```

Replace `HCEB` with the building code you want to process.

To process a list of building codes from a file:

```sh
cd v2
./bulk_run.sh buildings.txt
```

## Testing and Building

Run tests from `v2/`:

```sh
cd v2
go test ./...
```

Build the production binary:

```sh
cd v2
make build
```

The build output is written to `v2/dist/`.

## Repository Layout

```text
.
+-- v2/                         # Production code
|   +-- server.go               # Production entrypoint
|   +-- displayinputcaterpillar # Main processing package
|   +-- elkquery                # ELK query helpers
|   +-- caterpillarmssql        # SQL Server helpers
|   +-- makefile                # Production build/test targets
+-- caterpillar/                # Legacy code
+-- hatchery/                   # Legacy code
+-- nydus/                      # Legacy code
+-- config/                     # Legacy code
+-- server.go                   # Legacy entrypoint
+-- makefile                    # Legacy build file
+-- go.mod                      # Legacy module
```

When making operational or production changes, prefer changing only files under
`v2/` unless there is a specific reason to touch the legacy code.
