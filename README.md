# mybinlogdumper

A tiny MySQL binlog inspection helper built on top of
[`go-mysql`](https://github.com/go-mysql-org/go-mysql).

It can:

- **`gtid`** &nbsp; stream one GTID transaction and print every event in it
- **`search`** &nbsp; scan the binlog for a matching row change or DDL
- **`dump`** &nbsp; raw event dump (equivalent to `mysqlbinlog`, but from a live server)
- **`sptest`** &nbsp; compute a valid start GTID for a slave whose master has advanced `gtid_purged`
- **`status`** &nbsp; print `SHOW MASTER STATUS` / `@@global.gtid_executed` / `@@global.gtid_purged`

## Install

```bash
go install github.com/awayzhao/mybinlogdumper@latest
```

Or clone and build:

```bash
git clone https://github.com/away/mybinlogdumper.git
cd mybinlogdumper
go build -o mbd .
```

## Requirements

- Go &ge; 1.23
- MySQL 5.7 / 8.0 (or MariaDB) with:
  - `binlog_format = ROW`
  - `gtid_mode = ON` (recommended, required by `gtid` / `sptest`)
- MySQL user privileges:
  - `REPLICATION SLAVE, REPLICATION CLIENT` for streaming binlog
  - `SELECT` on the searched tables (only needed by `search -fields=...`)

## Usage

```bash
mbd <subcommand> [flags]
```

Get flags for a subcommand:

```bash
mbd gtid   -h
mbd search -h
```

### Common flags (all subcommands)

| Flag         | Default          | Description                                   |
|--------------|------------------|-----------------------------------------------|
| `-host`      | `127.0.0.1`      | MySQL host                                    |
| `-port`      | `3306`           | MySQL port                                    |
| `-user`      | `root`           | MySQL user                                    |
| `-password`  | `$MYSQL_PWD`     | Password (prefer the env var, not the flag)   |
| `-flavor`    | `mysql`          | `mysql` or `mariadb`                          |
| `-server-id` | random           | Replication server id (must be unique)        |

### Stream flags (`gtid`, `search`, `dump`)

| Flag       | Description                                     |
|------------|-------------------------------------------------|
| `-file`    | Start binlog filename (use with `-pos`)         |
| `-pos`     | Start binlog position (default `4`)             |
| `-gtid`    | Start from a GTID set (takes precedence)        |
| `-limit`   | Stop after N non-heartbeat events (`0` = never) |

### Examples

Dump one transaction starting at a GTID:

```bash
mbd gtid -host 10.0.0.1 -user rep \
  -gtid '3e11fa47-71ca-11e1-9e33-c80aa9429562:23'
```

Search for the row-change that touched `t_user.id=1024`:

```bash
mbd search -host 10.0.0.1 -user rep \
  -db '^app$' -table '^t_user$' \
  -fields 'id=1024' \
  -file mysql-bin.000042 -pos 4
```

Search for a DDL:

```bash
mbd search -host 10.0.0.1 -user rep \
  -ddl 'ALTER TABLE `app`\.`t_user`'
```

Raw dump:

```bash
mbd dump -host 10.0.0.1 -user rep -limit 100 \
  -file mysql-bin.000042 -pos 4
```

Show server status:

```bash
mbd status -host 10.0.0.1 -user rep
```

## Development

```bash
go build ./...
go vet   ./...
go test  ./...
gofmt -l .
```

CI runs the same checks on every push / PR (see `.github/workflows/ci.yml`).

## License

MIT. See [`LICENSE`](./LICENSE).
