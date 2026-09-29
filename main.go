// mybinlogdumper is a small CLI to inspect / search MySQL binlog events.
//
// Subcommands:
//
//	mybinlogdumper gtid    dump one GTID transaction
//	mybinlogdumper search  search for a DDL / row event by pattern
//	mybinlogdumper dump    raw dump of binlog events
//	mybinlogdumper sptest  compute a start GTID after gtid_purged
//	mybinlogdumper status  print SHOW MASTER STATUS / gtid_executed / gtid_purged
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	uuid "github.com/satori/go.uuid"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

// -----------------------------------------------------------------------------
// Config
// -----------------------------------------------------------------------------

// Config holds every flag needed by the tool. It is filled once in main and
// then passed to the runXxx functions, so those functions are easy to unit
// test.
type Config struct {
	// Connection.
	Host     string
	Port     int
	User     string
	Password string
	Flavor   string
	ServerID uint32

	// Where to start reading from.
	File string
	Pos  int
	GTID string

	// Common knobs.
	Limit    int
	RawMode  bool
	SemiSync bool
	Debug    bool

	// search filters.
	DBPattern    string
	TablePattern string
	FieldValues  string
	DDLPattern   string

	// sptest.
	LastUUID string
	NowUUID  string
}

// registerCommon registers the flags shared by every subcommand.
func (c *Config) registerCommon(fs *flag.FlagSet) {
	fs.StringVar(&c.Host, "host", "127.0.0.1", "MySQL host")
	fs.IntVar(&c.Port, "port", 3306, "MySQL port")
	fs.StringVar(&c.User, "user", "root", "MySQL user, must have replication privilege")
	fs.StringVar(&c.Password, "password", os.Getenv("MYSQL_PWD"), "MySQL password (or set MYSQL_PWD env)")
	fs.StringVar(&c.Flavor, "flavor", "mysql", "flavor: mysql or mariadb")
	fs.Func("server-id", "replication server id (default random)", func(s string) error {
		var v uint32
		if _, err := fmt.Sscanf(s, "%d", &v); err != nil {
			return err
		}
		c.ServerID = v
		return nil
	})
}

// registerStream registers flags that pick a start position in the binlog.
func (c *Config) registerStream(fs *flag.FlagSet) {
	fs.StringVar(&c.File, "file", "", "binlog filename")
	fs.IntVar(&c.Pos, "pos", 4, "binlog position")
	fs.StringVar(&c.GTID, "gtid", "", "gtid set (gtid_executed) to start from")
	fs.IntVar(&c.Limit, "limit", 0, "stop after N non-heartbeat events (0 = no limit)")
}

// binlogSyncerCfg builds the go-mysql BinlogSyncerConfig from the Config.
func (c *Config) binlogSyncerCfg() replication.BinlogSyncerConfig {
	return replication.BinlogSyncerConfig{
		ServerID:        c.serverID(),
		Flavor:          c.Flavor,
		Host:            c.Host,
		Port:            uint16(c.Port),
		User:            c.User,
		Password:        c.Password,
		RawModeEnabled:  c.RawMode,
		SemiSyncEnabled: c.SemiSync,
		UseDecimal:      true,
	}
}

func (c *Config) serverID() uint32 {
	if c.ServerID != 0 {
		return c.ServerID
	}
	// Pick something unlikely to clash with a real server. MySQL server_id
	// is a uint32 but conventional configurations use small numbers.
	return 1<<30 + rand.Uint32N(1<<20)
}

// addr returns "host:port".
func (c *Config) addr() string { return fmt.Sprintf("%s:%d", c.Host, c.Port) }

// -----------------------------------------------------------------------------
// Entry point / subcommand dispatch
// -----------------------------------------------------------------------------

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch cmd {
	case "gtid":
		err = cmdGtid(ctx, args)
	case "search":
		err = cmdSearch(ctx, args)
	case "dump":
		err = cmdDump(ctx, args)
	case "sptest":
		err = cmdSPTest(ctx, args)
	case "status":
		err = cmdStatus(args)
	case "version", "-v", "--version":
		fmt.Println(version)
		return
	case "help", "-h", "--help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Info("cancelled")
			return
		}
		slog.Error(cmd+" failed", "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `mybinlogdumper %s - MySQL binlog inspection helper

Usage:
  mybinlogdumper <subcommand> [flags]

Subcommands:
  gtid     dump one GTID transaction
  search   search binlog for a DDL / row event by pattern
  dump     raw event dump
  sptest   compute a valid start GTID after gtid_purged
  status   show master status / gtid_executed / gtid_purged
  version  print version
  help     show this help

Run 'mybinlogdumper <subcommand> -h' to see the flags of a subcommand.
`, version)
}

// -----------------------------------------------------------------------------
// dump
// -----------------------------------------------------------------------------

func cmdDump(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("dump", flag.ExitOnError)
	cfg := &Config{}
	cfg.registerCommon(fs)
	cfg.registerStream(fs)
	fs.BoolVar(&cfg.RawMode, "raw", false, "use raw mode (no event parsing)")
	fs.BoolVar(&cfg.SemiSync, "semisync", false, "enable semi-sync")
	if err := fs.Parse(args); err != nil {
		return err
	}

	b, s, err := startSyncer(cfg)
	if err != nil {
		return err
	}
	defer b.Close()

	return runDump(ctx, s, cfg.Limit)
}

func runDump(ctx context.Context, s *replication.BinlogStreamer, limit int) error {
	for i := 0; limit == 0 || i < limit; {
		e, err := s.GetEvent(ctx)
		if err != nil {
			// Try to output whatever is buffered before returning.
			for _, ev := range s.DumpEvents() {
				ev.Dump(os.Stdout)
			}
			return fmt.Errorf("get event: %w", err)
		}
		if e.Header.EventType != replication.HEARTBEAT_EVENT {
			e.Dump(os.Stdout)
			i++
		}
	}
	return nil
}

// -----------------------------------------------------------------------------
// gtid: dump one transaction
// -----------------------------------------------------------------------------

func cmdGtid(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("gtid", flag.ExitOnError)
	cfg := &Config{}
	cfg.registerCommon(fs)
	cfg.registerStream(fs)
	fs.BoolVar(&cfg.Debug, "debug", false, "print each column value in row events")
	if err := fs.Parse(args); err != nil {
		return err
	}

	b, s, err := startSyncer(cfg)
	if err != nil {
		return err
	}
	defer b.Close()

	return runGTID(ctx, s, cfg)
}

func runGTID(ctx context.Context, s *replication.BinlogStreamer, cfg *Config) error {
	txn := &GtidTransaction{debug: cfg.Debug}
	gtidGot := false
	for i := 0; cfg.Limit == 0 || i < cfg.Limit; {
		e, err := s.GetEvent(ctx)
		if err != nil {
			return fmt.Errorf("get event: %w", err)
		}
		switch e.Header.EventType {
		case replication.HEARTBEAT_EVENT:
			continue
		case replication.FORMAT_DESCRIPTION_EVENT:
			if fdev, ok := e.Event.(*replication.FormatDescriptionEvent); ok {
				fmt.Println("FormatDescriptionEvent:", string(fdev.ServerVersion), fdev.Version)
			}
			continue
		case replication.ROTATE_EVENT:
			if rtev, ok := e.Event.(*replication.RotateEvent); ok {
				fmt.Printf("Rotate binlog to %s %d\n", string(rtev.NextLogName), rtev.Position)
			}
			continue
		case replication.PREVIOUS_GTIDS_EVENT:
			if pgev, ok := e.Event.(*replication.PreviousGTIDsEvent); ok {
				fmt.Println("PreviousGTIDsEvent:", pgev.GTIDSets)
			}
			continue
		}
		fmt.Println("-------------------------------------------")
		if e.Header.EventType == replication.GTID_EVENT && gtidGot {
			break
		}
		if err := txn.HandleEvent(e); err != nil {
			return fmt.Errorf("handle event: %w", err)
		}
		if e.Header.EventType == replication.XID_EVENT {
			break
		}
		if e.Header.EventType == replication.GTID_EVENT {
			gtidGot = true
		}
		i++
	}
	return nil
}

// -----------------------------------------------------------------------------
// search
// -----------------------------------------------------------------------------

// searchFilters are the compiled filters for cmdSearch.
type searchFilters struct {
	dbRe    *regexp.Regexp
	tableRe *regexp.Regexp
	ddlRe   *regexp.Regexp
	fields  []fieldFilter

	// meta cache is scoped to a single search invocation.
	tables map[string]*schema.Table
	cfg    *Config
}

// fieldFilter is one "name=value" (or "name:value") condition from -fields.
type fieldFilter struct {
	name  string
	value string
}

func cmdSearch(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("search", flag.ExitOnError)
	cfg := &Config{}
	cfg.registerCommon(fs)
	cfg.registerStream(fs)
	fs.StringVar(&cfg.DBPattern, "db", "", "db name regex")
	fs.StringVar(&cfg.TablePattern, "table", "", "table name regex")
	fs.StringVar(&cfg.FieldValues, "fields", "", "field filter, e.g. id=1,name=foo")
	fs.StringVar(&cfg.DDLPattern, "ddl", "", "DDL query regex")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if cfg.TablePattern == "" && cfg.FieldValues == "" && cfg.DDLPattern == "" {
		return errors.New("search needs at least one of -table / -fields / -ddl")
	}

	filters, err := newSearchFilters(cfg)
	if err != nil {
		return err
	}

	b, s, err := startSyncer(cfg)
	if err != nil {
		return err
	}
	defer b.Close()

	return runSearch(ctx, s, filters, cfg.Limit)
}

func newSearchFilters(cfg *Config) (*searchFilters, error) {
	f := &searchFilters{tables: map[string]*schema.Table{}, cfg: cfg}
	var err error
	if cfg.DBPattern != "" {
		if f.dbRe, err = regexp.Compile(cfg.DBPattern); err != nil {
			return nil, fmt.Errorf("invalid db pattern %q: %w", cfg.DBPattern, err)
		}
	}
	if cfg.TablePattern != "" {
		if f.tableRe, err = regexp.Compile(cfg.TablePattern); err != nil {
			return nil, fmt.Errorf("invalid table pattern %q: %w", cfg.TablePattern, err)
		}
	}
	if cfg.DDLPattern != "" {
		if f.ddlRe, err = regexp.Compile(cfg.DDLPattern); err != nil {
			return nil, fmt.Errorf("invalid ddl pattern %q: %w", cfg.DDLPattern, err)
		}
	}
	f.fields, err = parseFieldFilters(cfg.FieldValues)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func parseFieldFilters(raw string) ([]fieldFilter, error) {
	if raw == "" {
		return nil, nil
	}
	var out []fieldFilter
	for _, fv := range strings.Split(raw, ",") {
		fv = strings.TrimSpace(fv)
		if fv == "" {
			continue
		}
		kv := strings.SplitN(fv, "=", 2)
		if len(kv) != 2 {
			kv = strings.SplitN(fv, ":", 2)
		}
		if len(kv) != 2 || kv[0] == "" {
			return nil, fmt.Errorf("invalid field filter %q, want name=value or name:value", fv)
		}
		out = append(out, fieldFilter{name: kv[0], value: kv[1]})
	}
	return out, nil
}

func runSearch(ctx context.Context, s *replication.BinlogStreamer, f *searchFilters, limit int) error {
	var rgtid mysql.GTIDSet
	var rowsQuery []byte
	var found bool
	for i := 0; limit == 0 || i < limit; i++ {
		e, err := s.GetEvent(ctx)
		if err != nil {
			return fmt.Errorf("get event: %w", err)
		}
		switch ev := e.Event.(type) {
		case *replication.QueryEvent:
			if f.ddlRe != nil && f.ddlRe.Match(ev.Query) {
				found = true
				rowsQuery = ev.Query
			}
		case *replication.RowsEvent:
			if f.ddlRe != nil {
				continue
			}
			matched, err := f.matchRowsEvent(ev)
			if err != nil {
				slog.Warn("row match error", "err", err)
			}
			if matched {
				found = true
			}
		case *replication.GTIDEvent:
			u, _ := uuid.FromBytes(ev.SID)
			rowsQuery = nil // a new transaction begins
			gtidset, gerr := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d", u.String(), ev.GNO))
			if gerr != nil {
				return fmt.Errorf("parse gtid: %w", gerr)
			}
			rgtid = gtidset
			slog.Debug("GTID", "gtid", rgtid)
		case *replication.RowsQueryEvent:
			rowsQuery = ev.Query
		}
		if found {
			fmt.Println(strings.Repeat("=", 63))
			fmt.Printf("--- gtid: %s [%s]\n", rgtid, time.Unix(int64(e.Header.Timestamp), 0))
			fmt.Printf("---  sql: %q\n", string(rowsQuery))
			return nil
		}
	}
	return nil
}

func (f *searchFilters) matchRowsEvent(e *replication.RowsEvent) (bool, error) {
	if f.dbRe != nil && !f.dbRe.Match(e.Table.Schema) {
		return false, nil
	}
	if f.tableRe != nil && !f.tableRe.Match(e.Table.Table) {
		return false, nil
	}
	if len(f.fields) == 0 {
		return true, nil
	}
	tblMeta, err := f.tableMeta(string(e.Table.Schema), string(e.Table.Table))
	if err != nil {
		return false, err
	}
	for _, row := range e.Rows {
		matched := 0
		for _, ff := range f.fields {
			for i, v := range row {
				if i >= len(tblMeta.Columns) {
					break
				}
				if ff.name != string(tblMeta.Columns[i].Name) {
					continue
				}
				if rowValueString(v) == ff.value {
					matched++
				}
				break
			}
		}
		if matched == len(f.fields) {
			return true, nil
		}
	}
	return false, nil
}

func (f *searchFilters) tableMeta(db, table string) (*schema.Table, error) {
	key := db + "." + table
	if tm, ok := f.tables[key]; ok {
		return tm, nil
	}
	c, err := client.Connect(f.cfg.addr(), f.cfg.User, f.cfg.Password, "")
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", f.cfg.addr(), err)
	}
	defer c.Close()

	tb, err := schema.NewTable(c, db, table)
	if err != nil {
		return nil, fmt.Errorf("load table %s: %w", key, err)
	}
	f.tables[key] = tb
	return tb, nil
}

func rowValueString(v any) string {
	if bv, ok := v.([]byte); ok {
		return string(bv)
	}
	return fmt.Sprintf("%v", v)
}

// -----------------------------------------------------------------------------
// sptest: locate a valid start GTID for a slave whose gtid_purged advanced.
// -----------------------------------------------------------------------------

func cmdSPTest(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sptest", flag.ExitOnError)
	cfg := &Config{}
	cfg.registerCommon(fs)
	fs.StringVar(&cfg.GTID, "gtid", "", "the slave's gtid_executed")
	fs.StringVar(&cfg.LastUUID, "last-uuid", "", "previous master uuid (optional, will be guessed)")
	fs.StringVar(&cfg.NowUUID, "now-uuid", "", "current master uuid (default: @@global.server_uuid)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	return runSPTest(ctx, cfg)
}

func runSPTest(ctx context.Context, cfg *Config) error {
	if cfg.GTID == "" {
		return errors.New("sptest needs -gtid")
	}
	gtidExecuted := selectOneString(cfg, "select @@global.gtid_executed")
	gtidPurged := selectOneString(cfg, "select @@global.gtid_purged")
	slog.Info("server state", "gtid_executed", gtidExecuted, "gtid_purged", gtidPurged)
	if gtidExecuted == "" || gtidPurged == "" {
		return errors.New("gtid_executed or gtid_purged is empty; nothing to compute")
	}

	currUUID := cfg.NowUUID
	if currUUID == "" {
		currUUID = selectOneString(cfg, "select @@global.server_uuid")
		slog.Info("using @@server_uuid", "uuid", currUUID)
	}

	prevUUID := cfg.LastUUID
	if prevUUID == "" {
		prevUUID = tryGetLastUUID(currUUID, cfg.GTID)
		if prevUUID == "" {
			return errors.New("cannot guess prev uuid; provide -last-uuid")
		}
		slog.Info("guessed prev uuid", "uuid", prevUUID)
	}

	lastUuExecuted := getUUIDSet(prevUUID, gtidExecuted)
	if lastUuExecuted == nil {
		return fmt.Errorf("prev UUID %s not in gtid_executed", prevUUID)
	}
	currUuExecuted := getUUIDSet(currUUID, gtidExecuted)
	if currUuExecuted == nil {
		return fmt.Errorf("curr UUID %s not in gtid_executed", currUUID)
	}
	// nil means nothing of currUUID has been purged yet.
	currUuPurged := getUUIDSet(currUUID, gtidPurged)

	newGtid := cfg.GTID
	myCurrUu := getUUIDSet(currUUID, newGtid)
	myLastUu := getUUIDSet(prevUUID, newGtid)
	if myLastUu == nil {
		return fmt.Errorf("prev UUID %s not in gtid %s", prevUUID, newGtid)
	}
	if !lastUuExecuted.Contain(myLastUu) && myLastUu.Contain(lastUuExecuted) {
		slog.Warn("slave gtid past current executed; data might be lost",
			"slave", myLastUu, "executed", lastUuExecuted)
		newGtid = replaceGtidByUUID(prevUUID, lastUuExecuted.String(), newGtid)
	}
	if myCurrUu != nil && (currUuPurged == nil || !currUuPurged.Contain(myCurrUu)) {
		fmt.Println("not purged gtid found, new gtid:", newGtid)
		return nil
	}

	b := replication.NewBinlogSyncer(cfg.binlogSyncerCfg())
	defer b.Close()

	newGtid, err := locateNotPurgedUUIDSet(ctx, currUUID, prevUUID, newGtid, currUuExecuted.String(), b)
	if err != nil {
		return fmt.Errorf("locate not purged uuid: %w", err)
	}
	fmt.Println(strings.Repeat("-", 78))
	fmt.Println("The new start GTID:", newGtid)
	fmt.Println(strings.Repeat("-", 78))
	return nil
}

// getUUIDSet returns the UUIDSet of the given uuid in gtidSetStr,
// it returns nil when gtidSetStr is empty or the uuid is not found.
func getUUIDSet(uuid, gtidSetStr string) *mysql.UUIDSet {
	if gtidSetStr == "" {
		return nil
	}
	for _, uset := range strings.Split(gtidSetStr, ",") {
		uset = strings.TrimSpace(uset)
		if uset == "" {
			continue
		}
		uuidSet, err := mysql.ParseUUIDSet(uset)
		if err != nil {
			slog.Warn("parse uuid set failed", "uset", uset, "err", err)
			return nil
		}
		if uuidSet.SID.String() == uuid {
			return uuidSet
		}
	}
	return nil
}

func tryGetLastUUID(nauuid, gtidSetStr string) string {
	if gtidSetStr == "" {
		return ""
	}
	var last string
	for _, uset := range strings.Split(gtidSetStr, ",") {
		uset = strings.TrimSpace(uset)
		if uset == "" {
			continue
		}
		uuidSet, err := mysql.ParseUUIDSet(uset)
		if err != nil {
			slog.Warn("parse uuid set failed", "uset", uset, "err", err)
			return ""
		}
		if uuidSet.SID.String() != nauuid {
			last = uuidSet.SID.String()
		}
	}
	return last
}

func replaceGtidByUUID(uuid, newSet, gtidSetStr string) string {
	var out []string
	for _, uset := range strings.Split(gtidSetStr, ",") {
		uset = strings.TrimSpace(uset)
		if uset == "" {
			continue
		}
		uuidSet, err := mysql.ParseUUIDSet(uset)
		if err != nil {
			slog.Warn("parse uuid set failed", "uset", uset, "err", err)
			return ""
		}
		if uuidSet.SID.String() != uuid {
			out = append(out, uset)
		}
	}
	out = append(out, newSet)
	return strings.Join(out, ",")
}

// maxPreviousGTIDProbe limits how many events we scan while looking for PREVIOUS_GTIDS_EVENT.
const maxPreviousGTIDProbe = 5

// ErrPrevGTIDNotFound is returned when the PREVIOUS_GTIDS_EVENT never appears.
var ErrPrevGTIDNotFound = errors.New("previous_gtids_event not found")

func locateNotPurgedUUIDSet(ctx context.Context, uuid, lastUu, gtid, currUUIDSet string, b *replication.BinlogSyncer) (string, error) {
	lastUuSet := getUUIDSet(lastUu, gtid)
	if lastUuSet == nil {
		return "", fmt.Errorf("uuid %s not found in gtid %s", lastUu, gtid)
	}
	lastUuSet = shrinkLastInterval(lastUuSet)
	if lastUuSet == nil {
		return "", fmt.Errorf("uuid %s has no usable interval in gtid %s", lastUu, gtid)
	}
	newGtid := replaceGtidByUUID(lastUu, lastUuSet.String(), gtid)
	newGtid = replaceGtidByUUID(uuid, currUUIDSet, newGtid)

	slog.Info("try start with gtid", "gtid", newGtid)
	gtidSet, err := mysql.ParseMysqlGTIDSet(newGtid)
	if err != nil {
		return "", fmt.Errorf("parse gtid: %w", err)
	}
	s, err := b.StartSyncGTID(gtidSet)
	if err != nil {
		return "", fmt.Errorf("start sync: %w", err)
	}
	for range maxPreviousGTIDProbe {
		e, err := s.GetEvent(ctx)
		if err != nil {
			return "", fmt.Errorf("get event: %w", err)
		}
		if e.Header.EventType != replication.PREVIOUS_GTIDS_EVENT {
			continue
		}
		pgev, ok := e.Event.(*replication.PreviousGTIDsEvent)
		if !ok {
			return "", fmt.Errorf("unexpected event %T for PREVIOUS_GTIDS_EVENT", e.Event)
		}
		slog.Info("PreviousGTIDsEvent", "gtids", pgev.GTIDSets)
		recent := getUUIDSet(uuid, pgev.GTIDSets)
		if recent == nil {
			return "", fmt.Errorf("uuid %s not found in previous gtids %s", uuid, pgev.GTIDSets)
		}
		return replaceGtidByUUID(uuid, recent.String(), gtid), nil
	}
	return "", ErrPrevGTIDNotFound
}

// shrinkLastInterval removes the last GID of the set, i.e. steps back one transaction.
// It returns nil when the set becomes empty.
func shrinkLastInterval(s *mysql.UUIDSet) *mysql.UUIDSet {
	if s == nil || len(s.Intervals) == 0 {
		return nil
	}
	intervals := make(mysql.IntervalSlice, len(s.Intervals))
	copy(intervals, s.Intervals)
	last := &intervals[len(intervals)-1]
	last.Stop--
	if last.Stop <= last.Start {
		intervals = intervals[:len(intervals)-1]
	}
	if len(intervals) == 0 {
		return nil
	}
	return mysql.NewUUIDSet(s.SID, intervals...)
}

// -----------------------------------------------------------------------------
// status
// -----------------------------------------------------------------------------

func cmdStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	cfg := &Config{}
	cfg.registerCommon(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	return runStatus(cfg)
}

func runStatus(cfg *Config) error {
	c, err := client.Connect(cfg.addr(), cfg.User, cfg.Password, "")
	if err != nil {
		return fmt.Errorf("connect %s: %w", cfg.addr(), err)
	}
	defer c.Close()

	for _, sql := range []string{"show master status", "select @@global.gtid_executed", "select @@global.gtid_purged"} {
		rs, err := c.Execute(sql)
		if err != nil {
			return fmt.Errorf("execute %q: %w", sql, err)
		}
		dumpResultSet(rs)
		rs.Close()
	}
	return nil
}

// -----------------------------------------------------------------------------
// Small helpers shared by several subcommands.
// -----------------------------------------------------------------------------

// startSyncer creates a BinlogSyncer according to the config's start position.
// The caller owns the returned BinlogSyncer and MUST call Close on it.
func startSyncer(cfg *Config) (*replication.BinlogSyncer, *replication.BinlogStreamer, error) {
	if cfg.Pos < 0 {
		return nil, nil, fmt.Errorf("invalid -pos %d", cfg.Pos)
	}
	b := replication.NewBinlogSyncer(cfg.binlogSyncerCfg())

	if cfg.GTID != "" {
		gtidSet, err := mysql.ParseMysqlGTIDSet(cfg.GTID)
		if err != nil {
			b.Close()
			return nil, nil, fmt.Errorf("parse gtid %q: %w", cfg.GTID, err)
		}
		s, err := b.StartSyncGTID(gtidSet)
		if err != nil {
			b.Close()
			return nil, nil, fmt.Errorf("start sync gtid: %w", err)
		}
		return b, s, nil
	}
	s, err := b.StartSync(mysql.Position{Name: cfg.File, Pos: uint32(cfg.Pos)})
	if err != nil {
		b.Close()
		return nil, nil, fmt.Errorf("start sync: %w", err)
	}
	return b, s, nil
}

func selectOneString(cfg *Config, sql string) string {
	c, err := client.Connect(cfg.addr(), cfg.User, cfg.Password, "")
	if err != nil {
		slog.Warn("connect failed", "addr", cfg.addr(), "user", cfg.User, "err", err)
		return ""
	}
	defer c.Close()

	rs, err := c.Execute(sql)
	if err != nil {
		slog.Warn("select failed", "sql", sql, "err", err)
		return ""
	}
	defer rs.Close()

	if rs.RowNumber() == 0 || rs.ColumnNumber() == 0 {
		return ""
	}
	v, err := rs.GetValue(0, 0)
	if err != nil {
		slog.Warn("get value failed", "err", err)
		return ""
	}
	switch vv := v.(type) {
	case []byte:
		return string(vv)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", vv)
	}
}

func dumpResultSet(rs *mysql.Result) {
	fmt.Println(strings.Repeat("=", 80))
	for i := 0; i < rs.RowNumber(); i++ {
		for j := 0; j < rs.ColumnNumber(); j++ {
			v, _ := rs.GetValue(i, j)
			switch vv := v.(type) {
			case []byte:
				fmt.Printf("%20s: %s\n", string(rs.Fields[j].Name), string(vv))
			default:
				fmt.Printf("%20s: %v\n", string(rs.Fields[j].Name), v)
			}
		}
		fmt.Println(strings.Repeat("-", 80))
	}
}

// -----------------------------------------------------------------------------
// GtidTransaction (single transaction dumper used by cmdGtid)
// -----------------------------------------------------------------------------

// GtidTransaction is a single MySQL transaction between GTID_EVENT and XID_EVENT.
type GtidTransaction struct {
	gtid      mysql.GTIDSet
	rowsQuery []byte
	debug     bool
}

// HandleEvent handles one event within the transaction.
func (txn *GtidTransaction) HandleEvent(ev *replication.BinlogEvent) error {
	fmt.Printf("event type: %s [%s]\n", ev.Header.EventType, time.Unix(int64(ev.Header.Timestamp), 0))
	switch e := ev.Event.(type) {
	case *replication.RowsEvent:
		return txn.handleRowsEvent(ev)
	case *replication.XIDEvent:
		fmt.Println("\tEND")
	case *replication.GTIDEvent:
		u, _ := uuid.FromBytes(e.SID)
		gtid, err := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d", u.String(), e.GNO))
		if err != nil {
			return fmt.Errorf("parse gtid: %w", err)
		}
		txn.gtid = gtid
		fmt.Println("\tGTID:", gtid, "logPos:", ev.Header.LogPos)
	case *replication.QueryEvent:
		fmt.Println("\tQuery:", string(e.Query))
	case *replication.TableMapEvent:
		fmt.Println("\tTableMap:", string(e.Schema), string(e.Table))
	case *replication.RowsQueryEvent:
		txn.rowsQuery = e.Query
		fmt.Printf("\tRowsQueryEvent: %q\n", e.Query)
	default:
		fmt.Println("==========")
		e.Dump(os.Stdout)
	}
	return nil
}

func (txn *GtidTransaction) handleRowsEvent(e *replication.BinlogEvent) error {
	ev, ok := e.Event.(*replication.RowsEvent)
	if !ok {
		return fmt.Errorf("unexpected event %T for rows event", e.Event)
	}

	dbName := string(ev.Table.Schema)
	tblName := string(ev.Table.Table)

	var op string
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		op = "INSERT"
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		op = "DELETE"
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		op = "UPDATE"
	default:
		return fmt.Errorf("%s not supported", e.Header.EventType)
	}
	fmt.Println("\thandleRowsEvent:", op, dbName, tblName)
	fmt.Printf("\t\trows %v, header:%v\n", ev.Rows, e.Header)
	if txn.debug {
		for _, row := range ev.Rows {
			for i, v := range row {
				fmt.Printf("\t field[%d]=(%T)%v\n", i, v, v)
			}
		}
	}
	return nil
}
