// Package config loads and validates the watcher configuration from the environment.
//
// An empty variable is treated exactly like an unset one, so compose files can
// pass through "${VAR}" without forcing a value.
package config

import (
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Environment variable names.
const (
	EnvKopiaServerURL                = "KOPIA_SERVER_URL"
	EnvKopiaServerUsername           = "KOPIA_SERVER_USERNAME"
	EnvKopiaServerPassword           = "KOPIA_SERVER_PASSWORD" //nolint:gosec // variable name, not a credential
	EnvKopiaServerCertFingerprint    = "KOPIA_SERVER_CERT_FINGERPRINT"
	EnvKopiaServerCAFile             = "KOPIA_SERVER_CA_FILE"
	EnvKopiaServerInsecureSkipVerify = "KOPIA_SERVER_INSECURE_SKIP_VERIFY"
	EnvHostPrefix                    = "HOST_PREFIX"
	EnvLogLevel                      = "LOG_LEVEL"
	EnvReconcileInterval             = "RECONCILE_INTERVAL"
	EnvSnapshotOnWatcherStart        = "SNAPSHOT_ON_WATCHER_START"
	EnvWatcherPathPrefix             = "WATCHER_PATH_PREFIX"
	EnvCleanupGrace                  = "CLEANUP_GRACE"
	EnvStateFile                     = "STATE_FILE"
	EnvListenAddr                    = "LISTEN_ADDR"
	EnvNotifyWebhookURL              = "NOTIFY_WEBHOOK_URL"
	EnvLabelPrefix                   = "LABEL_PREFIX"

	// Variables of the Python version that are no longer used.
	EnvKopiaExecContainer = "KOPIA_EXEC_CONTAINER"
	EnvKopiaPassword      = "KOPIA_PASSWORD"
)

// Defaults.
const (
	DefaultKopiaServerUsername = "kopia" // kopia server's default --server-username
	DefaultLogLevel            = slog.LevelInfo
	DefaultReconcileInterval   = 5 * time.Minute
	DefaultStateFile           = "/var/lib/kopia-docker-watcher/state.json"
	DefaultListenAddr          = ":8080"
	DefaultLabelPrefix         = "kopia"
	DefaultCleanupGrace        = 24 * time.Hour

	// MinReconcileInterval keeps a typo such as "5s" from turning every full
	// reconcile into a burst of policy writes, each of which makes kopia
	// server refresh the whole repository.
	MinReconcileInterval = 10 * time.Second

	// MinCleanupGrace keeps a typo such as "10s" from deleting the snapshots
	// of a source moments after docker compose down, the stop snapshot
	// included, when the containers may well come back.
	MinCleanupGrace = 10 * time.Minute
)

// MigrationGuideURL points to the guide for users of the Python version.
const MigrationGuideURL = "https://github.com/rfdrew/kopia-docker-watcher/blob/main/docs/migration.md"

// Config is the complete watcher configuration.
type Config struct {
	Kopia KopiaServer

	// HostPrefix is prepended to every kopia.path value. Empty or an absolute
	// path without a trailing slash.
	HostPrefix string

	LogLevel               slog.Level
	ReconcileInterval      time.Duration
	SnapshotOnWatcherStart bool

	// WatcherPathPrefix limits the sources the watcher may touch. Empty or a
	// clean absolute path, never "/", which limits nothing. It applies to
	// source paths, after HOST_PREFIX.
	WatcherPathPrefix string

	// CleanupGrace is how long a source must have been without a container
	// before the cleanup-on-remove label deletes anything of it.
	CleanupGrace time.Duration

	StateFile        string
	ListenAddr       string
	NotifyWebhookURL string
	LabelPrefix      string

	// Warnings lists non-fatal problems found while loading. They are
	// logged once the logger is set up.
	Warnings []string
}

// KopiaServer describes how to reach kopia server.
type KopiaServer struct {
	// URL is the base URL without a trailing slash, e.g. https://kopia:51515.
	URL string

	// Username and Password are sent with Basic auth. Both are empty when
	// kopia server runs with --without-password.
	Username string
	Password string

	// At most one of the TLS options is set, and only for an https URL.
	CertFingerprint    string // SHA-256 of the server certificate, lowercase hex
	CAFile             string
	InsecureSkipVerify bool
}

// FromEnv loads the configuration from the process environment.
func FromEnv() (*Config, error) {
	return Load(os.Getenv)
}

// Load builds the configuration from getenv and validates it. All problems
// are reported together: the error is an errors.Join of one error per problem.
func Load(getenv func(string) string) (*Config, error) {
	l := loader{getenv: getenv}

	cfg := &Config{
		LogLevel:          DefaultLogLevel,
		ReconcileInterval: DefaultReconcileInterval,
		CleanupGrace:      DefaultCleanupGrace,
		StateFile:         DefaultStateFile,
		ListenAddr:        DefaultListenAddr,
		LabelPrefix:       DefaultLabelPrefix,
	}

	if v := l.get(EnvKopiaExecContainer); v != "" {
		l.errorf("%s is no longer supported: the watcher talks to kopia server only over its HTTP API. "+
			"Remove %s and set %s, see %s", EnvKopiaExecContainer, EnvKopiaExecContainer, EnvKopiaServerURL, MigrationGuideURL)
	}

	if l.getenv(EnvKopiaPassword) != "" {
		l.warnf("%s is ignored: kopia server opens the repository itself, the watcher does not need the repository password", EnvKopiaPassword)
	}

	l.loadKopia(&cfg.Kopia)

	if v := l.get(EnvHostPrefix); v != "" {
		p, err := ParseHostPrefix(v)
		if err != nil {
			l.errorf("%s: %v", EnvHostPrefix, err)
		}

		cfg.HostPrefix = p
	}

	if v := l.get(EnvLogLevel); v != "" {
		lvl, err := ParseLogLevel(v)
		if err != nil {
			l.errorf("%s: %v", EnvLogLevel, err)
		}

		cfg.LogLevel = lvl
	}

	if v := l.get(EnvReconcileInterval); v != "" {
		d, err := time.ParseDuration(v)

		switch {
		case err != nil:
			l.errorf("%s: invalid duration %q, use a Go duration such as 5m or 1h30m", EnvReconcileInterval, v)
		case d < MinReconcileInterval:
			l.errorf("%s: %v is shorter than the minimum %v", EnvReconcileInterval, d, MinReconcileInterval)
		default:
			cfg.ReconcileInterval = d
		}
	}

	cfg.SnapshotOnWatcherStart = l.bool(EnvSnapshotOnWatcherStart, false)

	if v := l.get(EnvWatcherPathPrefix); v != "" {
		p, err := ParseWatcherPathPrefix(v)
		if err != nil {
			l.errorf("%s: %v", EnvWatcherPathPrefix, err)
		}

		cfg.WatcherPathPrefix = p
	}

	if v := l.get(EnvCleanupGrace); v != "" {
		d, err := time.ParseDuration(v)

		switch {
		case err != nil:
			l.errorf("%s: invalid duration %q, use a Go duration such as 24h or 168h (d is not a unit)", EnvCleanupGrace, v)
		case d < MinCleanupGrace:
			l.errorf("%s: %v is shorter than the minimum %v", EnvCleanupGrace, d, MinCleanupGrace)
		default:
			cfg.CleanupGrace = d
		}
	}

	if v := l.get(EnvStateFile); v != "" {
		cfg.StateFile = v
	}

	if v := l.get(EnvListenAddr); v != "" {
		if err := validateListenAddr(v); err != nil {
			l.errorf("%s: %v", EnvListenAddr, err)
		}

		cfg.ListenAddr = v
	}

	if v := l.get(EnvNotifyWebhookURL); v != "" {
		if _, err := parseHTTPURL(v, true); err != nil {
			l.errorf("%s: %v", EnvNotifyWebhookURL, err)
		}

		cfg.NotifyWebhookURL = v
	}

	if v := l.get(EnvLabelPrefix); v != "" {
		p, err := ParseLabelPrefix(v)
		if err != nil {
			l.errorf("%s: %v", EnvLabelPrefix, err)
		}

		cfg.LabelPrefix = p
	}

	// The joined error unwraps to one error per problem.
	if err := errors.Join(l.errs...); err != nil {
		return nil, err
	}

	cfg.Warnings = l.warnings

	return cfg, nil
}

func (l *loader) loadKopia(k *KopiaServer) {
	if v := l.get(EnvKopiaServerURL); v == "" {
		l.errorf("%s is required, e.g. https://kopia:51515", EnvKopiaServerURL)
	} else if u, err := parseHTTPURL(v, false); err != nil {
		l.errorf("%s: %v", EnvKopiaServerURL, err)
	} else {
		if u.User != nil {
			l.errorf("%s must not contain credentials, use %s and %s", EnvKopiaServerURL, EnvKopiaServerUsername, EnvKopiaServerPassword)
		}

		u.User = nil
		u.Path = strings.TrimRight(u.Path, "/")
		k.URL = u.String()
	}

	// Credentials are taken verbatim: spaces may be part of a password.
	k.Username = l.getenv(EnvKopiaServerUsername)
	k.Password = l.getenv(EnvKopiaServerPassword)

	switch {
	case k.Username == "" && k.Password == "":
		l.warnf("%s and %s are not set: this only works with kopia server started with --without-password", EnvKopiaServerUsername, EnvKopiaServerPassword)
	case k.Password == "":
		l.errorf("%s is set but %s is empty", EnvKopiaServerUsername, EnvKopiaServerPassword)
	case k.Username == "":
		k.Username = DefaultKopiaServerUsername
	}

	if v := l.get(EnvKopiaServerCertFingerprint); v != "" {
		fp, err := ParseCertFingerprint(v)
		if err != nil {
			l.errorf("%s: %v", EnvKopiaServerCertFingerprint, err)
		}

		k.CertFingerprint = fp
	}

	k.CAFile = l.get(EnvKopiaServerCAFile)
	k.InsecureSkipVerify = l.bool(EnvKopiaServerInsecureSkipVerify, false)

	var tlsOptions []string

	if k.CertFingerprint != "" {
		tlsOptions = append(tlsOptions, EnvKopiaServerCertFingerprint)
	}

	if k.CAFile != "" {
		tlsOptions = append(tlsOptions, EnvKopiaServerCAFile)
	}

	if k.InsecureSkipVerify {
		tlsOptions = append(tlsOptions, EnvKopiaServerInsecureSkipVerify)
	}

	if len(tlsOptions) > 1 {
		l.errorf("only one of %s may be set", strings.Join(tlsOptions, ", "))
	}

	// A TLS option on a plain HTTP URL means the user believes the
	// connection is protected while it is not.
	if len(tlsOptions) > 0 && k.URL != "" && !strings.HasPrefix(k.URL, "https://") {
		l.errorf("%s requires an https:// %s", strings.Join(tlsOptions, ", "), EnvKopiaServerURL)
	}
}

// ParseCertFingerprint normalizes the SHA-256 certificate fingerprint printed
// by kopia server as "SERVER CERT SHA256: <hex>". Colons and spaces between
// bytes are accepted, case is ignored.
func ParseCertFingerprint(s string) (string, error) {
	s = strings.TrimSpace(s)

	if strings.HasPrefix(strings.ToUpper(s), "SHA256:") {
		return "", fmt.Errorf("%q looks like an OpenSSH-style fingerprint; "+
			"use the hex value printed after \"SERVER CERT SHA256:\" in the kopia server log", s)
	}

	hexStr := strings.ToLower(strings.NewReplacer(":", "", " ", "").Replace(s))

	b, err := hex.DecodeString(hexStr)
	if err != nil || len(b) != 32 {
		return "", fmt.Errorf("%q is not a SHA-256 fingerprint: expected 64 hex digits "+
			"as printed after \"SERVER CERT SHA256:\" in the kopia server log", s)
	}

	return hexStr, nil
}

// ParseLogLevel accepts debug, info, warn, warning and error in any case.
// critical and fatal, valid in the Python version, map to error.
func ParseLogLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error", "critical", "fatal":
		return slog.LevelError, nil
	default:
		return DefaultLogLevel, fmt.Errorf("unknown log level %q, use debug, info, warning or error", s)
	}
}

// ParseLabelPrefix checks a LABEL_PREFIX value. Empty means
// DefaultLabelPrefix.
func ParseLabelPrefix(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return DefaultLabelPrefix, nil
	}

	if !labelPrefixRE.MatchString(s) {
		return s, fmt.Errorf("invalid label prefix %q: use letters, digits, '.', '-' and '_', starting and ending with a letter or digit", s)
	}

	return s, nil
}

// ParseHostPrefix checks a HOST_PREFIX value: empty, or an absolute path.
// The result is clean and has no trailing slash, so "/" means no prefix.
func ParseHostPrefix(s string) (string, error) {
	s = strings.TrimRight(strings.TrimSpace(s), "/")
	if s == "" {
		return "", nil
	}

	if !path.IsAbs(s) {
		return s, fmt.Errorf("%q is not an absolute path", s)
	}

	return path.Clean(s), nil
}

// ParseWatcherPathPrefix checks a WATCHER_PATH_PREFIX value: empty, or an
// absolute path. The result is clean, and "/", which limits nothing, is
// empty.
func ParseWatcherPathPrefix(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", nil
	}

	if !path.IsAbs(s) {
		return s, fmt.Errorf("%q is not an absolute path", s)
	}

	if s = path.Clean(s); s == "/" {
		return "", nil
	}

	return s, nil
}

// LogValue implements slog.LogValuer. Durations are logged as Go duration
// strings (5m0s): the JSON handler would print slog.Duration as nanoseconds.
// Secrets are never logged: only whether
// the password is set and the webhook host.
func (c *Config) LogValue() slog.Value {
	webhook := ""
	if u, err := url.Parse(c.NotifyWebhookURL); err == nil && c.NotifyWebhookURL != "" {
		webhook = u.Scheme + "://" + u.Host
	}

	return slog.GroupValue(
		slog.Group("kopia_server",
			slog.String("url", c.Kopia.URL),
			slog.String("username", c.Kopia.Username),
			slog.Bool("password_set", c.Kopia.Password != ""),
			slog.Bool("cert_fingerprint_set", c.Kopia.CertFingerprint != ""),
			slog.String("ca_file", c.Kopia.CAFile),
			slog.Bool("insecure_skip_verify", c.Kopia.InsecureSkipVerify),
		),
		slog.String("host_prefix", c.HostPrefix),
		slog.String("log_level", c.LogLevel.String()),
		slog.String("reconcile_interval", c.ReconcileInterval.String()),
		slog.Bool("snapshot_on_watcher_start", c.SnapshotOnWatcherStart),
		slog.String("watcher_path_prefix", c.WatcherPathPrefix),
		slog.String("cleanup_grace", c.CleanupGrace.String()),
		slog.String("state_file", c.StateFile),
		slog.String("listen_addr", c.ListenAddr),
		slog.String("notify_webhook", webhook),
		slog.String("label_prefix", c.LabelPrefix),
	)
}

var labelPrefixRE = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$`)

type loader struct {
	getenv   func(string) string
	errs     []error
	warnings []string
}

// get returns the trimmed value; empty means unset.
func (l *loader) get(name string) string {
	return strings.TrimSpace(l.getenv(name))
}

func (l *loader) errorf(format string, args ...any) {
	l.errs = append(l.errs, fmt.Errorf(format, args...))
}

func (l *loader) warnf(format string, args ...any) {
	l.warnings = append(l.warnings, fmt.Sprintf(format, args...))
}

func (l *loader) bool(name string, def bool) bool {
	v := l.get(name)
	if v == "" {
		return def
	}

	b, err := strconv.ParseBool(v)
	if err != nil {
		l.errorf("%s: invalid boolean %q, use true or false", name, v)
		return def
	}

	return b
}

// parseHTTPURL checks an http(s) URL. Errors do not echo the URL: a webhook
// URL may carry a token in its path or query.
func parseHTTPURL(s string, allowQuery bool) (*url.URL, error) {
	u, err := url.Parse(s)
	if err != nil {
		return nil, errors.New("invalid URL")
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, errors.New("URL must start with http:// or https://")
	}

	if u.Host == "" {
		return nil, errors.New("URL has no host")
	}

	if !allowQuery && u.RawQuery != "" {
		return nil, errors.New("URL must not have a query")
	}

	if u.Fragment != "" {
		return nil, errors.New("URL must not have a fragment")
	}

	return u, nil
}

func validateListenAddr(s string) error {
	_, port, err := net.SplitHostPort(s)
	if err != nil {
		return fmt.Errorf("invalid address %q, use host:port or :port", s)
	}

	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("invalid port in %q", s)
	}

	return nil
}
