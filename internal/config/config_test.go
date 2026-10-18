package config

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"
)

const testFingerprint = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"

func env(kv ...string) func(string) string {
	m := map[string]string{}
	for i := 0; i < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}

	return func(k string) string { return m[k] }
}

// base returns the minimal valid environment followed by extra pairs.
func base(extra ...string) func(string) string {
	return env(append([]string{
		EnvKopiaServerURL, "https://kopia:51515",
		EnvKopiaServerUsername, "admin",
		EnvKopiaServerPassword, "secret",
	}, extra...)...)
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(base())
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Kopia.URL != "https://kopia:51515" || cfg.Kopia.Username != "admin" || cfg.Kopia.Password != "secret" {
		t.Errorf("kopia = %+v", cfg.Kopia)
	}

	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v", cfg.LogLevel)
	}

	if cfg.ReconcileInterval != 5*time.Minute {
		t.Errorf("ReconcileInterval = %v", cfg.ReconcileInterval)
	}

	if cfg.SnapshotOnWatcherStart {
		t.Error("SnapshotOnWatcherStart = true")
	}

	if cfg.StateFile != "/var/lib/kopia-docker-watcher/state.json" {
		t.Errorf("StateFile = %q", cfg.StateFile)
	}

	if cfg.ListenAddr != ":8080" {
		t.Errorf("ListenAddr = %q", cfg.ListenAddr)
	}

	if cfg.LabelPrefix != "kopia" {
		t.Errorf("LabelPrefix = %q", cfg.LabelPrefix)
	}

	if cfg.HostPrefix != "" || cfg.WatcherPathPrefix != "" || cfg.NotifyWebhookURL != "" {
		t.Errorf("unexpected non-empty values: %+v", cfg)
	}

	if len(cfg.Warnings) != 0 {
		t.Errorf("Warnings = %q", cfg.Warnings)
	}
}

func TestLoadAllSet(t *testing.T) {
	cfg, err := Load(base(
		EnvKopiaServerURL, " https://kopia.example.com/prefix/ ",
		EnvKopiaServerCertFingerprint, strings.ToUpper(testFingerprint),
		EnvHostPrefix, "/host/",
		EnvLogLevel, "WARNING",
		EnvReconcileInterval, "90s",
		EnvSnapshotOnWatcherStart, "TRUE",
		EnvWatcherPathPrefix, "/host/var/lib/docker/volumes/",
		EnvStateFile, "/data/state.json",
		EnvListenAddr, "127.0.0.1:9090",
		EnvNotifyWebhookURL, "https://ntfy.sh/topic?auth=token",
		EnvLabelPrefix, "backup.kopia",
	))
	if err != nil {
		t.Fatal(err)
	}

	want := Config{
		Kopia: KopiaServer{
			URL:             "https://kopia.example.com/prefix",
			Username:        "admin",
			Password:        "secret",
			CertFingerprint: testFingerprint,
		},
		HostPrefix:             "/host",
		LogLevel:               slog.LevelWarn,
		ReconcileInterval:      90 * time.Second,
		SnapshotOnWatcherStart: true,
		WatcherPathPrefix:      "/host/var/lib/docker/volumes",
		StateFile:              "/data/state.json",
		ListenAddr:             "127.0.0.1:9090",
		NotifyWebhookURL:       "https://ntfy.sh/topic?auth=token",
		LabelPrefix:            "backup.kopia",
	}

	got := *cfg
	got.Warnings = nil

	if gotJSON, wantJSON := mustJSON(t, got), mustJSON(t, want); gotJSON != wantJSON {
		t.Errorf("config mismatch\n got: %s\nwant: %s", gotJSON, wantJSON)
	}
}

func TestEmptyIsUnset(t *testing.T) {
	cfg, err := Load(base(
		EnvKopiaServerCertFingerprint, "",
		EnvKopiaServerCAFile, " ",
		EnvKopiaServerInsecureSkipVerify, "",
		EnvHostPrefix, "",
		EnvLogLevel, "",
		EnvReconcileInterval, "",
		EnvSnapshotOnWatcherStart, "",
		EnvStateFile, "",
		EnvListenAddr, "",
		EnvLabelPrefix, "",
		EnvKopiaExecContainer, "",
		EnvKopiaPassword, "",
	))
	if err != nil {
		t.Fatal(err)
	}

	if cfg.StateFile != DefaultStateFile || cfg.ListenAddr != DefaultListenAddr || cfg.LabelPrefix != DefaultLabelPrefix ||
		cfg.ReconcileInterval != DefaultReconcileInterval || cfg.LogLevel != DefaultLogLevel {
		t.Errorf("defaults not applied: %+v", cfg)
	}

	if len(cfg.Warnings) != 0 {
		t.Errorf("Warnings = %q", cfg.Warnings)
	}
}

func TestPlainHTTPWithoutTLSOptions(t *testing.T) {
	cfg, err := Load(base(EnvKopiaServerURL, "http://kopia:51515"))
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Kopia.URL != "http://kopia:51515" {
		t.Errorf("URL = %q", cfg.Kopia.URL)
	}
}

func TestCredentials(t *testing.T) {
	t.Run("password only uses kopia default username", func(t *testing.T) {
		cfg, err := Load(env(EnvKopiaServerURL, "http://kopia:51515", EnvKopiaServerPassword, "pw"))
		if err != nil {
			t.Fatal(err)
		}

		if cfg.Kopia.Username != "kopia" {
			t.Errorf("Username = %q", cfg.Kopia.Username)
		}
	})

	t.Run("password is not trimmed", func(t *testing.T) {
		cfg, err := Load(base(EnvKopiaServerPassword, " pw "))
		if err != nil {
			t.Fatal(err)
		}

		if cfg.Kopia.Password != " pw " {
			t.Errorf("Password = %q", cfg.Kopia.Password)
		}
	})

	t.Run("no credentials warns", func(t *testing.T) {
		cfg, err := Load(env(EnvKopiaServerURL, "http://kopia:51515"))
		if err != nil {
			t.Fatal(err)
		}

		if len(cfg.Warnings) != 1 || !strings.Contains(cfg.Warnings[0], "--without-password") {
			t.Errorf("Warnings = %q", cfg.Warnings)
		}
	})
}

func TestKopiaPasswordWarns(t *testing.T) {
	cfg, err := Load(base(EnvKopiaPassword, "repo-password"))
	if err != nil {
		t.Fatal(err)
	}

	if len(cfg.Warnings) != 1 || !strings.Contains(cfg.Warnings[0], EnvKopiaPassword) {
		t.Fatalf("Warnings = %q", cfg.Warnings)
	}

	if strings.Contains(cfg.Warnings[0], "repo-password") {
		t.Fatal("warning leaks the password")
	}
}

func TestLoadErrors(t *testing.T) {
	tests := []struct {
		name string
		env  func(string) string
		want []string
	}{
		{"missing url", env(), []string{"KOPIA_SERVER_URL is required"}},
		{"exec container", base(EnvKopiaExecContainer, "kopia"), []string{"KOPIA_EXEC_CONTAINER is no longer supported", MigrationGuideURL}},
		{"url scheme", base(EnvKopiaServerURL, "kopia:51515"), []string{"KOPIA_SERVER_URL", "http:// or https://"}},
		{"url no host", base(EnvKopiaServerURL, "https:///x"), []string{"no host"}},
		{"url query", base(EnvKopiaServerURL, "https://kopia/?a=b"), []string{"must not have a query"}},
		{"url credentials", base(EnvKopiaServerURL, "https://u:p@kopia:51515"), []string{"must not contain credentials"}},
		{"username without password", env(EnvKopiaServerURL, "https://kopia", EnvKopiaServerUsername, "admin"), []string{"KOPIA_SERVER_PASSWORD is empty"}},
		{"openssh fingerprint", base(EnvKopiaServerCertFingerprint, "SHA256:abcdef"), []string{"SERVER CERT SHA256:"}},
		{"short fingerprint", base(EnvKopiaServerCertFingerprint, "abcd"), []string{"64 hex digits"}},
		{"two tls options", base(EnvKopiaServerCertFingerprint, testFingerprint, EnvKopiaServerCAFile, "/ca.pem"), []string{"only one of"}},
		{"tls option on http", base(EnvKopiaServerURL, "http://kopia", EnvKopiaServerInsecureSkipVerify, "true"), []string{"requires an https://"}},
		{"bad insecure bool", base(EnvKopiaServerInsecureSkipVerify, "yes"), []string{"KOPIA_SERVER_INSECURE_SKIP_VERIFY", "invalid boolean"}},
		{"relative host prefix", base(EnvHostPrefix, "host"), []string{"HOST_PREFIX", "not an absolute path"}},
		{"log level", base(EnvLogLevel, "verbose"), []string{"LOG_LEVEL", "unknown log level"}},
		{"reconcile duration", base(EnvReconcileInterval, "5"), []string{"RECONCILE_INTERVAL", "invalid duration"}},
		{"reconcile too short", base(EnvReconcileInterval, "1s"), []string{"shorter than the minimum"}},
		{"snapshot bool", base(EnvSnapshotOnWatcherStart, "maybe"), []string{"SNAPSHOT_ON_WATCHER_START"}},
		{"relative path prefix", base(EnvWatcherPathPrefix, "data"), []string{"WATCHER_PATH_PREFIX"}},
		{"listen addr", base(EnvListenAddr, "8080"), []string{"LISTEN_ADDR", "host:port"}},
		{"listen port", base(EnvListenAddr, ":0"), []string{"LISTEN_ADDR", "invalid port"}},
		{"webhook scheme", base(EnvNotifyWebhookURL, "ftp://x/secret-token"), []string{"NOTIFY_WEBHOOK_URL", "http:// or https://"}},
		{"label prefix", base(EnvLabelPrefix, "kopia."), []string{"LABEL_PREFIX"}},
		{"several errors at once", env(EnvLogLevel, "loud", EnvListenAddr, "x"), []string{"KOPIA_SERVER_URL", "LOG_LEVEL", "LISTEN_ADDR"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := Load(tt.env)
			if err == nil {
				t.Fatalf("expected error, got %+v", cfg)
			}

			for _, w := range tt.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not contain %q", err, w)
				}
			}

			if strings.Contains(err.Error(), "secret-token") {
				t.Errorf("error leaks the webhook URL: %q", err)
			}
		})
	}
}

func TestParseCertFingerprint(t *testing.T) {
	colons := ""
	for i := 0; i < len(testFingerprint); i += 2 {
		if i > 0 {
			colons += ":"
		}

		colons += strings.ToUpper(testFingerprint[i : i+2])
	}

	for _, in := range []string{testFingerprint, strings.ToUpper(testFingerprint), colons, " " + testFingerprint + "\n"} {
		got, err := ParseCertFingerprint(in)
		if err != nil {
			t.Errorf("ParseCertFingerprint(%q): %v", in, err)
		} else if got != testFingerprint {
			t.Errorf("ParseCertFingerprint(%q) = %q", in, got)
		}
	}

	for _, in := range []string{"", "zz" + testFingerprint[2:], testFingerprint + "00", "sha256:" + testFingerprint} {
		if _, err := ParseCertFingerprint(in); err == nil {
			t.Errorf("ParseCertFingerprint(%q): expected error", in)
		}
	}
}

func TestParseLabelPrefix(t *testing.T) {
	valid := map[string]string{
		"":                  DefaultLabelPrefix,
		"  ":                DefaultLabelPrefix,
		" backup ":          "backup",
		"k":                 "k",
		"com.example.kopia": "com.example.kopia",
		"my-app_2":          "my-app_2",
	}

	for in, want := range valid {
		got, err := ParseLabelPrefix(in)
		if err != nil || got != want {
			t.Errorf("ParseLabelPrefix(%q) = %q, %v; want %q", in, got, err, want)
		}
	}

	for _, in := range []string{"kopia.", ".kopia", "ko pia", "-kopia", "kopia/x"} {
		if _, err := ParseLabelPrefix(in); err == nil || !strings.Contains(err.Error(), "invalid label prefix") {
			t.Errorf("ParseLabelPrefix(%q): error = %v", in, err)
		}
	}
}

func TestParseLogLevel(t *testing.T) {
	tests := map[string]slog.Level{
		"debug":    slog.LevelDebug,
		"INFO":     slog.LevelInfo,
		"Warn":     slog.LevelWarn,
		"WARNING":  slog.LevelWarn,
		"error":    slog.LevelError,
		"CRITICAL": slog.LevelError,
	}

	for in, want := range tests {
		got, err := ParseLogLevel(in)
		if err != nil || got != want {
			t.Errorf("ParseLogLevel(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
}

func TestLogValueHidesSecrets(t *testing.T) {
	cfg, err := Load(base(
		EnvKopiaServerPassword, "super-secret",
		EnvNotifyWebhookURL, "https://hooks.example.com/services/T000/B000/token-xyz?key=abc",
	))
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer

	slog.New(slog.NewJSONHandler(&buf, nil)).Info("config", "config", cfg)

	out := buf.String()
	for _, secret := range []string{"super-secret", "token-xyz", "key=abc"} {
		if strings.Contains(out, secret) {
			t.Errorf("log output contains %q: %s", secret, out)
		}
	}

	for _, want := range []string{`"password_set":true`, `"notify_webhook":"https://hooks.example.com"`, `"url":"https://kopia:51515"`} {
		if !strings.Contains(out, want) {
			t.Errorf("log output lacks %s: %s", want, out)
		}
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()

	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}

	return string(b)
}
