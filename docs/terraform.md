# With the Terraform provider

[terraform-provider-kopia](https://github.com/rfdrew/terraform-provider-kopia) (`rfdrew/kopia`) manages the
configuration of a kopia server as code, through the same HTTP API the watcher uses. The two work side by side
when each owns its own part of kopia server's configuration: Terraform what is set once per server or host, the
watcher what follows the containers.

## Who owns what

| Configuration | Owner | Why |
|---|---|---|
| The global policy, `(global)` | Terraform | Retention, compression, ignore rules, error handling: the defaults every source inherits. |
| Host and user policies, `@host` and `user@host` | Terraform | Defaults for all sources of one host, such as a schedule for the sources that set none. |
| Notification profiles | Terraform | kopia server's own notifications, about the snapshots it runs: scheduled ones and those the watcher asks for. |
| Throttle (`/api/v1/repo/throttle`) | Terraform | One set of limits per server. Leave the watcher's `KOPIA_THROTTLE_` variables unset. |
| The policies of the paths of containers, `user@host:/path` | the watcher | Their schedule and pause follow the labels and the state of the containers. |
| Snapshots of the watcher, their descriptions, the cleanup of removed containers | the watcher | They follow container events. |
| Notifications about the watcher's own snapshots and labels | the watcher, `NOTIFY_WEBHOOK_URL` | kopia server does not know about labels; see [configuration.md](configuration.md#notifications). |

The rules behind the table:

- **Never describe a path policy the watcher manages in Terraform.** The provider owns the whole policy of a
  target and replaces it on every apply, while the watcher writes the fields of its labels into it: each would
  undo the other's changes at every apply and every full reconcile. Put what all sources share into the global
  or host policy instead; the path policies inherit every field they do not set.
- **The watcher never writes the global policy or a host policy.** It only reads the effective policy of its
  paths, to check a schedule before it applies it. Within a path policy it writes only the fields its labels set
  and the pause, and keeps everything else, so a field set on a path in the Kopia web UI stays.
- **One owner for the throttle.** The watcher sets the throttle only when a `KOPIA_THROTTLE_` variable is set, and
  then at every start. With Terraform managing it, leave all of them unset. kopia server keeps the limits in its
  repository configuration, so they survive its restarts either way.
- **Keep paths apart with `WATCHER_PATH_PREFIX`** if Terraform also manages some path policies, for example of
  directories outside Docker: the watcher never registers, pauses or deletes a source outside the prefix
  ([configuration.md](configuration.md#watcher_path_prefix)).

## Notifications from both

kopia server's notification profiles report the snapshots kopia server runs: those on the schedule of the
policies, and those the watcher asks for at a container start or stop. The watcher's `NOTIFY_WEBHOOK_URL`
reports what kopia server cannot: a snapshot of the watcher whose end got lost, as when kopia server restarted,
labels kopia server rejects, and kopia server failing for a path for a while after the watcher started. It also reports a failed
snapshot of the watcher, once per path until one succeeds again. Both may post to the same ntfy topic or Slack
channel.

## Credentials

The watcher and the provider sign in the same way, as kopia server's UI user (`--server-username` and
`--server-password`), and read the same environment variables: `KOPIA_SERVER_URL`, `KOPIA_SERVER_USERNAME`,
`KOPIA_SERVER_PASSWORD`, `KOPIA_SERVER_CERT_FINGERPRINT`, `KOPIA_SERVER_CA_FILE` and
`KOPIA_SERVER_INSECURE_SKIP_VERIFY`. One `.env` can serve both; only `KOPIA_SERVER_URL` may differ, when Terraform
reaches kopia server from outside the Docker network.

The provider block, as in the provider's own example:

```hcl
terraform {
  required_providers {
    kopia = {
      source  = "rfdrew/kopia"
      version = "~> 0.1"
    }
  }
}

# Credentials are best supplied through the environment so they stay out of the
# configuration: KOPIA_SERVER_URL, KOPIA_SERVER_USERNAME and
# KOPIA_SERVER_PASSWORD.
provider "kopia" {
  url      = "https://kopia.example.com:51515"
  username = "kopia"
  password = var.kopia_password

  # The server generated its own certificate (`kopia server start
  # --tls-generate-cert`) and printed its fingerprint as "SERVER CERT SHA256".
  tls_cert_fingerprint = "cd50843049f0c6da48255e8022728d28124cb5b72fc3a2d81be496219c2cccf3"
}

variable "kopia_password" {
  description = "Password of the kopia server's UI user, as given to `kopia server start --server-password`."
  type        = string
  sensitive   = true
}
```

## Resources

The provider's documentation announces `kopia_policy` for policies, `kopia_notification_profile` and
`kopia_throttle`, but its first release, 0.1.0, is not out yet and has no resources so far. This page gets an
example of each part of the table above once they are released; until then, set the global and host policies,
the notification profiles and the throttle in the Kopia web UI or with the `kopia` command line, and keep to the
same split.
