// Package control is the control socket of a running watcher: the list,
// trigger and restore commands, run with docker exec in the watcher's
// container, ask the watcher through it, so that a snapshot goes through
// its queue, with the hooks, the downtime and the minimum interval of the
// labels.
//
// The socket is a unix socket only its user can connect to, at
// CONTROL_SOCKET. The protocol is JSON lines: the client sends one
// Request, the watcher answers with any number of progress Messages, then
// one result or error Message, and closes the connection.
package control

import (
	"errors"
	"fmt"

	"github.com/rfdrew/kopia-docker-watcher/internal/reconcile"
	"github.com/rfdrew/kopia-docker-watcher/internal/restore"
)

// Version is the version of the protocol. A watcher answers a request of
// another version with an error.
const Version = 1

// Commands.
const (
	CommandList    = "list"
	CommandTrigger = "trigger"
	CommandRestore = "restore"
)

// Types of Message.
const (
	TypeProgress = "progress"
	TypeResult   = "result"
	TypeError    = "error"
)

// OutcomeQueued is the outcome of a trigger the client did not wait for.
const OutcomeQueued = "queued"

// maxRequest bounds the size of a request line.
const maxRequest = 64 << 10

// ErrNotRunning means that no watcher answers at the socket.
var ErrNotRunning = errors.New("no watcher answers")

// Request is what a command asks the watcher.
type Request struct {
	Version int    `json:"version"`
	Command string `json:"command"`
	// Target is the container or source path of trigger and restore.
	Target string `json:"target,omitempty"`

	// Force makes a trigger ignore the minimum interval; NoWait returns
	// once the trigger is queued.
	Force  bool `json:"force,omitempty"`
	NoWait bool `json:"noWait,omitempty"`

	// Snapshot is the snapshot ID a restore restores, empty for the newest
	// complete one; Into the directory on kopia server it restores into.
	// Overwrite lets it write into a directory that is not empty, Stop
	// stops the containers of the source while it runs.
	Snapshot  string `json:"snapshot,omitempty"`
	Into      string `json:"into,omitempty"`
	Overwrite bool   `json:"overwrite,omitempty"`
	Stop      bool   `json:"stop,omitempty"`
}

// Validate reports a request the watcher cannot serve.
func (r Request) Validate() error {
	switch {
	case r.Version != Version:
		return fmt.Errorf("protocol version %d, the watcher speaks %d: run the command of the watcher's own image", r.Version, Version)
	case r.Command == CommandList:
		return nil
	case r.Command != CommandTrigger && r.Command != CommandRestore:
		return fmt.Errorf("unknown command %q", r.Command)
	case r.Target == "":
		return errors.New("no container or path given")
	case r.Command == CommandRestore:
		return restore.CheckTarget(r.Into)
	}

	return nil
}

// Message is one line of the watcher's answer.
type Message struct {
	Type string `json:"type"`
	// Text is a progress message for the user.
	Text string `json:"text,omitempty"`
	// Error is the error of an error Message.
	Error string `json:"error,omitempty"`

	// The results of the commands.
	Sources  *reconcile.SourcesReport  `json:"sources,omitempty"`
	Triggers []reconcile.TriggerResult `json:"triggers,omitempty"`
	Restore  *restore.Result           `json:"restore,omitempty"`
	// RestoreProgress is how far a restore has got, in a progress Message.
	RestoreProgress *restore.Progress `json:"restoreProgress,omitempty"`
}
