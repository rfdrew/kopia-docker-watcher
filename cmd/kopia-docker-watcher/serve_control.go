package main

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/control"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
	"github.com/rfdrew/kopia-docker-watcher/internal/reconcile"
	"github.com/rfdrew/kopia-docker-watcher/internal/restore"
)

// listTimeout bounds the answer to list, which asks kopia server.
const listTimeout = 30 * time.Second

// serveControl serves the control socket of run, if it can: without it,
// the watcher runs, but the list, trigger and restore commands cannot
// reach it. The returned function closes it.
func (w *watcher) serveControl() func(context.Context) {
	listen := w.d.listenControl
	if listen == nil {
		listen = func(path string) (*control.Server, error) { return control.Listen(path, w.log) }
	}

	srv, err := listen(w.cfg.ControlSocket)
	if err != nil {
		w.log.Error("cannot serve the control socket; the list, trigger and restore commands will not reach the watcher",
			"path", w.cfg.ControlSocket, "error", err.Error())

		return func(context.Context) {}
	}

	w.control = srv
	done := make(chan struct{})

	go func() {
		defer close(done)

		if err := srv.Serve(); err != nil {
			w.log.Error("control socket failed; the list, trigger and restore commands no longer reach the watcher",
				"error", err.Error())
		}
	}()

	w.log.Info("serving the control socket", "path", srv.Path())

	return func(ctx context.Context) {
		if err := srv.Close(ctx); err != nil {
			w.log.Warn("control socket shutdown", "error", err.Error())
		}

		<-done
	}
}

// controlHandler serves the requests of the control socket with the
// running reconciler and kopia server's client.
func (w *watcher) controlHandler(rec *reconcile.Reconciler, kopia *kopiaclient.Client) control.Handler {
	return func(ctx context.Context, req control.Request, progress func(control.Message)) (control.Message, error) {
		switch req.Command {
		case control.CommandList:
			ctx, cancel := context.WithTimeout(ctx, listTimeout)
			defer cancel()

			rep, err := rec.Sources(ctx)
			if err != nil {
				return control.Message{}, err
			}

			return control.Message{Sources: &rep}, nil
		case control.CommandTrigger:
			return w.serveTrigger(ctx, rec, req, progress)
		case control.CommandRestore:
			return w.serveRestore(ctx, rec, kopia, req, progress)
		default:
			return control.Message{}, fmt.Errorf("unknown command %q", req.Command)
		}
	}
}

// serveTrigger asks for the snapshots and waits for them, unless the
// request says not to.
func (w *watcher) serveTrigger(ctx context.Context, rec *reconcile.Reconciler, req control.Request,
	progress func(control.Message),
) (control.Message, error) {
	ts, err := rec.Trigger(ctx, req.Target, req.Force)
	if err != nil {
		return control.Message{}, err
	}

	results := make([]reconcile.TriggerResult, len(ts))

	if req.NoWait {
		for i, t := range ts {
			results[i] = reconcile.TriggerResult{Path: t.Path(), Outcome: control.OutcomeQueued}
		}

		return control.Message{Triggers: results}, nil
	}

	for _, t := range ts {
		progress(control.Message{Text: "snapshot of " + t.Path() + " asked for; waiting for it to end"})
	}

	for i, t := range ts {
		select {
		case <-t.Done():
			results[i] = t.Result()
		case <-ctx.Done():
			return control.Message{}, ctx.Err()
		}

		w.log.Info("manual trigger done", "path", results[i].Path, "outcome", results[i].Outcome, "message", results[i].Message)
	}

	return control.Message{Triggers: results}, nil
}

// serveRestore restores a snapshot: it picks the snapshot, and with Stop
// stops the containers of the source around the restore.
func (w *watcher) serveRestore(ctx context.Context, rec *reconcile.Reconciler, kopia *kopiaclient.Client, req control.Request,
	progress func(control.Message),
) (control.Message, error) {
	text := func(s string) { progress(control.Message{Text: s}) }

	tgt, err := rec.RestoreTarget(ctx, req.Target)
	if err != nil {
		return control.Message{}, err
	}

	src := kopiaclient.SourceInfo{Host: tgt.Source.Host, UserName: tgt.Source.UserName, Path: tgt.Source.Path}

	snap, err := restore.Select(ctx, kopia, src, req.Snapshot)
	if err != nil {
		return control.Message{}, err
	}

	into := path.Clean(req.Into)
	desc := ""

	if snap.Description != "" {
		desc = " (" + snap.Description + ")"
	}

	text(fmt.Sprintf("restoring snapshot %s of %s, taken %s%s, into %s on kopia server", snap.ID, src,
		snap.StartTime.Local().Format(timeLayout), desc, into))

	if snap.Incomplete != "" {
		text("warning: the snapshot is incomplete (" + snap.Incomplete + "): it may lack files")
	}

	overlaps := within(into, src.Path) || within(src.Path, into)

	if overlaps && !req.Stop && len(tgt.Running) > 0 {
		text("warning: " + strings.Join(tgt.Running, ", ") + " run and may use the files while they are restored; " +
			"--stop stops them for the restore")
	}

	attrs := []any{"source", src.String(), "snapshot", snap.ID, "target", into, "overwrite", req.Overwrite, "stop", req.Stop}
	w.log.Info("restore asked for", attrs...)

	var res restore.Result

	run := func(ctx context.Context) error {
		var err error

		res, err = restore.Run(ctx, kopia, restore.Options{
			Snapshot:     snap,
			Target:       into,
			Overwrite:    req.Overwrite,
			CancelOnStop: req.Stop,
			Progress:     func(p restore.Progress) { progress(control.Message{RestoreProgress: &p}) },
		})
		if res.TaskID != "" && err == nil {
			text("kopia server's restore task " + res.TaskID + " ended: " + strings.ToLower(string(res.Status)))
		}

		return err
	}

	if req.Stop {
		err = rec.StopOwnersFor(ctx, tgt.Source, text, run)
	} else {
		err = run(ctx)
	}

	switch {
	case err != nil && res.TaskID == "":
		w.log.Error("restore failed", append(attrs, "error", err.Error())...)
		return control.Message{}, err
	case err != nil:
		if res.Error == "" {
			res.Error = err.Error()
		}

		if errors.Is(err, context.Canceled) && !req.Stop {
			res.Error = "stopped following the restore; kopia server's task " + res.TaskID + " goes on"
		}
	}

	if res.OK() {
		w.log.Info("restore done", append(attrs, "task", res.TaskID, "files", res.Files, "bytes", res.Bytes)...)
	} else {
		w.log.Error("restore failed", append(attrs, "task", res.TaskID, "status", string(res.Status), "error", res.Error)...)
	}

	return control.Message{Restore: &res}, nil
}

// within reports whether p is dir or lies under it.
func within(p, dir string) bool {
	return p == dir || strings.HasPrefix(p, strings.TrimSuffix(dir, "/")+"/")
}
