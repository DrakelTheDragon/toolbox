package httpkit

import (
	"context"
	"errors"
	"net/http"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"
)

func Serve(ctx context.Context, h http.Handler, opts ...ConfigOption) error {
	var cfg Config

	for _, opt := range opts {
		opt.applyToConfig(&cfg)
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	srv := &http.Server{
		Addr:         cfg.Addr(),
		Handler:      h,
		IdleTimeout:  cfg.IdleTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	eg, egCtx, stop := withErrGroupNotifyContext(ctx)
	defer stop()

	eg.Go(func() error {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	eg.Go(func() error {
		<-egCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(ctx, cfg.ShutdownTimeout)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	})

	return eg.Wait()
}

func withErrGroupNotifyContext(ctx context.Context) (*errgroup.Group, context.Context, context.CancelFunc) {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	eg, ctx := errgroup.WithContext(ctx)
	return eg, ctx, cancel
}
