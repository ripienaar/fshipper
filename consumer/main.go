package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/nats-io/nats.go"

	"github.com/ripienaar/fshipper/internal/util"
)

func main() {
	subject := os.Getenv("SHIPPER_SUBJECT")
	if subject == "" {
		log.Fatalf("Please set a NATS subject to consume using SHIPPER_SUBJECT\n")
	}

	output := os.Getenv("SHIPPER_OUTPUT")
	if output == "" {
		log.Fatalf("Please set a file to write using SHIPPER_OUTPUT\n")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	// start consuming messages until ctx interrupt
	err := consumer(ctx, done, subject, output)
	if err != nil {
		log.Fatalf("Consuming messages failed: %s", err)
	}

	for {
		select {
		case <-util.SigHandler():
			log.Println("Shutting down after interrupt signal")
			cancel()
		case <-done:
			log.Println("Shut down completed")
			return
		}
	}
}

func setupLog(output string) (*rotatelogs.RotateLogs, error) {
	// people can set their own file format, if no formatting characters are in the string we default
	if !strings.Contains(output, "%") {
		output = output + "-%Y%m%d%H%M"
	}

	return rotatelogs.New(output,
		rotatelogs.WithMaxAge(7*24*time.Hour),
		rotatelogs.WithRotationTime(24*time.Hour),
	)
}

func consumer(ctx context.Context, done chan struct{}, subject string, output string) error {
	nc, err := util.NewConnection()
	if err != nil {
		log.Fatalf("Could not connect to NATS: %s\n", err)
	}

	log.Printf("Waiting for messages on subject %s @ %s", subject, nc.ConnectedUrl())

	out, err := setupLog(output)
	if err != nil {
		return err
	}
	defer out.Close()

	lines := make(chan *nats.Msg, 8*1024)

	_, err = nc.ChanSubscribe(subject, lines)
	if err != nil {
		return err
	}

	// save lined in the background forever till context signals
	go func() {
		for {
			select {
			case m := <-lines:
				fmt.Fprintln(out, string(m.Data))
			case <-ctx.Done():
				nc.Close()
				close(lines)
				done <- struct{}{}
				return
			}
		}
	}()

	return nil
}
