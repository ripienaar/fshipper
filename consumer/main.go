package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/nats-io/nats.go"

	"github.com/ripienaar/fshipper/internal/util"
)

var mu = sync.Mutex{}
var logs = make(map[string]*rotatelogs.RotateLogs)
var subjectParse *regexp.Regexp

func main() {
	subject := os.Getenv("SHIPPER_SUBJECT")
	if subject == "" {
		log.Fatalf("Please set a NATS subject to consume using SHIPPER_SUBJECT")
	}

	directory := os.Getenv("SHIPPER_DIRECTORY")
	if directory == "" {
		log.Fatalf("Please set a directory to write using SHIPPER_DIRECTORY")
	}

	output := os.Getenv("SHIPPER_OUTPUT")
	if output == "" {
		log.Fatalf("Please set a file to write using SHIPPER_OUTPUT")
	}

	streamSource := os.Getenv("SHIPPER_STREAM_TARGET")
	if streamSource == "" {
		log.Fatalf("Please set a JetStream target subject using SHIPPER_STREAM_TARGET")
	}

	subjectParse = regexp.MustCompile(fmt.Sprintf(`%s\.p\d+\.(.+)`, subject))
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	// start consumers for every partition until ctx interrupt
	for _, partition := range partitions() {
		err := consumer(ctx, wg, streamSource, partition, directory, output)
		if err != nil {
			log.Fatalf("Consuming messages failed: %s", err)
		}
	}

	for {
		select {
		case <-util.SigHandler():
			log.Println("Shutting down after interrupt signal")
			cancel()
		case <-ctx.Done():
			wg.Wait()

			// we're the only routine now, no need to lock
			for _, logr := range logs {
				logr.Close()
			}

			return
		}
	}
}

// opens a RotateLogs instance, if output does not have any formatting will log to <output>-YYMMDDHHmm with daily
// rotation and weekly log aging
func setupLog(output string) (*rotatelogs.RotateLogs, error) {
	// people can set their own file format, if no formatting characters are in the string we default
	if !strings.Contains(output, "%") {
		output = output + "-%Y%m%d%H%M"
	}

	log.Printf("Creating new log %q", output)

	return rotatelogs.New(output,
		rotatelogs.WithMaxAge(7*24*time.Hour),
		rotatelogs.WithRotationTime(24*time.Hour),
	)
}

// partitions reads SHIPPER_READ_PARTITIONS and returns the list, '0' when unset
func partitions() []string {
	partitions := os.Getenv("SHIPPER_READ_PARTITIONS")
	if partitions == "" {
		log.Println("No SHIPPER_READ_PARTITIONS defaulting to '0'")
		return []string{"0"}
	}

	return strings.Split(partitions, ",")
}

// parse the subjects like xxx.p1.web1.example.net and extract "1"
func lineHost(s string) string {
	matches := subjectParse.FindStringSubmatch(s)
	if len(matches) != 2 {
		return ""
	}

	return matches[1]
}

// creates a map of RotateLogs by host, retrieve it or create it for the first line from a host
func getOrCreateLog(host string, path string) (log *rotatelogs.RotateLogs, err error) {
	mu.Lock()
	defer mu.Unlock()

	_, ok := logs[host]
	if !ok {
		logs[host], err = setupLog(path)
		if err != nil {
			return nil, err
		}
	}

	return logs[host], nil
}

// handleLine handles an individual line by parsing its subject and saving to the
// host specific log
func handleLine(directory string, output string, m *nats.Msg) (err error) {
	host := lineHost(m.Subject)
	if host == "" {
		return fmt.Errorf("could not extract host from %q", m.Subject)
	}

	logr, err := getOrCreateLog(host, filepath.Join(directory, host, output))
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(logr, string(m.Data))

	return err
}

func consumer(ctx context.Context, wg *sync.WaitGroup, prefixSubject string, partition string, directory string, output string) error {
	nc, err := util.NewConnection()
	if err != nil {
		return fmt.Errorf("could not connect to NATS: %s\n", err)
	}

	lines := make(chan *nats.Msg, 8*1024)
	subject := fmt.Sprintf("%s.p%s", prefixSubject, partition)

	_, err = nc.ChanSubscribe(subject, lines)
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		log.Printf("Waiting for messages on subject %s @ %s", subject, nc.ConnectedUrl())

		for {
			select {
			case m := <-lines:
				err = handleLine(directory, output, m)
				if err != nil {
					log.Printf("Could not handle line %q: %s", string(m.Data), err)
				}

			case <-ctx.Done():
				nc.Close()
				close(lines)

				return
			}
		}
	}()

	return nil
}
