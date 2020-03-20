package util

import (
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

// NewConnection creates a new NATS connection configured from the Environment
func NewConnection() (nc *nats.Conn, err error) {
	servers := os.Getenv("NATS_URL")
	if servers == "" {
		return nil, fmt.Errorf("specify a server to connect to using NATS_URL")
	}

	opts := []nats.Option{
		nats.MaxReconnects(-1),
		nats.ErrorHandler(errorHandler),
		nats.ReconnectHandler(reconnectHandler),
		nats.DisconnectErrHandler(disconnectHandler),
	}

	if os.Getenv("NATS_CREDS") != "" {
		opts = append(opts, nats.UserCredentials(os.Getenv("NATS_CREDS")))
	}

	if os.Getenv("NATS_CERTIFICATE") != "" && os.Getenv("NATS_KEY") != "" {
		opts = append(opts, nats.ClientCert(os.Getenv("NATS_CERTIFICATE"), os.Getenv("NATS_KEY")))
	}

	if os.Getenv("NATS_CA") != "" {
		opts = append(opts, nats.RootCAs(os.Getenv("NATS_CA")))
	}

	// initial connections can error due to DNS lookups etc, just retry, eventually with backoff
	for {
		nc, err := nats.Connect(servers, opts...)
		if err == nil {
			return nc, nil
		}

		log.Printf("could not connect to NATS: %s\n", err)

		time.Sleep(500 * time.Millisecond)
	}
}

// SigHandler sets up interrupt signal handlers
func SigHandler() chan os.Signal {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)

	return sigs
}

// Partition calculates a partition to publish data to based on the hostname
// always yields the same partition for the same name. Uses SHIPPER_PARTITIONS
// to determine the amount of desired partitions. SHIPPER_PARTITIONS of 0 will
// always return partition 0
func Partition() (part uint32, name string, err error) {
	name, err = hostname()
	if err != nil {
		return 0, "", fmt.Errorf("could not determine the hostname, set HOSTNAME to override: %s", err)
	}

	partitions := 0

	partString := os.Getenv("SHIPPER_PARTITIONS")
	if partString != "" {
		partitions, err = strconv.Atoi(partString)
		if err != nil {
			return 0, name, fmt.Errorf("could not process partitions as an integer: %s", err)
		}
	}

	if partitions == 0 {
		return 0, name, nil
	}

	if partitions < 0 {
		return 0, name, fmt.Errorf("partitions has to be >= 0")
	}

	h := fnv.New32a()
	h.Write([]byte(name))

	return h.Sum32() % uint32(partitions), name, nil
}

func hostname() (string, error) {
	if os.Getenv("HOSTNAME") != "" {
		return os.Getenv("HOSTNAME"), nil
	}

	return os.Hostname()
}

// called during errors subscriptions etc
func errorHandler(nc *nats.Conn, s *nats.Subscription, err error) {
	if s != nil {
		log.Printf("Error in NATS connection: %s: subscription: %s: %s", nc.ConnectedUrl(), s.Subject, err)
		return
	}

	log.Printf("Error in NATS connection: %s: %s", nc.ConnectedUrl(), err)
}

// called after reconnection
func reconnectHandler(nc *nats.Conn) {
	log.Printf("Reconnected to %s", nc.ConnectedUrl())
}

// called after disconnection
func disconnectHandler(nc *nats.Conn, err error) {
	if err != nil {
		log.Printf("Disconnected from NATS due to error: %v", err)
	} else {
		log.Printf("Disconnected from NATS")
	}
}
