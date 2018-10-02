package integrationferry

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/Shopify/ghostferry"
)

const (
	socketEnvName  string        = "GHOSTFERRY_INTEGRATION_SOCKET_PATH"
	socketTimeout  time.Duration = 30 * time.Second
	maxMessageSize int           = 256
)

const (
	CommandContinue string = "CONTINUE"
)

const (
	// Could only be sent once by the main thread
	StatusReady                  string = "READY"
	StatusReconcile              string = "RECONCILE"
	StatusStart                  string = "START"
	StatusBinlogStreamingStarted string = "BINLOG_STREAMING_STARTED"
	StatusRowCopyCompleted       string = "ROW_COPY_COMPLETED"
	StatusDone                   string = "DONE"

	// Could be sent by multiple goroutines in parallel
	StatusBeforeRowCopy     string = "BEFORE_ROW_COPY"
	StatusAfterRowCopy      string = "AFTER_ROW_COPY"
	StatusBeforeBinlogApply string = "BEFORE_BINLOG_APPLY"
	StatusAfterBinlogApply  string = "AFTER_BINLOG_APPLY"
)

type IntegrationFerry struct {
	*ghostferry.Ferry
}

// =========================================
// Code for integration server communication
// =========================================
func (f *IntegrationFerry) connect() (net.Conn, error) {
	socketAddress := os.Getenv(socketEnvName)
	if socketAddress == "" {
		return nil, fmt.Errorf("environment variable %s must be specified", socketEnvName)
	}

	return net.DialTimeout("unix", socketAddress, socketTimeout)
}

func (f *IntegrationFerry) send(conn net.Conn, status string) error {
	conn.SetDeadline(time.Now().Add(socketTimeout))

	_, err := conn.Write([]byte(status))
	return err
}

func (f *IntegrationFerry) receive(conn net.Conn) (string, error) {
	conn.SetDeadline(time.Now().Add(socketTimeout))

	var buf [maxMessageSize]byte

	n, err := conn.Read(buf[:])
	if err != nil {
		return "", err
	}

	return string(buf[0:n]), nil
}

// Sends a status string to the integration server and block until we receive
// "CONTINUE" from the server.
//
// We need to establish a new connection to the integration server for each
// message as there are multiple goroutines sending messages simultaneously.
func (f *IntegrationFerry) SendStatusAndWaitUntilContinue(status string) error {
	conn, err := f.connect()
	if err != nil {
		return err
	}
	defer conn.Close()

	err = f.send(conn, status)
	if err != nil {
		return err
	}

	command, err := f.receive(conn)
	if err != nil {
		return err
	}

	if command == CommandContinue {
		return nil
	}

	return fmt.Errorf("unrecognized command %s from integration server", command)
}

// Method override for Start in order to send status to the integration
// server.
func (f *IntegrationFerry) Start() error {
	f.Ferry.DataIterator.AddBatchListener(func(rowBatch *ghostferry.RowBatch) error {
		return f.SendStatusAndWaitUntilContinue(StatusBeforeRowCopy)
	})

	f.Ferry.BinlogStreamer.AddEventListener(func(events []ghostferry.DMLEvent) error {
		return f.SendStatusAndWaitUntilContinue(StatusBeforeBinlogApply)
	})

	err := f.Ferry.Start()
	if err != nil {
		return err
	}

	f.Ferry.DataIterator.AddBatchListener(func(rowBatch *ghostferry.RowBatch) error {
		return f.SendStatusAndWaitUntilContinue(StatusAfterRowCopy)
	})

	f.Ferry.BinlogStreamer.AddEventListener(func(events []ghostferry.DMLEvent) error {
		return f.SendStatusAndWaitUntilContinue(StatusAfterBinlogApply)
	})

	return nil
}

// ===========================================
// Code to handle an almost standard Ferry run
// ===========================================
// TODO: allow custom go code to hook into each stages so we can
// run things like the iterative verifier.
func (f *IntegrationFerry) Main() error {
	var err error

	err = f.SendStatusAndWaitUntilContinue(StatusReady)
	if err != nil {
		return err
	}

	err = f.Initialize()
	if err != nil {
		return err
	}

	// The reconciler currently does not emit any status while its running (i.e
	// its binlog streamer will not send anything to the integration server).
	// This is okay for now as we are not testing anything that involves
	// injecting races within the reconciler.
	f.SendStatusAndWaitUntilContinue(StatusReconcile)

	err = f.RunReconcilerIfNecessary()
	if err != nil {
		return err
	}

	f.SendStatusAndWaitUntilContinue(StatusStart)

	err = f.Start()
	if err != nil {
		return err
	}

	err = f.SendStatusAndWaitUntilContinue(StatusBinlogStreamingStarted)
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		f.Run()
	}()

	f.WaitUntilRowCopyIsComplete()
	err = f.SendStatusAndWaitUntilContinue(StatusRowCopyCompleted)
	if err != nil {
		return err
	}

	// TODO: this method should return errors rather than calling
	// the error handler to panic directly.
	f.FlushBinlogAndStopStreaming()
	wg.Wait()

	return f.SendStatusAndWaitUntilContinue(StatusDone)
}
