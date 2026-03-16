//go:build integration

package testutil

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type KafkaContainer struct {
	Container *kafka.KafkaContainer
	Brokers   []string
}

func StartKafkaContainer(ctx context.Context) (*KafkaContainer, error) {
	kafkaC, err := kafka.Run(ctx,
		"confluentinc/cp-kafka:7.6.0",
		kafka.WithClusterID("test-cluster"),
	)
	if err != nil {
		return nil, fmt.Errorf("start kafka container: %w", err)
	}

	brokers, err := kafkaC.Brokers(ctx)
	if err != nil {
		kafkaC.Terminate(ctx)
		return nil, fmt.Errorf("get kafka brokers: %w", err)
	}

	return &KafkaContainer{
		Container: kafkaC,
		Brokers:   brokers,
	}, nil
}

func (k *KafkaContainer) Terminate(ctx context.Context) {
	if k.Container != nil {
		k.Container.Terminate(ctx)
	}
}

type PostgresContainer struct {
	Container *postgres.PostgresContainer
	DSN       string
	Host      string
	Port      string
}

func StartPostgresContainer(ctx context.Context) (*PostgresContainer, error) {
	pgC, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("intu_test"),
		postgres.WithUsername("intu"),
		postgres.WithPassword("intu_test_pass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("start postgres container: %w", err)
	}

	dsn, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		pgC.Terminate(ctx)
		return nil, fmt.Errorf("get postgres connection string: %w", err)
	}

	host, err := pgC.Host(ctx)
	if err != nil {
		pgC.Terminate(ctx)
		return nil, fmt.Errorf("get postgres host: %w", err)
	}

	mappedPort, err := pgC.MappedPort(ctx, "5432")
	if err != nil {
		pgC.Terminate(ctx)
		return nil, fmt.Errorf("get postgres port: %w", err)
	}

	return &PostgresContainer{
		Container: pgC,
		DSN:       dsn,
		Host:      host,
		Port:      mappedPort.Port(),
	}, nil
}

func (p *PostgresContainer) Terminate(ctx context.Context) {
	if p.Container != nil {
		p.Container.Terminate(ctx)
	}
}

type SFTPContainer struct {
	Container testcontainers.Container
	Host      string
	Port      int
	User      string
	Password  string
}

func StartSFTPContainer(ctx context.Context) (*SFTPContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        "atmoz/sftp:alpine",
		ExposedPorts: []string{"22/tcp"},
		Cmd:          []string{"testuser:testpass:::upload"},
		WaitingFor:   wait.ForListeningPort("22/tcp").WithStartupTimeout(30 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("start sftp container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("get sftp host: %w", err)
	}

	mappedPort, err := container.MappedPort(ctx, "22")
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("get sftp port: %w", err)
	}

	return &SFTPContainer{
		Container: container,
		Host:      host,
		Port:      mappedPort.Int(),
		User:      "testuser",
		Password:  "testpass",
	}, nil
}

func (s *SFTPContainer) Terminate(ctx context.Context) {
	if s.Container != nil {
		s.Container.Terminate(ctx)
	}
}

type MailHogContainer struct {
	Container testcontainers.Container
	SMTPHost  string
	SMTPPort  int
	APIHost   string
	APIPort   int
}

func StartMailHogContainer(ctx context.Context) (*MailHogContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        "mailhog/mailhog:latest",
		ExposedPorts: []string{"1025/tcp", "8025/tcp"},
		WaitingFor:   wait.ForListeningPort("8025/tcp").WithStartupTimeout(30 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("start mailhog container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("get mailhog host: %w", err)
	}

	smtpPort, err := container.MappedPort(ctx, "1025")
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("get mailhog smtp port: %w", err)
	}

	apiPort, err := container.MappedPort(ctx, "8025")
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("get mailhog api port: %w", err)
	}

	return &MailHogContainer{
		Container: container,
		SMTPHost:  host,
		SMTPPort:  smtpPort.Int(),
		APIHost:   host,
		APIPort:   apiPort.Int(),
	}, nil
}

func (m *MailHogContainer) Terminate(ctx context.Context) {
	if m.Container != nil {
		m.Container.Terminate(ctx)
	}
}

// GreenMailContainer wraps a mail server Docker container that provides both
// SMTP (for sending test emails) and IMAP (for the EmailSource to poll).
type GreenMailContainer struct {
	Container testcontainers.Container
	Host      string
	SMTPPort  int
	IMAPPort  int
}

func StartGreenMailContainer(ctx context.Context) (*GreenMailContainer, error) {
	// Use Inbucket: SMTP (2500) + POP3 (1100). Reliable for CI; tests use POP3 to read mail.
	req := testcontainers.ContainerRequest{
		Image:        "inbucket/inbucket:latest",
		ExposedPorts: []string{"2500/tcp", "1100/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("2500/tcp").WithStartupTimeout(60*time.Second),
			wait.ForListeningPort("1100/tcp").WithStartupTimeout(60*time.Second),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("start mail container (inbucket): %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("get mail container host: %w", err)
	}

	smtpPort, err := container.MappedPort(ctx, "2500")
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("get mail container smtp port: %w", err)
	}

	pop3Port, err := container.MappedPort(ctx, "1100")
	if err != nil {
		container.Terminate(ctx)
		return nil, fmt.Errorf("get mail container pop3 port: %w", err)
	}

	return &GreenMailContainer{
		Container: container,
		Host:      host,
		SMTPPort:  smtpPort.Int(),
		IMAPPort:  pop3Port.Int(), // POP3 port; tests use this for "read" port
	}, nil
}

func (g *GreenMailContainer) SMTPAddr() string {
	return fmt.Sprintf("%s:%d", g.Host, g.SMTPPort)
}

func (g *GreenMailContainer) Terminate(ctx context.Context) {
	if g.Container != nil {
		g.Container.Terminate(ctx)
	}
}
