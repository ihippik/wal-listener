package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	primaryConnStr = "postgres://postgres:postgres@localhost:5432/test_db?sslmode=disable"
	replicaConnStr = "postgres://postgres:postgres@localhost:5433/test_db?sslmode=disable"
)

func TestOriginFilteringRealIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	
	if os.Getenv("CI") != "" {
		t.Skip("Skipping integration test in CI environment - requires Docker Compose")
	}

	err := startDockerCompose()
	require.NoError(t, err)
	defer stopDockerCompose()

	time.Sleep(15 * time.Second)

	primaryDB, err := sql.Open("postgres", primaryConnStr)
	require.NoError(t, err)
	defer primaryDB.Close()

	replicaDB, err := sql.Open("postgres", replicaConnStr)
	require.NoError(t, err)
	defer replicaDB.Close()

	err = waitForDatabase(primaryDB, "primary")
	require.NoError(t, err)

	err = waitForDatabase(replicaDB, "replica")
	require.NoError(t, err)

	err = setupLogicalReplication(primaryDB, replicaDB)
	require.NoError(t, err)

	walCmd := startWalListener()
	defer func() {
		if walCmd.Process != nil {
			walCmd.Process.Kill()
		}
	}()

	time.Sleep(10 * time.Second)

	timestamp := time.Now().Unix()
	
	_, err = primaryDB.Exec("INSERT INTO test_table (id, name, source) VALUES ($1, $2, $3)", 
		timestamp, "local_data_primary", "primary")
	require.NoError(t, err)

	_, err = replicaDB.Exec("INSERT INTO test_table (id, name, source) VALUES ($1, $2, $3)", 
		timestamp+1, "replicated_data_replica", "replica")
	require.NoError(t, err)

	time.Sleep(15 * time.Second)


	logs, err := getWalListenerLogs()
	require.NoError(t, err)

	assert.Contains(t, logs, `"dropForeignOrigin":true`, "WAL listener should start with dropForeignOrigin enabled")

	localMessageCount := strings.Count(logs, `"name": "local_data_primary"`)
	replicatedMessageCount := strings.Count(logs, `"name": "replicated_data_replica"`)
	
	t.Logf("Local messages published: %d", localMessageCount)
	t.Logf("Replicated messages published: %d", replicatedMessageCount)
	
	if strings.Contains(logs, "dropping message due to foreign origin") {
		t.Log("✅ Origin filtering is working - found drop messages in logs")
	} else {
		t.Log("ℹ️ No origin drop messages found - PostgreSQL may not be sending origin info")
	}
	
	if replicatedMessageCount > 0 {
		t.Errorf("Expected 0 replicated messages but got %d - origin filtering should have dropped them", replicatedMessageCount)
	} else {
		t.Log("✅ Origin filtering working correctly - no replicated messages published")
	}
	
	if localMessageCount == 0 {
		t.Error("Expected local messages to be published, but none were found")
	}
}

func startDockerCompose() error {
	cmd := exec.Command("docker", "compose", "-f", "docker/docker-compose-integration.yml", "up", "-d")
	return cmd.Run()
}

func stopDockerCompose() {
	cmd := exec.Command("docker", "compose", "-f", "docker/docker-compose-integration.yml", "down", "-v")
	cmd.Run()
}

func waitForDatabase(db *sql.DB, name string) error {
	for i := 0; i < 30; i++ {
		err := db.Ping()
		if err == nil {
			log.Printf("%s database is ready", name)
			return nil
		}
		log.Printf("Waiting for %s database... (%v)", name, err)
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("%s database not ready after 60 seconds", name)
}

func setupLogicalReplication(primaryDB, replicaDB *sql.DB) error {
	subscriptionSQL := `
		CREATE SUBSCRIPTION test_sub 
		CONNECTION 'host=postgres_primary port=5432 dbname=test_db user=replicator password=replicator_password' 
		PUBLICATION test_pub;
	`
	
	_, err := replicaDB.Exec(subscriptionSQL)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("failed to create subscription: %v", err)
	}
	
	time.Sleep(5 * time.Second)
	
	log.Println("Logical replication subscription created")
	return nil
}

func startWalListener() *exec.Cmd {
	cmd := exec.Command("docker", "compose", "-f", "docker/docker-compose-integration.yml", "logs", "-f", "wal_listener")
	
	err := cmd.Start()
	if err != nil {
		log.Printf("Failed to start wal listener logs: %v", err)
	}
	
	return cmd
}

func getWalListenerLogs() (string, error) {
	cmd := exec.Command("docker", "compose", "-f", "docker/docker-compose-integration.yml", "logs", "wal_listener")
	
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	
	return string(output), nil
}
