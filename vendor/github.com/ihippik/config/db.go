package config

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// DB represent struct for DB connection.
type DB struct {
	Host        string `yaml:"host" env:"HOST,required" valid:"required"`
	Port        int    `yaml:"host" env:"PORT,required" valid:"required"`
	User        string `yaml:"user" env:"USER,required" valid:"required"`
	Password    string `yaml:"password" env:"PASSWORD"`
	DBName      string `yaml:"db_name" env:"DB_NAME,required" valid:"required"`
	Schema      string `yaml:"schema" env:"SCHEMA"`
	MaxIdleConn int    `yaml:"max_idle_conn" env:"MAX_IDLE_CONN, default=2"`
	MaxOpenConn int    `yaml:"max_open_conn" env:"MAX_OPEN_CONN"`
}

// InitPG establish DB connection.
func InitPG(cfg *DB) (*sqlx.DB, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable search_path=%s",
		cfg.Host, cfg.Port,
		cfg.User, cfg.Password,
		cfg.DBName, cfg.Schema,
	)

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	db.SetMaxIdleConns(cfg.MaxIdleConn)
	db.SetMaxOpenConns(cfg.MaxOpenConn)

	return db, nil
}
