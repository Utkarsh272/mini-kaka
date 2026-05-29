package broker

import (
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// metadataStore persists topic configuration in a bbolt database so that
// topic definitions survive broker restarts.
//
// Schema:
//
//	bucket "topics"
//	  key:   topic name (string)
//	  value: JSON-encoded topicConfig
//
// bbolt is embedded — no external process needed.
// It holds a single write lock on the file, so only one broker process
// can open the same database at a time (acceptable for portfolio scope;
// a multi-broker setup would use separate data directories).
type metadataStore struct {
	db *bolt.DB
}

// topicConfig is the persisted form of a topic's configuration.
type topicConfig struct {
	Name              string `json:"name"`
	NumPartitions     int32  `json:"num_partitions"`
	ReplicationFactor int32  `json:"replication_factor"`
}

var topicsBucket = []byte("topics")

// openMetadataStore opens (or creates) the bbolt database at path.
func openMetadataStore(path string) (*metadataStore, error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("open metadata db %s: %w", path, err)
	}

	// Ensure the topics bucket exists.
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(topicsBucket)
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("create topics bucket: %w", err)
	}

	return &metadataStore{db: db}, nil
}

// saveTopic persists a topic config. Idempotent — calling it again with the
// same name overwrites the previous record.
func (m *metadataStore) saveTopic(cfg topicConfig) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(topicsBucket)
		return b.Put([]byte(cfg.Name), data)
	})
}

// loadTopics returns all persisted topic configs, in bbolt's byte-sorted order.
func (m *metadataStore) loadTopics() ([]topicConfig, error) {
	var configs []topicConfig
	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(topicsBucket)
		return b.ForEach(func(k, v []byte) error {
			var cfg topicConfig
			if err := json.Unmarshal(v, &cfg); err != nil {
				return fmt.Errorf("unmarshal topic %s: %w", k, err)
			}
			configs = append(configs, cfg)
			return nil
		})
	})
	return configs, err
}

// deleteTopic removes a topic config from the store.
func (m *metadataStore) deleteTopic(name string) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(topicsBucket).Delete([]byte(name))
	})
}

// close closes the underlying bbolt database.
func (m *metadataStore) close() error {
	return m.db.Close()
}
