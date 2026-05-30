package consumer_group_test

import (
	"testing"

	cg "github.com/Utkarsh272/mini-kafka/internal/consumer_group"
)

func TestOffsetStoreCommitAndFetch(t *testing.T) {
	dir := t.TempDir()
	store, err := cg.NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("NewOffsetStore: %v", err)
	}
	defer store.Close()

	// No commit yet → -1.
	if off := store.Fetch("group-A", "events", 0); off != -1 {
		t.Errorf("initial offset = %d, want -1", off)
	}

	// Commit and read back.
	if err := store.Commit("group-A", "events", 0, 42); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if off := store.Fetch("group-A", "events", 0); off != 42 {
		t.Errorf("committed offset = %d, want 42", off)
	}

	// Different group → still -1.
	if off := store.Fetch("group-B", "events", 0); off != -1 {
		t.Errorf("group-B offset = %d, want -1", off)
	}

	// Different partition → still -1.
	if off := store.Fetch("group-A", "events", 1); off != -1 {
		t.Errorf("partition 1 offset = %d, want -1", off)
	}
}

func TestOffsetStoreOverwrite(t *testing.T) {
	dir := t.TempDir()
	store, err := cg.NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("NewOffsetStore: %v", err)
	}
	defer store.Close()

	store.Commit("g", "topic", 0, 10)
	store.Commit("g", "topic", 0, 20)
	store.Commit("g", "topic", 0, 30)

	if off := store.Fetch("g", "topic", 0); off != 30 {
		t.Errorf("latest offset = %d, want 30", off)
	}
}

func TestOffsetStorePersistence(t *testing.T) {
	dir := t.TempDir()

	// Write offsets and close.
	{
		store, err := cg.NewOffsetStore(dir)
		if err != nil {
			t.Fatalf("NewOffsetStore: %v", err)
		}
		store.Commit("g1", "events", 0, 100)
		store.Commit("g1", "events", 1, 200)
		store.Commit("g2", "logs", 0, 50)
		store.Close()
	}

	// Reopen and verify replay.
	{
		store, err := cg.NewOffsetStore(dir)
		if err != nil {
			t.Fatalf("reopen NewOffsetStore: %v", err)
		}
		defer store.Close()

		if off := store.Fetch("g1", "events", 0); off != 100 {
			t.Errorf("g1/events/0 = %d, want 100", off)
		}
		if off := store.Fetch("g1", "events", 1); off != 200 {
			t.Errorf("g1/events/1 = %d, want 200", off)
		}
		if off := store.Fetch("g2", "logs", 0); off != 50 {
			t.Errorf("g2/logs/0 = %d, want 50", off)
		}
		if off := store.Fetch("unknown", "events", 0); off != -1 {
			t.Errorf("unknown group = %d, want -1", off)
		}
	}
}

func TestOffsetStoreManyGroups(t *testing.T) {
	dir := t.TempDir()
	store, err := cg.NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("NewOffsetStore: %v", err)
	}
	defer store.Close()

	// Write 5 groups × 3 partitions.
	for g := 0; g < 5; g++ {
		for p := int32(0); p < 3; p++ {
			offset := int64(g*100 + int(p))
			if err := store.Commit(
				"group-"+string(rune('A'+g)), "events", p, offset,
			); err != nil {
				t.Fatalf("Commit g=%d p=%d: %v", g, p, err)
			}
		}
	}

	// Verify each.
	for g := 0; g < 5; g++ {
		for p := int32(0); p < 3; p++ {
			want := int64(g*100 + int(p))
			got := store.Fetch("group-"+string(rune('A'+g)), "events", p)
			if got != want {
				t.Errorf("group-%c partition %d = %d, want %d",
					rune('A'+g), p, got, want)
			}
		}
	}
}
