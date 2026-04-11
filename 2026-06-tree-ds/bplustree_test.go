package bplustree

import (
	"math/rand"
	"sort"
	"testing"
)
func TestSearchEmptyTree(t *testing.T) {
	tree := New()
	_, found := tree.Search(42)
	if found {
		t.Fatal("expected nothing in empty tree")
	}
}

func TestInsertAndSearch(t *testing.T) {
	tree := New()
	tree.Insert(10, "ten")
	tree.Insert(20, "twenty")
	tree.Insert(5, "five")

	cases := []struct {
		key      int
		expected string
	}{
		{10, "ten"},
		{20, "twenty"},
		{5, "five"},
	}
	for _, c := range cases {
		v, ok := tree.Search(c.key)
		if !ok {
			t.Errorf("key %d not found", c.key)
		}
		if v != c.expected {
			t.Errorf("key %d: got %v, want %v", c.key, v, c.expected)
		}
	}
}

func TestSearchMissingKey(t *testing.T) {
	tree := New()
	tree.Insert(1, "a")
	tree.Insert(2, "b")
	_, found := tree.Search(99)
	if found {
		t.Fatal("key 99 should not exist")
	}
}

// Update 

func TestUpdateExistingKey(t *testing.T) {
	tree := New()
	tree.Insert(7, "original")
	tree.Insert(7, "updated")
	v, ok := tree.Search(7)
	if !ok {
		t.Fatal("key 7 not found after update")
	}
	if v != "updated" {
		t.Errorf("got %v, want 'updated'", v)
	}
	if tree.Size() != 1 {
		t.Errorf("size should be 1, got %d", tree.Size())
	}
}


func TestSize(t *testing.T) {
	tree := New()
	for i := 0; i < 20; i++ {
		tree.Insert(i, i)
	}
	if tree.Size() != 20 {
		t.Errorf("expected size 20, got %d", tree.Size())
	}
}


func TestManyInsertsSequential(t *testing.T) {
	tree := New()
	n := 100
	for i := 0; i < n; i++ {
		tree.Insert(i, i*10)
	}
	for i := 0; i < n; i++ {
		v, ok := tree.Search(i)
		if !ok {
			t.Fatalf("key %d not found", i)
		}
		if v != i*10 {
			t.Fatalf("key %d: got %v, want %v", i, v, i*10)
		}
	}
}
