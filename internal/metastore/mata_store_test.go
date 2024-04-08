package metastore

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"

	"github.com/koinos/koinos-proto-golang/v2/koinos/contract_meta_store"
)

const (
	MapBackendType    = 0
	BadgerBackendType = 1
)

var backendTypes = [...]int{MapBackendType, BadgerBackendType}

func NewBackend(backendType int) Backend {
	var backend Backend
	switch backendType {
	case MapBackendType:
		backend = NewMapBackend()
	case BadgerBackendType:
		dirname, err := ioutil.TempDir(os.TempDir(), "metastore-test-*")
		if err != nil {
			panic("unable to create temp directory")
		}
		opts := badger.DefaultOptions(dirname)
		backend = NewBadgerBackend(opts)
	default:
		panic("unknown backend type")
	}
	return backend
}

func CloseBackend(b interface{}) {
	switch t := b.(type) {
	case *MapBackend:
		break
	case *BadgerBackend:
		t.Close()
	default:
		panic("unknown backend type")
	}
}

type ErrorBackend struct {
}

func (backend *ErrorBackend) Reset() error {
	return nil
}

// Put returns an error
func (backend *ErrorBackend) Put(key []byte, value []byte) error {
	return errors.New("Error on put")
}

// Get gets an error
func (backend *ErrorBackend) Get(key []byte) ([]byte, error) {
	return nil, errors.New("Error on get")
}

type BadBackend struct {
}

func (backend *BadBackend) Reset() error {
	return nil
}

// Put returns an error
func (backend *BadBackend) Put(key []byte, value []byte) error {
	return nil
}

// Get gets an error
func (backend *BadBackend) Get(key []byte) ([]byte, error) {
	return []byte{0, 0, 255, 255, 255, 255, 255}, nil
}

type LongBackend struct {
}

func (backend *LongBackend) Reset() error {
	return nil
}

// Put returns an error
func (backend *LongBackend) Put(key []byte, value []byte) error {
	return nil
}

// Get gets an error
func (backend *LongBackend) Get(key []byte) ([]byte, error) {
	return []byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, nil
}

func TestAddMeta(t *testing.T) {
	// Add the contract meta
	for bType := range backendTypes {
		b := NewBackend(bType)
		store := NewContractMetaStore(b)

		// Test adding contract meta
		cmi := &contract_meta_store.ContractMetaItem{}
		cmi.Abi = "abcd1234"

		contractID := []byte{1, 2, 3}

		err := store.AddMeta(contractID, cmi)
		assert.NoError(t, err, "Error adding contract meta")

		meta, err := store.GetContractMeta(contractID)
		assert.NoError(t, err, "Error getting contract meta")
		assert.Equal(t, cmi.Abi, meta.Abi)

		// Test adding an already existing meta
		cmi.Abi = "abcd5678"
		err = store.AddMeta(contractID, cmi)
		assert.NoError(t, err, "Error adding contract meta")

		meta, err = store.GetContractMeta(contractID)
		assert.NoError(t, err, "Error getting contract meta")
		assert.Equal(t, cmi.Abi, meta.Abi)

		// Test adding second meta
		contractID = []byte{4, 5, 6}
		err = store.AddMeta(contractID, cmi)
		assert.NoError(t, err, "Error adding contract meta")

		meta, err = store.GetContractMeta(contractID)
		assert.NoError(t, err, "Error getting contract meta")
		assert.Equal(t, cmi.Abi, meta.Abi)

		CloseBackend(b)
	}

	// Test error backend
	{
		store := NewContractMetaStore(&ErrorBackend{})
		cmi := &contract_meta_store.ContractMetaItem{}

		err := store.AddMeta([]byte{1, 2, 3}, cmi)
		assert.ErrorIs(t, err, ErrBackend)

		_, err = store.GetContractMeta([]byte{1, 2, 3})
		assert.ErrorIs(t, err, ErrBackend)
	}
}
