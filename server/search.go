package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/QuantaStream/quantastream/searchindex"
	"github.com/akrylysov/pogreb"
	u "github.com/araddon/gou"
	"github.com/bbalet/stopwords"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/steakknife/bloomfilter"
	"golang.org/x/text/unicode/norm"
	"hash"
	"hash/fnv"
	"io"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

var (
	// Ensure StringSearch implements NodeService
	_ NodeService = (*StringSearch)(nil)
)

const (
	maxElements = 100
	probCollide = 0.0000001
)

var (
	wordSegmenter = regexp.MustCompile(`[\pL\p{Mc}\p{Mn}\p{Nd}-_']+`)
)

// StringSearch service state.
type StringSearch struct {
	*Node
	store        *pogreb.DB
	shutdownOnce sync.Once
}

// NewStringSearch - Construct server side state for search service.
func NewStringSearch(node *Node) *StringSearch {

	e := &StringSearch{Node: node}
	pb.RegisterStringSearchServer(node.server, e)
	return e
}

// Init search service.
func (m *StringSearch) Init() error {

	db, err := openVerifiedPogrebStore(filepath.Join(m.dataDir, "index", "search.dat"))
	if err != nil {
		return fmt.Errorf("cannot initialize string search service: %v", err)
	}
	m.store = db

	u.Info("Pre-warming  string search cache.")
	start := time.Now()
	count := 0
	it := db.Items()
	for {
		_, _, err := it.Next()
		if err != nil {
			if err != pogreb.ErrIterationDone {
				return fmt.Errorf("cannot initialize string search service: %v", err)
			}
			break
		}
		count++
	}
	elapsed := time.Since(start)
	u.Infof("Cache initialization complete %d items loaded in %s.\n", count, elapsed)
	return nil
}

// Shutdown search service.
func (m *StringSearch) Shutdown() {

	m.shutdownOnce.Do(func() {
		if m.store == nil {
			return
		}
		if err := m.store.Sync(); err != nil {
			u.Errorf("StringSearch sync failed: %v", err)
		}
		if err := m.store.Close(); err != nil {
			u.Errorf("StringSearch close failed: %v", err)
		}
		m.store = nil
	})
}

// JoinCluster - Join the cluster
func (m *StringSearch) JoinCluster() {
}

// BatchIndex - Insert a new batch of searchable strings.
func (m *StringSearch) BatchIndex(stream pb.StringSearch_BatchIndexServer) error {

	batch := make(map[string]struct{})
	for {
		sv, err := stream.Recv()
		if err == io.EOF {
			if err := m.BatchIndexStrings(stream.Context(), batch); err != nil {
				return err
			}
			return stream.SendAndClose(&empty.Empty{})
		}
		if err != nil {
			return err
		}
		str := sv.GetValue()
		if sv == nil || str == "" {
			return fmt.Errorf("String value must not be empty")
		}
		batch[str] = struct{}{}
	}
}

// BatchIndexStrings inserts a batch of searchable strings without requiring the
// streaming gRPC wrapper. Local in-process mode uses this path directly.
func (m *StringSearch) BatchIndexStrings(ctx context.Context, batch map[string]struct{}) error {
	if m.store == nil {
		return fmt.Errorf("string search service is not initialized")
	}
	for str := range batch {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Key is hash of original string
		hashVal := searchindex.HashValue(str)
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, hashVal)

		if found, err := m.store.Has(key); err != nil {
			return err
		} else if found {
			continue
		}

		// Construct bloom filter sans stopwords
		bloomFilter, err := constructBloomFilter(str)
		if err != nil {
			return err
		}

		bfBuf, err := bloomFilter.MarshalBinary()
		if err != nil {
			return err
		}

		if err := m.store.Put(key, bfBuf); err != nil {
			return err
		}
	}
	return m.store.Sync()
}

// Search - Execute a text search.
func (m *StringSearch) Search(searchStr *wrappers.StringValue, stream pb.StringSearch_SearchServer) error {

	search := searchStr.GetValue()
	if searchStr == nil || search == "" {
		return fmt.Errorf("Search string must not be empty")
	}
	results, err := m.SearchTerms(stream.Context(), search)
	if err != nil {
		return err
	}
	for v := range results {
		if err := stream.Send(&wrappers.UInt64Value{Value: v}); err != nil {
			return err
		}
	}
	return nil
}

// SearchTerms returns string hashes matching all search terms.
func (m *StringSearch) SearchTerms(ctx context.Context, search string) (map[uint64]struct{}, error) {
	if m.store == nil {
		return nil, fmt.Errorf("string search service is not initialized")
	}
	if search == "" {
		return nil, fmt.Errorf("Search string must not be empty")
	}
	terms := parseTerms(search)

	hashedTerms := make([]hash.Hash64, len(terms))

	for i, v := range terms {
		hasher := fnv.New64a()
		hasher.Write(v)
		hashedTerms[i] = hasher
	}

	bloomFilter, err := bloomfilter.NewOptimal(maxElements, probCollide)
	if err != nil {
		return nil, err
	}

	it := m.store.Items()
	results := make(map[uint64]struct{})
Top:
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		stringHash, val, err := it.Next()
		if err != nil {
			if err != pogreb.ErrIterationDone {
				return nil, err
			}
			break
		}

		if err := bloomFilter.UnmarshalBinary(val); err != nil {
			return nil, err
		}

		// Perform "and" comparison. Item will be selected if all terms are contained.
		for _, v := range hashedTerms {
			if !bloomFilter.Contains(v) {
				continue Top
			}
		}

		// return the hash of the original string value
		v := binary.LittleEndian.Uint64(stringHash[:8])
		results[v] = struct{}{}
	}
	return results, nil
}

func parseTerms(content string) [][]byte {

	cleanStr := stopwords.CleanString(content, "en", true)
	c := norm.NFC.Bytes([]byte(cleanStr))
	c = bytes.ToLower(c)
	return wordSegmenter.FindAll(c, -1)
}

func constructBloomFilter(content string) (*bloomfilter.Filter, error) {

	words := parseTerms(content)

	// Construct bloom filter sans stopwords
	//bloomFilter, err := bloomfilter.NewOptimal(uint64(len(words)), probCollide)
	bloomFilter, err := bloomfilter.NewOptimal(uint64(maxElements), probCollide)
	if err != nil {
		return nil, err
	}

	for _, v := range words {
		hasher := fnv.New64a()
		hasher.Write(v)
		bloomFilter.Add(hasher)
	}
	return bloomFilter, nil
}
