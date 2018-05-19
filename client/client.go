package client

import (

	"context"
	"time"
	"hash/fnv"
	
	"google.golang.org/grpc"

	"github.com/OneOfOne/xxhash"
	pb "github.com/wangthomas/bloomfield/interfaces/gRPC/bloomfieldpb"
)


type Client interface {
	Create(ctx context.Context, filter string) error
	Add(ctx context.Context, filter string, keys []string) ([]bool, error)
	Has(ctx context.Context, filter string, keys []string) ([]bool, error)
	Drop(ctx context.Context, filter string) error
	Shutdown()
}

type bloomClient struct {
	hostname string
	timeout  time.Duration
	conn     *grpc.ClientConn
	client   pb.BloomClient
}


func NewBloomClient(hostname string, timeout time.Duration) (Client, error) {
	conn, err := grpc.Dial(hostname, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(timeout))
	if err != nil {
		return nil, err
	}

	return &bloomClient{
		hostname: hostname,
		timeout:  timeout,
		conn:     conn,
		client:   pb.NewBloomClient(conn),
	}, nil
}

// Create issues a create command on the server with the name specified by filter
func (t *bloomClient) Create(ctx context.Context, filter string) error {
	req := &pb.FilterRequest{Name: filter}

	timedCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	_, err := t.client.CreateFilter(timedCtx, req)
	return err
}

// Add issues a command to add a specified key to a given filter
func (t *bloomClient) Add(ctx context.Context, filter string, keys []string) ([]bool, error) {

	req := &pb.KeyRequest{
		FilterName: filter,
		Hashes: getHashes(keys),
	}

	timedCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	resp, err := t.client.Add(timedCtx, req)
	var has []bool
	if resp != nil {
		has = resp.Has
	}
	return has, err
}

// Has checks if a given key exists in a specified filter
func (t *bloomClient) Has(ctx context.Context, filter string, keys []string) ([]bool, error) {
	req := &pb.KeyRequest{
		FilterName: filter,
		Hashes: getHashes(keys),
	}

	timedCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	resp, err := t.client.Has(timedCtx, req)
	var has []bool
	if resp != nil {
		has = resp.Has
	}
	return has, err
}


// Drop removes a given filter from the server
func (t *bloomClient) Drop(ctx context.Context, filter string) error {
	req := &pb.FilterRequest{Name: filter}

	timedCtx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()
	_, err := t.client.DropFilter(timedCtx, req)
	return err
}

// Shutdown terminates the connection to the server
func (t *bloomClient) Shutdown() {
	t.conn.Close()
}


func getHashes(keys []string) []*pb.Hashes {
	var hashes []*pb.Hashes

	for _, key := range keys {
		h1 := fnv.New64a()
	    h1.Write([]byte(key))
	    hash1 := h1.Sum64()

	    h2 := xxhash.New64()
	    h2.Write([]byte(key))
	    hash2 := h2.Sum64()

	    hashes = append(hashes, &pb.Hashes{Hash1 : hash1,
	    								   Hash2 : hash2,})

	}

    return hashes
}

