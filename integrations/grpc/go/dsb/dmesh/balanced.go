package dmesh

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"

	"dmeshgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// balancedConn spreads RPCs across gRPC connections, each backed by a native
// QP. All QPs share the process channel and native service placement.
type balancedConn struct {
	conns []*grpc.ClientConn
	next  uint64
}

func (b *balancedConn) pick() *grpc.ClientConn {
	i := atomic.AddUint64(&b.next, 1)
	return b.conns[i%uint64(len(b.conns))]
}

func (b *balancedConn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	return b.pick().Invoke(ctx, method, args, reply, opts...)
}

func (b *balancedConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return b.pick().NewStream(ctx, desc, method, opts...)
}

// DialReplicated returns a ClientConnInterface round-robining over one dmesh
// ClientConn per replica, or (nil, nil) when name is not a replicated dmesh
// edge (caller then uses the normal single-conn path).
func DialReplicated(name string) (grpc.ClientConnInterface, error) {
	if !Enabled() || ReplicasOf(name) <= 1 {
		return nil, nil
	}
	n := ReplicasOf(name)
	b := &balancedConn{}
	for r := 0; r < n; r++ {
		rr := r
		d := func(ctx context.Context, _ string) (net.Conn, error) {
			_, ip := replicaKey(name, rr)
			return dmeshgo.DialContext(ctx, ip, 8086)
		}
		cc, err := grpc.NewClient("passthrough:///"+fmt.Sprintf("%s-%d", name, rr),
			grpc.WithContextDialer(d),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			for _, opened := range b.conns {
				opened.Close()
			}
			return nil, err
		}
		b.conns = append(b.conns, cc)
	}
	return b, nil
}
