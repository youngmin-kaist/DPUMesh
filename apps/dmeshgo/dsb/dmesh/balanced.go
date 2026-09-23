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

// balancedConn round-robins RPCs over one gRPC ClientConn per replica. Each
// ClientConn is a single-channel passthrough dial, so it maps 1:1 onto a
// dmesh channel (1 channel = 1 h2 connection). This sidesteps gRPC's
// round_robin-over-subconns pooling, which does not deliver over the dmesh
// backend-channel spare-listener model.
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
	idx := keys[name]
	n := ReplicasOf(name)
	b := &balancedConn{}
	for r := 0; r < n; r++ {
		rr := r
		d := func(ctx context.Context, _ string) (net.Conn, error) {
			srv, ip := replicaKey(name, rr)
			return dmeshgo.Dial(srv, "127.0.0.1", clientBase()+idx*8+rr, ip, 8086, "hotelres-client")
		}
		cc, err := grpc.NewClient("passthrough:///"+fmt.Sprintf("%s-%d", name, rr),
			grpc.WithContextDialer(d),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		b.conns = append(b.conns, cc)
	}
	return b, nil
}
