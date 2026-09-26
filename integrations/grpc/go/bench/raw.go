// Package bench: a protoc-free gRPC echo service using a passthrough codec,
// for measuring RPC throughput over the DPUMesh DMA transport.
package bench

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
)

// RawCodec passes []byte through unchanged (no proto marshaling).
type RawCodec struct{}

func (RawCodec) Marshal(v interface{}) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("rawcodec: expected []byte, got %T", v)
	}
	return b, nil
}

func (RawCodec) Unmarshal(data []byte, v interface{}) error {
	p, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("rawcodec: expected *[]byte, got %T", v)
	}
	*p = append((*p)[:0], data...)
	return nil
}

func (RawCodec) Name() string { return "raw" }

// MethodPing is the full method name of the echo RPC.
const MethodPing = "/dmesh.Echo/Ping"

type echoServer struct{}

func pingHandler(srv interface{}, ctx context.Context, dec func(interface{}) error,
	_ grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new([]byte)
	if err := dec(in); err != nil {
		return nil, err
	}
	return *in, nil // echo
}

// ServiceDesc is the hand-written descriptor for the echo service.
var ServiceDesc = grpc.ServiceDesc{
	ServiceName: "dmesh.Echo",
	HandlerType: (*interface{})(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Ping", Handler: pingHandler},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "dmesh-raw-echo",
}

// RegisterEcho registers the echo service on s.
func RegisterEcho(s *grpc.Server) {
	s.RegisterService(&ServiceDesc, echoServer{})
}
