#!/usr/bin/env python3
"""
Checked-in skeleton for future protobuf descriptor -> codegen generation.

Expected future flow:
1. ingest FileDescriptorSet or equivalent schema JSON
2. normalize descriptor graph into MessageDesc / FieldDesc registry
3. emit:
   - schema registry artifacts
   - generic lowering bindings
   - selective-offload policy hooks
   - specialized DPA encoder stubs

This repo currently lacks protoc and protobuf runtime headers, so the sample
artifacts under codegen/generated/ are checked in manually.
"""

import argparse
import pathlib
import sys


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--descriptor-set", help="Path to FileDescriptorSet")
    parser.add_argument("--out-dir", required=True)
    parser.parse_args()
    out_dir = pathlib.Path(parser.parse_args().out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    print("proto_codegen_stub.py: no live generator implemented in this repo yet")
    print(f"proto_codegen_stub.py: out_dir={out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
