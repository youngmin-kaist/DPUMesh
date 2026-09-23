#!/usr/bin/env python3
"""Compile the public API as C and C++ and compare against the source ABI."""

import difflib
import os
from pathlib import Path
import shlex
import subprocess
import tempfile


def main():
    root = Path(__file__).resolve().parents[1]
    expected = (root / "tests/fixtures/native_abi_lp64.txt").read_text()
    with tempfile.TemporaryDirectory(prefix="dpumesh-abi-") as directory:
        for language, variable, default, standard in (
            ("c", "CC", "cc", "c11"),
            ("c++", "CXX", "c++", "c++17"),
        ):
            binary = Path(directory) / "probe"
            command = shlex.split(os.environ.get(variable, default))
            command += [
                "-x", language, f"-std={standard}", "-Wall", "-Wextra", "-Werror",
                "-I", str(root / "include"), str(root / "tests/native_abi_probe.c"),
                "-o", str(binary),
            ]
            subprocess.run(command, check=True)
            actual = subprocess.check_output([str(binary)], text=True)
            if actual != expected:
                diff = "".join(difflib.unified_diff(
                    expected.splitlines(True), actual.splitlines(True),
                    fromfile="native ABI reference (LP64)", tofile=language,
                ))
                raise SystemExit(diff)
            print(f"native_header_contract_test ({language}): PASS")


if __name__ == "__main__":
    main()
