# Moulinex

Moulinex is a simple and fast file shredder. It is primarily I/O-bound and designed to securely overwrite files. Note that it is **not cryptographically secure**, but sufficient for the large majority of use cases.

## Features
- High-performance multi-threaded processing with SIMD instructions.
- Achieves over 2 GB/s on NVMe M.2 SSDs (12 threads, AVX-512).
- Processes files entirely in RAM if sufficient memory is available; otherwise, it works in 512 MB blocks.
- File-level processing only (folders and entire drives are not supported yet).

## License
- Moulinex is released under the MIT license.
- Uses xsimd, which is licensed under BSD-3-Clause.
