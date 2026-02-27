# GrowthRS - Autoscaler

Requires Cmake.


## QA Testing

Adding/Removing Pods can be done using a standalone binary in this library. 

```bash
cargo run --bin test_pod -- create my-pod 2 4096Mi
cargo run --bin test_pod -- create gpu-pod 4 8192Mi --gpu 1
cargo run --bin test_pod -- delete my-pod
```
