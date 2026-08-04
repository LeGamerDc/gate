//go:build race

package gate

// raceEnabled 在开了竞态检测时为 true。
//
// 竞态插桩会改变逃逸分析并自带簿记分配：实测同一条 Send+flush 路径，
// 不带 -race 是 0 次分配，带 -race 稳定多 1 次。所以分配数的断言只在不开
// 竞态检测时才有意义，开了就跳过——`go test ./...` 会跑到它们，
// `go test -race ./...` 负责的是另一件事。
const raceEnabled = true
