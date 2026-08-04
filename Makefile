.PHONY: test race shuffle fuzz vet check bench

# -timeout 远小于 Go 默认的 10 分钟：这套用例里有不少真实 socket 收发，
# 一旦哪个用例卡住（对端不读、写阻塞、Wait 不返回），默认值会让 CI 白等十分钟
# 才给出一句没有测试名的 FAIL；短超时会直接打出 goroutine 栈，一眼看到是谁卡住。
GOTEST := go test -timeout 300s

# 常规一趟。分配断言（perf_test.go）只在这一趟生效：竞态插桩自带簿记分配，
# -race 下那个数不代表被测代码，所以只跑 race 的话性能回退不会被发现。
test:
	$(GOTEST) ./...

race:
	$(GOTEST) -race ./...

# 用例之间不得有顺序依赖或状态泄漏。
shuffle:
	$(GOTEST) -count=2 -shuffle=on ./...

vet:
	go vet ./...

# 合入前跑这四趟。
check: vet test race shuffle

# fuzz 单独跑：CI 用短时间，夜间用长时间（FUZZTIME=10m）。
FUZZTIME ?= 60s
fuzz:
	go test -run '^$$' -fuzz FuzzCodec -fuzztime $(FUZZTIME) .
	go test -run '^$$' -fuzz FuzzSenderRoundTrip -fuzztime $(FUZZTIME) .
	go test -run '^$$' -fuzz FuzzWebSocketInboundFrames -fuzztime $(FUZZTIME) .

bench:
	go test ./benchmark/... -run '^$$' -bench . -benchmem
