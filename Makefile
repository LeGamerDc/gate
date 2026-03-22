

bench:
	go test ./benchmark/... -run '^$$' -bench . -benchmem