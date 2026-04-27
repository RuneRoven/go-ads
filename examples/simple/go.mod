module github.com/RuneRoven/go-ads/examples/simple

go 1.26.2

require (
	github.com/RuneRoven/go-ads/v2 v0.0.0
	github.com/chzyer/readline v1.5.1
	gopkg.in/yaml.v3 v3.0.1
)

require (
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.12.0 // indirect
)

replace github.com/RuneRoven/go-ads/v2 => ../..
