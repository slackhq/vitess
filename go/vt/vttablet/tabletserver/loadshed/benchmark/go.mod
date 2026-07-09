module benchmark

go 1.24.13

require vitess.io/vitess v0.0.0

require (
	github.com/golang/glog v1.2.4 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20241121165744-79df5c4772f2 // indirect
	github.com/slok/noglog v0.2.0 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	google.golang.org/protobuf v1.36.5 // indirect
)

replace vitess.io/vitess => ../../../../../..
