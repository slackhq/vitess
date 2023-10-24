module vitess-mixin

go 1.13

require (
	github.com/evanphx/json-patch v5.6.0+incompatible
	github.com/google/go-jsonnet v0.16.0
	github.com/jsonnet-bundler/jsonnet-bundler v0.4.0
	github.com/krishicks/yaml-patch v0.0.10
	// Believe it or not, this is actually version 2.13.1
	// See https://github.com/prometheus/prometheus/issues/5590#issuecomment-546368944
	github.com/prometheus/prometheus v1.8.2-0.20191017095924-6f92ce560538
	vitess.io/vitess v0.17.2
)

replace k8s.io/client-go v2.0.0-alpha.0.0.20181121191925-a47917edff34+incompatible => k8s.io/client-go v2.0.0-alpha.1+incompatible
