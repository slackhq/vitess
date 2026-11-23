#!/bin/bash

# Runs `make proto` in a Speck devspace pod, using the protoc and vtadmin proto
# tools baked into the pod's image (see devspaces.yaml).
#
# Plain `make proto` first runs bootstrap.sh, which downloads protoc, etcd,
# consul and toxiproxy into dist/, and `npm install` for all of web/vtadmin.
# This links the image's protoc where the Makefile expects bootstrap.sh to have
# put it, and the image's pbjs/pbts where generate-proto-types.sh looks for
# them, then runs the Makefile's own `proto` target with those two install steps
# skipped. The Go plugin versions, protoc flags and vtctldclient codegen all
# come from the checked-out Makefile, so the output should match what CI's
# tools/check_make_proto.sh and tools/check_make_vtadmin_web_proto.sh generate.
# On main those checks currently fail on web/vtadmin/src/proto/vtadmin.js
# itself, so until a regenerated copy lands there, expect it to show as modified.
#
# It doesn't source build.env, which also installs git hooks.
#
# Usage, from anywhere in the repo: tools/speck_make_proto.sh

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
export VTROOT="$PWD"

protoc_ver=$(sed -n 's/^export PROTOC_VER=//p' build.env)
image_protoc_dir="/opt/vt-protoc-${protoc_ver}"
image_node_modules=/opt/vtadmin-proto-tools/node_modules

if [[ -z "$protoc_ver" || ! -x "$image_protoc_dir/bin/protoc" ]]; then
	echo "ERROR: no protoc at $image_protoc_dir. build.env pins protoc '$protoc_ver', and the devspace" \
		"image (slack/vitess-bedrock vtgate/Dockerfile.devspace) must ship the same release." >&2
	exit 1
fi
if [[ ! -x "$image_node_modules/.bin/pbjs" || ! -x "$image_node_modules/.bin/pbts" ]]; then
	echo "ERROR: no pbjs/pbts in $image_node_modules; this script needs the devspace image's vtadmin proto tools." >&2
	exit 1
fi

mkdir -p bin dist
# Leave a real protoc that bootstrap.sh unpacked alone.
if [[ ! -e "dist/vt-protoc-${protoc_ver}" || -L "dist/vt-protoc-${protoc_ver}" ]]; then
	ln -snf "$image_protoc_dir" "dist/vt-protoc-${protoc_ver}"
fi
ln -snf "$VTROOT/dist/vt-protoc-${protoc_ver}/bin/protoc" bin/protoc

# Use a real `npm install` of web/vtadmin when there is one; otherwise borrow the
# image's packages for this run only.
linked_node_modules=false
cleanup() {
	if $linked_node_modules; then
		rm -f web/vtadmin/node_modules
	fi
}
trap cleanup EXIT
if [[ ! -e web/vtadmin/node_modules || "$(readlink web/vtadmin/node_modules)" == "$image_node_modules" ]]; then
	# A link this script left behind (a run killed before its EXIT trap) counts as missing.
	ln -snf "$image_node_modules" web/vtadmin/node_modules
	linked_node_modules=true
	# Other versions would rewrite the bindings for reasons unrelated to the .proto change.
	for pkg in protobufjs protobufjs-cli; do
		want=$(node -p "require('./web/vtadmin/package-lock.json').packages['node_modules/$pkg'].version")
		have=$(node -p "require('$image_node_modules/$pkg/package.json').version")
		if [[ "$want" != "$have" ]]; then
			echo "ERROR: web/vtadmin/package-lock.json pins $pkg $want, but the devspace image has $have." >&2
			exit 1
		fi
	done
fi

# -o treats a target as already made without running its recipe.
make -o minimaltools -o vtadmin_web_install proto

git status --short -- go/vt/proto web/vtadmin/src/proto go/vt/vtctl
