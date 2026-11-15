#!/usr/bin/env bash
# Reproducible release archive for the PeeStream AutoPublish standalone connector.
# Same layout/practice as the WordPress and DLE packages: the archive is a release
# asset, SHA256SUMS sits in the repository root and names the asset only.
set -euo pipefail
cd "$(dirname "$0")/.."

VER=$(sed -n 's/^__version__ = "\(.*\)"$/\1/p' peestream_connector/__init__.py)
[ -n "$VER" ] || { echo "cannot read __version__" >&2; exit 1; }
NAME="peestream-autopublish-connector_v${VER}.tar.gz"
PREFIX="peestream-autopublish-connector-${VER}/"
MTIME=1763164800   # 2026-11-15T00:00:00Z, fixed so the archive is byte-reproducible

MANIFEST=(peestream_connector config.json.example packaging scripts tests \
          README.md CHANGELOG.md LICENSE SECURITY.md CONTRIBUTING.md)

mkdir -p dist
tar --sort=name --owner=0 --group=0 --numeric-owner --mtime="@${MTIME}" --format=gnu \
    -cf - --exclude=__pycache__ --exclude='*.pyc' --transform "s,^,${PREFIX}," \
    "${MANIFEST[@]}" | gzip -n -9 > "dist/${NAME}"

( cd dist && sha256sum "${NAME}" ) > SHA256SUMS
echo "built dist/${NAME}"
cat SHA256SUMS
