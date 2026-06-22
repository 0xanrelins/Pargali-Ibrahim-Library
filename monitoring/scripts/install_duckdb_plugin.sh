#!/usr/bin/env bash
# Install MotherDuck DuckDB Grafana plugin (unsigned, manual) for local analytics panels.
set -euo pipefail

VERSION="${DUCKDB_PLUGIN_VERSION:-0.4.3}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PLUGIN_DIR="$ROOT/grafana/plugins/motherduck-duckdb-datasource"
ZIP_NAME="motherduck-duckdb-datasource-${VERSION}.zip"
DOWNLOAD_URL="https://github.com/motherduckdb/grafana-duckdb-datasource/releases/download/v${VERSION}/${ZIP_NAME}"
CACHE_ZIP="${TMPDIR:-/tmp}/${ZIP_NAME}"

if [[ -f "$PLUGIN_DIR/plugin.json" ]]; then
  installed_version="$(grep -o '"version": "[^"]*"' "$PLUGIN_DIR/plugin.json" | head -1 | cut -d'"' -f4)"
  if [[ "$installed_version" == "$VERSION" ]]; then
    echo "DuckDB plugin v${VERSION} already installed at $PLUGIN_DIR"
    exit 0
  fi
fi

echo "Downloading DuckDB Grafana plugin v${VERSION}..."
curl -fsSL "$DOWNLOAD_URL" -o "$CACHE_ZIP"

rm -rf "$PLUGIN_DIR"
mkdir -p "$ROOT/grafana/plugins"
unzip -q "$CACHE_ZIP" -d "$ROOT/grafana/plugins"

if [[ ! -f "$PLUGIN_DIR/plugin.json" ]]; then
  echo "Install failed: plugin.json not found in $PLUGIN_DIR" >&2
  exit 1
fi

echo "Installed DuckDB plugin to $PLUGIN_DIR"
