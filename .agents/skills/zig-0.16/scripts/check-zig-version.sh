#!/bin/bash
# Verify Zig 0.16.x is available
set -e

ZIG_CMD="${ZIG_CMD:-zig}"

if ! command -v "$ZIG_CMD" &> /dev/null; then
    echo "ERROR: zig not found. Set ZIG_CMD or add zig to PATH."
    exit 1
fi

VERSION=$("$ZIG_CMD" version)
MAJOR=$(echo "$VERSION" | cut -d. -f1)
MINOR=$(echo "$VERSION" | cut -d. -f2)

if [[ "$MAJOR" -eq 0 && "$MINOR" -ge 16 ]] || [[ "$MAJOR" -ge 1 ]]; then
    echo "OK: Zig $VERSION detected."
else
    echo "WARNING: Zig $VERSION detected, this skill is for 0.16+."
    exit 1
fi
