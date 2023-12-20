#!/bin/sh

set -x

rsync -azvh --progress ../crypto_bot \
  --exclude-from 'scripts/.rsync_ignore' \
  --prune-empty-dirs \
