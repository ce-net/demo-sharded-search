#!/bin/sh
# Replay the kill test from a fresh clone, with public dependencies only.
#   scripts/replay-kill-test.sh            # 500,000 documents (a few minutes)
#   LIMIT=all scripts/replay-kill-test.sh  # the whole corpus, 4,635,922 documents (the recorded run)
# Env: SHARDS (32), MACHINES (machine-a,machine-b,machine-c), KILL (last machine), ADD (machine-d, "" to skip),
#      BEFORE (30), AFTER (30), CONC (8), DATA (./data), OUT (./proof-local), DASHBOARD (127.0.0.1:7930, "" = none)
set -eu
cd "$(dirname "$0")/.."
DATA=${DATA:-$PWD/data}; OUT=${OUT:-$PWD/proof-local}; LIMIT=${LIMIT:-500000}; SHARDS=${SHARDS:-32}
URL=https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/dbpedia-entity.zip
mkdir -p "$DATA"
if [ ! -f "$DATA/corpus/corpus.jsonl" ]; then
  [ -f "$DATA/dbpedia-entity.zip" ] || curl -fL -o "$DATA/dbpedia-entity.zip" "$URL"
  mkdir -p "$DATA/corpus" && unzip -o -j -d "$DATA/corpus" "$DATA/dbpedia-entity.zip" >/dev/null
fi
cargo build --release --locked
BIN=${CARGO_TARGET_DIR:-$PWD/target}/release
L=""; [ "$LIMIT" = all ] || L="--limit $LIMIT"
RUN="$DATA/run-$LIMIT-$SHARDS"
if [ ! -f "$RUN/manifest.json" ]; then
  "$BIN/corpus-loader" --corpus "$DATA/corpus/corpus.jsonl" $L --shards "$SHARDS" --replicas 3 --out "$RUN" --cache "$RUN/cache" \
    --source "$URL (corpus.jsonl)" --name "BEIR DBpedia-Entity (English Wikipedia abstracts)"
fi
[ -f "$RUN/baseline.json" ] || "$BIN/search-eval" baseline --corpus "$DATA/corpus/corpus.jsonl" $L \
  --queries "$DATA/corpus/queries.jsonl" --qrels "$DATA/corpus/test.tsv" --out "$RUN/baseline.json"
D=""; [ -n "${DASHBOARD-127.0.0.1:7930}" ] && D="--dashboard ${DASHBOARD:-127.0.0.1:7930} --web $PWD"
exec "$BIN/kill-test" --bin "$BIN" --work "$RUN/kill-test" --out "$OUT" --corpus-dir "$RUN" \
  --queries "$DATA/corpus/queries.jsonl" --qrels "$DATA/corpus/test.tsv" --baseline "$RUN/baseline.json" \
  --machines "${MACHINES:-machine-a,machine-b,machine-c}" ${KILL:+--kill "$KILL"} --add "${ADD-machine-d}" \
  --before-s "${BEFORE:-30}" --after-s "${AFTER:-30}" --conc "${CONC:-8}" --seed-caches "${SEED:-first}" \
  --physical "${PHYSICAL:-All machines run on one computer}" $D
