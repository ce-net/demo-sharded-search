#!/bin/sh
# CI: the whole kill test on a small synthetic corpus (no download): 4 shards x 3 replicas on 3 simulated machines,
# lose one, add one, and check the recorded numbers hold.
set -eu
cd "$(dirname "$0")/.."
D=${D:-$PWD/ci-data}; mkdir -p "$D"
python3 - "$D" <<'PY'
import json, random, sys
d = sys.argv[1]; r = random.Random(7)
words = ["gear%d" % i for i in range(3000)]
with open(d + "/corpus.jsonl", "w") as f:
    for i in range(20000):
        f.write(json.dumps({"_id": "doc%d" % i, "title": "Doc %d" % i, "text": " ".join(r.choice(words) for _ in range(30))}) + "\n")
with open(d + "/queries.jsonl", "w") as f:
    for i in range(50):
        f.write(json.dumps({"_id": "q%02d" % i, "text": " ".join(r.choice(words) for _ in range(3))}) + "\n")
PY
B=${CARGO_TARGET_DIR:-$PWD/target}/release
"$B/corpus-loader" --corpus "$D/corpus.jsonl" --shards 4 --replicas 3 --out "$D/run" --cache "$D/run/cache" --name synthetic
"$B/search-eval" baseline --corpus "$D/corpus.jsonl" --queries "$D/queries.jsonl" --out "$D/run/baseline.json"
"$B/kill-test" --bin "$B" --work "$D/run/kt" --out "$D/proof" --corpus-dir "$D/run" --queries "$D/queries.jsonl" \
  --baseline "$D/run/baseline.json" --before-s 5 --after-s 5 --conc 4 --add machine-d --store-port 7951 --router-port 7952
python3 - "$D/proof/run.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1])); k = r["kill"]
assert r["boot"]["instances"] == 12, r["boot"]
assert r["answers_identical"] and r["recall_identical"], "recall changed"
assert k["elect_under_two_leases"] or k["leaders_lost"] == 0, k
assert r["latency"]["errors"] == 0, r["latency"]
assert r["cap_test"]["query"].startswith("refused"), r["cap_test"]
assert r["cap_test"]["audit_rows"], "no audit row"
assert r["add_machine"]["ready_instances_per_machine"].get("machine-d", 0) > 0, r["add_machine"]
print("ok:", k["leaders_lost"], "leaders lost, new within", k["elect_ms_max"], "ms; whole in", k["replicas_whole_ms"], "ms; recall", r["recall_after"]["recall_at_10"])
PY
