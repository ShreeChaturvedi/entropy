let meta = #{
    name: "fdb-facing-push",
    description: "Execute the FDB-facing push plan with phased implement+verify gates",
    phases: [
        #{ title: "Baseline", detail: "confirm worktree clean and tests build" },
        #{ title: "Round1", detail: "WS1 interfaces, WS3.1 truncate API, WS4 tooling" },
        #{ title: "Round2", detail: "WS2 index correctness and DDL, WS3 schedules" },
        #{ title: "Gate", detail: "full suite, sanitizers, issue hygiene checklist" },
    ],
};

let status_schema = #{
    "type": "object", "required": ["ok", "summary", "evidence"],
    "properties": #{
        "ok": #{ "type": "boolean" },
        "summary": #{ "type": "string" },
        "evidence": #{ "type": "string" },
        "blockers": #{ "type": "array", "items": #{ "type": "string" } },
    },
};

let wt = if args == () { () } else { args.worktree };
if wt == () {
    pause("verification", "Pass args.worktree — absolute path to feat/fdb-facing-push worktree.");
}

// ── Baseline ──────────────────────────────────────────────────────────────
phase("Baseline");
let base_prompt = "";
base_prompt += "Worktree: " + wt + "\n";
base_prompt += "Confirm git status is clean (or only expected .grok files), branch is feat/fdb-facing-push.\n";
base_prompt += "Configure and build tests if no build dir: cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DENTROPY_BUILD_TESTS=ON && cmake --build build -j.\n";
base_prompt += "Run a quick ctest subset or full if fast enough. Report ok=false on red baseline.\n";
base_prompt += "Do not modify product code. Use shell tools.\n";

let base = agent(base_prompt, #{
    label: "baseline",
    capability_mode: "execute",
    output_schema: status_schema,
});
if base == () || !base.success || base.output == () || base.output.ok != true {
    let s = "baseline failed";
    if base != () && base.success && base.output != () { s = base.output.summary; }
    complete(#{ summary: s, phase: "baseline", ok: false });
}
log("Baseline ok: " + base.output.evidence);

// ── Round 1 verification (implementation may already be in tree from orchestrator) ──
phase("Round1");
let r1_jobs = [];
r1_jobs.push(#{
    prompt: "Worktree " + wt + ". Spec-review WS1: abstract Index in src/storage/index.hpp, "
        + "TableStore/table_store.hpp, BPlusTree:Index, TableHeap:TableStore, IndexInfo shared_ptr<Index> "
        + "+ is_unique, manifest version bump. Use grep/read_file. ok=true only if all present and "
        + "project builds. Evidence must cite files. No product edits unless fixing a broken compile from "
        + "half-merged WS1 (prefer report blocker).",
    label: "verify-ws1",
    capability_mode: "execute",
    output_schema: status_schema,
});
r1_jobs.push(#{
    prompt: "Worktree " + wt + ". Spec-review WS3.1: LogStore has truncate (or discard_before), "
        + "FileLogStore + SimLogStore implement it, checkpoint path can truncate safely. "
        + "grep/read_file + optional build. ok only with evidence. No speculative product rewrites.",
    label: "verify-ws3-api",
    capability_mode: "execute",
    output_schema: status_schema,
});
r1_jobs.push(#{
    prompt: "Worktree " + wt + ". Spec-review WS4 tooling: .clang-format exists, ENTROPY_WERROR option, "
        + "ci CMake preset, nightly seed-sweep workflow, format-check without mass reformat. "
        + "ok only with file evidence.",
    label: "verify-ws4",
    capability_mode: "execute",
    output_schema: status_schema,
});
let r1 = parallel(r1_jobs);
let r1_fail = [];
for r in r1 {
    if r == () || !r.success || r.output == () || r.output.ok != true {
        let msg = "round1 verify failed";
        if r != () && r.success && r.output != () { msg = r.output.summary; }
        r1_fail.push(msg);
    }
}
if r1_fail.len() > 0 {
    complete(#{ summary: "Round1 incomplete", failures: r1_fail, ok: false });
}
log("Round1 verified");

// ── Round 2 verification ──────────────────────────────────────────────────
phase("Round2");
let r2_jobs = [];
r2_jobs.push(#{
    prompt: "Worktree " + wt + ". Spec-review WS2: red tests for index desync, non-unique find_all, "
        + "build_index error propagation; DML maintains indexes with transactional undo; "
        + "CREATE/DROP INDEX execute end-to-end. Run relevant ctest binaries. ok only if tests pass. "
        + "On failure use systematic debugging: reproduce, root cause, do not shotgun-fix.",
    label: "verify-ws2",
    capability_mode: "all",
    output_schema: status_schema,
});
r2_jobs.push(#{
    prompt: "Worktree " + wt + ". Spec-review WS3.2-3: truncation crash sim schedule in schedule_names, "
        + "must_fire contract, WAL bounded test under long workload, entropy-sim passes. "
        + "Run sim_tests. ok only with command evidence.",
    label: "verify-ws3-sched",
    capability_mode: "all",
    output_schema: status_schema,
});
let r2 = parallel(r2_jobs);
let r2_fail = [];
for r in r2 {
    if r == () || !r.success || r.output == () || r.output.ok != true {
        let msg = "round2 verify failed";
        if r != () && r.success && r.output != () { msg = r.output.summary; }
        r2_fail.push(msg);
    }
}
if r2_fail.len() > 0 {
    complete(#{ summary: "Round2 incomplete", failures: r2_fail, ok: false });
}
log("Round2 verified");

// ── Final gate ────────────────────────────────────────────────────────────
phase("Gate");
let gate_prompt = "";
gate_prompt += "Worktree " + wt + ". Run the full push gate:\n";
gate_prompt += "1) CI=1 cmake configure with ENTROPY_WERROR if available, Release, tests ON\n";
gate_prompt += "2) full ctest --output-on-failure\n";
gate_prompt += "3) If sanitizer builds exist/configured, run them; else document how CI runs them\n";
gate_prompt += "4) Build entropy-sim and run all schedules at modest seeds\n";
gate_prompt += "5) Checklist DoD: CREATE INDEX shell path, non-unique equality, UPDATE/DELETE index consistency, DROP INDEX, WAL bound, issues list for gh\n";
gate_prompt += "Use systematic-debugging on any failure. ok=true only with fresh command evidence. No success claims without running commands.\n";

let gate = agent(gate_prompt, #{
    label: "push-gate",
    capability_mode: "all",
    output_schema: status_schema,
});
if gate == () || !gate.success || gate.output == () || gate.output.ok != true {
    let s = "gate failed";
    if gate != () && gate.success && gate.output != () { s = gate.output.summary; }
    complete(#{ summary: s, ok: false, phase: "gate" });
}

complete(#{
    summary: "FDB-facing push verification workflow passed",
    ok: true,
    baseline: base.output.evidence,
    gate: gate.output.evidence,
});
