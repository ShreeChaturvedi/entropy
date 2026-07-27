let meta = #{
    name: "fdb-facing-finish",
    description: "Issue hygiene and push for feat/fdb-facing-push after gate green",
    phases: [
        #{ title: "Issues", detail: "close fixed issues, open audit remainders" },
        #{ title: "Push", detail: "push branch after re-check clean status" },
        #{ title: "Verify", detail: "confirm remote and issue states" },
    ],
};

let status_schema = #{
    "type": "object", "required": ["ok", "summary", "evidence"],
    "properties": #{
        "ok": #{ "type": "boolean" },
        "summary": #{ "type": "string" },
        "evidence": #{ "type": "string" },
        "issue_numbers": #{ "type": "array", "items": #{ "type": "integer" } },
    },
};

let wt = if args == () { () } else { args.worktree };
if wt == () {
    pause("verification", "Pass args.worktree — absolute path to feat/fdb-facing-push worktree.");
}

phase("Issues");
let issues_prompt = "";
issues_prompt += "Worktree: " + wt + "\n";
issues_prompt += "Repo github.com/ShreeChaturvedi/entropy. Use gh CLI. Branch feat/fdb-facing-push.\n";
issues_prompt += "Human-writing for all comments: plain, short, no AI trailers, no self-credit, no em dashes.\n\n";
issues_prompt += "1) Close #8 with comment linking commits that fix non-unique indexes / find_all / build_index:\n";
issues_prompt += "   542ae8f, bf1a1b2, 76bfc65 (and related on the branch). State what was fixed.\n";
issues_prompt += "2) Close #11 with comment linking c476e75, 7293b27 (DML index maintenance + abort undo).\n";
issues_prompt += "3) Close #12: already fixed on main via PR #85 / commits like 867cee4, a729ccc, aa57119, a085a49.\n";
issues_prompt += "   Short comment that the filed bugs are fixed; close.\n";
issues_prompt += "4) Close #35: parser work already on main (PR #77: CREATE/DROP INDEX parse, DECIMAL/TIMESTAMP,\n";
issues_prompt += "   unterminated strings, dup columns). This branch also executes CREATE/DROP INDEX (fe67dc0).\n";
issues_prompt += "   If true remainders exist (narrow), open one small follow-up issue and link it; else just close.\n";
issues_prompt += "5) Close #41: sanitizers/benches landed on main (PR #79 / 42bb626). This branch adds\n";
issues_prompt += "   ENTROPY_WERROR, ci preset, format-check (changed files), nightly seed sweep.\n";
issues_prompt += "   Open a small follow-up only if clang-tidy/coverage still missing and worth tracking.\n";
issues_prompt += "6) Close #39: docs align commit 967a143 on main; note remaining doc drift only if real, else close.\n";
issues_prompt += "7) Open NEW issues (one each, focused titles/bodies):\n";
issues_prompt += "   a) EXPLAIN builds text by hand instead of walking PlanNode tree (database.cpp)\n";
issues_prompt += "   b) Statistics never collected at runtime (no ANALYZE; collect_statistics only from tests)\n";
issues_prompt += "   c) Buffer pool eviction never crash-tested (sim pool sized to avoid eviction)\n";
issues_prompt += "   d) LogStore not injectable through public Database API\n";
issues_prompt += "Use gh issue close and gh issue create. Report all numbers in evidence. ok=true only if all closes/creates succeed.\n";

let issues = agent(issues_prompt, #{
    label: "issue-hygiene",
    capability_mode: "all",
    output_schema: status_schema,
});
if issues == () || !issues.success || issues.output == () || issues.output.ok != true {
    let s = "issue hygiene failed";
    if issues != () && issues.success && issues.output != () { s = issues.output.summary; }
    complete(#{ summary: s, ok: false, phase: "issues" });
}
log("Issues: " + issues.output.evidence);

phase("Push");
let push_prompt = "";
push_prompt += "Worktree: " + wt + "\n";
push_prompt += "1) git status: must be clean except untracked build dirs. If uncommitted workflow file changes exist for\n";
push_prompt += "   fdb-facing-finish.rhai or similar plan tooling, commit with a plain message, no AI trailer.\n";
push_prompt += "2) git push -u origin feat/fdb-facing-push\n";
push_prompt += "3) Confirm with git status -sb and git log origin/feat/fdb-facing-push -1 --oneline\n";
push_prompt += "Do not force-push. Do not merge to main. ok=true only with successful push evidence.\n";

let push = agent(push_prompt, #{
    label: "push-branch",
    capability_mode: "all",
    output_schema: status_schema,
});
if push == () || !push.success || push.output == () || push.output.ok != true {
    let s = "push failed";
    if push != () && push.success && push.output != () { s = push.output.summary; }
    complete(#{ summary: s, ok: false, phase: "push", issues: issues.output.evidence });
}

phase("Verify");
let verify_prompt = "";
verify_prompt += "Repo ShreeChaturvedi/entropy. Confirm with gh/git tools:\n";
verify_prompt += "- origin has feat/fdb-facing-push\n";
verify_prompt += "- issues #8 #11 #12 #35 #41 #39 are CLOSED\n";
verify_prompt += "- four new issues exist for EXPLAIN, ANALYZE/stats, eviction sim, injectable LogStore\n";
verify_prompt += "ok=true only with gh output evidence.\n";

let v = agent(verify_prompt, #{
    label: "verify-remote",
    capability_mode: "execute",
    output_schema: status_schema,
});
if v == () || !v.success || v.output == () || v.output.ok != true {
    let s = "remote verify failed";
    if v != () && v.success && v.output != () { s = v.output.summary; }
    complete(#{ summary: s, ok: false, phase: "verify" });
}

complete(#{
    summary: "Issues updated and feat/fdb-facing-push pushed",
    ok: true,
    issues: issues.output.evidence,
    push: push.output.evidence,
    verify: v.output.evidence,
});
