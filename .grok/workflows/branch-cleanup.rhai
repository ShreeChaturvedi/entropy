let meta = #{
    name: "branch-cleanup",
    description: "Audit local branches and worktrees against origin/main; recommend safe deletes",
    phases: [
        #{ title: "Audit", detail: "classify each branch as merged, obsolete, or keep" },
        #{ title: "Verify", detail: "adversarial check of keep vs delete decisions" },
    ],
};

let report_schema = #{
    "type": "object", "required": ["merged_delete", "obsolete_delete", "keep", "notes"],
    "properties": #{
        "merged_delete": #{ "type": "array", "items": #{ "type": "string" } },
        "obsolete_delete": #{ "type": "array", "items": #{ "type": "string" } },
        "keep": #{ "type": "array", "items": #{ "type": "string" } },
        "notes": #{ "type": "string" },
    },
};

let verdict_schema = #{
    "type": "object", "required": ["safe", "reason"],
    "properties": #{
        "safe": #{ "type": "boolean" },
        "reason": #{ "type": "string" },
    },
};

let root = if args == () { () } else { args.root };
if root == () {
    pause("verification", "Pass args.root — absolute path to the entropy repo root.");
}

phase("Audit");
let audit_prompt = "";
audit_prompt += "You are auditing git branches in " + root + ".\n";
audit_prompt += "Run git commands with the shell tool. For every local branch except main and feat/fdb-facing-push:\n";
audit_prompt += "1) Count commits ahead/behind origin/main.\n";
audit_prompt += "2) If ahead=0 and branch is ancestor of origin/main, put it in merged_delete.\n";
audit_prompt += "3) If ahead>0, inspect the unique commits with git log and git diff against origin/main.\n";
audit_prompt += "   If main already has equivalent work (same subject or later superseding TUI galaxy work), obsolete_delete.\n";
audit_prompt += "   Otherwise keep.\n";
audit_prompt += "4) List associated worktrees under .worktrees/ that can be removed with the branch.\n";
audit_prompt += "Never delete anything yourself. Return only the classification.\n";
audit_prompt += "Also note feat/fdb-facing-push must stay in keep.\n";

let audit = agent(audit_prompt, #{
    label: "branch-audit",
    capability_mode: "execute",
    output_schema: report_schema,
});

if audit == () || !audit.success || audit.output == () {
    pause("verification", "Branch audit agent failed; resume after fixing git access.");
}

phase("Verify");
let verify_prompt = "";
verify_prompt += "Adversarially verify this branch cleanup plan for repo " + root + ".\n";
verify_prompt += "Proposed merged_delete: " + json_encode(audit.output.merged_delete) + "\n";
verify_prompt += "Proposed obsolete_delete: " + json_encode(audit.output.obsolete_delete) + "\n";
verify_prompt += "Proposed keep: " + json_encode(audit.output.keep) + "\n";
verify_prompt += "Use git to confirm every delete candidate has no unique unmerged work vs origin/main.\n";
verify_prompt += "Set safe=false if any branch still has unique commits not on main.\n";
verify_prompt += "main and feat/fdb-facing-push must not appear in delete lists.\n";

let v = agent(verify_prompt, #{
    label: "branch-verify",
    capability_mode: "execute",
    output_schema: verdict_schema,
});

if v == () || !v.success || v.output == () || v.output.safe != true {
    let reason = "verification failed";
    if v != () && v.success && v.output != () && v.output.reason != () {
        reason = v.output.reason;
    }
    complete(#{
        summary: "Cleanup plan NOT safe: " + reason,
        plan: audit.output,
        safe: false,
    });
}

complete(#{
    summary: "Cleanup plan verified safe",
    plan: audit.output,
    safe: true,
});
