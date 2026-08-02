let meta = #{
    name: "validate-java-zig-core-port",
    description: "Validate the Java→Zig exchange core port under TigerBeetle deterministic simulation discipline",
    when_to_use: "After core port changes, before merging, or when auditing seed/replay/fault coverage in src/zig/core",
    phases: [
        #{ title: "Scope", detail: "Inventory core modules and existing seeded simulations" },
        #{ title: "DST audit", detail: "Score TigerBeetle simulation readiness per domain" },
        #{ title: "Execute", detail: "Run core tests / gate commands" },
        #{ title: "Parity", detail: "Cross-check Java wire, fixtures, and class-map evidence" },
        #{ title: "Verify", detail: "Adversarially verify high-severity gaps" },
        #{ title: "Report", detail: "Write a deterministic validation report" },
    ],
};

// --- Schemas -----------------------------------------------------------------

let inventory_schema = #{
    "type": "object",
    "required": ["modules", "seeded_tests", "non_determinism_hotspots", "summary"],
    "properties": #{
        "summary": #{ "type": "string" },
        "modules": #{
            "type": "array",
            "maxItems": 24,
            "items": #{
                "type": "object",
                "required": ["path", "role", "has_tests"],
                "properties": #{
                    "path": #{ "type": "string" },
                    "role": #{ "type": "string" },
                    "has_tests": #{ "type": "boolean" },
                    "notes": #{ "type": "string" },
                },
            },
        },
        "seeded_tests": #{
            "type": "array",
            "maxItems": 32,
            "items": #{
                "type": "object",
                "required": ["file", "test_name", "dst_traits"],
                "properties": #{
                    "file": #{ "type": "string" },
                    "test_name": #{ "type": "string" },
                    "dst_traits": #{ "type": "string" },
                },
            },
        },
        "non_determinism_hotspots": #{
            "type": "array",
            "maxItems": 16,
            "items": #{
                "type": "object",
                "required": ["file", "issue"],
                "properties": #{
                    "file": #{ "type": "string" },
                    "issue": #{ "type": "string" },
                },
            },
        },
    },
};

let dst_domain_schema = #{
    "type": "object",
    "required": ["domain", "score", "strengths", "gaps", "must_fix"],
    "properties": #{
        "domain": #{ "type": "string" },
        "score": #{ "type": "integer", "minimum": 0, "maximum": 10 },
        "strengths": #{
            "type": "array",
            "maxItems": 8,
            "items": #{ "type": "string" },
        },
        "gaps": #{
            "type": "array",
            "maxItems": 8,
            "items": #{
                "type": "object",
                "required": ["severity", "file", "gap", "recommendation"],
                "properties": #{
                    "severity": #{ "type": "string" },
                    "file": #{ "type": "string" },
                    "gap": #{ "type": "string" },
                    "recommendation": #{ "type": "string" },
                },
            },
        },
        "must_fix": #{ "type": "boolean" },
    },
};

let exec_schema = #{
    "type": "object",
    "required": ["command", "passed", "exit_code", "highlights", "failures"],
    "properties": #{
        "command": #{ "type": "string" },
        "passed": #{ "type": "boolean" },
        "exit_code": #{ "type": "integer" },
        "highlights": #{ "type": "string" },
        "failures": #{
            "type": "array",
            "maxItems": 12,
            "items": #{ "type": "string" },
        },
        "seed_hints": #{
            "type": "array",
            "maxItems": 8,
            "items": #{ "type": "string" },
        },
    },
};

let parity_schema = #{
    "type": "object",
    "required": ["status", "checks", "findings"],
    "properties": #{
        "status": #{ "type": "string" },
        "checks": #{
            "type": "array",
            "maxItems": 16,
            "items": #{
                "type": "object",
                "required": ["name", "ok", "detail"],
                "properties": #{
                    "name": #{ "type": "string" },
                    "ok": #{ "type": "boolean" },
                    "detail": #{ "type": "string" },
                },
            },
        },
        "findings": #{
            "type": "array",
            "maxItems": 12,
            "items": #{
                "type": "object",
                "required": ["severity", "issue", "evidence"],
                "properties": #{
                    "severity": #{ "type": "string" },
                    "issue": #{ "type": "string" },
                    "evidence": #{ "type": "string" },
                },
            },
        },
    },
};

let verdict_schema = #{
    "type": "object",
    "required": ["real", "severity", "reason", "evidence", "fix_hint"],
    "properties": #{
        "real": #{ "type": "boolean" },
        "severity": #{ "type": "string" },
        "reason": #{ "type": "string" },
        "evidence": #{ "type": "string" },
        "fix_hint": #{ "type": "string" },
    },
};

// --- Args --------------------------------------------------------------------

// mode: quick | full | audit-only
// focus: path under src/zig/core (default src/zig/core)
// seed: optional report seed label for reproducibility notes
let mode = "quick";
let focus = "src/zig/core";
let seed_label = "default";

if args != () {
    if args.mode != () && type_of(args.mode) == "string" && args.mode != "" {
        mode = args.mode;
        mode.trim();
    }
    if args.focus != () && type_of(args.focus) == "string" && args.focus != "" {
        focus = args.focus;
        focus.trim();
    }
    if args.seed != () {
        if type_of(args.seed) == "string" {
            seed_label = args.seed;
            seed_label.trim();
        } else {
            seed_label = args.seed.to_string();
        }
    }
}

if mode != "quick" && mode != "full" && mode != "audit-only" {
    pause("verification", "args.mode must be quick, full, or audit-only");
}

// Keep every finding under the core port root.
let core_root = "src/zig/core";
if !focus.starts_with(core_root) {
    pause("verification", "args.focus must be under src/zig/core");
}

log("validate-java-zig-core-port mode=" + mode + " focus=" + focus + " seed=" + seed_label);

// Fixed deterministic work-list — agents judge coverage; Rhai owns scope.
let dst_domains = [
    #{
        id: "nondeterminism",
        title: "Non-determinism control",
        brief: "time, PRNG, threads, wall-clock, real I/O, OS scheduling",
    },
    #{
        id: "seeded-campaigns",
        title: "Seeded simulation campaigns",
        brief: "same seed => same hashes/results; multi-seed campaigns; seed printed on failure",
    },
    #{
        id: "fault-injection",
        title: "Failure injection",
        brief: "crash tails, torn writes, partial spawn, persistence faults, backpressure, poison",
    },
    #{
        id: "invariants",
        title: "Continuous invariant checking",
        brief: "post-op asserts, validateInternalState, book/risk/hash invariants after each transition",
    },
    #{
        id: "java-parity-sim",
        title: "Java semantic parity under simulation",
        brief: "wire codes, result merge, L2/events, state hashes vs Java fixtures and class map",
    },
    #{
        id: "replay",
        title: "Replay and crash recovery",
        brief: "journal replay, snapshot+journal suffix, bootstrap policies, XCJ3 crash tails",
    },
];

// --- Helpers -----------------------------------------------------------------

fn path_in_focus(path, focus_path, core_path) {
    if type_of(path) != "string" || path == "" {
        return false;
    }
    if path.starts_with(focus_path) {
        return true;
    }
    if path.starts_with(core_path) && focus_path == core_path {
        return true;
    }
    // Allow docs and tools that name core modules without a full path.
    if path.starts_with("docs/zig") || path.starts_with("src/zig/tools") || path.starts_with("src/zig/fixtures") {
        return true;
    }
    false
}

fn severity_rank(sev) {
    if type_of(sev) != "string" {
        return 0;
    }
    let s = sev;
    s.trim();
    // lower rank = higher priority for verification budget
    if s == "critical" || s == "Critical" || s == "CRITICAL" {
        return 0;
    }
    if s == "high" || s == "High" || s == "HIGH" {
        return 1;
    }
    if s == "medium" || s == "Medium" || s == "MEDIUM" {
        return 2;
    }
    3
}

// --- Phase 1: Scope inventory ------------------------------------------------

phase("Scope");

let inv_prompt = "";
inv_prompt += "You are inventorying the Java→Zig exchange core port for deterministic-simulation validation.\n";
inv_prompt += "Scope root: " + focus + " (must stay under " + core_root + ").\n";
inv_prompt += "Use read-only tools only: list_dir, grep, read_file, and shell read-only commands like `rg`/`cx`.\n";
inv_prompt += "Do not answer from memory — open the files.\n\n";
inv_prompt += "Tasks:\n";
inv_prompt += "1) List important modules under the focus path with a one-line role (kernel, book, journal, API, sharding, reports, wire, etc.) and whether they contain `test \"...\"` blocks.\n";
inv_prompt += "2) Grep for seeded/deterministic/simulation/fault/crash/replay tests under " + core_root + ". Capture file + exact test name + which DST traits they exercise (seeded PRNG, fake clock, crash tail, fault injection, hash parity, journal replay).\n";
inv_prompt += "3) Grep for non-determinism hotspots: wall-clock time, unseeded random, threads/async scheduling dependence, real network/disk without injection, std.time without a fake.\n";
inv_prompt += "4) Skim docs/zig/TIGER_STYLE_CHECKLIST.md and docs/zig/MIGRATION_COVERAGE.md for verification expectations.\n\n";
inv_prompt += "Return structured JSON only. Prefer concrete paths under " + focus + ". Empty arrays are valid only after you searched.";

let inventory = agent(inv_prompt, #{
    label: "inventory-core",
    capability_mode: "read-only",
    output_schema: inventory_schema,
});

if inventory == () || !inventory.success || inventory.output == () {
    complete(#{
        ok: false,
        summary: "Inventory agent failed; cannot validate port safely.",
        mode: mode,
        focus: focus,
    });
}

let inv = inventory.output;
// Canned validate_only outputs and partial agents may omit arrays — normalize.
if inv.summary == () || type_of(inv.summary) != "string" {
    inv.summary = "(inventory summary unavailable)";
}
if inv.modules == () {
    inv.modules = [];
}
if inv.seeded_tests == () {
    inv.seeded_tests = [];
}
if inv.non_determinism_hotspots == () {
    inv.non_determinism_hotspots = [];
}
log("Inventory: " + inv.modules.len().to_string() + " modules, "
    + inv.seeded_tests.len().to_string() + " seeded tests, "
    + inv.non_determinism_hotspots.len().to_string() + " nondet hotspots");

// --- Phase 2: TigerBeetle DST audit (parallel domains) -----------------------

phase("DST audit");

let dst_jobs = [];
for d in dst_domains {
    let p = "";
    p += "Audit TigerBeetle-style deterministic simulation readiness for the Java→Zig port.\n";
    p += "Domain: " + d.title + " (" + d.id + ")\n";
    p += "Focus: " + d.brief + "\n";
    p += "Code root: " + focus + "\n";
    p += "Report seed label (for recommendations only): " + seed_label + "\n\n";
    p += "Philosophy to apply (TigerBeetle / VOPR):\n";
    p += "- Control all non-determinism (time, network, disk, random).\n";
    p += "- Same seed => same execution; print seed + replay filter on failure.\n";
    p += "- Inject failures; do not wait for them.\n";
    p += "- Check invariants after every state transition, not only at the end.\n";
    p += "- Prefer single-threaded deterministic simulation over multi-threaded races.\n";
    p += "- Time compression / multi-seed campaigns over a handful of happy-path tests.\n\n";
    p += "You MUST inspect real code with grep/read_file/cx. Do not invent coverage.\n";
    p += "Prioritize modules that implement exchange matching, risk, journal/snapshot, runtime grouping, sharding, and Java wire/hash parity.\n";
    p += "Known reference points already in tree (verify still true):\n";
    p += "- docs/zig/TIGER_STYLE_CHECKLIST.md\n";
    p += "- seeded fault-injection / deterministic simulation tests in exchange_kernel.zig, exchange_api.zig, exchange_core.zig, journal.zig, orderbook_direct.zig\n";
    p += "- FakeMonotonicClock in exchange_runtime.zig\n\n";
    p += "Score 0-10 for this domain only. List concrete strengths and gaps.\n";
    p += "Each gap: severity (critical|high|medium|low), file path, gap description, recommendation that names a test shape (seeded campaign, crash-tail loop, invariant after op, etc.).\n";
    p += "Set must_fix=true if this domain currently blocks claiming production-grade DST coverage for the core port.\n";
    p += "Keep file paths under " + core_root + " when possible.";

    dst_jobs.push(#{
        prompt: p,
        label: "dst:" + d.id,
        capability_mode: "read-only",
        output_schema: dst_domain_schema,
    });
}

let dst_results = parallel(dst_jobs);

let domain_scores = [];
let all_gaps = [];
let domain_failures = 0;
let score_sum = 0;
let score_count = 0;
let di = 0;
for r in dst_results {
    let expected = dst_domains[di];
    if r == () || !r.success || r.output == () {
        domain_failures += 1;
        log("DST domain failed: " + expected.id);
        all_gaps.push(#{
            severity: "high",
            domain: expected.id,
            file: focus,
            gap: "DST audit agent failed for domain " + expected.id,
            recommendation: "Re-run workflow; manually review " + expected.title,
        });
    } else {
        let out = r.output;
        let score = 0;
        if out.score != () {
            score = out.score;
            // Best-effort numeric accumulation; non-numeric scores stay 0 and skip avg.
            if type_of(score) != "string" && type_of(score) != "bool" && type_of(score) != "array" && type_of(score) != "map" {
                score_sum += score;
                score_count += 1;
            } else {
                score = 0;
            }
        }
        let strengths = [];
        if out.strengths != () {
            strengths = out.strengths;
        }
        let must_fix = false;
        if out.must_fix == true {
            must_fix = true;
        }
        domain_scores.push(#{
            domain: expected.id,
            title: expected.title,
            score: score,
            must_fix: must_fix,
            strengths: strengths,
        });
        if out.gaps != () {
            for g in out.gaps {
                let gfile = "";
                if g.file != () {
                    gfile = g.file;
                }
                let ggap = "";
                if g.gap != () {
                    ggap = g.gap;
                }
                let grec = "";
                if g.recommendation != () {
                    grec = g.recommendation;
                }
                // Script-enforced scope: drop out-of-root paths that aren't docs/tools/fixtures.
                if gfile != "" && !path_in_focus(gfile, focus, core_root) {
                    log("Dropped out-of-scope gap path: " + gfile);
                } else {
                    let sev = "medium";
                    if g.severity != () {
                        sev = g.severity;
                    }
                    all_gaps.push(#{
                        severity: sev,
                        domain: expected.id,
                        file: gfile,
                        gap: ggap,
                        recommendation: grec,
                    });
                }
            }
        }
        if must_fix {
            log("Domain " + expected.id + " marked must_fix (score=" + score.to_string() + ")");
        }
    }
    di += 1;
}

// Merge inventory non-determinism hotspots as candidate gaps.
if inv.non_determinism_hotspots != () {
    for h in inv.non_determinism_hotspots {
        let hf = "";
        if h.file != () {
            hf = h.file;
        }
        if hf == "" || path_in_focus(hf, focus, core_root) {
            all_gaps.push(#{
                severity: "medium",
                domain: "nondeterminism",
                file: hf,
                gap: h.issue,
                recommendation: "Inject through a seeded PRNG / fake clock / simulated I/O boundary; never call wall-clock or unseeded random on the simulation path.",
            });
        }
    }
}

log("DST audit complete: " + domain_scores.len().to_string() + " domains, "
    + all_gaps.len().to_string() + " raw gaps, "
    + domain_failures.to_string() + " domain agent failures");

// --- Phase 3: Execute tests --------------------------------------------------

phase("Execute");

let exec_results = [];
let tests_passed = true;
let commands_run = [];

if mode != "audit-only" {
    let exec_jobs = [];

    // Always run Debug unit tests — primary core signal.
    let cmd_test = "zig build test";
    let p_test = "";
    p_test += "Run the Zig Debug unit test suite for the exchange-core port and report structured results.\n";
    p_test += "Working directory: repository root.\n";
    p_test += "Command to run exactly:\n  " + cmd_test + "\n";
    p_test += "Use the shell/run_terminal_command tool. Wait for completion (this can take several minutes).\n";
    p_test += "If the suite fails, capture the first failing test names and any `seed=` / `--test-filter` replay hints.\n";
    p_test += "Focus interpretation on " + focus + " failures first.\n";
    p_test += "Set passed=true only if the command exits 0.\n";
    p_test += "highlights: short summary of pass counts or notable seeded-simulation tests that ran.\n";
    p_test += "failures: concrete failing test names or empty if none.\n";
    p_test += "seed_hints: any seed/replay lines from output (or empty).";
    exec_jobs.push(#{
        prompt: p_test,
        label: "exec:zig-build-test",
        capability_mode: "execute",
        output_schema: exec_schema,
    });

    if mode == "full" {
        let cmd_gate = "zig build -j1 gate --summary all";
        let p_gate = "";
        p_gate += "Run the full serial Java→Zig migration gate and report structured results.\n";
        p_gate += "Working directory: repository root.\n";
        p_gate += "Command to run exactly (keep -j1 — persistence tests share Java-compatible artifact names):\n  " + cmd_gate + "\n";
        p_gate += "This can take a long time. Wait for completion.\n";
        p_gate += "Set passed=true only if exit code is 0.\n";
        p_gate += "Summarize step/test counts if printed. Capture first failures and seed replay hints.\n";
        p_gate += "Persistence-disabled and injected-fault warnings are expected test evidence, not failures.";
        exec_jobs.push(#{
            prompt: p_gate,
            label: "exec:gate",
            capability_mode: "execute",
            output_schema: exec_schema,
        });

        let cmd_rs = "zig build test-release-safe";
        let p_rs = "";
        p_rs += "Run ReleaseSafe Zig tests for the port.\n";
        p_rs += "Command exactly:\n  " + cmd_rs + "\n";
        p_rs += "Wait for completion. passed=true only on exit 0.\n";
        p_rs += "Capture failures and seed/replay hints. Highlight anything under " + focus + ".";
        exec_jobs.push(#{
            prompt: p_rs,
            label: "exec:test-release-safe",
            capability_mode: "execute",
            output_schema: exec_schema,
        });
    }

    let er = parallel(exec_jobs);
    for item in er {
        if item == () || !item.success || item.output == () {
            tests_passed = false;
            exec_results.push(#{
                command: "(agent failed)",
                passed: false,
                exit_code: -1,
                highlights: "execute agent failed or returned empty",
                failures: ["execute agent failed"],
                seed_hints: [],
            });
            commands_run.push("(agent failed)");
        } else {
            let o = item.output;
            let cmd_name = "(unknown command)";
            if o.command != () {
                cmd_name = o.command;
            }
            let passed = o.passed == true;
            let exit_code = -1;
            if o.exit_code != () {
                exit_code = o.exit_code;
            }
            let highlights = "";
            if o.highlights != () {
                highlights = o.highlights;
            }
            let failures = [];
            if o.failures != () {
                failures = o.failures;
            }
            let seed_hints = [];
            if o.seed_hints != () {
                seed_hints = o.seed_hints;
            }
            let normalized = #{
                command: cmd_name,
                passed: passed,
                exit_code: exit_code,
                highlights: highlights,
                failures: failures,
                seed_hints: seed_hints,
            };
            if !passed {
                tests_passed = false;
            }
            exec_results.push(normalized);
            commands_run.push(cmd_name);
            if passed {
                log("PASS: " + cmd_name);
            } else {
                log("FAIL: " + cmd_name + " exit=" + exit_code.to_string());
                // Elevate test failures into the gap list for verification.
                if failures.len() > 0 {
                    for f in failures {
                        all_gaps.push(#{
                            severity: "critical",
                            domain: "execute",
                            file: focus,
                            gap: "Test failure: " + f,
                            recommendation: "Reproduce with the failing test filter; if a seed was printed, replay that seed first.",
                        });
                    }
                } else {
                    all_gaps.push(#{
                        severity: "critical",
                        domain: "execute",
                        file: focus,
                        gap: "Command failed: " + cmd_name,
                        recommendation: "Re-run the command locally and inspect the first error.",
                    });
                }
            }
        }
    }
} else {
    log("audit-only: skipping zig build execution");
}

// --- Phase 4: Java parity evidence -------------------------------------------

phase("Parity");

let parity_prompt = "";
parity_prompt += "Validate Java→Zig parity evidence for the core port at " + focus + ".\n";
parity_prompt += "Use tools; do not invent results.\n\n";
parity_prompt += "Read / search:\n";
parity_prompt += "- docs/zig/MIGRATION_COVERAGE.md\n";
parity_prompt += "- docs/zig/JAVA_PRODUCTION_CLASS_MAP.md (or evidence under docs/zig/evidence/)\n";
parity_prompt += "- docs/zig/evidence/EXCHANGE_API_PARITY_AUDIT.md if present\n";
parity_prompt += "- src/zig/core/compat/java/ for wire/hash/report compatibility\n";
parity_prompt += "- src/zig/fixtures/ for java_exchange_* fixtures\n";
parity_prompt += "- src/zig/tools/java_exchange_api_parity_audit.zig and java_production_class_map_check.zig\n\n";

if mode == "full" {
    parity_prompt += "Also RUN these if not already covered by gate (execute is allowed):\n";
    parity_prompt += "  zig build api-surface-check\n";
    parity_prompt += "  zig build java-api-parity-audit\n";
    parity_prompt += "  zig build java-class-map-check\n";
    parity_prompt += "  zig build parity-diff\n";
    parity_prompt += "If a command fails, record it as a finding with evidence from stdout/stderr.\n";
} else if mode == "quick" {
    parity_prompt += "Do NOT run long gates. Statically verify that evidence files exist and that recent audit claims reference " + core_root + ".\n";
    parity_prompt += "Optionally run a cheap check if fast: `zig build api-surface-check` only if it looks quick; otherwise document as not-run.\n";
} else {
    parity_prompt += "audit-only: do not run build commands. Static evidence review only.\n";
}

parity_prompt += "\nReturn status: pass | pass_with_gaps | fail.\n";
parity_prompt += "checks: named checks with ok bool and detail.\n";
parity_prompt += "findings: only concrete parity risks (severity critical|high|medium|low).";

let parity_cap = "read-only";
if mode == "full" || mode == "quick" {
    parity_cap = "execute";
}

let parity_agent = agent(parity_prompt, #{
    label: "parity-evidence",
    capability_mode: parity_cap,
    output_schema: parity_schema,
});

let parity_status = "unknown";
let parity_checks = [];
if parity_agent != () && parity_agent.success && parity_agent.output != () {
    if parity_agent.output.status != () {
        parity_status = parity_agent.output.status;
    } else {
        parity_status = "unknown";
    }
    if parity_agent.output.checks != () {
        parity_checks = parity_agent.output.checks;
    }
    if parity_agent.output.findings != () {
        for f in parity_agent.output.findings {
            let sev = "medium";
            if f.severity != () {
                sev = f.severity;
            }
            let issue = "";
            if f.issue != () {
                issue = f.issue;
            }
            let evidence = "";
            if f.evidence != () {
                evidence = f.evidence;
            }
            all_gaps.push(#{
                severity: sev,
                domain: "java-parity-sim",
                file: focus,
                gap: issue,
                recommendation: "Confirm against Java fixtures / class map; add seeded parity or wire regression if missing. Evidence: " + evidence,
            });
        }
    }
} else {
    parity_status = "agent_failed";
    all_gaps.push(#{
        severity: "high",
        domain: "java-parity-sim",
        file: focus,
        gap: "Parity evidence agent failed",
        recommendation: "Re-run workflow parity phase; inspect docs/zig/MIGRATION_COVERAGE.md manually",
    });
}

log("Parity status: " + parity_status);

// --- Phase 5: Adversarial verification of top gaps ---------------------------

phase("Verify");

// Sort-select by severity without fancy sorts: multi-pass pick critical→high→medium.
let candidates = [];
for g in all_gaps {
    candidates.push(g);
}

let to_verify = [];
let max_verify = 8;
let ranks = [0, 1, 2]; // critical, high, medium only
for rank in ranks {
    if to_verify.len() >= max_verify {
        break;
    }
    for g in candidates {
        if to_verify.len() >= max_verify {
            break;
        }
        if severity_rank(g.severity) == rank {
            // de-dupe by gap text
            let already = false;
            for t in to_verify {
                if t.gap == g.gap && t.file == g.file {
                    already = true;
                    break;
                }
            }
            if !already {
                to_verify.push(g);
            }
        }
    }
}

log("Verifying " + to_verify.len().to_string() + " high-priority gaps");

let confirmed = [];
let refuted = [];
let unverified = [];

if to_verify.len() > 0 {
    let vjobs = [];
    let vi = 0;
    for g in to_verify {
        let vp = "";
        vp += "Adversarially verify this Java→Zig core port / deterministic-simulation finding.\n";
        vp += "Claim: " + g.gap + "\n";
        vp += "Alleged file: " + g.file + "\n";
        vp += "Domain: " + g.domain + "\n";
        vp += "Proposed fix: " + g.recommendation + "\n\n";
        vp += "You are a skeptic. Use read-only tools to inspect the shipped code and tests.\n";
        vp += "Set real=true ONLY if you independently find concrete evidence the gap still exists\n";
        vp += "(missing test, real wall-clock on a hot path, no seed replay, parity hole, etc.).\n";
        vp += "If the tree already covers the concern with a seeded simulation or invariant test, set real=false and cite that test.\n";
        vp += "severity: critical|high|medium|low for the residual risk if real, else low.\n";
        vp += "fix_hint: a concrete next step (e.g. `zig build test --test-filter \"seeded fault\"` or a new test sketch).\n";
        vp += "Default to real=false when evidence is weak.";
        vjobs.push(#{
            prompt: vp,
            label: "verify:" + vi.to_string(),
            capability_mode: "read-only",
            output_schema: verdict_schema,
        });
        vi += 1;
    }

    let verdicts = parallel(vjobs);
    let vj = 0;
    for v in verdicts {
        let claim = to_verify[vj];
        if v == () || !v.success || v.output == () {
            // Fail closed for the evidence gate.
            unverified.push(claim);
        } else {
            let out = v.output;
            let evidence = "";
            if out.evidence != () {
                evidence = out.evidence;
            }
            let reason = "";
            if out.reason != () {
                reason = out.reason;
            }
            let fix_hint = "";
            if out.fix_hint != () {
                fix_hint = out.fix_hint;
            }
            let sev = claim.severity;
            if out.severity != () {
                sev = out.severity;
            }
            if out.real == true && evidence != "" {
                confirmed.push(#{
                    severity: sev,
                    domain: claim.domain,
                    file: claim.file,
                    gap: claim.gap,
                    reason: reason,
                    evidence: evidence,
                    fix_hint: fix_hint,
                    recommendation: claim.recommendation,
                });
            } else {
                refuted.push(#{
                    domain: claim.domain,
                    file: claim.file,
                    gap: claim.gap,
                    reason: reason,
                });
            }
        }
        vj += 1;
    }
}

log("Verified: " + confirmed.len().to_string() + " confirmed, "
    + refuted.len().to_string() + " refuted, "
    + unverified.len().to_string() + " unverified");

// --- Phase 6: Report ---------------------------------------------------------

phase("Report");

let avg_score = 0;
if score_count > 0 {
    avg_score = score_sum / score_count;
}

let overall = "pass";
if !tests_passed {
    overall = "fail";
} else if confirmed.len() > 0 {
    // any confirmed critical/high => pass_with_gaps or fail
    let has_crit = false;
    let has_high = false;
    for c in confirmed {
        let r = severity_rank(c.severity);
        if r == 0 {
            has_crit = true;
        }
        if r == 1 {
            has_high = true;
        }
    }
    if has_crit {
        overall = "fail";
    } else if has_high || parity_status == "fail" {
        overall = "pass_with_gaps";
    } else {
        overall = "pass_with_gaps";
    }
} else if domain_failures > 0 || parity_status == "agent_failed" {
    overall = "pass_with_gaps";
} else if mode == "audit-only" {
    overall = "audit_only_ok";
}

// Build markdown report
let md = "";
md += "# Java → Zig Core Port Validation Report\n\n";
md += "- **Overall**: `" + overall + "`\n";
md += "- **Mode**: `" + mode + "`\n";
md += "- **Focus**: `" + focus + "`\n";
md += "- **Seed label**: `" + seed_label + "`\n";
md += "- **DST domain score average**: " + avg_score.to_string() + " / 10\n";
md += "- **Tests passed**: " + tests_passed.to_string() + "\n";
md += "- **Parity status**: `" + parity_status + "`\n";
md += "- **Confirmed gaps**: " + confirmed.len().to_string() + "\n";
md += "- **Refuted claims**: " + refuted.len().to_string() + "\n";
md += "- **Unverified claims**: " + unverified.len().to_string() + "\n\n";

md += "## TigerBeetle Deterministic Simulation Rubric\n\n";
md += "This run scores `src/zig/core` against TigerBeetle/VOPR discipline:\n\n";
md += "1. Control all non-determinism (time, I/O, random, threads)\n";
md += "2. Seeded campaigns with perfect replay (`seed=` + test-filter)\n";
md += "3. Failure injection (crash tails, torn writes, partial failures)\n";
md += "4. Invariants after every state transition\n";
md += "5. Java semantic/wire parity under simulation\n";
md += "6. Journal/snapshot crash recovery and replay\n\n";

md += "### Domain scores\n\n";
md += "| Domain | Score | Must fix |\n";
md += "|---|---:|:---:|\n";
for ds in domain_scores {
    let mf = "no";
    if ds.must_fix == true {
        mf = "yes";
    }
    md += "| " + ds.title + " | " + ds.score.to_string() + " | " + mf + " |\n";
}
md += "\n";

md += "### Domain strengths\n\n";
for ds in domain_scores {
    md += "#### " + ds.title + "\n\n";
    if ds.strengths != () {
        for s in ds.strengths {
            md += "- " + s + "\n";
        }
    } else {
        md += "- (none reported)\n";
    }
    md += "\n";
}

md += "## Inventory snapshot\n\n";
md += inv.summary + "\n\n";
md += "### Modules sampled\n\n";
if inv.modules != () {
    for m in inv.modules {
        let ht = "no";
        if m.has_tests == true {
            ht = "yes";
        }
        md += "- `" + m.path + "` — " + m.role + " (tests: " + ht + ")\n";
    }
}
md += "\n### Existing seeded / DST-flavored tests\n\n";
if inv.seeded_tests != () {
    for t in inv.seeded_tests {
        md += "- `" + t.file + "`: \"" + t.test_name + "\" — " + t.dst_traits + "\n";
    }
} else {
    md += "- (none found by inventory)\n";
}
md += "\n";

md += "## Execution\n\n";
if mode == "audit-only" {
    md += "_Skipped (audit-only)._\n\n";
} else {
    for e in exec_results {
        let mark = "FAIL";
        if e.passed == true {
            mark = "PASS";
        }
        md += "### " + mark + ": `" + e.command + "`\n\n";
        md += "- exit_code: " + e.exit_code.to_string() + "\n";
        md += "- highlights: " + e.highlights + "\n";
        if e.failures != () && e.failures.len() > 0 {
            md += "- failures:\n";
            for f in e.failures {
                md += "  - " + f + "\n";
            }
        }
        if e.seed_hints != () && e.seed_hints.len() > 0 {
            md += "- seed / replay hints:\n";
            for h in e.seed_hints {
                md += "  - " + h + "\n";
            }
        }
        md += "\n";
    }
}

md += "## Java parity evidence\n\n";
md += "Status: `" + parity_status + "`\n\n";
if parity_checks.len() > 0 {
    for c in parity_checks {
        let ok = "no";
        if c.ok == true {
            ok = "yes";
        }
        md += "- **" + c.name + "** (ok=" + ok + "): " + c.detail + "\n";
    }
    md += "\n";
}

md += "## Confirmed DST / port gaps\n\n";
if confirmed.len() == 0 {
    md += "_No gaps survived adversarial verification._\n\n";
} else {
    let ci = 1;
    for c in confirmed {
        md += "### " + ci.to_string() + ". [" + c.severity + "] " + c.domain + "\n\n";
        md += "- **File**: `" + c.file + "`\n";
        md += "- **Gap**: " + c.gap + "\n";
        md += "- **Evidence**: " + c.evidence + "\n";
        md += "- **Why it matters**: " + c.reason + "\n";
        md += "- **Recommendation**: " + c.recommendation + "\n";
        md += "- **Fix / replay hint**: " + c.fix_hint + "\n\n";
        ci += 1;
    }
}

if refuted.len() > 0 {
    md += "## Refuted claims (already covered)\n\n";
    for r in refuted {
        md += "- `" + r.file + "`: " + r.gap + " — " + r.reason + "\n";
    }
    md += "\n";
}

if unverified.len() > 0 {
    md += "## Unverified claims (fail-closed)\n\n";
    md += "These could not be independently confirmed or refuted; treat as open until rechecked.\n\n";
    for u in unverified {
        md += "- [" + u.severity + "] `" + u.file + "`: " + u.gap + "\n";
    }
    md += "\n";
}

md += "## Recommended next simulation campaigns\n\n";
md += "Apply TigerBeetle shape to any confirmed gap:\n\n";
md += "```zig\n";
md += "// same seed => same path; print seed on failure\n";
md += "var prng = std.Random.DefaultPrng.init(seed);\n";
md += "const random = prng.random();\n";
md += "defer if (failed) std.debug.print(\"seed={d}  zig build test --test-filter ...\\n\", .{seed});\n";
md += "// after every op: check invariants / state hashes\n";
md += "// inject crash tails / persistence faults with random.chance\n";
md += "```\n\n";
md += "Campaign ideas:\n";
md += "1. Multi-seed orderbook direct vs naive parity (already partially present — extend seeds).\n";
md += "2. Kernel split matching/risk fault injection across more seeds and symbols.\n";
md += "3. Journal XCJ3 crash-tail at every byte boundary under multi-symbol streams.\n";
md += "4. Exchange API callback vs poll convergence under scheduling perturbation.\n";
md += "5. Persistence pair commit campaign: never advertise failed pairs; retry clean.\n\n";

md += "## How to re-run this workflow\n\n";
md += "```text\n";
md += "/validate-java-zig-core-port {\"mode\":\"quick\"}\n";
md += "/validate-java-zig-core-port {\"mode\":\"full\",\"seed\":\"42\"}\n";
md += "/validate-java-zig-core-port {\"mode\":\"audit-only\",\"focus\":\"src/zig/core/exchange_kernel.zig\"}\n";
md += "```\n\n";
md += "Manual gate (serial):\n";
md += "```bash\n";
md += "zig build test\n";
md += "zig build test-release-safe\n";
md += "zig build -j1 gate --summary all\n";
md += "```\n";

let report_path = write_scratch_file("java-zig-core-port-validation.md", md);

let summary = "overall=" + overall
    + " mode=" + mode
    + " dst_avg=" + avg_score.to_string()
    + " confirmed=" + confirmed.len().to_string()
    + " tests_passed=" + tests_passed.to_string()
    + " parity=" + parity_status;

complete(#{
    ok: overall == "pass" || overall == "audit_only_ok" || overall == "pass_with_gaps",
    overall: overall,
    summary: summary,
    mode: mode,
    focus: focus,
    seed: seed_label,
    dst_average: avg_score,
    tests_passed: tests_passed,
    parity_status: parity_status,
    confirmed_count: confirmed.len(),
    refuted_count: refuted.len(),
    unverified_count: unverified.len(),
    confirmed: confirmed,
    domain_scores: domain_scores,
    report_path: report_path,
});
