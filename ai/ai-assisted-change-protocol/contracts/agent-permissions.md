# Agent Permissions (modes)

This document defines permission modes for AI agents. Project SSOT selects a mode via `policy.mode`.

## safe (default)
Allowed without PROCEED:
- read files
- create/update evidence artifacts (run.log, manifest.json)
- update PROJECT_STATE SSOT
- run tests/bench commands defined in verify recipes
- edit docs / display-only reports (optional)

Requires explicit PROCEED:
- any source code changes
- dependency changes
- network access
- destructive filesystem actions
- git history rewrite

## standard
Allowed without PROCEED:
- everything in safe
- small, scoped source code changes ONLY when tied to a specific next_action (closes C#/F#; produces E#)
- running wider test suites as required

Requires explicit PROCEED:
- broad refactors
- dependency changes
- network
- destructive fs
- git rewrite

## aggressive
Allowed without PROCEED:
- autonomous multi-step execution including source code changes, limited refactors, and test iteration
- only recommended after CI/PR gatecheck is stable

Requires explicit PROCEED:
- destructive fs
- network
- git rewrite

## Hard rule
Even in aggressive mode, actions must remain audit-bound:
- update SSOT with what changed, what evidence was produced, and what claims/findings were closed.

