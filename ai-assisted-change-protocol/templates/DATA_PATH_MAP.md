# Data Path Map

> Required for pipeline-like features. Fill concrete modules, functions, and artifacts.
> Governance-layer template: tool/vendor agnostic.

## Step 1 — Trigger / Source
- Source:
- Key(s) produced:
- Where in code:

## Step 2 — Correlation
- correlation key(s):
- time window definition:
- ambiguity handling:

## Step 3 — Collection
- logs:
- metrics:
- traces:
- configs / env:

## Step 4 — Aggregation / Processing
- aggregation points:
- caps / backpressure:
- drop policy + metrics:

## Step 5 — Build output / Bundle
- bundle schema:
- required fields:
- versioning:
- bundle id / correlation fields:

## Step 6 — Transport / Storage / Bundle export
- transport:
- storage:
- retention:
- export mechanism (where/how bundle is exported):

## Step 7 — Consumer / SOP
- consumer:
- SOP routing rules:
