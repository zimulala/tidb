# TOPRU_COMPAT_SPEC.md

> Purpose: make compatibility policy explicit (protocol + behaviors), especially for old agents/clients.

## Protocol evolution
- Protobuf policy:
  - additive-only? (required)
  - new messages/fields:
  - deprecated fields behavior:

## Old client / agent behavior
- If client does not understand TopRU messages:
  - server behavior:
  - client behavior (ignore unknown fields? Unimplemented?):
- Capability negotiation strategy (if any):

## PubSub vs SingleTarget parity
- Must both paths:
  - deliver RU records?
  - deliver SQLMeta/PlanMeta required for interpreting digests?
  - have the same enable/disable semantics?
- If not symmetric, document the divergence and rationale:

## TopSQL disabled + TopRU enabled
- Is this allowed? If yes:
  - required behavior for meta emission:
  - fallback if meta is missing:

## Maintainer GO (required)
- [ ] Approved by maintainer: <name/date>
- Notes:
