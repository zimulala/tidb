TL: TL1-TL4


Task:
Given a set of claims about tools/tests, classify each claim as:
- Unverified (no artifact)
- Verified-by-test (has artifact)


Rules:
- If artifact is missing, the claim must be marked Unverified.
- Suggest what artifact would be required.


Input:
<PASTE claims + any logs/links>


Output:
A table:
| Claim | Status | Required evidence | Notes |
