TL: TL0/TL1


Task:
Transform the following raw change request into PHASE_0_INTAKE.md.


Rules:
- Do not invent missing details.
- If key details are missing, STOP and ask up to 5 questions.

Track classifier (optional, non-blocking):
- Recommend a track to load (or "none") based on the change type.
- Output:
  - Recommended track: none / observability-pipeline / other
  - Why (2–3 bullets)
  - Extra required templates/artifacts if the track is used
- Reverse exclusion rule:
  - If the change is only an internal refactor and does not change any data flow / schema / drop policy / SOP,
    recommend "none" (avoid over-design).
- This recommendation must never block the core protocol; the maintainer can override.


Input:
<PASTE raw request / issue / notes here>


Output:
- A filled PHASE_0_INTAKE.md draft
- Track recommendation (optional)
- A STOP section if needed
