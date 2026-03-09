# Plan

Plan the exact seed-level edits needed to make the failing test pass.

## Task

1. **Read the research** from `{path}/step_2.yml` to understand the root cause.
2. **Read the constraints** from `{path}/constraints.yml` to see which values are locked
   from prior iterations. If the file does not exist, there are no constraints yet.
3. **Plan changes** that resolve the root cause without violating any existing constraints.

## Rules

- Every edit MUST be consistent with all existing constraints.
- Each change MUST specify the seed file path, row (0-based index), column name, and the
  corrected value.
- For every changed cell, define at least one constraint (invariant) that must hold in
  future iterations, e.g. "capacity must be between 1 and 10" or "status must be one of
  (active, inactive)".
