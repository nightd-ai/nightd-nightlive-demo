# Research

A dbt test failed. Your task is to diagnose the root cause.

## Task

1. **Read the failure details** from `{path}/step_1.yml`. The file contains:
   - `failure.status` — `"error"` or `"fail"`
   - `failure.message` — the error or assertion message
   - `failure.node` — the failing test node with `name`, `unique_id`, `resource_type`,
     `original_file_path`, and `compiled_path`
   - `failure.node.depends_on` — a list of all transitive dependencies, each with `name`,
     `unique_id`, `resource_type`, `original_file_path`, and optionally `compiled_path`
2. **Read the test** via the `compiled_path` from the failure details to understand the
   assertion it makes.
3. **Trace the data lineage** through every upstream model and seed listed in `depends_on`
   — use their `compiled_path` or `original_file_path` to read each one.
4. **Identify the root cause** — pinpoint which seed file(s), row(s) (0-based index), and
   column(s) contain NULL, missing, or inconsistent values that ultimately cause the test
   to fail.

## Rules

- Trace the full lineage from seed to staging to serving to test.
- Identify ALL seed files and rows involved.
- Explain *why* the current values are wrong.
