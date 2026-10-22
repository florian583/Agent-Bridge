---
name: agent-availability
description: Check Agent Bridge engine and model limits before dispatching work in Claude Hybrid. Use before a worker batch, when a provider reports a rate or monthly limit, when the user gives a reset date, or when choosing another available engine.
---

# Choose an available worker

1. Read the current coordinator preferences from `list_agents`. Call `check_availability` before assigning work. Use the Agent Bridge MCP tool, or the Bridge Live tool with the same name; both use the same daemon state. Do not start a worker merely to test its quota.
2. Filter out `blocked`, `unavailable`, and `busy` engines. Prefer `ready` engines that meet the task's reasoning needs, in the user's configured order. `unknown` means quota could not be verified; it is neither exhausted nor a guarantee of availability. Use an unknown engine when allowed and no suitable known-ready engine is available. Do not override explicit engine exclusions or use paid overflow without authorization.
3. For a specific model, call `check_availability(agent, model)` before dispatch. `model_required` means select and check an explicit model. A model block does not imply that every model is blocked. Separate configured accounts, such as Devin and Devin-Well, have separate limits. An alternative client using the same blocked account is not a fresh quota pool.
4. When the user or a provider reports a limit, call `report_usage_limit` with the configured `agent`, optional exact `model`, and typed `reason`: `monthly_limit`, `usage_limit`, or `rate_limit`. Set `resets_at` to an ISO timestamp with a timezone. Convert a stated timezone; never invent a reset date. If the reset is unknown, omit `resets_at` and tell the coordinator that explicit recovery or a later reset report must clear the block. A missing error body does not prevent recording the user's report. Do not label a generic network or task failure as a quota limit.
5. Move new work to the next suitable available engine in the same turn. Preserve running tasks. Do not repeatedly retry a blocked engine or create loops that poll a monthly reset.
6. At or after the reset, recheck availability. Expiry removes the retained block but does not prove fresh quota. Use `clear_usage_limit` only after the user corrects the report or recovery is verified. Clear the exact engine/model scope; provider quota still applies.

Automatic provider-error cooldowns last five minutes when no reset is known. They are retry delays, not inferred subscription reset dates. A known operator limit takes precedence. If the tool returns an error, report availability as unavailable to check; do not treat a failed check as an empty block list.

If the new tools are absent, reload the MCP connection after the update. Do not bypass a known block with direct worker CLIs. Keep this skill in Claude Hybrid's user-level skills directory; it is not a repository-local workflow.
