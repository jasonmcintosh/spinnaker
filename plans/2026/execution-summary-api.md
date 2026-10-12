# Execution summary API + on-demand expansion

**Status**: Proposed - for discussion. Nothing implemented. The new API is alpha, additive, and behind flags (off by default).

| Phase | Description | Status |
|---|---|---|
| 0 | Baseline measurement | Not started |
| 1 | Cheap wins on existing endpoints | Not started |
| 2 | Orca data model (summary column, repository API) | Not started |
| 3 | New Orca/Gate API | Not started |
| 4 | Deck consumption behind flag | Not started |
| 5 | MCP tools | Not started |
| 6 | Pipelines UI redesign | Open - see section 8 |

Update the status table and the relevant phase section as each phase lands.

## 1. Context / problem

Findings below come from reading the code; payload sizes are not quoted because they have not been measured yet (Phase 0).

- Deck's 5s poll of in-flight executions uses `expand=true`: up to 30 executions with full stage context/outputs/tasks, overwriting the lighter list items. The 30s history poll uses `expand=false`; `GET /executions` (pipeline/tag filter) omits `expand`, so Gate defaults it to true.
- N+1: `mergeRunningExecutionsIntoExecutions` calls `GET /pipelines/{id}` per active execution missing from the running payload.
- Orca `expand=false` (`TaskController.groovy` `unexpandPipelineExecutions`) strips context/outputs/tasks after the full load and Jackson deserialization of every stage body. It saves wire bytes only.
- A stage body is an opaque JSON blob per stage (`pipeline_stages.body`, optionally in a compressed table), reported up to 1MB+.
- Gate re-serializes untyped `Map<String,Object>`; `GET /pipelines/{id}` has no `expand`; no stage-level GET exists.
- MCP: `get_latest_executions` hardcodes `expand=true`; `get_pipeline_execution` always returns the full execution.
- Deck CPU: JSON-stringify change detection per execution per refresh, in-place mutation, per-card/per-marker timers, per-group guessed-height virtualization, search limited to loaded executions.

Goals: (a) in-flight and list views read a small summary; (b) stage detail, artifact lists/contents and manifests load on demand; (c) Orca does less DB/CPU work, not just less network; (d) MCP/LLM clients get bounded payloads.

Non-goals: changing existing endpoints' default behavior; removing the legacy Deck path in this effort.

## 2. Data flow: today vs proposed

```
TODAY
 Deck --5s--> Gate --> Orca --> SQL: SELECT execution body + ALL stage bodies (context/outputs/tasks)
      <-full-  <-Map-  <-Jackson full-object  (expand=true)
 Deck --30s-> same, then Orca drops context/outputs/tasks AFTER load (expand=false)
 Click card -> GET /pipelines/{id}  (entire execution, every stage)

PROPOSED
 Deck --5s/30s(since=)--> Gate --> Orca --> SQL: execution row + stage `summary` column only
      <-typed DTO,small-  <-typed-  <-ExecutionSummary
 Click stage  -> GET /pipelines/{id}/stages/{stageId}?fields=context,outputs,tasks
 Open artifacts tab -> GET /pipelines/{id}/artifacts   (contents via existing artifact fetch)
 Open manifests    -> GET /pipelines/{id}/stages/{stageId}/manifests (live status via existing /manifests/...)
```

## 3. API reference (alpha)

All endpoints use the same Fiat application-read authorization as their sibling endpoint. Gate flag `gate.execution-summary.enabled` and Orca flag `orca.execution-summary.enabled` (404 when off, which Deck uses to fall back).

### 3.1 List summaries

`GET /applications/{app}/pipelines/summary` (Gate) -> `GET /v2/applications/{app}/pipelines/summary` (Orca)

| Param | Type | Default | Notes |
|---|---|---|---|
| limit | int | existing `gate.execution.fetch.limit` | per pipeline config, as today |
| statuses | csv | all | e.g. `RUNNING,SUSPENDED,PAUSED,NOT_STARTED` |
| pipelineConfigIds | csv | all | |
| ids | csv | - | fetch specific executions (replaces the N+1) |
| since | epoch ms | - | delta: executions updated after this OR still active |
| cursor | opaque | - | keyset on (buildTime,id) for paging |
| pageSize | int | 50 | max 200 |

Response `ExecutionSummaryPage`:

```json
{
  "items": [{
    "id": "01J...", "application": "myapp", "name": "deploy-prod",
    "pipelineConfigId": "a1b2...", "status": "RUNNING",
    "buildTime": 1760000000000, "startTime": 1760000001000, "endTime": null,
    "revision": 1760000042000,
    "canceledBy": null, "cancellationReason": null,
    "limitConcurrent": true, "keepWaitingPipelines": false, "fromTemplate": false,
    "user": "jane@example.com",
    "trigger": {
      "type": "jenkins", "user": "jane@example.com", "dryRun": false,
      "parentExecution": null,
      "buildInfo": {"name": "build-job", "number": 412, "url": "https://ci/job/412"},
      "parameters": {"env": "prod"}, "parametersTruncated": false,
      "artifacts": [{"id": "x1", "name": "app.yaml", "type": "github/file", "reference": "..."}],
      "link": null
    },
    "stages": [{
      "id": "s1", "refId": "1", "name": "Bake", "type": "bake",
      "status": "SUCCEEDED", "startTime": 1760000001000, "endTime": 1760000100000,
      "requisiteStageRefIds": [], "parentStageId": null,
      "syntheticStageOwner": null, "initializationStage": false,
      "group": null, "comments": null, "alias": null, "cloudProvider": "aws",
      "hints": {
        "accounts": ["prod"],
        "labelLines": ["Bake ami-0abc"],
        "failureMessage": null,
        "artifactCount": 1, "manifestCount": 0
      }
    }]
  }],
  "nextCursor": null,
  "serverTime": 1760000050000
}
```

Rules: no stage `context`/`outputs`/`tasks`; `hints` capped (~512 B per stage); `parameters` capped with `parametersTruncated`; `parentExecution` is id+application only. ETag `W/"<maxRevision>-<count>"` supported; `If-None-Match` -> 304.

Delta semantics: the client sends the previous `serverTime` minus ~5s overlap; a periodic full refresh (~5 min) reconciles deletions.

### 3.2 Stage detail

`GET /pipelines/{id}/stages/{stageId}?fields=context,outputs,tasks` (default: all three). 404 if the stage does not belong to the execution; 403 per application permission. Replaces whole-execution hydration.

### 3.3 Artifacts

- `GET /pipelines/{id}/artifacts` and `GET /pipelines/{id}/stages/{stageId}/artifacts` -> `[{ "stageId", "kind": "trigger|resolvedExpected|stageOutput", "name", "type", "reference", "sizeBytes?" }]` (list only).
- Contents: no new endpoint; the UI calls the existing artifact fetch path (own authz/URL restrictions) when the user expands one artifact.

### 3.4 Manifests

- `GET /pipelines/{id}/stages/{stageId}/manifests` -> `[{ "account", "location", "name", "kind", "manifestUri?" }]`. Live status stays on the existing `/manifests/{account}/{location}/{name}`; Deck fetches only for the visible/expanded stage and does not re-fetch on every app refresh.

### 3.5 Changes to existing endpoints (compatible)

- Gate+Orca `GET /pipelines/{id}`: optional `expand` (default `true`).
- Deck sends `expand=false` on `GET /executions` with `pipelineConfigIds`.
- MCP: see section 6.

## 4. Backend design

**Orca persistence.** Add a nullable `summary` JSON column to the stage tables (plus compressed-table variants where applicable) via Liquibase, following the existing MySQL/Postgres/MariaDB changelog patterns; written in the same statement as `body`. A NULL `summary` (old rows) falls back to parse-and-project; no mandatory backfill. Add an indexed, DB-assigned `updated_at`/revision on the execution row for `since` and ETag (reuse an existing column if one fits - check first). Cost to measure: write amplification and index cost.

**Repository API** (`ExecutionRepository`): `retrieveSummariesForApplication(app, criteria)` and `retrieveStage(executionId, stageId)` as default methods (full-load + project fallback) so `InMemoryExecutionRepository`, `DualExecutionRepository` and plugin repositories keep working. Models `ExecutionSummary`/`StageSummary` in orca-core. The SQL query must be index-scoped (application/status/config_id) and never scan-then-filter (see CODE_STYLE.md). Redis: HMGET only the needed fields. One shared contract test for SQL and Redis.

**Hints SPI.** A `StageSummaryContributor` produces the `hints` at write time. Defaults cover: deployment-target accounts (today Deck's `accountExtractor` reads `context.clusters`/`credentials`), label lines (jenkins/travis/bake read context/outputs today), failure message (truncated `context.exception`), artifact/manifest counts. Plugins register their own. Fallback: Deck lazily hydrates the stage if hints are absent.

**Gate.** Typed DTOs instead of `Map`, same Fiat annotations, ETag pass-through.

**Alternatives considered for the SQL projection:** (A) load the full body, drop fields in Java (no DB/CPU saving); (B) `readTree` + drop (cheaper, still a full read); (C) SQL JSON functions (dialect-specific, unusable with compressed bodies); (D/E) summary column (chosen). Only (D/E) avoids reading the body at all.

## 5. Deck consumption

Behind `SETTINGS.feature.executionSummary`, falling back to the legacy path on 404.

- One summary datasource replaces `executions` + `runningExecutions`: 30s base poll with `since`, 5s only while running executions exist and the tab is visible, nothing in hidden tabs.
- Id-keyed immutable store with revision-based change detection (removes stringify diffing).
- `ExecutionsTransformer` prefers `stage.hints`.
- Per-stage hydration stored separately from list items so polls cannot wipe it.
- Lazy artifacts/manifests; a single shared ticker for durations; remove the N+1.

## 6. MCP

- `get_latest_executions`: summary by default, optional `includeStages`.
- `get_pipeline_execution`: `view=summary|full`, `stageId`.
- New `get_stage_details` (`fields`, size cap with truncation notice).
- Manual-judgment tools: summary list, then detail only for pending MJ stages.

## 7. Phases

- **0 Baseline:** fixtures (50 executions x ~20 stages; 10 in-flight with 1MB+ bodies; k8s manifest-heavy); Orca/Gate/Deck metrics; acceptance table (e.g. 5s poll < 50KB for 30 running executions).
- **1 Cheap wins:** Deck running poll -> `expand=false` with a non-clobbering merge; `expand=false` on `/executions`; remove the N+1; `expand` on `GET /pipelines/{id}`; verify compression Orca->Gate->client; MCP `expand` default false; Redis stage-field skipping; benchmark `optimizeExecutionRetrieval`.
- **2-3** Orca model + API, **4** Deck, **5** MCP, as above.
- **Tests:** SQL (MySQL/Postgres testcontainers) + Redis contract suite (summary == projection of full, `since`, cursor stability, NULL-summary fallback, compressed tables, hint caps); Gate authz (403, 304, headers); Deck merge/Profiler tests; payload-size regression tests with large fixtures; end-to-end flag on/off comparison against Phase 0.
- **Risks:** hint drift for plugin stages (SPI + lazy fallback); write amplification; online DDL on big stage tables; clock skew for `since` (DB-assigned time + overlap); authz regressions on new endpoints; Deck/Gate/Orca version skew (flag + 404 fallback).

## 8. Pipelines UI - for broad discussion (no decision)

Current pain: variable-height cards; the stage marker bar conveys little without hover/click; detail is embedded in the card; no real pagination; search only sees loaded executions; grouping/filtering is hard to follow; failures and "what needs me" are hard to spot.

The wireframes below are ASCII, drawn from a screenshot of the current UI (the `demo` application). They are wireframes of proposals, not screenshots of built UI.

**Current layout**

```
+ SPINNAKER  Search Projects Applications Pipeline Templates         user v  [Search] +
+--------+---------------------+--------------------------------------------------------+
| demo   | Filters       [<<]  | [+][-] Group by [Pipeline v] Show [2] executions per    |
|PIPELINES| [Search by field]? |   pipeline  [x] stage durations   [Create][Configure][Start Manual Exec]
|CANARY.. | PIPELINES          | v [K8S-NAMESPACE-PROD] Deployment    Trigger:enabled  Configure  Start Manual
|CLUSTERS | [ ] Deployment     |   +----------------------------------------------------------------+
|LOAD BAL | [ ] Run curl..     |   | MANUAL START   |00:01|00:07|00:07|00:06|05:26|05:03|..|00:07|  |
|FIREWALLS| [ ] Simple         |   | jason@..       |=====================================|  Dur 05:42 |
|KUBERNETES [ ] canary show    |   | 3 days ago     Status: SUCCEEDED              [retry][trash]|
|FUNCTIONS| [ ] Bake Manifest  |   | > View All Parameters/Artifacts (1/1)                          |
|TASKS    | [ ] Test[example]  |   | > Execution Details                                            |
|CONFIG   | [Reorder Pipelines]|   +----------------------------------------------------------------+
|         | STATUS             |   | MANUAL START   |00:00|00:07|00:07|00:06|[red 00:00]|05:00|- - - -| TERMINAL
|         | [ ] Running        | > Run curl on demo   (collapsed group; one header row each)
|         | [ ] Awaiting Judg. | > Simple   > canary show   v Bake Manifest Example ...
|         | [ ] Terminal ...   |
+--------+---------------------+--------------------------------------------------------+
```

What this makes concrete (each card repeats it every row): a long stage bar where each segment shows only a duration (no stage name until hover), a second "Status: X / Duration: Y" line, a left trigger block ("MANUAL START / user / age"), two disclosure links per card, and per-card retry/delete icons. A failed stage is identifiable only by a red segment. Collapsed groups hide everything, including a pipeline that is currently failing. Filters (status and pipeline checkboxes) are a long left rail separate from the content.

**A. "Needs attention" lane + list (same chrome, keeps left rail and group headers)**

```
+--------------------------------------------------------------------------+
| demo    Group by [Pipeline v]  Show [2] per pipeline  [x] stage durations |
+--------------------------------------------------------------------------+
| NEEDS ATTENTION (3)                                                      |
| ! Deployment #412   Awaiting judgment @ "Approve"   2h   [Judge] [Open]  |
| > Bake Manifest #9  Running 4m  [Bake ok][Deploy ....][Verify .]          |
| x Deployment #411   Failed @ "Deploy": quota exceeded    [Retry] [Open]  |
+--------------------------------------------------------------------------+
| v [K8S-NAMESPACE-PROD] Deployment        2 recent: 1 ok, 1 failed        |
|   #412 o paused  jason  manual  3d   [#####o....]                         |
|   #411 x failed  jason  manual  3d   [##x.......]  "quota exceeded"       |
```

Collapsed group headers also show a health summary ("2 recent: 1 ok, 1 failed") so failure is visible without expanding. Data source: summary `status`, stage statuses, and `hints.failureMessage`.

**B. Dense table (inline expand) - same data as the cards, one line each**

```
| Status | Pipeline    | Account/NS        | Trigger       | Started | Dur  | Stages (named on hover)   |
| RUN    | Bake Manif. | -                 | manual jason  | 2m ago  | 2m   | [ok][ok][>.][..]          |
| JUDGE  | Deployment  | K8S-NAMESPACE-PROD| manual jason  | 3d ago  | 3d   | [ok][ok][J][..]           |
| FAIL   | Deployment  | K8S-NAMESPACE-PROD| manual jason  | 3d ago  | 0:15 | [ok][ok][ok][x][-] quota  |
|  v Deployment #411  Failed at "Deploy": quota exceeded     [Open stage] [Restart stage] [Params/Artifacts(1)] |
```

Params/artifacts and execution details move into the inline expansion and load on expand (stage detail, artifact list).

**C. Master/detail split with deep link**

```
+-- executions ----------------+-- deploy-prod #412 ---------------------+
| > deploy-prod #412  judge    |  Bake ──> Deploy ──> [Judge] ──> Verify |
|   deploy-stg  #98  running   |  Stage: Judge (selected)                 |
|   build-img   #77  failed    |  Instructions ... [Continue] [Stop]      |
|   ... (virtualized, paged)   |  Artifacts(2)  Manifests(3)  JSON        |
+------------------------------+------------------------------------------+
```

**D. Stage timeline (gantt)**

```
Bake    |#########|
Deploy            |##########......|   (parallel stages stack vertically)
Judge                             |~~~~~~~~ waiting 2h
Verify                                              |..|
        0m        5m        10m       15m
```

Foundation work regardless of look: windowed list, cursor paging, server-side search/filter (status, pipeline, trigger, date), failure summary on the row from `hints.failureMessage`.

Proposed next step: prototype A+B+C against the summary API with the flag on, collect feedback, then choose; coordinate with other UI initiatives that should be bundled.

## 9. Open questions

- Which databases do deployments use (MySQL/Postgres/Redis)?
- Is SQL compression enabled in key environments (affects any SQL JSON approach)?
- Is plugin stage UI in scope for the hints SPI?
- Delta polling in the first cut, or ETag only?
- Staging environment for the Phase 0 baseline, or synthetic fixtures?
