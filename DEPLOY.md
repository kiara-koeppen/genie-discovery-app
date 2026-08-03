# Redeploying the Genie Discovery App

Every command and behavior in this guide was verified against a live Databricks workspace
(CLI v0.292.0) rather than inferred. Where a tool does something surprising, it is called out.

## The two things that make redeploys fail silently

**1. `databricks sync` respects `.gitignore`, and `static/` is gitignored.**
The compiled frontend is not in the repo. A sync-based deploy therefore uploads the backend
and skips the frontend entirely, with no error. The app keeps serving whatever bundle reached
the workspace previously.

**2. The JS bundle filename is content-hashed.**
`npm run build` emits `static/assets/index-<hash>.js`, and `static/index.html` contains a
`<script src="/assets/index-<hash>.js">` pointing at its own matching bundle. So:

- New `index.html` without its matching `.js` → blank page (404 on the script).
- Neither uploaded → old app, no error at all.
- Old bundles are never cleaned up automatically (see "Stale bundles" below).

## What `npm run build` produces

Exactly two files. Verified by building `main` in a clean checkout:

```
static/index.html                  0.61 kB
static/assets/index-<hash>.js    651.29 kB
```

Nothing else. No CSS file (MUI injects styles at runtime via emotion, and nothing in
`frontend/src` imports a stylesheet), no `public/` folder on this branch, no fonts (Inter loads
from Google Fonts via a `<link>` in `index.html`), no source maps, no favicon.

`vite.config.ts` sets `emptyOutDir: true`, so each build wipes `static/` clean locally first.
Local staleness is not possible; the problem is always on the workspace side.

### `frontend/tsconfig.tsbuildinfo` is irrelevant — don't chase it

- It is **already gitignored** (`.gitignore` line 7). `git` correctly reports no changes to it.
- `tsconfig.json` sets `"noEmit": true`, and the build script is `tsc -b && vite build`, so
  TypeScript only type-checks and emits no application output. The entire bundle comes from Vite.
- It is regenerated on every build and never lands in `static/`.

Deleting it is harmless but accomplishes nothing. The only real consequence of a stale
`tsbuildinfo` is that `tsc -b` may skip type-checking, so type errors go unreported.

## Redeploy

Replace `<you>` and `<profile>`. This app has no `databricks.yml`, so `databricks apps deploy`
uses the direct API path with `--source-code-path` (mode `SNAPSHOT`) — it reads the **workspace**
folder, never your local disk. Step 4 is what actually moves your build; step 5 only promotes
what is already in the workspace.

```bash
# 1. Get the right code
git checkout main
git pull

# 2. Build the frontend
cd frontend
npm install          # only on a fresh clone or after package.json changes
npm run build
cd ..

# 3. Confirm exactly two files, and note the hash
find static -type f
# → static/index.html
# → static/assets/index-<hash>.js

# 4. Push to the workspace. --include static overrides .gitignore for this run.
databricks sync --include static . \
  /Workspace/Users/<you>/genie-discovery-app --profile <profile>

# 5. Promote it
databricks apps deploy genie-discovery \
  --source-code-path /Workspace/Users/<you>/genie-discovery-app \
  --profile <profile>
```

Then **hard-refresh**, or open the app URL in a private window. A cached `index.html` points at
the old bundle hash.

### Why `--include static`

It forces the gitignored path in for that invocation only — no `.gitignore` edit, no build
artifacts committed to git. Verified it uploads precisely `static/index.html` and
`static/assets/index-<hash>.js` and nothing else. `node_modules/` stays excluded, so this does
not hit the timeout problem that `import-dir` has.

The alternative — un-commenting `static/` in `.gitignore` — also works, but it commits a 650 kB
hash-named bundle on every change and guarantees merge conflicts on it. Prefer the flag.

### Do not use `databricks workspace import-dir`

It sweeps up `node_modules` and `.git` (deploy timeouts) and overwrites the workspace `app.yaml`
with the placeholder version from the repo, wiping your real warehouse ID, catalog, and schema.

## Stale bundles: verified behavior

`sync` deletes remote files that disappeared locally, but **only files it uploaded itself** — it
tracks its own state in a local snapshot. Verified across four live test passes:

| Scenario | Result |
| --- | --- |
| Rebuild changes the hash, then re-sync | Old bundle **deleted** (`DELETE: .../index-OLD.js`) |
| Bundle was uploaded by a different tool (e.g. `workspace import`) | **Not deleted** — sync ignores it |
| `sync --full` against a foreign bundle | **Still not deleted** |
| `workspace delete static/assets --recursive`, then re-sync | Clean: exactly one bundle |

This is why a long-lived app folder accumulates bundles. The reference deployment in `kk_test`
had **19** files in `static/assets/`, all but one dead.

They are inert — `index.html` only ever references one — so this is hygiene, not an outage. To
clean up, delete the directory and re-sync:

```bash
databricks workspace delete /Workspace/Users/<you>/genie-discovery-app/static/assets \
  --recursive --profile <profile>
databricks sync --include static . \
  /Workspace/Users/<you>/genie-discovery-app --profile <profile>
databricks apps deploy genie-discovery \
  --source-code-path /Workspace/Users/<you>/genie-discovery-app --profile <profile>
```

> **`sync --dry-run` is not read-only.** It prints "no actual changes will be made" and then
> creates the destination directory anyway (verified). Don't point a dry run at a path you care
> about.

## Verify the deploy landed

This fails silently, so check rather than assume.

**1. Hashes match.** The strongest single check — compare what `index.html` requests against
what is actually present:

```bash
databricks workspace export \
  /Workspace/Users/<you>/genie-discovery-app/static/index.html --profile <profile> | grep assets/
databricks workspace list \
  /Workspace/Users/<you>/genie-discovery-app/static/assets --profile <profile>
```

The hash in the `<script src=...>` must match your local `find static -type f`, and a file with
that name must exist in `static/assets/`.

**2. App is healthy.**

```bash
databricks apps get genie-discovery --profile <profile>
```

Expect `app_status: RUNNING`, `compute: ACTIVE`, deployment `SUCCEEDED`.

**3. In the browser — four session tabs, not seven:** `1: Business Context`,
`2: Key Terms & Questions`, `3: Technical Design`, `4: COE Review`. Seven tabs means the old
AI build is still being served.

**4. Session 3 has no AI sections.** Seeing `Classify Terms`, `SQL Expressions`,
`Example Queries`, `Clarifying Questions`, or `Metric View` means the old build.

**5. `app.yaml` survived.** Confirm the workspace copy still has real values, not the
`<your-warehouse-id>` placeholders that ship on `main`:

```bash
databricks workspace export \
  /Workspace/Users/<you>/genie-discovery-app/app.yaml --profile <profile> | grep -A1 'name: '
```

**6. OAuth scopes are set** (one-time, but worth re-checking after any app recreate). Required:
`sql`, `catalog.catalogs:read`, `catalog.schemas:read`, `catalog.tables:read`. `app.yaml` cannot
set these — see README "Step 2b". Without them the warehouse dropdown and UC pickers go blank.

## Branch reference

| Branch | Contents |
| --- | --- |
| `main` | Deterministic 4-session app, no AI. Deploy this. |
| `ai_features` | Archived pre-strip version: 7 sessions, LLM calls. Do not deploy. |
| `strip-ai-features` | Merged into `main` via PR #3; identical to `main`. |

Verified: `main` and `strip-ai-features` are byte-for-byte identical, and `main:app.py` contains
no references to `serving-endpoints`, `LLM_ENDPOINT`, `ai_generate`, `openai`, or
`MODEL_ENDPOINT`.
