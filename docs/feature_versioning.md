# Changing a feature definition without breaking what is serving

> *How can we decouple a change or update in a feature definition between training and
> inference? Can we add versioning to feature definitions?*

Short answer: **table features are already decoupled, on-demand functions are not, and
neither Feature Views nor feature tables have a built-in version number.** Versioning is
a naming discipline plus two tags, and this repo implements and measures all three
halves of that.

Everything below is demonstrated by `notebooks/30_advanced/31_feature_versioning.py`
(`make versioning`) against the live rail-ranking endpoint, not argued from docs.

## The mechanism that already exists

`fe.log_model` writes a **feature spec** into the model artifact: every table, lookup
key, feature column and on-demand function the model was trained against. At inference
time Model Serving reads *that spec*, not today's definitions.

So a model version is a pinned contract, and you can read it:

```python
from src.crfs import versioning as V

spec = V.feature_spec_of("models:/cat.sch.crunchyroll_rail_ranker/10")
print(V.render_spec(spec))
```

The consequence is the important part:

| A model depends on… | The endpoint resolves it… | An in-place change therefore… |
|---|---|---|
| feature **tables** and the columns it looks up | from the spec **inside the model version** | does **not** change what this model serves |
| on-demand **functions** (`FeatureFunction` → a UC function) | **by name, at request time** | **does** change what this model serves, immediately |

That asymmetry is the whole answer, and it is measurable. Notebook 31 §4 scores a fixed
request against the live endpoint, runs `CREATE OR REPLACE FUNCTION` on
`cr_rail_taste_match` so it returns a constant, scores the same request again, then
restores the function from `src/crfs/udfs.py` and asserts the original ranking comes
back. The measured before/after difference is recorded in
[verification_log.md](verification_log.md).

## Rules

1. **Never edit a definition in place if a model pins it.** For a table this is merely
   confusing — training moves, serving does not. For a UC function it is a production
   change with no deploy, no version bump and no audit trail on any model.
2. **Version by name, additively.** `x_v2` as a new column, a new table, a new
   `Feature`, or `cr_..._v2` as a new function. The old name keeps serving the old
   models until nothing pins it.
3. **Tag every model version with what it was trained against.** Two tags, written by
   `V.training_tags(spark, spec)`:
   * `feature_spec_hash` — the *shape*: which tables, functions, features.
   * `feature_definition_fingerprint` — the *content*: table schemas and function
     bodies as they were at training time.
   A version whose spec hash matches but whose fingerprint does not is precisely "same
   lookups, redefined underneath".
4. **Check drift before you retrain or debug.** `V.drift_report(spark, model_uri, tag)`
   reports missing tables and functions (which break an endpoint) separately from
   changed ones (which do not change behaviour, but mean a retrain would learn something
   different).
5. **Ask who pins an object before deleting it.** Notebook 31 §7 prints every registered
   model in the schema, what its latest version pins, and a reverse index — object →
   models that depend on it. A feature nobody pins is safe to drop.
6. **Roll a definition change out as a model rollout.** A feature change only reaches
   traffic when a model version trained against it is deployed, so the canary you
   already have covers it. Notebook 31 §6 splits traffic 90/10 across two versions of
   the rail ranker and restores 100% afterwards.

## The three classes of change

| Change | GA feature tables | Feature Views | Who breaks |
|---|---|---|---|
| **add** a column or a feature | add it; existing specs ignore it | register a new `Feature` | nobody |
| **change the maths** of an existing feature | write `x_v2`, or a new table; retrain to bind it | new `Feature` name | nobody, provided the old name is untouched |
| **change an on-demand function** | create `cr_..._v2`; retrain to bind it | new `CustomUDF` binding | everything serving it, immediately, if edited in place |
| **remove** a feature | drop only after §7 shows nothing pins it | same | whatever still pins it |

## Do Feature Views have versions?

No. There is no version number, no `@v1` suffix and no alias on a Feature View, and the
docs are silent on evolution. A registered `Feature` is a UC object identified by name,
so the naming discipline above *is* the versioning mechanism for both paths. Model
versions and aliases remain the only numbered, immutable thing in the chain — which is
also the thing serving pins, so it is the right place for the guarantee to live.

## What this buys, concretely

* A feature engineer can rebuild `viewer_features_current` with different maths at 2pm
  and the endpoint serving `crunchyroll_rail_ranker` v10 returns exactly what it
  returned at 1pm. That is not a convention being followed carefully; there is no code
  path by which the endpoint could read the new definition.
* A retrain at 3pm picks up the new maths, gets a new version, a new fingerprint tag,
  and reaches traffic only when someone deploys it.
* If instead someone edits `cr_rail_taste_match` at 2pm, the homepage changes at 2pm.
  Notebook 31 measures exactly that, which is why rule 1 is first.

## Where it is enforced

* `src/crfs/versioning.py` — spec reading, fingerprinting, drift reporting.
* `notebooks/30_advanced/31_feature_versioning.py` — the demonstration, incl. the canary.
* `notebooks/30_advanced/32_gpu_train.py` — tags its model version at registration, so
  every new model starts with a baseline.
* `scripts/verify.sh` — already asserts, by name, that every table and UC function the
  rail ranker's spec pins exists and is non-empty. It is deliberately existence-only:
  verify must stay read-only and fast, and fingerprint comparison needs the model
  artifact. `make versioning` is the full check.

## Related

* [`feature_views.md`](feature_views.md) — the declarative authoring path
* [`vertical_ranking.md`](vertical_ranking.md) §2 — model and version management
* [`open_items.md`](open_items.md) §3 — traffic splitting, which notebook 31 §6 closes
