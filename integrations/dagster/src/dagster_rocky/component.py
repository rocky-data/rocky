"""RockyComponent — state-backed Dagster component for Rocky.

The component has two phases:

1. ``write_state_to_path()`` calls ``rocky discover`` (and ``rocky compile``
   when models are present) and persists the result as JSON.
2. ``build_defs_from_state()`` reads that cached JSON on every code-server
   load and turns it into a set of subset-aware ``multi_asset`` definitions.

When materialized, each multi-asset shells out to ``rocky run`` for the
selected subset of sources and yields:

* one :class:`dagster.MaterializeResult` per copied table, with strategy,
  rows-copied, watermark and duration metadata,
* one :class:`dagster.AssetCheckResult` per declared check
  (``row_count`` / ``column_match`` / ``freshness``), and
* placeholder ``AssetCheckResult`` events for declared checks that Rocky
  did not produce, so the Dagster UI never shows "did not yield expected
  outputs" warnings.
"""

from __future__ import annotations

import importlib
import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal

import dagster as dg
from dagster._core.definitions.metadata.metadata_set import NamespacedMetadataSet
from dagster.components.component.state_backed_component import StateBackedComponent
from dagster.components.utils.defs_state import (
    DefsStateConfig,
    DefsStateConfigArgs,
    ResolvedDefsStateConfig,
)
from dagster.components.utils.translation import TranslationFn, TranslationFnResolver

from .checks import check_metadata
from .contracts import (
    ContractRules,
    contract_check_results_from_diagnostics,
    contract_check_specs_for_model,
    discover_contract_rules,
)
from .derived_models import (
    ModelGroup,
    build_model_specs,
    split_model_specs_by_partition_shape,
)
from .freshness import freshness_policy_from_checks, per_model_freshness_policies
from .observability import (
    ANOMALY_CHECK_NAME,
    anomaly_check_results,
    drift_observations,
    optimize_metadata_for_keys,
)
from .resource import RockyResource
from .sensor import rocky_source_sensor
from .translator import RockyDagsterTranslator
from .types import (
    CompileResult,
    DagResult,
    Diagnostic,
    DiscoverResult,
    OptimizeResult,
    RunResult,
    Severity,
    SourceInfo,
    TableInfo,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

#: Built-in Rocky check names. Pre-declared as ``AssetCheckSpec`` so the
#: Dagster UI knows about them before any execution happens. The
#: ``row_count_anomaly`` check is emitted with severity WARN whenever Rocky
#: detects a row-count deviation above its anomaly threshold; the other
#: three are pass/fail with severity ERROR.
DEFAULT_CHECK_NAMES: tuple[str, ...] = (
    "row_count",
    "column_match",
    "freshness",
    ANOMALY_CHECK_NAME,
)


@dataclass(frozen=True)
class RockyTableProps:
    """Properties of a Rocky source table, exposed as Jinja template variables
    in the ``translation`` YAML field.

    Users can reference these in ``defs.yaml`` to customize asset keys, groups,
    and tags without writing Python::

        translation:
          key: "raw/{{ source_type }}/{{ group_name }}/{{ table_name }}"
          group: "{{ group_name }}"
          tags:
            team: "{{ source_type }}"
    """

    source_id: str
    source_type: str
    group_name: str
    table_name: str
    row_count: int | None


def _rocky_table_template_vars(props: RockyTableProps) -> dict[str, object]:
    return {
        "source_id": props.source_id,
        "source_type": props.source_type,
        "group_name": props.group_name,
        "table_name": props.table_name,
        "row_count": props.row_count,
    }


class RockyMetadataSet(NamespacedMetadataSet):
    """Structured metadata for Rocky assets, visible in the Dagster UI."""

    source_id: str | None = None
    source_type: str | None = None
    strategy: str | None = None
    watermark: str | None = None
    rows_copied: int | None = None
    duration_ms: int | None = None
    #: Fully-qualified target table identifier
    #: (``catalog.schema.table``). T1.4.
    target_table_full_name: str | None = None
    #: 16-char hex fingerprint of the SQL the engine sent to the
    #: warehouse. Lets users detect "what changed?" between runs. T1.4.
    sql_hash: str | None = None
    #: Number of columns in the materialized table's typed schema.
    #: T1.4.
    column_count: int | None = None
    #: Compile time in milliseconds. T1.4.
    compile_time_ms: int | None = None

    @classmethod
    def namespace(cls) -> str:
        return "dagster-rocky"


# ---------------------------------------------------------------------------
# Internal data structures
# ---------------------------------------------------------------------------


@dataclass
class _GroupBuild:
    """Per-group context accumulated while walking the discover output.

    A "group" is the unit of execution: one Dagster ``multi_asset`` per
    Rocky group (typically tenant/client). The group bundles every spec,
    every source id and the filter that materializes them.
    """

    name: str
    specs: list[dg.AssetSpec] = field(default_factory=list)
    source_ids: set[str] = field(default_factory=set)
    filter: str = ""
    key_to_source_id: dict[dg.AssetKey, str] = field(default_factory=dict)
    rocky_key_to_dagster_key: dict[tuple[str, ...], dg.AssetKey] = field(default_factory=dict)


@dataclass
class _CompileState:
    """Compile diagnostics + error flag pulled from cached state."""

    has_errors: bool = False
    diagnostics: list[Diagnostic] = field(default_factory=list)

    @classmethod
    def from_result(cls, result: CompileResult | None) -> _CompileState:
        if result is None:
            return cls()
        return cls(has_errors=result.has_errors, diagnostics=result.diagnostics)


# ---------------------------------------------------------------------------
# Component
# ---------------------------------------------------------------------------


class RockyComponent(StateBackedComponent, dg.Model, dg.Resolvable):
    """Loads Rocky-managed tables as materializable Dagster assets.

    Calls ``rocky discover`` during state refresh and caches the result.
    On code-location load, builds executable assets from the cached state.
    When materialized, assets run ``rocky run --filter <key>=<value>`` and
    yield rich metadata + check results.

    Example:

        .. code-block:: yaml

            # defs.yaml
            type: dagster_rocky.RockyComponent
            attributes:
              binary_path: rocky
              config_path: config/rocky.toml
    """

    binary_path: str = "rocky"
    config_path: str = "rocky.toml"
    state_path: str = ".rocky-state.redb"
    models_dir: str = "models"
    contracts_dir: str | None = None
    translator_class: str | None = None
    #: Optional YAML-level asset spec translation. Allows customizing asset
    #: keys, groups, and tags directly in ``defs.yaml`` without writing a
    #: Python ``RockyDagsterTranslator`` subclass. Template variables
    #: ``source_id``, ``source_type``, ``group_name``, ``table_name``, and
    #: ``row_count`` are available. Applied *after* the translator builds
    #: the base spec, so both mechanisms compose.
    translation: (
        Annotated[
            TranslationFn[RockyTableProps],
            TranslationFnResolver(
                template_vars_for_translation_fn=_rocky_table_template_vars,
            ),
        ]
        | None
    ) = None
    defs_state: ResolvedDefsStateConfig = DefsStateConfigArgs.local_filesystem()
    #: When ``True``, ``write_state_to_path`` also runs ``rocky optimize`` and
    #: persists the result alongside discover/compile. ``build_defs_from_state``
    #: then merges per-model strategy recommendations into ``AssetSpec.metadata``
    #: at load time so the Dagster UI surfaces ``rocky/current_strategy``,
    #: ``rocky/recommended_strategy``, ``rocky/estimated_monthly_savings``, and
    #: ``rocky/optimize_reasoning`` without requiring a run. ``rocky optimize``
    #: analyzes run history — on first invocation with no history it is a no-op.
    surface_optimize_metadata: bool = True
    #: When ``True``, ``build_defs_from_state`` also surfaces derived models
    #: (the entries in ``compile.models_detail``) as their own Dagster
    #: assets. One multi-asset is created per partitioning shape so all
    #: specs inside it share a single ``PartitionsDefinition``. Defaults
    #: to ``False`` for backwards compat — turning it on adds new asset
    #: keys to the code location which may require run-config or
    #: alerting updates.
    #:
    #: Limitation: until ``rocky run --model <name>`` lands on the engine
    #: side, derived-model multi-assets use ``can_subset=False`` (selecting
    #: any subset materializes the whole group). Per-model freshness,
    #: optimize metadata, contract checks, and partition definitions all
    #: still apply per-asset.
    surface_derived_models: bool = False
    #: When ``True``, use the DAG-driven asset builder that creates a
    #: single connected asset graph from ``rocky dag``. Every pipeline
    #: stage (source, load, transformation, seed, quality, snapshot)
    #: becomes a Dagster asset with fully resolved upstream dependencies.
    #: Supersedes the separate ``discover`` + ``surface_derived_models``
    #: paths. Defaults to ``False`` for backward compatibility; will
    #: become the default in a future release.
    dag_mode: bool = False
    #: When ``True``, ``build_defs_from_state`` includes a
    #: :func:`rocky_source_sensor` definition that polls ``rocky discover``
    #: and emits ``RunRequest`` events when upstream connectors produce
    #: new data. Bundling the sensor inside the component follows Dagster's
    #: design guide: the component is the single source of truth for all
    #: definitions related to this integration. Defaults to ``False``
    #: so existing deployments are unaffected.
    enable_sensor: bool = False
    #: Controls whether the sensor emits one ``RunRequest`` per source
    #: (``"per_source"``) or per Dagster group (``"per_group"``).
    sensor_granularity: Literal["per_source", "per_group"] = "per_source"
    #: Minimum interval between sensor evaluations in seconds.
    sensor_interval_seconds: int = 300

    @property
    def defs_state_config(self) -> DefsStateConfig:
        return DefsStateConfig.from_args(
            self.defs_state,
            default_key=f"RockyComponent[{self.config_path}]",
        )

    # ------------------------------------------------------------------ #
    # Resource and translator construction                               #
    # ------------------------------------------------------------------ #

    def _get_rocky_resource(self) -> RockyResource:
        return RockyResource(
            binary_path=self.binary_path,
            config_path=self.config_path,
            state_path=self.state_path,
            models_dir=self.models_dir,
            contracts_dir=self.contracts_dir,
        )

    def _get_translator(self) -> RockyDagsterTranslator:
        if self.translator_class is None:
            return RockyDagsterTranslator()

        if "." not in self.translator_class:
            raise ValueError(
                f"translator_class must be a dotted module path "
                f"(e.g. 'my_module.MyClass'), got {self.translator_class!r}"
            )
        module_path, class_name = self.translator_class.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)()

    # ------------------------------------------------------------------ #
    # State refresh                                                      #
    # ------------------------------------------------------------------ #

    def write_state_to_path(self, state_path: Path) -> None:
        """Persist ``rocky discover`` (and ``rocky compile`` when possible) as JSON.

        The state file stores discovery and compile (and optionally optimize)
        results so that :meth:`build_defs_from_state` can surface compiler
        diagnostics as asset checks and merge optimize recommendations into
        ``AssetSpec.metadata``. Compile and optimize are best-effort: if either
        fails, discovery state is still written and the slot is left empty.
        """
        rocky = self._get_rocky_resource()

        state: dict = {
            "discover": json.loads(rocky.discover().model_dump_json()),
        }

        compile_payload = self._compile_payload(rocky)
        if compile_payload is not None:
            state["compile"] = compile_payload

        if self.surface_optimize_metadata:
            optimize_payload = self._optimize_payload(rocky)
            if optimize_payload is not None:
                state["optimize"] = optimize_payload

        # DAG mode: cache the full unified DAG alongside discover/compile.
        if self.dag_mode:
            dag_payload = self._dag_payload(rocky)
            if dag_payload is not None:
                state["dag"] = dag_payload

        state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def _compile_payload(self, rocky: RockyResource) -> dict | None:
        """Return compile JSON, or ``None`` if compile cannot/should not run."""
        if not Path(self.models_dir).is_dir():
            return None
        try:
            return json.loads(rocky.compile().model_dump_json())
        except dg.Failure:
            # Compile is best-effort: missing binary, partial errors and
            # config issues should not block discovery state from being
            # written. The diagnostics surfaced through `rocky compile`
            # already cover the user-facing errors when it does run.
            return None

    def _optimize_payload(self, rocky: RockyResource) -> dict | None:
        """Return optimize JSON, or ``None`` if optimize cannot run.

        Same best-effort semantics as ``_compile_payload``: a missing binary
        or empty run history (no recommendations) does not block state
        persistence.
        """
        try:
            return json.loads(rocky.optimize().model_dump_json())
        except dg.Failure:
            return None

    def _dag_payload(self, rocky: RockyResource) -> dict | None:
        """Return DAG JSON, or ``None`` if the DAG command fails.

        Same best-effort semantics: a missing binary or config issue does
        not block state persistence.
        """
        try:
            return json.loads(
                rocky.dag(column_lineage=True).model_dump_json()
            )
        except dg.Failure:
            return None

    def _build_defs_from_dag(self, state_path: Path) -> dg.Definitions:
        """Build asset definitions from the cached ``rocky dag`` output.

        Falls back to the discover-based flow if the DAG slot is missing
        from the cached state (e.g., first run before ``dag_mode`` was set).
        """
        from .dag_assets import build_dag_specs, split_dag_specs_by_group

        raw = json.loads(state_path.read_text(encoding="utf-8"))
        if "dag" not in raw:
            # Graceful fallback: re-enter the non-DAG path.
            self.dag_mode = False
            return self.build_defs_from_state(None, state_path)

        dag_result = DagResult.model_validate(raw["dag"])
        translator = self._get_translator()
        rocky = self._get_rocky_resource()

        specs, node_id_to_key = build_dag_specs(
            dag_result,
            translator=translator,
        )

        return dg.Definitions(
            assets=specs,
            resources={"rocky": rocky},
        )

    # ------------------------------------------------------------------ #
    # Definition building                                                #
    # ------------------------------------------------------------------ #

    def build_defs_from_state(
        self,
        context: dg.ComponentLoadContext,
        state_path: Path | None,
    ) -> dg.Definitions:
        """Build materializable assets from the cached discover/compile state."""
        if state_path is None:
            return dg.Definitions()

        # DAG-mode: build the full connected asset graph from ``rocky dag``.
        if self.dag_mode:
            return self._build_defs_from_dag(state_path)

        discover, compile_result, optimize_result = _load_state(state_path)
        compile_state = _CompileState.from_result(compile_result)

        translator = self._get_translator()
        rocky = self._get_rocky_resource()

        # Per-model freshness policies — sourced from `rocky compile`
        # frontmatter rather than `[checks.freshness]`. The component
        # surfaces source-replication tables as assets, so a per-model
        # policy attaches when the source table name matches a compiled
        # model name (typically when models reference a source via
        # `[[sources]]`). The pipeline-level freshness from
        # `[checks.freshness]` is the fallback for everything else.
        model_policies = per_model_freshness_policies(compile_result)

        groups = _build_group_contexts(
            discover,
            translator,
            model_policies,
            translation=self.translation,
        )

        # Optimize metadata — merged into AssetSpec.metadata at load time
        # when surface_optimize_metadata=True. The model_to_key map is
        # built from source-replication assets where the table name
        # matches a Rocky-optimize model name. Mostly a no-op until
        # derived models are surfaced as their own assets, but defensible
        # plumbing for the case where source table names match.
        if optimize_result is not None:
            model_to_key: dict[str, dg.AssetKey] = {
                spec.key.path[-1]: spec.key for group in groups for spec in group.specs
            }
            optimize_meta = optimize_metadata_for_keys(optimize_result, model_to_key=model_to_key)
            _merge_optimize_metadata(groups, optimize_meta)

        # Contract checks (T4.3) — when contracts_dir is configured, walk
        # the directory for .contract.toml files and pre-declare check
        # specs per matching asset. The match heuristic is the same as
        # optimize: source table name must equal the contract file's
        # model name. Mostly a no-op until derived models are surfaced
        # as their own assets.
        contract_rules_by_model: dict[str, ContractRules] = (
            discover_contract_rules(Path(self.contracts_dir))
            if self.contracts_dir is not None
            else {}
        )

        check_specs = _build_check_specs(groups, contract_rules_by_model)

        assets: list[dg.AssetsDefinition] = [
            _make_rocky_asset(
                group=group,
                check_specs=[
                    cs for cs in check_specs if any(cs.asset_key == s.key for s in group.specs)
                ],
                rocky=rocky,
                compile_state=compile_state,
                contract_rules_by_model=contract_rules_by_model,
            )
            for group in groups
        ]

        # Derived-model surface (opt-in via surface_derived_models). Each
        # partition shape becomes its own multi-asset so the
        # PartitionsDefinition is consistent within each multi-asset.
        if self.surface_derived_models and compile_result is not None:
            model_specs = build_model_specs(
                compile_result,
                translator=translator,
                optimize_result=optimize_result,
                contract_rules_by_model=contract_rules_by_model,
            )
            for model_group in split_model_specs_by_partition_shape(model_specs):
                assets.append(
                    _make_derived_model_asset(
                        group=model_group,
                        rocky=rocky,
                        models_dir=self.models_dir,
                        fallback_filter=_first_source_filter(discover),
                        compile_state=compile_state,
                    )
                )

        sensors: list[dg.SensorDefinition] = []
        if self.enable_sensor and assets:
            sensors.append(
                rocky_source_sensor(
                    rocky_resource=rocky,
                    target=assets,
                    granularity=self.sensor_granularity,
                    translator=translator,
                    name=f"rocky_source_sensor_{_safe_asset_name(self.config_path)}",
                    minimum_interval_seconds=self.sensor_interval_seconds,
                )
            )

        return dg.Definitions(
            assets=assets,
            sensors=sensors or None,
            resources={"rocky": rocky},
        )


# ---------------------------------------------------------------------------
# State loading
# ---------------------------------------------------------------------------


def _load_state(
    state_path: Path,
) -> tuple[DiscoverResult, CompileResult | None, OptimizeResult | None]:
    """Read the on-disk state file.

    Expected format::

        {"discover": {...}, "compile": {...}, "optimize": {...}}

    The ``compile`` and ``optimize`` slots are optional. Returns ``None``
    for missing slots so callers can handle each independently.
    """
    raw = json.loads(state_path.read_text(encoding="utf-8"))

    discover = DiscoverResult.model_validate(raw["discover"])
    compile_data = raw.get("compile")
    compile_result = (
        CompileResult.model_validate(compile_data) if compile_data is not None else None
    )
    optimize_data = raw.get("optimize")
    optimize_result = (
        OptimizeResult.model_validate(optimize_data) if optimize_data is not None else None
    )
    return discover, compile_result, optimize_result


def _merge_optimize_metadata(
    groups: list[_GroupBuild],
    optimize_meta: dict[dg.AssetKey, dict[str, dg.MetadataValue]],
) -> None:
    """Merge per-key optimize metadata into the AssetSpecs of the given groups.

    Mutates the specs in place by replacing them with new specs that have
    the optimize metadata merged into ``AssetSpec.metadata``. ``AssetSpec``
    is immutable, so we use ``dataclasses.replace``-style ``replace_attributes``.
    Specs without a matching entry in ``optimize_meta`` are left unchanged.
    """
    for group in groups:
        new_specs: list[dg.AssetSpec] = []
        for spec in group.specs:
            extra = optimize_meta.get(spec.key)
            if extra is None:
                new_specs.append(spec)
                continue
            merged = dict(spec.metadata or {})
            merged.update(extra)
            new_specs.append(spec.replace_attributes(metadata=merged))
        group.specs = new_specs


# ---------------------------------------------------------------------------
# Group / spec construction
# ---------------------------------------------------------------------------


def _build_filter(source: SourceInfo) -> str:
    """Build a ``key=value`` filter string from the first scalar component."""
    for k, v in source.components.items():
        if isinstance(v, str):
            return f"{k}={v}"
    return ""


def _native_rocky_key(source: SourceInfo, table_name: str) -> tuple[str, ...]:
    """Mirror the asset key Rocky's CLI emits in run output.

    Rocky uses ``[source_type, *components, table]``. The translator may
    remap that to a different Dagster ``AssetKey``, in which case we need
    the original tuple to look up the remapped key on the way back in.
    """
    parts: list[str] = [source.source_type]
    for v in source.components.values():
        if isinstance(v, list):
            parts.extend(v)
        else:
            parts.append(str(v))
    parts.append(table_name)
    return tuple(parts)


def _build_group_contexts(
    discover: DiscoverResult,
    translator: RockyDagsterTranslator,
    model_policies: dict[str, dg.FreshnessPolicy] | None = None,
    *,
    translation: TranslationFn[RockyTableProps] | None = None,
) -> list[_GroupBuild]:
    """Walk the discover output and accumulate one ``_GroupBuild`` per group.

    Args:
        discover: The discover result to walk.
        translator: Translator for asset key / group / tag derivation.
        model_policies: Optional per-model freshness policies indexed by
            model name. When a source table's name matches a model name
            in this map, the per-model policy wins over the pipeline-level
            ``[checks.freshness]`` default. ``None`` means the default
            applies to every asset.
        translation: Optional YAML-level translation function. Applied after
            the translator builds the base spec.
    """
    groups: dict[str, _GroupBuild] = defaultdict(lambda: _GroupBuild(name=""))
    default_policy = freshness_policy_from_checks(discover.checks)
    model_policies = model_policies or {}

    for source in discover.sources:
        group_name = translator.get_group_name(source)
        group = groups[group_name]
        if not group.name:
            group.name = group_name
            group.filter = _build_filter(source)
        group.source_ids.add(source.id)

        for table in source.tables:
            # Per-model freshness wins over pipeline-level when the source
            # table name matches a compiled model name. This makes
            # `models/<table>.toml [freshness]` overrides the default for
            # that one asset.
            policy = model_policies.get(table.name, default_policy)
            spec = _build_asset_spec(
                source,
                table,
                translator,
                policy,
                translation=translation,
            )
            group.specs.append(spec)
            group.key_to_source_id[spec.key] = source.id
            group.rocky_key_to_dagster_key[_native_rocky_key(source, table.name)] = spec.key

    return list(groups.values())


def _build_asset_spec(
    source: SourceInfo,
    table: TableInfo,
    translator: RockyDagsterTranslator,
    freshness_policy: dg.FreshnessPolicy | None,
    *,
    translation: TranslationFn[RockyTableProps] | None = None,
) -> dg.AssetSpec:
    """Build a single ``AssetSpec`` for one Rocky table."""
    key = translator.get_asset_key(source, table)
    deps = translator.get_asset_deps(source, table)
    metadata = {
        **translator.get_metadata(source, table),
        **RockyMetadataSet(source_id=source.id, source_type=source.source_type),
    }
    # Declare upstream deps for lineage visibility in the UI. This does NOT
    # block Rocky execution on upstream materialization — execution ordering
    # is controlled by automation policies, not deps.
    if deps:
        metadata["rocky/upstream_keys"] = dg.MetadataValue.text(", ".join(str(d) for d in deps))

    spec = dg.AssetSpec(
        key=key,
        group_name=translator.get_group_name(source),
        tags=translator.get_tags(source, table),
        metadata=metadata,
        deps=deps or None,
        kinds={"rocky", source.source_type},
        freshness_policy=freshness_policy,
    )

    if translation is not None:
        props = RockyTableProps(
            source_id=source.id,
            source_type=source.source_type,
            group_name=translator.get_group_name(source),
            table_name=table.name,
            row_count=table.row_count,
        )
        spec = translation(spec, props)

    return spec


def _build_check_specs(
    groups: list[_GroupBuild],
    contract_rules_by_model: dict[str, ContractRules] | None = None,
) -> list[dg.AssetCheckSpec]:
    """Pre-declare check specs for every asset in every group.

    Always emits the four ``DEFAULT_CHECK_NAMES`` per asset (row_count,
    column_match, freshness, row_count_anomaly). When
    ``contract_rules_by_model`` is provided, additionally emits one
    AssetCheckSpec per declared contract rule kind for assets whose
    table name matches a key in the map. The contract specs are gated
    on the presence of the matching rule kind in the contract file
    (e.g. a contract with only `[[columns]]` constraints does NOT get a
    `contract_required_columns` spec).
    """
    contract_rules_by_model = contract_rules_by_model or {}
    specs: list[dg.AssetCheckSpec] = []

    for group in groups:
        for spec in group.specs:
            # Default checks (4 per asset)
            for check_name in DEFAULT_CHECK_NAMES:
                specs.append(dg.AssetCheckSpec(name=check_name, asset=spec.key))

            # Contract checks (per declared rule kind, when matched)
            table_name = spec.key.path[-1]
            rules = contract_rules_by_model.get(table_name)
            if rules is not None:
                specs.extend(contract_check_specs_for_model(spec.key, rules))

    return specs


# ---------------------------------------------------------------------------
# Asset factory
# ---------------------------------------------------------------------------


def _safe_asset_name(group_name: str) -> str:
    return group_name.replace("-", "_").replace(".", "_")


def _first_source_filter(discover: DiscoverResult) -> str:
    """Return a sentinel filter suitable for ``rocky run`` model-only execution.

    ``rocky run`` requires a ``--filter`` argument. Derived-model
    materialization wants to run models without (necessarily) refreshing
    a specific source. Picking the first scalar component of the first
    discovered source produces a filter that matches at least one
    source's replication phase, so the source-replication phase isn't
    a no-op error. The resulting source materializations are dropped by
    ``_emit_results`` because they're not in the multi-asset's
    ``selected_keys``.

    Returns ``"_dagster_models_only=__none__"`` if no source is
    discovered — that filter matches nothing, so the source-replication
    phase is a no-op (the model execution phase still runs).
    """
    for source in discover.sources:
        for k, v in source.components.items():
            if isinstance(v, str):
                return f"{k}={v}"
    return "_dagster_models_only=__none__"


def _make_derived_model_asset(
    *,
    group: ModelGroup,
    rocky: RockyResource,
    models_dir: str,
    fallback_filter: str,
    compile_state: _CompileState,
) -> dg.AssetsDefinition:
    """Build a multi-asset that runs every derived model in a partition shape group.

    All specs in the group share one ``PartitionsDefinition`` (or none).
    The multi-asset uses ``can_subset=False`` because Rocky's ``rocky run``
    has no per-model filter today — selecting any subset of the group's
    assets materializes the whole group. When the engine adds
    ``rocky run --model <name>``, this can be flipped to ``can_subset=True``
    with a per-model filter pass.

    Materialization invokes ``rocky run --filter <fallback>
    --models <dir> --all``. The ``--filter`` is a sentinel pointing at
    the first discovered source so the engine accepts the command;
    source-replication MaterializeResult events for that source are
    dropped naturally because the multi-asset only DECLARES derived-model
    AssetSpecs.

    Partition flags (``--partition`` / ``--from`` / ``--to``) are
    threaded from ``context.partition_key`` / ``context.partition_key_range``
    when the group is partitioned.
    """
    asset_name = f"rocky_{_safe_asset_name(group.name)}"

    @dg.multi_asset(
        name=asset_name,
        specs=group.specs,
        can_subset=False,
        partitions_def=group.partitions_def,
    )
    def _asset(context):
        _log_compile_diagnostics(context, compile_state)

        partition_kwargs: dict[str, object] = {}
        if group.partitions_def is not None:
            if context.has_partition_key_range:
                key_range = context.partition_key_range
                partition_kwargs["partition_from"] = key_range.start
                partition_kwargs["partition_to"] = key_range.end
            elif context.has_partition_key:
                partition_kwargs["partition"] = context.partition_key

        context.log.info(
            f"Executing rocky run --models {models_dir} --all "
            f"(filter={fallback_filter!r}, partition_kwargs={partition_kwargs})"
        )
        result = rocky.run(
            filter=fallback_filter,
            run_models=True,
            **partition_kwargs,  # type: ignore[arg-type]
        )
        context.log.info(
            f"Rocky completed: {result.tables_copied} copied, "
            f"{result.tables_failed} failed in {result.duration_ms}ms"
        )
        _log_run_diagnostics(context, result)

        # Yield MaterializeResult only for the model assets in this group.
        # The selected_keys are all of group.specs (can_subset=False).
        selected_keys = {spec.key for spec in group.specs}
        yield from _emit_derived_model_results(
            result=result,
            selected_keys=selected_keys,
        )

    return _asset


def _emit_derived_model_results(
    *,
    result: RunResult,
    selected_keys: set[dg.AssetKey],
) -> Iterator[dg.MaterializeResult]:
    """Yield ``MaterializeResult`` for the derived-model assets in ``selected_keys``.

    Walks ``result.materializations`` and emits one MaterializeResult per
    materialization whose remapped asset key is in ``selected_keys``.
    Source-replication materializations (which Rocky also emits when the
    fallback filter matches a source) are dropped silently.
    """
    for mat in result.materializations:
        # Derived-model materializations identify their key directly via
        # mat.asset_key, which is the ``[catalog, schema, table]`` tuple
        # the engine uses internally.
        asset_key = dg.AssetKey(mat.asset_key)
        if asset_key not in selected_keys:
            continue
        metadata: dict[str, dg.MetadataValue] = {
            **RockyMetadataSet(
                strategy=mat.metadata.strategy,
                duration_ms=mat.duration_ms,
                rows_copied=mat.rows_copied,
                watermark=(
                    mat.metadata.watermark.isoformat()
                    if mat.metadata.watermark is not None
                    else None
                ),
                target_table_full_name=mat.metadata.target_table_full_name,
                sql_hash=mat.metadata.sql_hash,
                column_count=mat.metadata.column_count,
                compile_time_ms=mat.metadata.compile_time_ms,
            ),
            "dagster/duration_ms": dg.MetadataValue.int(mat.duration_ms),
        }
        if mat.rows_copied is not None:
            metadata["dagster/row_count"] = dg.MetadataValue.int(mat.rows_copied)
        if mat.partition is not None:
            metadata["rocky/partition_key"] = dg.MetadataValue.text(mat.partition.key)
        yield dg.MaterializeResult(
            asset_key=asset_key,
            metadata=metadata,
        )


def _make_rocky_asset(
    *,
    group: _GroupBuild,
    check_specs: list[dg.AssetCheckSpec],
    rocky: RockyResource,
    compile_state: _CompileState,
    contract_rules_by_model: dict[str, ContractRules] | None = None,
) -> dg.AssetsDefinition:
    """Create a multi-asset that executes ``rocky run`` for one group.

    Subset-aware: when Dagster selects only some of the group's assets,
    we run ``--filter id=<source_id>`` per selected source instead of the
    full group filter.
    """
    contract_rules_by_model = contract_rules_by_model or {}

    @dg.multi_asset(
        name=f"rocky_{_safe_asset_name(group.name)}",
        specs=group.specs,
        check_specs=check_specs,
        can_subset=True,
    )
    def _asset(context):
        _log_compile_diagnostics(context, compile_state)

        selected_keys = set(context.selected_asset_keys)
        filters = _select_filters(group, selected_keys, context)
        results = _run_filters(context, rocky, filters)

        if len(filters) > 1:
            context.log.info(
                f"Total across {len(filters)} sources: "
                f"{sum(r.tables_copied for r in results)} tables in "
                f"{sum(r.duration_ms for r in results)}ms"
            )

        yield from _emit_results(
            results=results,
            check_specs=check_specs,
            selected_keys=selected_keys,
            rocky_key_to_dagster_key=group.rocky_key_to_dagster_key,
        )

        # Contract check results — sourced from compile diagnostics, not
        # from the run. Each declared contract spec gets exactly one
        # result (pass/fail) per materialization. Specs without
        # corresponding rule entries are skipped.
        if contract_rules_by_model:
            yield from _emit_contract_check_results(
                group=group,
                selected_keys=selected_keys,
                compile_state=compile_state,
                contract_rules_by_model=contract_rules_by_model,
            )

    return _asset


def _emit_contract_check_results(
    *,
    group: _GroupBuild,
    selected_keys: set[dg.AssetKey],
    compile_state: _CompileState,
    contract_rules_by_model: dict[str, ContractRules],
) -> Iterator[dg.AssetCheckResult]:
    """Yield ``AssetCheckResult`` for every declared contract spec in the group.

    Walks each spec in the group, checks if the spec's table name matches
    a contract in ``contract_rules_by_model``, and emits one
    ``AssetCheckResult`` per declared rule kind by translating compile
    diagnostics. Skips specs that aren't in ``selected_keys`` so
    partial-subset runs don't emit results for unselected assets.
    """
    for spec in group.specs:
        if spec.key not in selected_keys:
            continue
        table_name = spec.key.path[-1]
        rules = contract_rules_by_model.get(table_name)
        if rules is None:
            continue
        yield from contract_check_results_from_diagnostics(
            compile_state.diagnostics,
            asset_key=spec.key,
            model_name=table_name,
            rules=rules,
        )


# ---------------------------------------------------------------------------
# Asset execution helpers
# ---------------------------------------------------------------------------


def _log_compile_diagnostics(
    context,
    compile_state: _CompileState,
) -> None:
    for diag in compile_state.diagnostics:
        msg = f"[{diag.code}] {diag.model}: {diag.message}"
        if diag.severity == Severity.error:
            context.log.error(f"Compile error: {msg}")
        elif diag.severity == Severity.warning:
            context.log.warning(f"Compile warning: {msg}")
    if compile_state.has_errors:
        context.log.error("Rocky compilation has errors — execution may produce unexpected results")


def _select_filters(
    group: _GroupBuild,
    selected_keys: set[dg.AssetKey],
    context,
) -> list[str]:
    """Pick group-level filter or per-source filters depending on subset."""
    needed_source_ids = {
        group.key_to_source_id[k] for k in selected_keys if k in group.key_to_source_id
    }

    if not needed_source_ids or needed_source_ids == group.source_ids:
        return [group.filter]

    context.log.info(
        f"Subset execution: {len(needed_source_ids)}/{len(group.source_ids)} sources "
        f"selected for group '{group.name}'"
    )
    return [f"id={sid}" for sid in sorted(needed_source_ids)]


def _run_filters(
    context,
    rocky: RockyResource,
    filters: list[str],
    *,
    streaming: bool = True,
) -> list[RunResult]:
    """Execute ``rocky run`` for each filter and emit per-run log lines.

    By default uses :meth:`RockyResource.run_streaming` so the engine's
    stderr (where the Rust ``tracing`` layer writes ``info!()`` /
    ``warn!()`` macros) flows live into the Dagster run viewer instead
    of buffering until the subprocess exits. Set ``streaming=False`` to
    fall back to the buffered :meth:`run` path (mostly useful for
    tests that don't want to mock ``subprocess.Popen``).
    """
    results: list[RunResult] = []
    for f in filters:
        context.log.info(f"Executing: rocky run --filter {f}")
        result = rocky.run_streaming(context, filter=f) if streaming else rocky.run(filter=f)
        context.log.info(
            f"Rocky completed: {result.tables_copied} copied, "
            f"{result.tables_failed} failed in {result.duration_ms}ms"
        )
        _log_run_diagnostics(context, result)
        results.append(result)
    return results


def _log_run_diagnostics(
    context,
    result: RunResult,
) -> None:
    """Log run-level errors and contract violations.

    Drift and anomalies are NOT logged here — they're emitted as
    structured Dagster events (``AssetObservation`` and ``AssetCheckResult``
    respectively) by :func:`_emit_results`. Logging them here too would
    duplicate the signal in the Dagster UI.
    """
    for err in result.errors:
        context.log.error(f"Table failed: {err.error}")

    if result.contracts is not None and not result.contracts.passed:
        for violation in result.contracts.violations:
            context.log.error(f"Contract violation: {violation.column} — {violation.message}")


def _emit_results(
    *,
    results: list[RunResult],
    check_specs: list[dg.AssetCheckSpec],
    selected_keys: set[dg.AssetKey],
    rocky_key_to_dagster_key: dict[tuple[str, ...], dg.AssetKey],
) -> Iterator[dg.MaterializeResult | dg.AssetCheckResult | dg.AssetObservation]:
    """Yield Dagster events for every materialization, check, drift event and anomaly.

    Three filters apply:

    1. **Subset filter**: Rocky always runs at source granularity, so a single
       ``rocky run`` may emit results for tables the caller did not actually
       request. Yielding those would crash with
       ``DagsterInvariantViolationError``.
    2. **Declared-check filter**: Rocky may emit additional check kinds that
       were not pre-declared (e.g. ``null_rate``). Dagster rejects checks
       outside the declared ``check_specs``, so we drop them too.
    3. **Subset filter on drift/anomaly**: drift and anomaly events are also
       restricted to ``selected_keys`` so partial-subset runs don't emit
       events for tables the caller did not request.

    Three structured event kinds are emitted:

    * :class:`dg.MaterializeResult` — one per copied table.
    * :class:`dg.AssetCheckResult` — one per declared Rocky check, plus one
      ``row_count_anomaly`` (severity WARN) per Rocky-detected anomaly.
    * :class:`dg.AssetObservation` — one per drift action (ALTER COLUMN,
      DROP+RECREATE, etc.). Drift is a structural change, not pass/fail,
      so observation is the right primitive.
    """

    def remap(raw_key: list[str]) -> dg.AssetKey:
        return rocky_key_to_dagster_key.get(tuple(raw_key)) or dg.AssetKey(raw_key)

    selected_resolver = _build_table_resolver(rocky_key_to_dagster_key, selected_keys)

    declared_checks: set[tuple[dg.AssetKey, str]] = {(cs.asset_key, cs.name) for cs in check_specs}

    materializations = [mat for r in results for mat in r.materializations]
    table_checks = [tc for r in results for tc in r.check_results]

    materialized_keys: set[dg.AssetKey] = set()
    for mat in materializations:
        asset_key = remap(mat.asset_key)
        if asset_key not in selected_keys:
            continue
        materialized_keys.add(asset_key)
        # Emit BOTH the rocky-namespaced metadata (via RockyMetadataSet) AND
        # the canonical Dagster Insights field aliases (`dagster/row_count`,
        # `dagster/duration_ms`). Dagster+ Insights picks up the
        # `dagster/`-prefixed fields automatically and renders them as
        # cost / throughput metrics in the Insights dashboards. The
        # rocky-namespaced fields stay for Rocky-specific consumers.
        metadata: dict[str, dg.MetadataValue] = {
            **RockyMetadataSet(
                strategy=mat.metadata.strategy,
                duration_ms=mat.duration_ms,
                rows_copied=mat.rows_copied,
                watermark=(
                    mat.metadata.watermark.isoformat()
                    if mat.metadata.watermark is not None
                    else None
                ),
                target_table_full_name=mat.metadata.target_table_full_name,
                sql_hash=mat.metadata.sql_hash,
                column_count=mat.metadata.column_count,
                compile_time_ms=mat.metadata.compile_time_ms,
            ),
            "dagster/duration_ms": dg.MetadataValue.int(mat.duration_ms),
        }
        if mat.rows_copied is not None:
            metadata["dagster/row_count"] = dg.MetadataValue.int(mat.rows_copied)

        yield dg.MaterializeResult(
            asset_key=asset_key,
            metadata=metadata,
        )

    yielded_checks: set[tuple[dg.AssetKey, str]] = set()
    for table_check in table_checks:
        asset_key = remap(table_check.asset_key)
        if asset_key not in selected_keys:
            continue
        for check in table_check.checks:
            if (asset_key, check.name) not in declared_checks:
                continue
            yielded_checks.add((asset_key, check.name))
            yield dg.AssetCheckResult(
                asset_key=asset_key,
                check_name=check.name,
                passed=check.passed,
                metadata=check_metadata(check),
            )

    # Drift events → AssetObservation. Drift is a structural change, not a
    # pass/fail check, so observation is the correct primitive.
    for run_result in results:
        yield from drift_observations(run_result, key_resolver=selected_resolver)

    # Anomalies → AssetCheckResult with severity WARN. The check name is
    # pre-declared in DEFAULT_CHECK_NAMES so the spec is visible in the UI
    # before any run; placeholders below cover the no-anomaly case.
    for run_result in results:
        for anomaly_result in anomaly_check_results(run_result, key_resolver=selected_resolver):
            if (anomaly_result.asset_key, ANOMALY_CHECK_NAME) not in declared_checks:
                continue
            yielded_checks.add((anomaly_result.asset_key, ANOMALY_CHECK_NAME))
            yield anomaly_result

    yield from _emit_placeholder_checks(
        check_specs=check_specs,
        selected_keys=selected_keys,
        yielded_checks=yielded_checks,
        materialized_keys=materialized_keys,
    )


def _build_table_resolver(
    rocky_key_to_dagster_key: dict[tuple[str, ...], dg.AssetKey],
    selected_keys: set[dg.AssetKey],
) -> Callable[[str], dg.AssetKey | None]:
    """Return a resolver that maps Rocky table identifiers to selected keys.

    Rocky's drift/anomaly events identify tables by a single string that
    may be the bare table name, ``schema.table``, or
    ``catalog.schema.table``. The resolver:

    1. Splits the identifier on ``.`` and tries each trailing tuple
       against ``rocky_key_to_dagster_key``.
    2. Falls back to matching by the last path segment.
    3. Filters the result through ``selected_keys`` so partial-subset
       runs don't emit events for unselected tables.

    Returns ``None`` for any unresolvable or unselected table.
    """

    def resolve(table_name: str) -> dg.AssetKey | None:
        parts = table_name.split(".")
        for start in range(len(parts)):
            tup = tuple(parts[start:])
            if tup in rocky_key_to_dagster_key:
                key = rocky_key_to_dagster_key[tup]
                return key if key in selected_keys else None
        # Fallback: match by last path segment.
        last = parts[-1]
        for tup, key in rocky_key_to_dagster_key.items():
            if tup and tup[-1] == last and key in selected_keys:
                return key
        return None

    return resolve


def _emit_placeholder_checks(
    *,
    check_specs: list[dg.AssetCheckSpec],
    selected_keys: set[dg.AssetKey],
    yielded_checks: set[tuple[dg.AssetKey, str]],
    materialized_keys: set[dg.AssetKey],
) -> Iterator[dg.AssetCheckResult]:
    """Emit placeholders for declared checks Rocky did not produce.

    Without these, Dagster logs "did not yield expected outputs" warnings
    for any pre-declared check that the actual run didn't cover.
    """
    for cs in check_specs:
        if cs.asset_key not in selected_keys:
            continue
        if (cs.asset_key, cs.name) in yielded_checks:
            continue

        materialized = cs.asset_key in materialized_keys
        if materialized:
            reason = f"not produced by rocky (check type: {cs.name})"
            severity = dg.AssetCheckSeverity.ERROR
        else:
            reason = "table not materialized"
            severity = dg.AssetCheckSeverity.WARN

        yield dg.AssetCheckResult(
            asset_key=cs.asset_key,
            check_name=cs.name,
            passed=materialized,
            severity=severity,
            metadata={"status": dg.MetadataValue.text(reason)},
        )
