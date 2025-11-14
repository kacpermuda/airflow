# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_2_PLUS

if TYPE_CHECKING:
    from typing import Any

    from airflow.lineage.hook import LineageContext


def _add_extra_polyfill(collector):
    """
    Add support for extra lineage information on Airflow versions < 3.2.

    This polyfill adds the `add_extra` method and modifies `collected_assets` property to include
    extra lineage information. Should be called after renaming from dataset to asset. It's rewriting
    `collected_assets` method and not `collected_datasets` method.
    """
    # We already added it, skip
    if hasattr(collector, "add_extra"):
        return collector

    import hashlib
    import json
    import types
    from collections import defaultdict

    import attr

    from airflow.lineage.hook import HookLineage as _BaseHookLineage

    # Add `extra` to HookLineage returned by `collected_assets` property
    @attr.define
    class ExtraLineageInfo:
        """
        Holds lineage information for arbitrary non-asset metadata.

        This class represents additional lineage context captured during a hook execution that is not
        associated with a specific asset. It includes the metadata payload itself, the count of
        how many times it has been encountered, and the context in which it was encountered.
        """

        key: str
        value: Any
        count: int
        context: LineageContext

    @attr.define
    class HookLineage(_BaseHookLineage):
        # mypy is not happy, as base class is using other ExtraLineageInfo, but this code will never
        # run on AF3.2, where this other one is used, so this is fine - we can ignore.
        extra: list[ExtraLineageInfo] = attr.field(factory=list)  # type: ignore[assignment]

    # Initialize extra tracking attributes
    collector._extra = {}
    collector._extra_counts = defaultdict(int)

    # Save the original `collected_assets` getter
    _original_collected_assets = collector.__class__.collected_assets

    def _compat_collected_assets(self) -> HookLineage:
        """Get the collected hook lineage information."""
        # call the original `collected_assets` getter
        lineage = _original_collected_assets.fget(self)
        extra_list = [
            ExtraLineageInfo(
                key=key,
                value=value,
                count=self._extra_counts[count_key],
                context=context,
            )
            for count_key, (key, value, context) in self._extra.items()
        ]
        return HookLineage(
            inputs=lineage.inputs,
            outputs=lineage.outputs,
            extra=extra_list,
        )

    setattr(
        collector.__class__,
        "collected_assets",
        property(lambda c: _compat_collected_assets(c)),
    )

    # Save the original `has_collected` getter
    _original_has_collected = collector.__class__.has_collected

    def _compat_has_collected(self) -> bool:
        # call the original `has_collected` getter
        has_collected = _original_has_collected.fget(self)
        return bool(has_collected or self._extra)

    setattr(
        collector.__class__,
        "has_collected",
        property(lambda c: _compat_has_collected(c)),
    )

    # Add `add_extra` method if it does not exist
    def _compat_add_extra(self, context, key, value):
        """Add extra information for older Airflow versions."""
        _max_collected_extra = 200

        if len(self._extra) >= _max_collected_extra:
            if hasattr(self, "log"):
                self.log.debug("Maximum number of extra exceeded. Skipping.")
            return

        if not key or not value:
            if hasattr(self, "log"):
                self.log.debug("Missing required parameter: both 'key' and 'value' must be provided.")
            return

        extra_str = json.dumps(value, sort_keys=True, default=str)
        value_hash = hashlib.md5(extra_str.encode()).hexdigest()
        entry_id = f"{key}_{value_hash}_{id(context)}"
        if entry_id not in self._extra:
            self._extra[entry_id] = (key, value, context)
        self._extra_counts[entry_id] += 1

        if len(self._extra) == _max_collected_extra:
            if hasattr(self, "log"):
                self.log.warning("Maximum number of extra exceeded. Skipping subsequent inputs.")

    collector.add_extra = types.MethodType(_compat_add_extra, collector)

    return collector


def _get_af2_asset_compat_hook_lineage_collector(collector):
    """
    Handle AF 2.x compatibility for dataset -> asset terminology rename.

    This is only called for AF 2.x where we need to provide asset-named methods
    that wrap the underlying dataset methods.
    """
    # We already added it, skip
    if all(
        getattr(collector, asset_method_name, None)
        for asset_method_name in ("add_input_asset", "add_output_asset", "collected_assets", "add_extra")
    ):
        return collector

    from functools import wraps

    # in AF3 it's AssetLineageInfo, but here we need to import AF2 DatasetLineageInfo
    from airflow.lineage.hook import DatasetLineageInfo, HookLineage

    DatasetLineageInfo.asset = DatasetLineageInfo.dataset

    def rename_asset_kwargs_to_dataset_kwargs(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            if "asset_kwargs" in kwargs:
                kwargs["dataset_kwargs"] = kwargs.pop("asset_kwargs")

            if "asset_extra" in kwargs:
                kwargs["dataset_extra"] = kwargs.pop("asset_extra")

            return function(*args, **kwargs)

        return wrapper

    # HookLineageCollector on AF2 has the `*_dataset` methods, so if type checker complains it can be ignored
    collector.create_asset = rename_asset_kwargs_to_dataset_kwargs(collector.create_dataset)
    collector.add_input_asset = rename_asset_kwargs_to_dataset_kwargs(collector.add_input_dataset)
    collector.add_output_asset = rename_asset_kwargs_to_dataset_kwargs(collector.add_output_dataset)

    def _compat_collected_assets(self) -> HookLineage:
        """Get the collected hook lineage information."""
        lineage = self.collected_datasets
        return HookLineage(
            [
                DatasetLineageInfo(dataset=item.dataset, count=item.count, context=item.context)
                for item in lineage.inputs
            ],
            [
                DatasetLineageInfo(dataset=item.dataset, count=item.count, context=item.context)
                for item in lineage.outputs
            ],
        )

    setattr(
        collector.__class__,
        "collected_assets",
        property(lambda c: _compat_collected_assets(c)),
    )

    # Add `add_extra` polyfill for AF 2.x, needs to be called after setting `collected_assets`
    collector = _add_extra_polyfill(collector)

    return collector


def get_hook_lineage_collector():
    """
    Get hook lineage collector with appropriate compatibility layers.

    - AF 2.x: Apply both dataset->asset rename and then `add_extra` polyfill
    - AF 3.0-3.1: Apply only `add_extra` polyfill, no renaming needed
    - AF 3.2+: Use native implementation (no renaming or polyfill needed)
    """
    from airflow.lineage.hook import get_hook_lineage_collector as get_global_collector

    global_collector = get_global_collector()

    # AF 2.x: needs dataset->asset rename + `add_extra` polyfill
    if not AIRFLOW_V_3_0_PLUS:
        return _get_af2_asset_compat_hook_lineage_collector(global_collector)

    # AF 3.0-3.1: needs only polyfill for `add_extra`
    if not AIRFLOW_V_3_2_PLUS:
        return _add_extra_polyfill(global_collector)

    # AF 3.2+: no changes needed
    return global_collector
