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
from urllib.parse import urlsplit

from airflow.providers.common.compat.assets import Asset

if TYPE_CHECKING:
    from urllib.parse import SplitResult

    from airflow.providers.common.compat.openlineage.facet import Dataset as OpenLineageDataset


def sanitize_uri(uri: SplitResult) -> SplitResult:
    if not uri.netloc:
        raise ValueError("URI format pubsub:// must contain a project ID")
    if not uri.path or uri.path == "/":
        raise ValueError("URI format pubsub:// must contain topics/<topic> or subscriptions/<subscription>")
    return uri


def create_asset(*, project_id: str, topic: str, extra: dict | None = None) -> Asset:
    return Asset(uri=f"pubsub://{project_id}/topics/{topic}", extra=extra)


def convert_asset_to_openlineage(asset: Asset, lineage_context) -> OpenLineageDataset:
    """
    Translate Asset with valid AIP-60 uri to OpenLineage.

    URI format: pubsub://project_id/topics/topic_name
    OpenLineage: namespace=pubsub://project_id, name=topics/topic_name
    """
    from airflow.providers.common.compat.openlineage.facet import Dataset as OpenLineageDataset

    parsed = urlsplit(asset.uri)
    return OpenLineageDataset(namespace=f"pubsub://{parsed.netloc}", name=parsed.path.lstrip("/") or "/")
