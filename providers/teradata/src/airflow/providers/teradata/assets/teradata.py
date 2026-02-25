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
        raise ValueError("URI format teradata:// must contain a host")
    if not uri.path or uri.path == "/":
        raise ValueError("URI format teradata:// must contain a database and table")
    return uri


def create_asset(
    *, host: str, port: int = 1025, database: str, table: str, extra: dict | None = None
) -> Asset:
    return Asset(uri=f"teradata://{host}:{port}/{database}/{table}", extra=extra)


def convert_asset_to_openlineage(asset: Asset, lineage_context) -> OpenLineageDataset:
    from airflow.providers.common.compat.openlineage.facet import Dataset as OpenLineageDataset

    parsed = urlsplit(asset.uri)
    authority = parsed.netloc
    path_parts = parsed.path.strip("/").split("/")
    database = path_parts[0] if len(path_parts) > 0 else ""
    table = path_parts[1] if len(path_parts) > 1 else ""
    return OpenLineageDataset(namespace=f"teradata://{authority}", name=f"{database}.{table}")
