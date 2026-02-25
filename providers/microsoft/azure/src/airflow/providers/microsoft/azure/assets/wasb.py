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

import re
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from airflow.providers.common.compat.assets import Asset

if TYPE_CHECKING:
    from urllib.parse import SplitResult

    from airflow.providers.common.compat.openlineage.facet import Dataset as OpenLineageDataset


def sanitize_uri(uri: SplitResult) -> SplitResult:
    if not uri.netloc:
        raise ValueError("URI format wasbs:// must contain container@account info")
    if not uri.path:
        raise ValueError("URI format wasbs:// must contain a path")
    return uri


def create_asset(
    *, container: str, account: str, path: str, scheme: str = "wasbs", extra: dict | None = None
) -> Asset:
    return Asset(uri=f"{scheme}://{container}@{account}.blob.core.windows.net{path}", extra=extra)


def convert_asset_to_openlineage(asset: Asset, lineage_context) -> OpenLineageDataset:
    """
    Translate Asset with valid AIP-60 uri to OpenLineage.

    URI format: wasbs://container@account.blob.core.windows.net/path
    OpenLineage: namespace=wasbs://container@account, name=path

    Following the OpenLineage naming spec for Azure Blob Storage.
    """
    from airflow.providers.common.compat.openlineage.facet import Dataset as OpenLineageDataset

    parsed = urlsplit(asset.uri)
    netloc = parsed.netloc
    # Extract account name from netloc: container@account.blob.core.windows.net -> container@account
    match = re.match(r"^(.+@[^.]+)", netloc)
    namespace_authority = match.group(1) if match else netloc
    return OpenLineageDataset(
        namespace=f"wasbs://{namespace_authority}",
        name=parsed.path.lstrip("/") or "/",
    )
