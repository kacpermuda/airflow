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

from urllib.parse import urlsplit

import pytest

from airflow.providers.exasol.assets.exasol import convert_asset_to_openlineage, create_asset, sanitize_uri


class TestSanitizeUri:
    def test_valid_uri(self):
        result = sanitize_uri(urlsplit("exasol://host:8563/my_schema/my_table"))
        assert result.scheme == "exasol"

    def test_missing_host(self):
        with pytest.raises(ValueError, match="must contain a host"):
            sanitize_uri(urlsplit("exasol:///schema/table"))

    def test_missing_path(self):
        with pytest.raises(ValueError, match="must contain a schema and table"):
            sanitize_uri(urlsplit("exasol://host:8563"))


class TestCreateAsset:
    def test_basic(self):
        asset = create_asset(host="myhost", schema="my_schema", table="my_table")
        assert asset.uri == "exasol://myhost:8563/my_schema/my_table"

    def test_custom_port(self):
        asset = create_asset(host="myhost", port=9999, schema="s", table="t")
        assert asset.uri == "exasol://myhost:9999/s/t"


class TestConvertAssetToOpenlineage:
    def test_basic(self):
        asset = create_asset(host="myhost", schema="my_schema", table="my_table")
        result = convert_asset_to_openlineage(asset, None)
        assert result.namespace == "exasol://myhost:8563"
        assert result.name == "my_schema.my_table"
