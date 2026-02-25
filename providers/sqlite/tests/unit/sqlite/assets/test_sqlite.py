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

from airflow.providers.sqlite.assets.sqlite import convert_asset_to_openlineage, create_asset, sanitize_uri


class TestSanitizeUri:
    def test_valid_uri(self):
        result = sanitize_uri(urlsplit("sqlite:///path/to/db/mytable"))
        assert result.scheme == "sqlite"

    def test_missing_path(self):
        with pytest.raises(ValueError, match="must contain a database path"):
            sanitize_uri(urlsplit("sqlite://"))


class TestCreateAsset:
    def test_basic(self):
        asset = create_asset(path="path/to/my.db", table="mytable")
        assert asset.uri == "sqlite:///path/to/my.db/mytable"

    def test_absolute_path(self):
        asset = create_asset(path="/var/data/my.db", table="users")
        assert asset.uri == "sqlite:////var/data/my.db/users"


class TestConvertAssetToOpenlineage:
    def test_basic(self):
        asset = create_asset(path="path/to/my.db", table="mytable")
        result = convert_asset_to_openlineage(asset, None)
        assert result.namespace == "sqlite:/path/to/my.db"
        assert result.name == "mytable"
