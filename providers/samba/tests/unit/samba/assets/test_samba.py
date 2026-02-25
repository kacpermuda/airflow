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

from airflow.providers.samba.assets.samba import (
    convert_asset_to_openlineage,
    create_asset,
    sanitize_uri,
)


class TestSanitizeUri:
    def test_valid_uri(self):
        uri = urlsplit("smb://myhost/share/path/to/file")
        result = sanitize_uri(uri)
        assert result.scheme == "smb"
        assert result.netloc == "myhost"
        assert result.path == "/share/path/to/file"

    def test_valid_uri_with_port(self):
        uri = urlsplit("smb://myhost:4455/share/path")
        result = sanitize_uri(uri)
        assert result.netloc == "myhost:4455"

    def test_missing_host(self):
        uri = urlsplit("smb:///share/path")
        with pytest.raises(ValueError, match="must contain a host"):
            sanitize_uri(uri)

    def test_missing_path(self):
        uri = urlsplit("smb://myhost")
        with pytest.raises(ValueError, match="must contain a share and path"):
            sanitize_uri(uri)

    def test_root_only_path(self):
        uri = urlsplit("smb://myhost/")
        with pytest.raises(ValueError, match="must contain a share and path"):
            sanitize_uri(uri)


class TestCreateAsset:
    def test_basic(self):
        asset = create_asset(host="myhost", share="myshare", path="/path/to/file")
        assert asset.uri == "smb://myhost/myshare/path/to/file"

    def test_default_port_omitted(self):
        asset = create_asset(host="myhost", share="myshare", path="/file.txt")
        assert asset.uri == "smb://myhost/myshare/file.txt"

    def test_custom_port(self):
        asset = create_asset(host="myhost", share="myshare", path="/file.txt", port=4455)
        assert asset.uri == "smb://myhost:4455/myshare/file.txt"

    def test_default_path(self):
        asset = create_asset(host="myhost", share="docs")
        assert asset.uri == "smb://myhost/docs/"

    def test_with_extra(self):
        asset = create_asset(host="h", share="s", path="/f", extra={"share_type": "windows"})
        assert asset.uri == "smb://h/s/f"
        assert asset.extra == {"share_type": "windows"}


class TestConvertAssetToOpenlineage:
    def test_basic(self):
        asset = create_asset(host="myhost", share="myshare", path="/path/to/file")
        result = convert_asset_to_openlineage(asset, None)
        assert result.namespace == "smb://myhost"
        assert result.name == "myshare/path/to/file"

    def test_with_port(self):
        asset = create_asset(host="myhost", share="myshare", path="/file.txt", port=4455)
        result = convert_asset_to_openlineage(asset, None)
        assert result.namespace == "smb://myhost:4455"
        assert result.name == "myshare/file.txt"
