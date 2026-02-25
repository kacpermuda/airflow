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

from airflow.providers.google.assets.pubsub import convert_asset_to_openlineage, create_asset, sanitize_uri


class TestSanitizeUri:
    def test_valid_uri(self):
        result = sanitize_uri(urlsplit("pubsub://my-project/topics/my-topic"))
        assert result.scheme == "pubsub"
        assert result.netloc == "my-project"

    def test_missing_project(self):
        with pytest.raises(ValueError, match="must contain a project ID"):
            sanitize_uri(urlsplit("pubsub:///topics/my-topic"))

    def test_missing_path(self):
        with pytest.raises(ValueError, match="must contain topics"):
            sanitize_uri(urlsplit("pubsub://my-project"))


class TestCreateAsset:
    def test_basic(self):
        asset = create_asset(project_id="my-project", topic="my-topic")
        assert asset.uri == "pubsub://my-project/topics/my-topic"


class TestConvertAssetToOpenlineage:
    def test_topic(self):
        asset = create_asset(project_id="my-project", topic="my-topic")
        result = convert_asset_to_openlineage(asset, None)
        assert result.namespace == "pubsub://my-project"
        assert result.name == "topics/my-topic"
