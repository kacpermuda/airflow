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

from airflow.providers.apache.kafka.assets.kafka import (
    convert_asset_to_openlineage,
    create_asset,
    sanitize_uri,
)


class TestSanitizeUri:
    def test_valid_uri(self):
        result = sanitize_uri(urlsplit("kafka://broker1:9092/my-topic"))
        assert result.scheme == "kafka"
        assert result.netloc == "broker1:9092"

    def test_missing_host(self):
        with pytest.raises(ValueError, match="must contain a bootstrap server"):
            sanitize_uri(urlsplit("kafka:///my-topic"))

    def test_missing_topic(self):
        with pytest.raises(ValueError, match="must contain a topic name"):
            sanitize_uri(urlsplit("kafka://broker1:9092"))


class TestCreateAsset:
    def test_basic(self):
        asset = create_asset(server="broker1:9092", topic="my-topic")
        assert asset.uri == "kafka://broker1:9092/my-topic"


class TestConvertAssetToOpenlineage:
    def test_basic(self):
        asset = create_asset(server="broker1:9092", topic="my-topic")
        result = convert_asset_to_openlineage(asset, None)
        assert result.namespace == "kafka://broker1:9092"
        assert result.name == "my-topic"
