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

from airflow.providers.microsoft.azure.assets.asb import (
    convert_asset_to_openlineage,
    create_asset,
    sanitize_uri,
)


class TestSanitizeUri:
    def test_valid_uri(self):
        result = sanitize_uri(urlsplit("azservicebus://mynamespace.servicebus.windows.net/my-queue"))
        assert result.scheme == "azservicebus"
        assert result.netloc == "mynamespace.servicebus.windows.net"

    def test_missing_namespace(self):
        with pytest.raises(ValueError, match="must contain a namespace"):
            sanitize_uri(urlsplit("azservicebus:///my-queue"))

    def test_missing_queue(self):
        with pytest.raises(ValueError, match="must contain a queue or topic"):
            sanitize_uri(urlsplit("azservicebus://mynamespace.servicebus.windows.net"))


class TestCreateAsset:
    def test_basic(self):
        asset = create_asset(namespace="mynamespace.servicebus.windows.net", queue_or_topic="my-queue")
        assert asset.uri == "azservicebus://mynamespace.servicebus.windows.net/my-queue"


class TestConvertAssetToOpenlineage:
    def test_basic(self):
        asset = create_asset(namespace="mynamespace.servicebus.windows.net", queue_or_topic="my-queue")
        result = convert_asset_to_openlineage(asset, None)
        assert result.namespace == "azservicebus://mynamespace.servicebus.windows.net"
        assert result.name == "my-queue"
