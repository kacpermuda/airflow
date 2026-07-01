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

from constants import API_BASE_URL, DAGS_FOLDER, console
from harness import OpenLineageE2ERunner, discover_expected_dag_ids


def test_all_openlineage_dags_succeed(auth_headers):
    """
    Trigger every OpenLineage system-test DAG in the deployment and require each run to succeed.

    The terminal ``OpenLineageTestOperator`` task inside each DAG validates the emitted OpenLineage
    events against the expected templates, so a ``success`` run state means the lineage matched.
    """
    expected_dag_ids = discover_expected_dag_ids(DAGS_FOLDER)
    assert expected_dag_ids, "No expected OpenLineage DAG ids were discovered from the prepared dags"

    runner = OpenLineageE2ERunner(API_BASE_URL, auth_headers)
    statuses = runner.run(expected_dag_ids)

    console.print(f"[blue]OpenLineage e2e results ({len(statuses)} DAGs): {statuses}")
    failed = {dag_id: state for dag_id, state in statuses.items() if state != "success"}
    assert not failed, f"OpenLineage e2e DAG runs were not successful: {failed}"
