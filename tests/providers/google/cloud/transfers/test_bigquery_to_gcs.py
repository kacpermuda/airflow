#
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

from unittest import mock
from unittest.mock import MagicMock

import pytest
from google.cloud.bigquery.retry import DEFAULT_RETRY
from google.cloud.bigquery.table import Table
from openlineage.client.facet import (
    ColumnLineageDatasetFacet,
    ColumnLineageDatasetFacetFieldsAdditional,
    ColumnLineageDatasetFacetFieldsAdditionalInputFields,
    DocumentationDatasetFacet,
    ExternalQueryRunFacet,
    SchemaDatasetFacet,
    SchemaField,
    SymlinksDatasetFacet,
    SymlinksDatasetFacetIdentifiers,
)
from openlineage.client.run import Dataset

from airflow.exceptions import TaskDeferred
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.triggers.bigquery import BigQueryInsertJobTrigger

TASK_ID = "test-bq-create-table-operator"
TEST_DATASET = "test-dataset"
TEST_TABLE_ID = "test-table-id"
PROJECT_ID = "test-project-id"
JOB_PROJECT_ID = "job-project-id"
TEST_BUCKET = "test-bucket"
TEST_FOLDER = "test-folder"
TEST_OBJECT_NO_WILDCARD = "file.extension"
TEST_OBJECT_WILDCARD = "file_*.extension"
TEST_TABLE_API_REPR = {
    "tableReference": {"projectId": PROJECT_ID, "datasetId": TEST_DATASET, "tableId": TEST_TABLE_ID},
    "description": "Table description.",
    "schema": {
        "fields": [
            {"name": "field1", "type": "STRING", "description": "field1 description"},
            {"name": "field2", "type": "INTEGER"},
        ]
    },
}
TEST_TABLE: Table = Table.from_api_repr(TEST_TABLE_API_REPR)
TEST_EMPTY_TABLE_API_REPR = {
    "tableReference": {"projectId": PROJECT_ID, "datasetId": TEST_DATASET, "tableId": TEST_TABLE_ID}
}
TEST_EMPTY_TABLE: Table = Table.from_api_repr(TEST_EMPTY_TABLE_API_REPR)


class TestBigQueryToGCSOperator:
    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryHook")
    def test_execute(self, mock_hook):
        source_project_dataset_table = f"{PROJECT_ID}:{TEST_DATASET}.{TEST_TABLE_ID}"
        destination_cloud_storage_uris = ["gs://some-bucket/some-file.txt"]
        compression = "NONE"
        export_format = "CSV"
        field_delimiter = ","
        print_header = True
        labels = {"k1": "v1"}
        job_id = "123456"
        hash_ = "hash"
        real_job_id = f"{job_id}_{hash_}"

        expected_configuration = {
            "extract": {
                "sourceTable": {
                    "projectId": "test-project-id",
                    "datasetId": "test-dataset",
                    "tableId": "test-table-id",
                },
                "compression": "NONE",
                "destinationUris": ["gs://some-bucket/some-file.txt"],
                "destinationFormat": "CSV",
                "fieldDelimiter": ",",
                "printHeader": True,
            },
            "labels": {"k1": "v1"},
        }

        mock_hook.return_value.split_tablename.return_value = (PROJECT_ID, TEST_DATASET, TEST_TABLE_ID)
        mock_hook.return_value.generate_job_id.return_value = real_job_id
        mock_hook.return_value.insert_job.return_value = MagicMock(job_id="real_job_id", error_result=False)
        mock_hook.return_value.project_id = JOB_PROJECT_ID

        operator = BigQueryToGCSOperator(
            task_id=TASK_ID,
            source_project_dataset_table=source_project_dataset_table,
            destination_cloud_storage_uris=destination_cloud_storage_uris,
            compression=compression,
            export_format=export_format,
            field_delimiter=field_delimiter,
            print_header=print_header,
            labels=labels,
            project_id=JOB_PROJECT_ID,
        )
        operator.execute(context=mock.MagicMock())

        mock_hook.return_value.insert_job.assert_called_once_with(
            job_id="123456_hash",
            configuration=expected_configuration,
            project_id=JOB_PROJECT_ID,
            location=None,
            timeout=None,
            retry=DEFAULT_RETRY,
            nowait=False,
        )

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryHook")
    def test_execute_deferrable_mode(self, mock_hook):
        source_project_dataset_table = f"{PROJECT_ID}:{TEST_DATASET}.{TEST_TABLE_ID}"
        destination_cloud_storage_uris = ["gs://some-bucket/some-file.txt"]
        compression = "NONE"
        export_format = "CSV"
        field_delimiter = ","
        print_header = True
        labels = {"k1": "v1"}
        job_id = "123456"
        hash_ = "hash"
        real_job_id = f"{job_id}_{hash_}"

        expected_configuration = {
            "extract": {
                "sourceTable": {
                    "projectId": "test-project-id",
                    "datasetId": "test-dataset",
                    "tableId": "test-table-id",
                },
                "compression": "NONE",
                "destinationUris": ["gs://some-bucket/some-file.txt"],
                "destinationFormat": "CSV",
                "fieldDelimiter": ",",
                "printHeader": True,
            },
            "labels": {"k1": "v1"},
        }

        mock_hook.return_value.split_tablename.return_value = (PROJECT_ID, TEST_DATASET, TEST_TABLE_ID)
        mock_hook.return_value.generate_job_id.return_value = real_job_id
        mock_hook.return_value.insert_job.return_value = MagicMock(job_id="real_job_id", error_result=False)
        mock_hook.return_value.project_id = JOB_PROJECT_ID

        operator = BigQueryToGCSOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            source_project_dataset_table=source_project_dataset_table,
            destination_cloud_storage_uris=destination_cloud_storage_uris,
            compression=compression,
            export_format=export_format,
            field_delimiter=field_delimiter,
            print_header=print_header,
            labels=labels,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context=mock.MagicMock())

        assert isinstance(
            exc.value.trigger, BigQueryInsertJobTrigger
        ), "Trigger is not a BigQueryInsertJobTrigger"

        mock_hook.return_value.insert_job.assert_called_once_with(
            configuration=expected_configuration,
            job_id="123456_hash",
            project_id=JOB_PROJECT_ID,
            location=None,
            timeout=None,
            retry=DEFAULT_RETRY,
            nowait=True,
        )

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryHook")
    def test_execute_to_no_wildcard_openlineage(self, mock_hook):
        source_project_dataset_table = f"{PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}"
        destination_cloud_storage_uris = [f"gs://{TEST_BUCKET}/{TEST_FOLDER}/{TEST_OBJECT_NO_WILDCARD}"]
        real_job_id = "123456_hash"
        bq_namespace = "bigquery"

        schema_facet = SchemaDatasetFacet(
            fields=[
                SchemaField(name="field1", type="STRING", description="field1 description"),
                SchemaField(name="field2", type="INTEGER"),
            ]
        )

        expected_input_dataset_facets = {
            "schema": schema_facet,
            "documentation": DocumentationDatasetFacet(description="Table description."),
        }

        expected_output_dataset_facets = {
            "schema": schema_facet,
            "columnLineage": ColumnLineageDatasetFacet(
                fields={
                    "field1": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=bq_namespace,
                                name=source_project_dataset_table,
                                field="field1",
                            )
                        ],
                        transformationType="IDENTITY",
                        transformationDescription="identical",
                    ),
                    "field2": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=bq_namespace,
                                name=source_project_dataset_table,
                                field="field2",
                            )
                        ],
                        transformationType="IDENTITY",
                        transformationDescription="identical",
                    ),
                }
            ),
        }

        mock_hook.return_value.split_tablename.return_value = (PROJECT_ID, TEST_DATASET, TEST_TABLE_ID)
        mock_hook.return_value.generate_job_id.return_value = real_job_id
        mock_hook.return_value.get_client.return_value.get_table.return_value = TEST_TABLE

        operator = BigQueryToGCSOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            source_project_dataset_table=source_project_dataset_table,
            destination_cloud_storage_uris=destination_cloud_storage_uris,
        )

        operator.execute(context=mock.MagicMock())

        lineage = operator.get_openlineage_facets_on_complete(None)
        assert len(lineage.inputs) == 1
        assert len(lineage.outputs) == 1
        assert lineage.inputs[0] == Dataset(
            namespace=bq_namespace,
            name=source_project_dataset_table,
            facets=expected_input_dataset_facets,
        )
        assert lineage.outputs[0] == Dataset(
            namespace=f"gs://{TEST_BUCKET}",
            name=f"{TEST_FOLDER}/{TEST_OBJECT_NO_WILDCARD}",
            facets=expected_output_dataset_facets,
        )
        assert lineage.run_facets == {
            "externalQuery": ExternalQueryRunFacet(externalQueryId=real_job_id, source=bq_namespace)
        }
        assert lineage.job_facets == {}

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryHook")
    def test_execute_empty_table_to_no_wildcard_openlineage(self, mock_hook):
        source_project_dataset_table = f"{PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}"
        destination_cloud_storage_uris = [f"gs://{TEST_BUCKET}/{TEST_FOLDER}/{TEST_OBJECT_NO_WILDCARD}"]
        real_job_id = "123456_hash"
        bq_namespace = "bigquery"

        expected_input_facets = {
            "schema": SchemaDatasetFacet(fields=[]),
            "documentation": DocumentationDatasetFacet(description=""),
        }

        expected_output_facets = {
            "schema": SchemaDatasetFacet(fields=[]),
            "columnLineage": ColumnLineageDatasetFacet(fields={}),
        }

        mock_hook.return_value.split_tablename.return_value = (PROJECT_ID, TEST_DATASET, TEST_TABLE_ID)
        mock_hook.return_value.generate_job_id.return_value = real_job_id
        mock_hook.return_value.get_client.return_value.get_table.return_value = TEST_EMPTY_TABLE

        operator = BigQueryToGCSOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            source_project_dataset_table=source_project_dataset_table,
            destination_cloud_storage_uris=destination_cloud_storage_uris,
        )

        operator.execute(context=mock.MagicMock())

        lineage = operator.get_openlineage_facets_on_complete(None)
        assert len(lineage.inputs) == 1
        assert len(lineage.outputs) == 1
        assert lineage.inputs[0] == Dataset(
            namespace=bq_namespace, name=source_project_dataset_table, facets=expected_input_facets
        )
        assert lineage.outputs[0] == Dataset(
            namespace=f"gs://{TEST_BUCKET}",
            name=f"{TEST_FOLDER}/{TEST_OBJECT_NO_WILDCARD}",
            facets=expected_output_facets,
        )
        assert lineage.run_facets == {
            "externalQuery": ExternalQueryRunFacet(externalQueryId=real_job_id, source=bq_namespace)
        }
        assert lineage.job_facets == {}

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryHook")
    def test_execute_to_wildcard_openlineage(self, mock_hook):
        source_project_dataset_table = f"{PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}"
        destination_cloud_storage_uris = [f"gs://{TEST_BUCKET}/{TEST_FOLDER}/{TEST_OBJECT_WILDCARD}"]
        real_job_id = "123456_hash"
        bq_namespace = "bigquery"

        schema_facet = SchemaDatasetFacet(
            fields=[
                SchemaField(name="field1", type="STRING", description="field1 description"),
                SchemaField(name="field2", type="INTEGER"),
            ]
        )
        expected_input_facets = {
            "schema": schema_facet,
            "documentation": DocumentationDatasetFacet(description="Table description."),
        }

        expected_output_facets = {
            "schema": schema_facet,
            "columnLineage": ColumnLineageDatasetFacet(
                fields={
                    "field1": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=bq_namespace, name=source_project_dataset_table, field="field1"
                            )
                        ],
                        transformationType="IDENTITY",
                        transformationDescription="identical",
                    ),
                    "field2": ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace=bq_namespace, name=source_project_dataset_table, field="field2"
                            )
                        ],
                        transformationType="IDENTITY",
                        transformationDescription="identical",
                    ),
                }
            ),
            "symlink": SymlinksDatasetFacet(
                identifiers=[
                    SymlinksDatasetFacetIdentifiers(
                        namespace=f"gs://{TEST_BUCKET}",
                        name=f"{TEST_FOLDER}/{TEST_OBJECT_WILDCARD}",
                        type="file",
                    )
                ]
            ),
        }

        mock_hook.return_value.split_tablename.return_value = (PROJECT_ID, TEST_DATASET, TEST_TABLE_ID)
        mock_hook.return_value.generate_job_id.return_value = real_job_id
        mock_hook.return_value.get_client.return_value.get_table.return_value = TEST_TABLE

        operator = BigQueryToGCSOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            source_project_dataset_table=source_project_dataset_table,
            destination_cloud_storage_uris=destination_cloud_storage_uris,
        )

        operator.execute(context=mock.MagicMock())

        lineage = operator.get_openlineage_facets_on_complete(None)
        assert len(lineage.inputs) == 1
        assert len(lineage.outputs) == 1
        assert lineage.inputs[0] == Dataset(
            namespace=bq_namespace, name=source_project_dataset_table, facets=expected_input_facets
        )
        assert lineage.outputs[0] == Dataset(
            namespace=f"gs://{TEST_BUCKET}", name=TEST_FOLDER, facets=expected_output_facets
        )
        assert lineage.run_facets == {
            "externalQuery": ExternalQueryRunFacet(externalQueryId=real_job_id, source=bq_namespace)
        }
        assert lineage.job_facets == {}

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryHook")
    def test_execute_empty_table_to_wildcard_openlineage(self, mock_hook):
        source_project_dataset_table = f"{PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}"
        destination_cloud_storage_uris = [f"gs://{TEST_BUCKET}/{TEST_FOLDER}/{TEST_OBJECT_WILDCARD}"]
        real_job_id = "123456_hash"
        bq_namespace = "bigquery"

        expected_input_facets = {
            "schema": SchemaDatasetFacet(fields=[]),
            "documentation": DocumentationDatasetFacet(description=""),
        }

        expected_output_facets = {
            "schema": SchemaDatasetFacet(fields=[]),
            "columnLineage": ColumnLineageDatasetFacet(fields={}),
            "symlink": SymlinksDatasetFacet(
                identifiers=[
                    SymlinksDatasetFacetIdentifiers(
                        namespace=f"gs://{TEST_BUCKET}",
                        name=f"{TEST_FOLDER}/{TEST_OBJECT_WILDCARD}",
                        type="file",
                    )
                ]
            ),
        }

        mock_hook.return_value.split_tablename.return_value = (PROJECT_ID, TEST_DATASET, TEST_TABLE_ID)
        mock_hook.return_value.generate_job_id.return_value = real_job_id
        mock_hook.return_value.get_client.return_value.get_table.return_value = TEST_EMPTY_TABLE

        operator = BigQueryToGCSOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            source_project_dataset_table=source_project_dataset_table,
            destination_cloud_storage_uris=destination_cloud_storage_uris,
        )

        operator.execute(context=mock.MagicMock())

        lineage = operator.get_openlineage_facets_on_complete(None)
        assert len(lineage.inputs) == 1
        assert len(lineage.outputs) == 1
        assert lineage.inputs[0] == Dataset(
            namespace=bq_namespace, name=source_project_dataset_table, facets=expected_input_facets
        )
        assert lineage.outputs[0] == Dataset(
            namespace=f"gs://{TEST_BUCKET}", name=TEST_FOLDER, facets=expected_output_facets
        )
        assert lineage.run_facets == {
            "externalQuery": ExternalQueryRunFacet(externalQueryId=real_job_id, source=bq_namespace)
        }
        assert lineage.job_facets == {}
