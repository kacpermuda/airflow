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
"""This module contains code related to OpenLineage and lineage extraction."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from openlineage.client.facet import (
    ColumnLineageDatasetFacet,
    ColumnLineageDatasetFacetFieldsAdditional,
    ColumnLineageDatasetFacetFieldsAdditionalInputFields,
    DocumentationDatasetFacet,
    SchemaDatasetFacet,
    SchemaField,
)

if TYPE_CHECKING:
    from google.cloud.bigquery.table import Table


def get_facets_from_bq_table(table: Table) -> dict[Any, Any]:
    """
    Get all possible facets from BigQuery table object.

    :param table: BigQuery table to extract facets from
    :return: Dataset facets for BigQuery table
    """
    facets = {
        "schema": SchemaDatasetFacet(
            fields=[
                SchemaField(name=field.name, type=field.field_type, description=field.description)
                for field in table.schema
            ]
        ),
        "documentation": DocumentationDatasetFacet(description=table.description or ""),
    }

    return facets


def get_identity_column_lineage_facet_from_bq_table(
    table: Table, selected_fields: list[str] | None = None
) -> ColumnLineageDatasetFacet:
    """
    Get column lineage facet from BigQuery table object.

    Simple lineage will be created, where each source column corresponds to single destination column,
     and there are no transformations.

    This is useful when performing extract jobs, where no transformations are being made.

    :param table: BigQuery table to extract facets from
    :param selected_fields: Fields to be included in facet.
     If not provided, all fields from table schema are used.
    :return: Column lineage facet
    """
    schema = table.schema

    if selected_fields:
        schema = [field for field in schema if field.name in selected_fields]

    column_lineage_facet = ColumnLineageDatasetFacet(
        fields={
            field.name: ColumnLineageDatasetFacetFieldsAdditional(
                inputFields=[
                    ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                        namespace="bigquery", name=str(table.reference), field=field.name
                    )
                ],
                transformationType="IDENTITY",
                transformationDescription="identical",
            )
            for field in schema
        }
    )
    return column_lineage_facet
