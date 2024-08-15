from dagster import (
    job,
    op,
    file_relative_path,
    config_from_files,
    graph,
    Out,
    GraphOut,
    List,
    DynamicOut,
    DynamicOutput,
    OpExecutionContext
)
# from typing import List

import pandas as pd

from realestate.common import resource_def
from realestate.common.helper_functions import reading_delta_table
from realestate.common.types_realestate import SearchCoordinate, PropertyDataFrame, DeltaCoordinate

from realestate.common.solids_scraping import (
    list_props_domain,
    cache_properties_from_rest_api,
)


@graph(
    description="Downloads full dataset (JSON) from ImmoScout24, cache it, zip it and and upload it to S3",
    # ins={"search_criteria": In(SearchCoordinate)}, #define in function below
    # out={"properties": Out(dagster_type=PropertyDataFrame, is_required=False)},
)
def list_changed_properties(search_criteria: SearchCoordinate):
    # def list_changed_properties():
    return get_changed_or_new_properties(
        properties=list_props_immo24(searchCriteria=search_criteria),
        property_table=property_table(),
    )

@op(
    description="Collects Search Coordinates and spawns dynamically Pipelines downstream.",
    # ins={"search_criterias": In("search_criterias", List[SearchCoordinate])},
    # out={"sarch_coordinates": DynamicOutput(SearchCoordinate)},
    required_resource_keys={"fs_io_manager"},
    out=DynamicOut(io_manager_key="fs_io_manager"),
)
def collect_search_criterias(context: OpExecutionContext, search_criterias: List[SearchCoordinate]):
    for search in search_criterias:
        key = (
            "_".join(
                [
                    search["city"],
                    search["rentOrBuy"],
                    search["propertyType"],
                    str(search["radius"]),
                ]
            )
            .replace("-", "_")
            .lower()
        )

        yield DynamicOutput(
            search,
            mapping_key=key,
        )

@job(
    resource_defs=resource_def["local"],
    config=config_from_files(
        [
            file_relative_path(__file__, "config_environments/local_base.yaml"),
            file_relative_path(__file__, "config_pipelines/scrape_realestate.yaml"),
        ]
    ),
)
def scrape_realestate():
    search_criterias = collect_search_criterias().map(list_changed_properties)

    data_exploration(
        merge_staging_to_delta_table_composite.alias("merge_staging_to_delta_table")(
            collect_properties(search_criterias.collect())
        )
    )
