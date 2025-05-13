#!/usr/bin/env python3
import json
import os
import time

import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

import tap_ukg.streams

REQUIRED_CONFIG_KEYS = ["api_key", "username", "password", "company"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """Load schemas from schemas folder"""
    schemas = {}
    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = []
        # key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=None,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method="FULL_TABLE",
            )
        )
    return Catalog(streams)


def sync(config, state, catalog):
    """Sync data from tap source"""
    # Get auth token
    auth_token = tap_ukg.streams.api.get_auth_token(
        config["api_key"],
        config["username"],
        config["password"],
        config["company"],
    )

    # Loop over selected streams in catalog
    for stream in catalog.streams:
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )
        getattr(tap_ukg.streams, stream.tap_stream_id).stream(
            company=config["company"],
            token=auth_token,
        )

    return


def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()