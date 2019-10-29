#!/usr/bin/env python3

import singer
from singer import utils
from singer.catalog import write_catalog

from .client import Client
from .discover import discover
from .sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date", "email", "password", "user_key"]


@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    client = Client(args.config)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        LOGGER.info("Starting discovery mode")
        catalog = discover(client)
        write_catalog(catalog)
    # Otherwise run in sync mode
    else:
        LOGGER.info("Starting sync mode")
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover(client)

        sync(client, args.config, args.state, catalog)


if __name__ == "__main__":
    main()
