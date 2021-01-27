#!/usr/bin/env python3

import singer
from singer import utils
from singer.catalog import write_catalog

from .client import Client
from .sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date", "email", "password", "user_key"]


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    client = Client(**args.config)

    LOGGER.info("Starting sync mode")
    sync(client, args.config, args.state)


if __name__ == "__main__":
    main()
