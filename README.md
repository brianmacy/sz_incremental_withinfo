# sz_incremental_withinfo
Example script to add increment batches of records to Senzing and receive the list of changed entities.

# API demonstrated
## Core
* addRecordWithInfo: Does an add/replace on the record and returns the affected entities
* processRedoRecordWithInfo: Process the internal redo records for things like historical generic correction and returns the affected entities
* getEntityByEntityID: Used to retrieve the changed entities
## Supporting
* init: To initialize the G2Engine object
* stats: To retrieve internal engine diagnostic information as to what is going on in the engine

For more details on the Senzing API go to https://docs.senzing.com


# Overview

This script uses Python futures to parallelize processing.  The G2Engine is thread-safe.

In the first two phases it processes the input file in Senzing JSON format (https://senzing.zendesk.com/hc/en-us/articles/231925448-Generic-Entity-Specification-JSON-CSV-Mapping), writes a temporary file of the affected entities, and then reads that temporary file to get the final state of those entities with each entity represented once.


# Pre-requisites

You will need a Senzing v3 repository, binary installation, and a JSON configururation to it set in your environment (https://senzing.zendesk.com/hc/en-us/articles/360038774134-G2Module-Configuration-and-the-Senzing-API).  This could be done via the Quickstart (https://senzing.zendesk.com/hc/en-us/articles/115002408867-Quickstart-Guide) or by setting up a Docker env (https://hub.docker.com/r/senzing/senzingapi-runtime and https://hub.docker.com/r/senzing/init-postgresql).
