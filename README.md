# Replicate data to multiple clusters

This Python example opens a change stream to replicate data from one
source database in a source cluster to multiple target clusters.

Example run:

```shell
SOURCE_MONGODB_URI="mongodb://localhost:1000/?directConnection=true" \
TARGET_MONGODB_URI_0="mongodb://localhost:2000/?directConnection=true" \
TARGET_MONGODB_URI_1="mongodb://localhost:3000/?directConnection=true" \
TARGET_MONGODB_URI_2="mongodb://localhost:4000/?directConnection=true" \
SOURCE_DB="globalData" \
TARGET_DB="data" \
poetry run python -m run
```