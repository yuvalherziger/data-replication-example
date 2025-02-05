# Replicate data to multiple clusters

This Python example opens a change stream to replicate data from one
source database in a source cluster to multiple target clusters.

I built it with Poetry package manager, but it can be run in plain Python
environments or virtual environments with the following constraints:

- Python >=3.9
- [motor](https://pypi.org/project/motor/): MongoDB's official
  driver for async Python. You can also install it with pip (i.e., `pip install motor==3.7.0`)

## Run it locally

1. First, start the listener's code with all the proper configurations.
   For example, you're reading from global cluster `mongodb://localhost:1000/?directConnection=true`
   and publishing changes to three different clusters (ports, 2000, 3000, and 4000),
   your command could look like the following one:

    ```shell
    SOURCE_MONGODB_URI="mongodb://localhost:1000/?directConnection=true" \
    TARGET_MONGODB_URI_0="mongodb://localhost:2000/?directConnection=true" \
    TARGET_MONGODB_URI_1="mongodb://localhost:3000/?directConnection=true" \
    TARGET_MONGODB_URI_2="mongodb://localhost:4000/?directConnection=true" \
    SOURCE_DB="globalData" \
    TARGET_DB="data" \
    poetry run python -m run
    ```

2. In a separate terminal session, you can use [`mongosh`](https://www.mongodb.com/docs/mongodb-shell/) to insert some mock data. For example:

   ```shell
   mongosh --port 1000 --quiet \
     --eval "db.getSiblingDB('globalData').cves.insertOne({ cve: 'CVE-2024-0000' })"
   ```

3. Then, verify that the change propagated to the target clusters:

   ```shell
   mongosh --port 2000 --quiet \
     --eval "db.getSiblingDB('data').cves.find({ cve: 'CVE-2024-0000' })"
   ```
