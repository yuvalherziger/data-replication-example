import asyncio
import logging
import os
import signal
from typing import List, Optional
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

SOURCE_MONGODB_URI = os.getenv("SOURCE_MONGODB_URI")

SOURCE_DB = os.getenv("SOURCE_DB", "globalData")
TARGET_DB = os.getenv("TARGET_DB", "data")
TARGET_CLUSTERS_URIS = [
    value for key, value in os.environ.items() if key.startswith("TARGET_MONGODB_URI_")
]


class ChangeStreamHandler:
    def __init__(self, source_uri: str, target_uris: List[str]):
        self.source_uri: str = source_uri
        self.target_uris: List[str] = target_uris
        self.source_client: Optional[AsyncIOMotorClient] = None
        self.target_clients: List[Optional[AsyncIOMotorClient]] = []
        self.change_stream = None
        self.should_stop = False

    async def connect(self):
        try:
            self.source_client = AsyncIOMotorClient(self.source_uri)
            for i, target_uri in enumerate(self.target_uris):
                self.target_clients.append(AsyncIOMotorClient(target_uri))
        except Exception:
            logger.exception(f"Failed to connect to MongoDB clusters")
            raise

    async def write_to_target(self, change, resume_token):
        try:
            coll_name = change.get("ns", {}).get("coll")
            operation_type = change.get("operationType")
            _id = change.get("documentKey", {}).get("_id")
            update_desc = change.get("updateDescription", {})

            if operation_type == "insert":
                full_doc = change.get("fullDocument")
                # Parallel insert ops to all target clusters:
                await asyncio.gather(
                    *[
                        c[TARGET_DB][coll_name].insert_one(full_doc)
                        for c in self.target_clients
                    ]
                )

            elif operation_type == "update":
                logger.info(change)
                update_doc = {}

                if update_desc.get("updatedFields"):
                    update_doc["$set"] = update_desc["updatedFields"]

                if update_desc.get("removedFields"):
                    update_doc["$unset"] = {
                        field: "" for field in update_desc.get("removedFields")
                    }
                # Parallel update ops to all target clusters:
                await asyncio.gather(
                    *[
                        c[TARGET_DB][coll_name].update_one({"_id": _id}, update_doc)
                        for c in self.target_clients
                    ]
                )
            elif operation_type == "delete":
                # Parallel delete ops to all target clusters:
                await asyncio.gather(
                    *[
                        c[TARGET_DB][coll_name].delete_one({"_id": _id})
                        for c in self.target_clients
                    ]
                )
            # Upsert the resume token to keep a pointer to the last handled event:
            await self.source_client["globalDataReplication"]["status"].update_one(
                {"_id": "resumeToken"}, {"$set": {"token": resume_token}},
                upsert=True
            )

            logger.info(f"Successfully processed {operation_type} operation")
        except Exception as e:
            logger.error(f"Error writing to target collection: {e}")

    async def watch_changes(self):
        try:
            # Read the last resume token in case the process broke unexpectedly:
            last_resume_token_doc = await self.source_client["globalDataReplication"]["status"].find_one({"_id": "resumeToken"})
            last_resume_token = last_resume_token_doc.get("token") if last_resume_token_doc else None
            if last_resume_token:
                self.change_stream = self.source_client[SOURCE_DB].watch([], resume_after=last_resume_token)
            else:
                self.change_stream = self.source_client[SOURCE_DB].watch([])

            logger.info(f"Starting to watch changes on {SOURCE_DB}")

            async with self.change_stream as stream:
                async for change in stream:
                    if self.should_stop:
                        break
                    await self.write_to_target(change, stream.resume_token)

        except Exception as e:
            logger.error(f"Error in change stream: {e}")
            raise

    async def cleanup(self):
        self.should_stop = True

        if self.change_stream:
            await self.change_stream.close()

        if self.source_client:
            self.source_client.close()

        if len(self.target_clients):
            for c in self.target_clients:
                c.close()

        logger.info("Cleanup completed")


async def main():
    handler = ChangeStreamHandler(
        source_uri=SOURCE_MONGODB_URI, target_uris=TARGET_CLUSTERS_URIS
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(handler.cleanup()))

    try:
        await handler.connect()
        await handler.watch_changes()
    except Exception as e:
        logger.error(f"Error in main function: {e}")
    finally:
        await handler.cleanup()


if __name__ == "__main__":
    if not SOURCE_MONGODB_URI or not TARGET_CLUSTERS_URIS:
        logger.error("MongoDB connection strings not found in env vars")
        exit(1)
    asyncio.run(main())
