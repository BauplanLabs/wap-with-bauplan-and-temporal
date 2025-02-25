"""

Running a workflow (minimal example). Code re-adapted from:

https://learn.temporal.io/getting_started/python/hello_world_in_python/

"""

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
import bauplan
from activities import MyActivities
from temporalio.worker import Worker
from temporalio.client import Client
from workflows import WapWithBauplan
import os
from dotenv import load_dotenv
# load environment variables
load_dotenv()
# make sure the necessary environment variables are set, for example TABLE_NAME
assert "TABLE_NAME" in os.environ and os.environ["TABLE_NAME"], "TABLE_NAME must be set"


async def main():
    client = await Client.connect("localhost:7233")
    bpln_client = bauplan.Client()
    my_activities = MyActivities(
        bpl_client=bpln_client,
        table_name=os.getenv("TABLE_NAME"),
        branch_name=os.getenv("BRANCH_NAME"),
        s3_path=os.getenv("S3_PATH"),
        namespace=os.getenv("NAMESPACE")
    )
        
    # Run a worker for the workflow. Pattern from temporal samples:
    # https://github.com/temporalio/samples-python/blob/main/hello/hello_activity_threaded.py
    async with Worker(
        client,
        task_queue="hello-bauplan-queue",
        workflows=[WapWithBauplan],
        activities=[
            my_activities.create_branch_if_not_exists,
            my_activities.create_namespace_if_not_exists,
            my_activities.create_or_replace_table,
            my_activities.import_data_to_iceberg,
            my_activities.run_quality_checks,
            my_activities.merge_branch,
            my_activities.delete_branch
        ],
        activity_executor=ThreadPoolExecutor(5),
    ):

        # Note, in many production setups, the client
        # would be in a completely separate process from the worker.
        result = await client.execute_workflow(
            WapWithBauplan.run,
            id="hello-bauplan-wap",
            task_queue="hello-bauplan-queue"
        )
        print(f"Result on thread {threading.get_ident()}: {result}")


if __name__ == "__main__":
    asyncio.run(main())