"""

Defining the workflow corresponding to the WAP with Bauplan:

* create a branch in Bauplan and import data
* run quality checks
* merge the branch if quality checks pass
* delete the branch to clean up

Each step is an activity defined in `activities.py`: the workflow just defines the
structure of the process, not the business logic.

"""
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from datetime import timedelta
    from activities import MyActivities
    
    
@workflow.defn()
class WapWithBauplan:
    
    @workflow.run
    async def run(self) -> str:
        # say hello
        workflow.logger.info("Running WAP with Bauplan workflow")
        
        workflow.logger.info("\n====> Step 1: Creating a branch and namespace")
        await workflow.execute_activity(
            MyActivities.create_branch_if_not_exists, start_to_close_timeout=timedelta(seconds=120)
        )
        
        await workflow.execute_activity(
            MyActivities.create_namespace_if_not_exists, start_to_close_timeout=timedelta(seconds=120)
        )
        
        workflow.logger.info("\n====> Step 2: Creating the table and importing data")
        await workflow.execute_activity(
            MyActivities.create_or_replace_table, start_to_close_timeout=timedelta(seconds=120)
        )
        
        await workflow.execute_activity(
            MyActivities.import_data_to_iceberg, start_to_close_timeout=timedelta(seconds=120)
        )

        workflow.logger.info("\n====> Step 3: Running quality checks")
        await workflow.execute_activity(
            MyActivities.run_quality_checks, start_to_close_timeout=timedelta(seconds=120)
        )
        
        workflow.logger.info("\n====> Step 4: Merging the branch")
        await workflow.execute_activity(
            MyActivities.merge_branch, start_to_close_timeout=timedelta(seconds=120)
        )
        
        workflow.logger.info("\n====> Step 5: Deleting the branch")
        await workflow.execute_activity(
            MyActivities.delete_branch, start_to_close_timeout=timedelta(seconds=120)
        )

        # say goodbye
        workflow.logger.info("Workflow completed!")
    
        return "See you, Space Cowboy!"