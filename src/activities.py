"""

Temporal activities for the WAP-with-Bauplan workflow.

Each activity is a step in the workflow, and is defined as a Python function.
Note that each function is just standard Python code leveraging the bauplan SDK,
with a decorator to make it an activity.

"""
from temporalio import activity


class MyActivities:
    
    def __init__(self, bpl_client, table_name, branch_name, s3_path, namespace):
        self.bauplan_client = bpl_client
        self.table_name = table_name
        self.branch_name = branch_name
        self.s3_path = s3_path
        self.namespace = namespace
        
        return
    
    @activity.defn
    def import_data_to_iceberg(self):
        """
        
        Wrap the table creation and upload process in Bauplan.
        
        """
        # instantiate the Bauplan client
        
        is_imported = self.bauplan_client.import_data(
            table=self.table_name,
            search_uri=self.s3_path,
            namespace=self.namespace,
            branch=self.branch_name
        )

        return False if is_imported.error else True

    @activity.defn
    def run_quality_checks(self):
        """
        
        We check the data quality by running the checks in-process: we use 
        Bauplan SDK to query the data as an Arrow table, and check if the 
        target column is not null through vectorized PyArrow operations.
        
        """
        # we retrieve the data and check if the table is column has any nulls
        # make sure the column you're checking is in the table, so change this appropriately
        # if you're using a different dataset
        column_to_check = 'passenger_count'
        # NOTE if you don't want to use any SQL, you can interact with the lakehouse in pure Python
        # and still back an Arrow table (in this one column) through a performant scan.
        wap_table = self.bauplan_client.scan(
            table=self.table_name,
            ref=self.branch_name,
            namespace=self.namespace,
            columns=[column_to_check]
        )
        assert wap_table[column_to_check].null_count > 0, "Quality check failed"
        print("Quality check passed")
        
        return True

    @activity.defn
    def merge_branch(self):
        """
        
        We merge the ingestion branch into the main branch.
        
        """
       # we merge the branch into the main branch
        return self.bauplan_client.merge_branch(
            source_ref=self.branch_name,
            into_branch='main'
        )

    @activity.defn
    def delete_branch(self):
        """
        
        We delete the branch to avoid clutter!
        
        """
        return self.bauplan_client.delete_branch(self.branch_name)

    @activity.defn
    def create_branch_if_not_exists(self):
        """
        
        We create the branch if it doesn't exist.
        
        """
        if not self.bauplan_client.has_branch(self.branch_name):
            # create the branch from main HEAD
            self.bauplan_client.create_branch(self.branch_name, from_ref='main')
        # return if the branch is there (and learn a new API method ;-))
        return self.bauplan_client.has_branch(self.branch_name)

    @activity.defn
    def create_namespace_if_not_exists(self):
        """
        
        We create the namespace if it doesn't exist.
        
        """
        # create namespace if it doesn't exist
        if not self.bauplan_client.has_namespace(self.namespace, ref=self.branch_name):
            self.bauplan_client.create_namespace(self.namespace, branch=self.branch_name)
        # return if the namespace is there (and learn a new API method ;-))
        return self.bauplan_client.has_namespace(self.namespace, ref=self.branch_name)

    @activity.defn
    def create_or_replace_table(self):
        """
        
        We create the table / replace it if it exists.
        
        """
        # now we create the table in the branch
        self.bauplan_client.create_table(
            table=self.table_name,
            search_uri=self.s3_path,
            namespace=self.namespace,
            branch=self.branch_name,
            # just in case the test table is already there for other reasons
            replace=True
        )
        # we check if the table is there (and learn a new API method ;-))
        fq_name = f"{self.namespace}.{self.table_name}"
        return self.bauplan_client.has_table(table=fq_name, ref=self.branch_name)
