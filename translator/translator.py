import random

from workflow import *


def translate_wf_to_faasr_gh(
    workflow,
    compute_server,
    data_store="My_Minio_Bucket",
    data_endpoint="https://play.min.io",
    bucket="faasr",
    region="us-east-1",
    writable="TRUE",
    files_folder="synthetic_files",
    funtion_gitrepos={"synthetic_faas_function": "nolcut/FaaSr-synthetic"},
    python_percentage=0,
    r_container=None,
    python_container=None,
):
    """
    Converts a WfFormatWorkflow object into a SyntheticFaaSrWorkflow object for FaaSr.

    Args:
        workflow (WfFormatWorkflow): The input workflow object.
        compute_server (ComputeServer): FaaS compute server.
        data_store (str): Name of data store.
        data_endpoint (str): S3 endpoint.
        bucket (str): Name of S3 bucket.
        region (str): Region for S3 data_store.
        writable (str): Specifies if S3 bucket is writable.
        files_folder (str): Name of folder in S3 containing workflow files.
        funtion_gitrepos (dict): Mapping of function names to their GitHub repos.
        python_percentage (float): Percentage of actions that should be Python (0-100).
        r_container (str): Container image for R actions.
        python_container (str): Container image for Python actions.

    Returns:
        SyntheticFaaSrWorkflow: The translated workflow object.
    """
    function_list = []
    entry_functions = []

    # Convert each task in the workflow to a SyntheticFaaSrAction
    for t in workflow.tasks:
        # Randomly assign action type based on python_percentage
        action_type = "Python" if random.uniform(0, 100) < python_percentage else "R"
        # Select container based on type
        action_container = python_container if action_type == "Python" else r_container

        function = SyntheticFaaSrAction(
            compute_server=compute_server,
            execution_time=t.runtime,
            name=t.id,
            action_container=action_container,
            input_files=t.input_files,
            output_files=t.output_files,
            invoke_next=t.children,
            type=action_type,
        )
        function_list.append(function)

        # Identify entry functions (no parents)
        if len(t.parents) == 0:
            entry_functions.append(function)

    # Handle entry function(s)
    if len(entry_functions) == 0:
        print("No entry function")
        quit()
    elif len(entry_functions) > 1:
        # If multiple entry nodes, create a synthetic start node that invokes all entry nodes
        rand_num = random.getrandbits(32)
        entry_function_names = [action.name for action in entry_functions]
        # Decide type of entry function based on python_percentage
        entry_type = "Python" if random.uniform(0, 100) < python_percentage else "R"
        entry_container = python_container if entry_type == "Python" else r_container
        entry = SyntheticFaaSrAction(
            compute_server=compute_server,
            execution_time=0,
            name=f"start{rand_num}",
            action_container=entry_container,
            invoke_next=entry_function_names,
            type=entry_type,
        )
        function_list.append(entry)
    else:
        # Only one entry node
        entry = entry_functions[0]
        function_list.append(entry)

    # Construct and return the SyntheticFaaSrWorkflow object
    return SyntheticFaaSrWorkflow(
        compute_server=compute_server,
        data_store=data_store,
        data_endpoint=data_endpoint,
        bucket=bucket,
        region=region,
        writable=writable,
        files_folder=files_folder,
        files=workflow.files,
        function_list=function_list,
        start_function=entry,
        function_git_repos=funtion_gitrepos,
    )


def compile_faasr_to_wrench_sim(workflow):
    """
    Placeholder for compiling a FaaSr workflow to a WRENCH simulation.
    """
    print("compile_faasr_to_wrench_sim not implemented")
