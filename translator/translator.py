import random

from workflow import *


def sanitize_action_name(name):
    """
    Sanitize action name to be lowercase with hyphens instead of underscores.
    
    Args:
        name (str): Original action name
    
    Returns:
        str: Sanitized action name
    """
    return name.lower().replace("_", "-")


def translate_wf_to_faasr(
    workflow,
    compute_servers,
    data_store="My_S3_Bucket",
    data_endpoint="https://s3.us-west-2.amazonaws.com",
    bucket="faasr-bucket-0001",
    region="us-west-2",
    writable="true",
    files_folder="synthetic_files",
    funtion_gitrepos={"synthetic_faas_function": "nolcut/FaaSr-synthetic"},
    python_percentage=0,
    server_containers=None,
):
    """
    Converts a WfFormatWorkflow object into a SyntheticFaaSrWorkflow object for FaaSr.
    Distributes tasks roughly equally across all compute servers using round-robin.
    Lambda (AWS) is skipped for tasks with runtime > 10 minutes (600 seconds).

    Args:
        workflow (WfFormatWorkflow): The input workflow object.
        compute_servers (list[ComputeServer]): List of FaaS compute servers.
        data_store (str): Name of data store.
        data_endpoint (str): S3 endpoint.
        bucket (str): Name of S3 bucket.
        region (str): Region for S3 data_store.
        writable (str): Specifies if S3 bucket is writable.
        files_folder (str): Name of folder in S3 containing workflow files.
        funtion_gitrepos (dict): Mapping of function names to their GitHub repos.
        python_percentage (float): Percentage of actions that should be Python (0-100).
        server_containers (dict): Dictionary mapping server names to their containers.
                                  Format: {server_name: {"R": r_container, "Python": python_container}}

    Returns:
        SyntheticFaaSrWorkflow: The translated workflow object.
    """
    function_list = []
    entry_functions = []
    
    # Ensure compute_servers is a list
    if not isinstance(compute_servers, list):
        compute_servers = [compute_servers]
    
    # Round-robin counter for distributing tasks across compute servers
    server_index = 0
    num_servers = len(compute_servers)
    
    # Lambda runtime limit in seconds (10 minutes)
    LAMBDA_MAX_RUNTIME = 600

    # Convert each task in the workflow to a SyntheticFaaSrAction
    for t in workflow.tasks:
        # Randomly assign action type based on python_percentage
        action_type = "Python" if random.uniform(0, 100) < python_percentage else "R"

        # Assign compute server using round-robin with Lambda runtime check
        assigned_server = None
        attempts = 0
        while assigned_server is None and attempts < num_servers:
            candidate_server = compute_servers[server_index % num_servers]
            server_index += 1
            attempts += 1
            
            # Check if this is Lambda and task runtime exceeds limit
            if candidate_server.name == "AWS" and t.runtime > LAMBDA_MAX_RUNTIME:
                # Skip Lambda for this task, try next server
                continue
            else:
                assigned_server = candidate_server
                break
        
        # If all servers were Lambda or unavailable, assign to first non-Lambda server
        if assigned_server is None:
            for server in compute_servers:
                if server.name != "AWS":
                    assigned_server = server
                    break
            # If only Lambda exists, assign anyway (shouldn't happen with 5 servers)
            if assigned_server is None:
                assigned_server = compute_servers[0]
        
        # Select container based on assigned server and action type
        if server_containers and assigned_server.name in server_containers:
            action_container = server_containers[assigned_server.name][action_type]
        else:
            # Fallback to default container if not specified
            action_container = "ghcr.io/faasr/github-actions-tidyverse:latest"

        # Sanitize action name and children names
        sanitized_name = sanitize_action_name(t.id)
        sanitized_children = [sanitize_action_name(child) for child in t.children]

        function = SyntheticFaaSrAction(
            compute_server=assigned_server,
            execution_time=t.runtime,
            name=sanitized_name,
            action_container=action_container,
            input_files=t.input_files,
            output_files=t.output_files,
            invoke_next=sanitized_children,
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
        # Select container for entry function from first server
        if server_containers and compute_servers[0].name in server_containers:
            entry_container = server_containers[compute_servers[0].name][entry_type]
        else:
            entry_container = "ghcr.io/faasr/github-actions-tidyverse:latest"
        # Assign first compute server to the entry function
        entry = SyntheticFaaSrAction(
            compute_server=compute_servers[0],
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

    # Construct and return the SyntheticFaaSrWorkflow object
    return SyntheticFaaSrWorkflow(
        compute_servers=compute_servers,
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
    pass