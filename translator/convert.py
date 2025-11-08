#!/usr/bin/env python3
import argparse
import json

from wfformat_reader import *
from workflow import *
from writer import *
from translator import *

""""
python3 convert.py [WfFormat JSON] [output file name] [--test]

This program does the following:
1. Initializes a WfFormatWorkflow using a WfFormat JSON
2. Initializes a SyntheticFaaSrWorkflow using the WfFormatWorkflow with multiple compute servers
3. Dumps the FaaSr workflow to a JSON file
4. Optionally downloads the files in the FaaSr workflow to S3
5. Optionally creates faasr_env for the workflow

Test mode (--test flag): Uses only 5 tasks to quickly test across all compute servers
"""


def prompt_input(prompt, default=None, validator=None):
    """
    Prompt user for input with optional default and validation.
    """
    while True:
        if default is not None:
            value = input(f"{prompt} [{default}]: ") or default
        else:
            value = input(f"{prompt}: ")
        if validator:
            if validator(value):
                return value
            else:
                print("Invalid input. Please try again.")
        else:
            return value


def prompt_y_or_n(prompt):
    """Prompts user for 'y' or 'n' input and returns a boolean."""
    while True:
        response = input(f"{prompt} (y/n): ").strip().lower()
        if response in ("y", "n"):
            return response == "y"
        print("Please input 'y' or 'n'.")


def prompt_python_percentage():
    """Prompt for a valid Python percentage (0-100)."""
    while True:
        try:
            val = float(
                input("Enter percentage of actions that should be Python (0-100): ")
            )
            if 0 <= val <= 100:
                return val
            else:
                print("Please enter a value between 0 and 100.")
        except ValueError:
            print("Please enter a valid number.")


def prompt_file_size(prompt_text):
    """Prompt for a uniform file size in bytes (must be non-negative integer)."""
    while True:
        try:
            val = int(input(prompt_text))
            if val >= 0:
                return val
            else:
                print("Please enter a non-negative integer.")
        except ValueError:
            print("Please enter a valid integer.")


def create_default_compute_servers():
    """
    Creates all 5 default compute servers with updated configurations.
    
    Returns:
        list[ComputeServer]: List of all compute servers
    """
    compute_servers = []
    
    # GitHub Actions
    gh_server = GH_ComputeServer(
        name="GH",
        faastype="GitHubActions",
        username="Ashish-Ramrakhiani",
        action_repo_name="FaaSr-workflow-public",
        branch="main",
        use_secret_store=True,
    )
    compute_servers.append(gh_server)
    
    # AWS Lambda
    aws_server = Lambda_ComputeServer(
        name="AWS",
        faastype="Lambda",
        region="us-east-1",
        cpus_per_task=1,
        memory=512,
        time_limit=900,
        use_secret_store=True,
    )
    compute_servers.append(aws_server)
    
    # OpenWhisk
    ow_server = OW_ComputeServer(
        name="OW",
        faastype="OpenWhisk",
        namespace="guest",
        ssl="False",
        endpoint="ow.faasr.io:31002",
    )
    compute_servers.append(ow_server)
    
    # Google Cloud Platform
    gcp_server = GCP_ComputeServer(
        name="GCP",
        faastype="GoogleCloud",
        namespace="nsf-20210119-renatof-420467",
        region="us-central1",
        endpoint="https://run.googleapis.com/v2/projects/",
        use_secret_store=True,
        client_email="faasr-experiments@nsf-20210119-renatof-420467.iam.gserviceaccount.com",
        token_uri="https://oauth2.googleapis.com/token",
        cpus_per_task=1,
        memory=512,
        time_limit=3600,
    )
    compute_servers.append(gcp_server)
    
    # SLURM
    slurm_server = SLURM_ComputeServer(
        name="SLURM",
        faastype="SLURM",
        endpoint="http://slurm.faasr.io:6820",
        api_version="v0.0.37",
        partition="faasr",
        nodes=2,
        tasks=2,
        cpus_per_task=4,
        username="ubuntu",
        memory=2048,
        time_limit=30,
        working_directory="/tmp",
    )
    compute_servers.append(slurm_server)
    
    return compute_servers


def create_test_workflow(wf_workflow, num_tasks=5):
    """
    Create a test workflow with only a subset of tasks.
    Maintains task dependencies by creating a simple linear chain.
    
    Args:
        wf_workflow: Original WfFormatWorkflow
        num_tasks: Number of tasks to include (default 5)
    
    Returns:
        WfFormatWorkflow: Reduced workflow for testing
    """
    # Get the first task (entry point)
    if not wf_workflow.tasks:
        return wf_workflow
    
    # Find the entry task (task with no parents)
    entry_tasks = [t for t in wf_workflow.tasks if len(t.parents) == 0]
    
    if not entry_tasks:
        # If no entry task found, just use first task
        selected_tasks = wf_workflow.tasks[:num_tasks]
    else:
        # Start with the first entry task
        selected_tasks = [entry_tasks[0]]
        current_task = entry_tasks[0]
        
        # Follow the chain of children to build a path
        while len(selected_tasks) < num_tasks:
            if current_task.children:
                # Find the next task in the original workflow
                next_task_id = current_task.children[0]
                next_task = next((t for t in wf_workflow.tasks if t.id == next_task_id), None)
                
                if next_task and next_task not in selected_tasks:
                    selected_tasks.append(next_task)
                    current_task = next_task
                else:
                    # Can't continue the chain, break
                    break
            else:
                # No more children, break
                break
        
        # If we don't have enough tasks, add more from the beginning
        if len(selected_tasks) < num_tasks:
            for task in wf_workflow.tasks:
                if task not in selected_tasks:
                    selected_tasks.append(task)
                    if len(selected_tasks) >= num_tasks:
                        break
    
    # Take only the first num_tasks
    selected_tasks = selected_tasks[:num_tasks]
    
    # Build a set of selected task IDs for quick lookup
    selected_ids = {t.id for t in selected_tasks}
    
    # Update children to only include tasks that are in our selection
    for task in selected_tasks:
        task.children = [child for child in task.children if child in selected_ids]
    
    # Update parents to only include tasks that are in our selection
    for task in selected_tasks:
        task.parents = [parent for parent in task.parents if parent in selected_ids]
    
    # Get only the files used by selected tasks
    selected_files = {}
    for task in selected_tasks:
        for file in task.input_files + task.output_files:
            if file in wf_workflow.files:
                selected_files[file] = wf_workflow.files[file]
    
    print(f"\nTest workflow task chain:")
    for i, task in enumerate(selected_tasks, 1):
        children_str = f" → {task.children[0] if task.children else 'END'}"
        print(f"  {i}. {task.id}{children_str}")
    
    return WfFormatWorkflow(files=selected_files, tasks=selected_tasks)


def main():
    # Parse args
    parser = argparse.ArgumentParser(description="converts wfformat json to faasr json")
    parser.add_argument("data_file", help="JSON instance file")
    parser.add_argument("output_name", help="name for output file")
    parser.add_argument("--test", action="store_true", help="Test mode: use only 5 tasks")
    args = parser.parse_args()
    data = json.loads(open(args.data_file).read())

    # Create WfWorkflow
    wf_workflow = wfformat_to_workflow_obj(data)
    
    # Apply test mode if flag is set
    if args.test:
        print("\n" + "="*60)
        print("TEST MODE ENABLED")
        print("="*60)
        print(f"Original workflow: {len(wf_workflow.tasks)} tasks")
        wf_workflow = create_test_workflow(wf_workflow, num_tasks=5)
        print(f"Test workflow: {len(wf_workflow.tasks)} tasks")
        print("="*60 + "\n")

    # Prompt for input file sizes
    use_default_input_size = prompt_y_or_n(
        "Use default input file sizes from the workflow file?"
    )
    if not use_default_input_size:
        uniform_input_size = prompt_file_size(
            "Enter uniform input file size in bytes for all input files (0 for no input): "
        )
        if uniform_input_size == 0:
            for t in wf_workflow.tasks:
                t.input_files = []
            wf_workflow.files = {}
        else:
            for t in wf_workflow.tasks:
                t.input_files = ["uniform_input_file"]
            wf_workflow.files = {"uniform_input_file": uniform_input_size}

    # Prompt for output file sizes
    use_default_output_size = prompt_y_or_n(
        "Use default output file sizes from the workflow file?"
    )
    if not use_default_output_size:
        uniform_output_size = prompt_file_size(
            "Enter uniform output file size in bytes for all output files (0 for no output): "
        )
        all_output_files = set()
        for t in wf_workflow.tasks:
            all_output_files.update(t.output_files)
        for fname in all_output_files:
            wf_workflow.files[fname] = uniform_output_size

    # Use updated S3 data store defaults
    data_store = "My_S3_Bucket"
    data_endpoint = "https://s3.us-west-2.amazonaws.com"
    bucket = "faasr-bucket-0001"
    region = "us-west-2"
    writable = "true"
    files_folder = "synthetic_files"

    # Prompt for data store configuration override
    use_default_datastore = prompt_y_or_n(
        "Use default S3 data store configuration (faasr-bucket-0001 in us-west-2)?"
    )
    if not use_default_datastore:
        data_endpoint = prompt_input(
            "Enter S3 endpoint", default="https://s3.us-west-2.amazonaws.com"
        )
        bucket = prompt_input("Enter S3 bucket name", default="faasr-bucket-0001")
        region = prompt_input("Enter S3 region", default="us-west-2")
        writable_input = prompt_y_or_n("Is the S3 bucket writable?")
        writable = "true" if writable_input else "false"

    # Create all 5 default compute servers
    compute_servers = create_default_compute_servers()
    
    print(f"\nUsing {len(compute_servers)} compute servers:")
    for server in compute_servers:
        print(f"  - {server.name} ({server.faastype})")
    print(f"\nTasks will be distributed roughly equally across all {len(compute_servers)} servers using round-robin.")
    print("Note: Lambda (AWS) will skip tasks with runtime > 10 minutes.\n")
    
    if args.test:
        print("TEST MODE: Each of the 5 tasks will be assigned to a different server.\n")

    # Prompt for Python percentage
    python_percentage = prompt_python_percentage()

    # Prompt for containers - 10 total (5 servers × 2 languages each)
    use_default_containers = prompt_y_or_n(
        "Use default container configurations for all servers?"
    )
    
    server_containers = {}
    
    if use_default_containers:
        # Use default containers for all servers
        default_container = "ghcr.io/faasr/github-actions-tidyverse:latest"
        for server in compute_servers:
            server_containers[server.name] = {
                "R": default_container,
                "Python": default_container
            }
        print("Using default containers for all servers")
    else:
        # Prompt for container for each server and language
        print("\nPlease provide container images for each compute server:")
        for server in compute_servers:
            print(f"\n--- {server.name} ({server.faastype}) ---")
            r_container = prompt_input(
                f"  R container for {server.name}",
                default="ghcr.io/faasr/github-actions-tidyverse:latest",
            )
            python_container = prompt_input(
                f"  Python container for {server.name}",
                default="ghcr.io/faasr/github-actions-python-slim:latest",
            )
            server_containers[server.name] = {
                "R": r_container,
                "Python": python_container
            }

    # Display workflow data
    print(wf_workflow)

    # Create SyntheticFaaSrWorkflow from WfWorkflow with multiple compute servers
    faasr_workflow = translate_wf_to_faasr(
        workflow=wf_workflow,
        compute_servers=compute_servers,
        data_store=data_store,
        data_endpoint=data_endpoint,
        bucket=bucket,
        region=region,
        writable=writable,
        files_folder=files_folder,
        python_percentage=python_percentage,
        server_containers=server_containers,
    )

    # Display FaaSr workflow
    print(faasr_workflow)
    
    # Show task distribution in test mode
    if args.test:
        print("\n" + "="*60)
        print("TEST MODE - Task Distribution:")
        print("="*60)
        for i, action in enumerate(faasr_workflow.function_list, 1):
            print(f"Task {i}: {action.name[:40]:40} → {action.compute_server.name}")
        print("="*60 + "\n")

    # Dumps FaaSr workflow to JSON
    write_faasr_obj_to_json(faasr_workflow, args.output_name)
    
    print(f"\n✓ FaaSr workflow written to {args.output_name}/")
    print(f"  - {args.output_name}.json")
    print(f"  - {args.output_name}_files.json")
    
    if args.test:
        # Calculate file sizes for test mode
        total_size = sum(faasr_workflow.files.values())
        print(f"\nTest mode file summary:")
        print(f"  - Total files: {len(faasr_workflow.files)}")
        print(f"  - Total size: {total_size:,} bytes ({total_size/(1024*1024):.2f} MB)")

    # Ask if user wants to download files to S3
    download_files = prompt_y_or_n("\nWould you like to download files to S3?")
    if download_files:
        from download_faasr_files import download_files_to_s3_from_json
        access_key = prompt_input("Enter S3 access key")
        secret_key = prompt_input("Enter S3 secret key")
        download_files_to_s3_from_json(
            f"{args.output_name}/{args.output_name}_files.json",
            bucket_name=bucket,
            endpoint=data_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            folder=files_folder,
        )


if __name__ == "__main__":
    main()