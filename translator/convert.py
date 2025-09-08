#!/usr/bin/env python3
import argparse
import json

from faasr_reader import *
from wfformat_reader import *
from workflow import *
from writer import *

from translator import *

""""
python3 convert.py [WfFormat JSON] [output file name]

This program does the following:
1. Initializes a WfFormatWorkflow using a WfFormat JSON
2. Initializes a SyntheticFaaSrWorkflow using the WfFormatWorkflow
3. Dumps the FaaSr workflow to a JSON file
4. Optionally downloads the files in the FaaSr workflow to S3
5. Optionally creates faasr_env for the workflow
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


def prompt_file_size_choice():
    """Prompt user to choose between default file sizes or a uniform size."""
    while True:
        response = (
            input("Use default file sizes from the workflow file? (y/n): ")
            .strip()
            .lower()
        )
        if response in ("y", "n"):
            return response == "y"
        print("Please input 'y' or 'n'.")


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


def main():
    # Parse args
    parser = argparse.ArgumentParser(description="converts wfformat json to faasr json")
    parser.add_argument("data_file", help="JSON instance file")
    parser.add_argument("output_name", help="name for output file")
    args = parser.parse_args()
    data = json.loads(open(args.data_file).read())

    # Create WfWorkflow
    wf_workflow = wfformat_to_workflow_obj(data)

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

    # Prompt for data store info or use MinIO defaults
    use_minio = prompt_y_or_n("Use default MinIO data store configuration?")
    if use_minio:
        data_store = "My_Minio_Bucket"
        data_endpoint = "https://play.min.io"
        bucket = "faasr"
        region = "us-east-1"
        writable = "TRUE"
        files_folder = "synthetic_files"
    else:
        data_store = prompt_input(
            "Enter data store name", validator=lambda x: len(x.strip()) > 0
        )
        data_endpoint = prompt_input(
            "Enter S3 endpoint URL", validator=lambda x: len(x.strip()) > 0
        )
        bucket = prompt_input(
            "Enter S3 bucket name", validator=lambda x: len(x.strip()) > 0
        )
        region = prompt_input("Enter S3 region", validator=lambda x: len(x.strip()) > 0)
        writable = prompt_input(
            "Is the bucket writable? (TRUE/FALSE)",
            validator=lambda x: x.strip() in ("TRUE", "FALSE"),
        )
        files_folder = prompt_input(
            "Enter folder in S3 bucket to store files",
            validator=lambda x: len(x.strip()) > 0,
        )

    # Select FaaS provider
    faas_type = prompt_input(
        "What FaaS provider do you want to use for this workflow? [GH, OW, Lambda]",
        validator=lambda x: x.lower() in ("gh", "ow", "lambda"),
    ).lower()

    # Prompt for default or custom FaaS config
    default_faas = prompt_y_or_n(
        "Would you like to use the default compute server configuration?"
    )

    # Gather compute server info
    if faas_type == "gh":
        if default_faas:
            compute_server = GH_ComputeServer(
                name="My_GitHub_Account",
                faastype="GitHubActions",
                username="YOUR_GITHUB_USERNAME",
                action_repo_name="faasr-synthetic-example",
                branch="main",
            )
        else:
            username = prompt_input("Enter your GitHub username")
            action_repo_name = prompt_input("Enter your action repo name")
            branch = prompt_input("Enter branch name", default="main")
            compute_server = GH_ComputeServer(
                name="My_GitHub_Account",
                faastype="GitHubActions",
                username=username,
                action_repo_name=action_repo_name,
                branch=branch,
            )
    elif faas_type == "ow":
        if default_faas:
            compute_server = OW_ComputeServer(
                name="My_OW_Account",
                faastype="OpenWhisk",
                namespace="YOUR_OW_USERNAME",
                ssl="False",
                endpoint="YOUR_OW_ENDPOINT",
            )
        else:
            namespace = prompt_input("Please enter your OpenWhisk namespace")
            ssl = "True" if prompt_y_or_n("Are you using SSL?") else "False"
            endpoint = prompt_input("Please enter your OpenWhisk endpoint")
            compute_server = OW_ComputeServer(
                name="My_OW_Account",
                faastype="OpenWhisk",
                namespace=namespace,
                ssl=ssl,
                endpoint=endpoint,
            )
    elif faas_type == "lambda":
        if default_faas:
            compute_server = Lambda_ComputeServer(
                name="My_Lambda_Account", faastype="Lambda", region="us-east-1"
            )
        else:
            region = prompt_input("Please enter AWS Lambda region", default="us-east-1")
            compute_server = Lambda_ComputeServer(
                name="My_Lambda_Account", faastype="Lambda", region=region
            )

    # Prompt for Python percentage
    python_percentage = prompt_python_percentage()

    # Prompt for R and Python containers (required)
    r_container = prompt_input(
        "Enter the container image for R functions",
        validator=lambda x: len(x.strip()) > 0,
    )
    python_container = prompt_input(
        "Enter the container image for Python functions",
        validator=lambda x: len(x.strip()) > 0,
    )

    # Display workflow data
    print(wf_workflow)

    # Create SyntheticFaaSrWorkflow from WfWorkflow
    faasr_workflow = translate_wf_to_faasr_gh(
        workflow=wf_workflow,
        compute_server=compute_server,
        data_store=data_store,
        data_endpoint=data_endpoint,
        bucket=bucket,
        region=region,
        writable=writable,
        files_folder=files_folder,
        python_percentage=python_percentage,
        r_container=r_container,
        python_container=python_container,
    )

    # Display FaaSr workflow
    print(faasr_workflow)

    # Dumps FaaSr workflow to JSON
    write_faasr_obj_to_json(faasr_workflow, args.output_name)

    exit(1)


if __name__ == "__main__":
    main()
