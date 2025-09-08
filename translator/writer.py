import json
import os

import boto3
from botocore.exceptions import ClientError
from workflow import *


def write_faasr_obj_to_json(workflow, output_name):
    """
    Serializes a SyntheticFaaSrWorkflow object to FaaSr JSON files.

    Args:
        workflow (SyntheticFaaSrWorkflow): The workflow object to serialize.
        output_name (str): The name of the output directory and files.
    """
    data_bucket = workflow.data_store
    faasr_data = {
        "ComputeServers": {},
        "DataStores": {},
        "ActionList": {},
        "ActionContainers": {},
        "FunctionGitRepo": workflow.function_git_repos,
        "FunctionInvoke": workflow.start_function.name,
        "InvocationID": "",
        "FaaSrLog": "FaaSrLog",
        "LoggingDataStore": data_bucket,
        "DefaultDataStore": data_bucket,
        "FunctionCRANPackage": {"synthetic_faas_function": []},
        "FunctionGitHubPackage": {"synthetic_faas_function": []},
    }

    # Add compute server details
    server = workflow.compute_server
    faasr_data["ComputeServers"][server.name] = {"FaaSType": server.faastype}
    match server.faastype:
        case "GitHubActions":
            faasr_data["ComputeServers"][server.name].update(
                {
                    "UserName": server.username,
                    "ActionRepoName": server.action_repo_name,
                    "Branch": server.branch,
                }
            )
        case "Lambda":
            faasr_data["ComputeServers"][server.name]["Region"] = server.region
        case "OpenWhisk":
            faasr_data["ComputeServers"][server.name].update(
                {
                    "Namespace": server.namespace,
                    "Endpoint": server.endpoint,
                }
            )

    # Add data store details
    faasr_data["DataStores"][workflow.data_store] = {
        "Endpoint": workflow.data_endpoint,
        "Bucket": workflow.bucket,
        "Region": workflow.region,
        "Writable": workflow.writable,
    }

    # Add function and action container details
    for function in workflow.function_list:
        faasr_data["ActionContainers"][function.name] = function.action_container
        faasr_data["ActionList"][function.name] = {
            "FunctionName": function.function_name,
            "FaaSServer": server.name,
            "Type": function.type,
            "Arguments": {
                "execution_time": function.execution_time,
                "folder": workflow.files_folder,
                "input_files": function.input_files,
                "input_size_in_bytes": sum(
                    workflow.files[file] for file in function.input_files
                ),
                "output_size_in_bytes": sum(
                    workflow.files[file] for file in function.output_files
                ),
                "actionid": function.name,
            },
            "InvokeNext": function.invoke_next,
        }

    # Write main workflow JSON
    os.mkdir(output_name)
    with open(f"{output_name}/{output_name}.json", "w") as outfile:
        json.dump(faasr_data, outfile, indent=4)

    # Write files JSON
    faasr_data_files = {"files": workflow.files}
    with open(f"{output_name}/{output_name}_files.json", "w") as file_outfile:
        json.dump(faasr_data_files, file_outfile, indent=4)


def create_file_of_size(file_path, size_in_bytes):
    """
    Creates a file at the given path with the specified size in bytes.

    Args:
        file_path (str): Path to the file to create.
        size_in_bytes (int): Size of the file in bytes.
    """
    with open(file_path, "w") as newfile:
        newfile.truncate(size_in_bytes)


def download_files_to_s3_from_json(
    faasr_file_path: str,
    bucket_name: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
    folder: str,
):
    """
    Uploads files described in a FaaSr file JSON to an S3 bucket using boto3.

    Args:
        faasr_file_path (str): Path to the FaaSr file JSON.
        bucket_name (str): S3 bucket name.
        endpoint (str): S3 endpoint.
        access_key (str): S3 access key.
        secret_key (str): S3 secret key.
        folder (str): Folder inside the S3 bucket to store data.
    """
    # Initialize boto3 S3 client
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    # Ensure bucket exists, create if not
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError:
        s3.create_bucket(Bucket=bucket_name)

    # Create temp directory for file staging
    if not os.path.exists("temp"):
        os.mkdir("temp")
    else:
        raise FileExistsError("Directory temp already exists.")

    # Ensure .json extension
    path = (
        faasr_file_path
        if faasr_file_path.endswith(".json")
        else f"{faasr_file_path}.json"
    )

    # Load file sizes from JSON
    with open(path, "r") as f:
        faasr_files = json.load(f)

    # Create files and upload to S3
    for file, size in faasr_files["files"].items():
        temp_file_path = os.path.join("temp", file)
        os.makedirs(os.path.dirname(temp_file_path), exist_ok=True)
        create_file_of_size(temp_file_path, size)
        destination = f"{folder}/{file}"
        s3.upload_file(temp_file_path, bucket_name, destination)
        os.remove(temp_file_path)

    os.rmdir("temp")
    print("Files downloaded to S3")
