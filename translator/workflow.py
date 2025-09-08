import textwrap
from typing import Dict, List, Optional


class ComputeServer:
    """
    Holds data about a compute server in the FaaSr workflow.

    Args:
        faastype (str): FaaS provider type (e.g., GitHubActions, Lambda, OpenWhisk).
        name (str): Name of the compute server.
    """

    def __init__(self, faastype, name):
        self.name = name
        self.faastype = faastype


class OW_ComputeServer(ComputeServer):
    """
    Holds data about an OpenWhisk compute server.

    Args:
        name (str): Name of compute server.
        faastype (str): FaaS provider type.
        namespace (str): OpenWhisk namespace.
        ssl (str): Whether SSL is used.
        endpoint (str): OpenWhisk endpoint.
    """

    def __init__(self, name, faastype, namespace, ssl, endpoint):
        super().__init__(faastype=faastype, name=name)
        self.namespace = namespace
        self.ssl = ssl
        self.endpoint = endpoint


class Lambda_ComputeServer(ComputeServer):
    """
    Holds data about an AWS Lambda compute server.

    Args:
        name (str): Name of compute server.
        faastype (str): FaaS provider type.
        region (str): AWS region.
    """

    def __init__(self, name, faastype, region):
        super().__init__(faastype=faastype, name=name)
        self.region = region


class GH_ComputeServer(ComputeServer):
    """
    Holds data about a GitHub Actions compute server.

    Args:
        name (str): Name of compute server.
        faastype (str): FaaS provider type.
        username (str): GitHub username.
        action_repo_name (str): GitHub repo for actions.
        branch (str): Branch of the action repo.
    """

    def __init__(self, name, faastype, username, action_repo_name, branch):
        super().__init__(faastype=faastype, name=name)
        self.username = username
        self.action_repo_name = action_repo_name
        self.branch = branch


class SyntheticFaaSrAction:
    """
    Represents an action in the FaaSr workflow.

    Args:
        compute_server (ComputeServer): The compute server for this action.
        execution_time (float): Execution time in seconds.
        name (str): Action name.
        action_container (str): Path to the function's container image.
        input_files (list[str]): List of input files.
        output_files (list[str]): List of output files.
        invoke_next (list[str]): List of next actions to invoke.
        function_name (str): Name of the FaaS function.
        type (str): Type of the action (e.g., "R", "Python"). Default is "R".
    """

    def __init__(
        self,
        compute_server,
        execution_time,
        name,
        action_container,
        input_files=None,
        output_files=None,
        invoke_next=None,
        function_name="synthetic_faas_function",
        type="R",
    ):
        if execution_time < 0:
            raise ValueError("Execution time cannot be negative")
        self.compute_server = compute_server
        self.execution_time = execution_time
        self.name = name
        self.input_files = input_files if input_files is not None else []
        self.output_files = output_files if output_files is not None else []
        self.invoke_next = invoke_next if invoke_next is not None else []
        self.function_name = function_name
        self.action_container = action_container
        self.type = type


class Task:
    """
    Represents a task in the WfFormat workflow.

    Args:
        runtime (float): Time the task runs after it begins execution.
        name (str): Task name.
        id (str): Unique task identifier.
        children (list[str]): List of dependent tasks (children).
        parents (list[str]): List of parent tasks.
        input_files (list[str]): List of input files.
        output_files (list[str]): List of output files.
    """

    def __init__(
        self,
        runtime,
        name,
        id,
        children=None,
        parents=None,
        input_files=None,
        output_files=None,
    ):
        if runtime < 0:
            raise ValueError("Runtime cannot be negative")
        self.runtime = runtime
        self.name = name
        self.id = id
        self.children = children if children is not None else []
        self.parents = parents if parents is not None else []
        self.input_files = input_files if input_files is not None else []
        self.output_files = output_files if output_files is not None else []


class WfFormatWorkflow:
    """
    Represents a WfFormat workflow.

    Args:
        files (dict[str, int]): Mapping of file names to sizes.
        tasks (list[Task]): List of tasks in the workflow.
    """

    def __init__(self, files=None, tasks=None):
        self.files = files if files is not None else {}
        self.tasks = tasks if tasks is not None else []

    def __str__(self):
        """Returns WfFormatWorkflow data as a readable string."""
        file_names = self.files.keys()
        output = "--------------WF WORKFLOW--------------\nFiles: "
        for f in file_names:
            output += f" {f} | "
        output += "\n\nTasks: "
        for t in self.tasks:
            output += f"{t.name} ({t.runtime:.3f}s) | "
        return output


class SyntheticFaaSrWorkflow:
    """
    Represents a FaaSr workflow.

    Args:
        compute_server (ComputeServer): Compute server for FaaS calls.
        data_store (str): Name of S3 data store.
        data_endpoint (str): S3 endpoint.
        bucket (str): Name of S3 bucket.
        region (str): S3 region.
        writable (str): Whether the S3 bucket is writable.
        files_folder (str): Folder in S3 containing workflow files.
        files (dict[str, int]): Mapping of file names to sizes.
        function_list (list[SyntheticFaaSrAction]): List of FaaSr actions.
        start_function (SyntheticFaaSrAction): Entry action for the workflow.
        function_git_repos (dict[str, str]): Mapping of function names to their GitHub repos.
    """

    def __init__(
        self,
        compute_server,
        data_store="My_Minio_Bucket",
        data_endpoint="https://play.min.io",
        bucket="faasr",
        region="us-east-1",
        writable="TRUE",
        files_folder="synthetic_files",
        files=None,
        function_list=None,
        start_function=None,
        function_git_repos=None,
    ):
        self.compute_server = compute_server
        self.data_store = data_store
        self.data_endpoint = data_endpoint
        self.bucket = bucket
        self.region = region
        self.writable = writable
        self.files_folder = files_folder
        self.files = files if files is not None else {}
        self.function_list = function_list if function_list is not None else []
        self.start_function = start_function
        self.function_git_repos = (
            function_git_repos
            if function_git_repos is not None
            else {"synthetic_faas_function": "nolcut/FaaSr-synthetic"}
        )

    def __str__(self):
        """Returns FaaSr workflow data as a readable string."""
        files_str = " ".join([f"{f} |" for f in self.files.keys()])
        functions_str = " ".join(
            [
                f"{function.name} ({function.execution_time:.3f}s) |"
                for function in self.function_list
            ]
        )
        output = (
            f"--------------FAASR WORKFLOW--------------\n"
            f"Compute Server: {self.compute_server.name}\n"
            f"FaaS Type: {self.compute_server.faastype}\n"
        )
        # Add provider-specific details
        match self.compute_server.faastype:
            case "GitHubActions":
                output += (
                    f"Username: {getattr(self.compute_server, 'username', '')}\n"
                    f"Action Repo Name: {getattr(self.compute_server, 'action_repo_name', '')}\n"
                    f"Branch: {getattr(self.compute_server, 'branch', '')}\n"
                )
            case "Lambda":
                output += f"Region: {getattr(self.compute_server, 'region', '')}\n"
            case "OpenWhisk":
                output += (
                    f"Namespace: {getattr(self.compute_server, 'namespace', '')}\n"
                    f"Endpoint: {getattr(self.compute_server, 'endpoint', '')}\n"
                )
        output += (
            f"Data Store: {self.data_store}\n"
            f"Data Endpoint: {self.data_endpoint}\n"
            f"Bucket: {self.bucket}\n"
            f"Region: {self.region}\n"
            f"Writable: {self.writable}\n"
            f"\nFiles: {files_str}\n"
            f"\nFunction List: {functions_str}\n"
        )
        if self.start_function:
            output += f"\nStart Function: {self.start_function.name} ({self.start_function.execution_time:.3f}s)\n"
        output += f"Function Git Repos: {self.function_git_repos}"
        return output
