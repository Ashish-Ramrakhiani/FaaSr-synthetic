import textwrap
from typing import Dict, List, Optional


class ComputeServer:
    """
    Holds data about a compute server in the FaaSr workflow.

    Args:
        faastype (str): FaaS provider type (e.g., GitHubActions, Lambda, OpenWhisk, GoogleCloud, SLURM).
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
        cpus_per_task (int): CPUs per task.
        memory (int): Memory in MB.
        time_limit (int): Time limit in seconds.
        use_secret_store (bool): Whether to use secret store.
    """

    def __init__(self, name, faastype, region, cpus_per_task=1, memory=512, time_limit=900, use_secret_store=True):
        super().__init__(faastype=faastype, name=name)
        self.region = region
        self.cpus_per_task = cpus_per_task
        self.memory = memory
        self.time_limit = time_limit
        self.use_secret_store = use_secret_store


class GH_ComputeServer(ComputeServer):
    """
    Holds data about a GitHub Actions compute server.

    Args:
        name (str): Name of compute server.
        faastype (str): FaaS provider type.
        username (str): GitHub username.
        action_repo_name (str): GitHub repo for actions.
        branch (str): Branch of the action repo.
        use_secret_store (bool): Whether to use secret store.
    """

    def __init__(self, name, faastype, username, action_repo_name, branch, use_secret_store=True):
        super().__init__(faastype=faastype, name=name)
        self.username = username
        self.action_repo_name = action_repo_name
        self.branch = branch
        self.use_secret_store = use_secret_store


class GCP_ComputeServer(ComputeServer):
    """
    Holds data about a Google Cloud Platform compute server.

    Args:
        name (str): Name of compute server.
        faastype (str): FaaS provider type.
        namespace (str): GCP namespace/project ID.
        region (str): GCP region.
        endpoint (str): GCP endpoint.
        use_secret_store (bool): Whether to use secret store.
        client_email (str): Service account client email.
        token_uri (str): OAuth2 token URI.
        cpus_per_task (int): CPUs per task.
        memory (int): Memory in MB.
        time_limit (int): Time limit in seconds.
    """

    def __init__(self, name, faastype, namespace, region, endpoint, use_secret_store=True, 
                 client_email="", token_uri="", cpus_per_task=1, memory=512, time_limit=3600):
        super().__init__(faastype=faastype, name=name)
        self.namespace = namespace
        self.region = region
        self.endpoint = endpoint
        self.use_secret_store = use_secret_store
        self.client_email = client_email
        self.token_uri = token_uri
        self.cpus_per_task = cpus_per_task
        self.memory = memory
        self.time_limit = time_limit


class SLURM_ComputeServer(ComputeServer):
    """
    Holds data about a SLURM compute server.

    Args:
        name (str): Name of compute server.
        faastype (str): FaaS provider type.
        endpoint (str): SLURM endpoint.
        api_version (str): SLURM API version.
        partition (str): SLURM partition.
        nodes (int): Number of nodes.
        tasks (int): Number of tasks.
        cpus_per_task (int): CPUs per task.
        username (str): SLURM username.
        memory (int): Memory in MB.
        time_limit (int): Time limit in minutes.
        working_directory (str): Working directory.
    """

    def __init__(self, name, faastype, endpoint, api_version, partition, nodes, tasks, 
                 cpus_per_task, username, memory, time_limit, working_directory):
        super().__init__(faastype=faastype, name=name)
        self.endpoint = endpoint
        self.api_version = api_version
        self.partition = partition
        self.nodes = nodes
        self.tasks = tasks
        self.cpus_per_task = cpus_per_task
        self.username = username
        self.memory = memory
        self.time_limit = time_limit
        self.working_directory = working_directory


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
        compute_servers (list[ComputeServer]): List of compute servers for FaaS calls.
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
        compute_servers,
        data_store="My_S3_Bucket",
        data_endpoint="https://s3.us-west-2.amazonaws.com",
        bucket="faasr-bucket-0001",
        region="us-west-2",
        writable="true",
        files_folder="synthetic_files",
        files=None,
        function_list=None,
        start_function=None,
        function_git_repos=None,
    ):
        self.compute_servers = compute_servers if isinstance(compute_servers, list) else [compute_servers]
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
        output = "--------------FAASR WORKFLOW--------------\n"
        output += "Compute Servers:\n"
        for server in self.compute_servers:
            output += f"  - {server.name} ({server.faastype})\n"
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