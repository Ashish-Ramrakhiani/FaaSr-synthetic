import json

from workflow import *


def faasr_json_to_workflow_obj(faasr_json, faasr_file_json):
    """
    This function converts a synthetic FaaSr JSON and it's associated FaaSr file JSON and uses them to initialize a SynhteticFaaSrWorkflow object
    :param_faasr_json: FaaSr JSON
    :param_faasr_file_json: FaaSr file JSON
    """
    file_json = json.load(open(faasr_file_json, "r"))
    faasr_json = json.load(open(faasr_json, "r"))
    function_list = []
    compute_server_name = next(iter(faasr_json["ComputeServers"]))
    faas_type = faasr_json["ComputeServers"][compute_server_name]["FaaSType"]

    # create compute server
    match faas_type:
        case "GitHubActions":
            username = faasr_json["ComputeServers"][compute_server_name]["UserName"]
            action_repo_name = faasr_json["ComputeServers"][compute_server_name][
                "ActionRepoName"
            ]
            branch = faasr_json["ComputeServers"][compute_server_name]["Branch"]
            compute_server = GH_ComputeServer(
                name=compute_server_name,
                faastype=faas_type,
                username=username,
                action_repo_name=action_repo_name,
                branch=branch,
            )
        case "OpenWhisk":
            if faasr_json["ComputeServers"][compute_server_name].hasOwnProperty("SSL"):
                ssl = faasr_json["ComputeServers"][compute_server_name]["SSL"]
            else:
                ssl = "False"
            namespace = faasr_json["ComputeServers"][compute_server_name]["Namespace"]
            endpoint = faasr_json["ComputeServers"][compute_server_name]["Endpoint"]
            compute_server = OW_ComputeServer(
                name=compute_server_name,
                faastype=faas_type,
                namespace=namespace,
                ssl=ssl,
                endpoint=endpoint,
            )
        case "Lambda":
            region = faasr_json["ComputeServers"][compute_server_name]["Region"]
            compute_server = Lambda_ComputeServer(
                name=compute_server_name, faastype=faas_type, region=region
            )
    data_store_name = next(iter(faasr_json["DataStores"]))

    # create functions
    for name, configuration in faasr_json["ActionList"].items():
        function = SyntheticFaaSrAction(
            name=name,
            execution_time=configuration["Arguments"]["execution_time"],
            input_files=configuration["Arguments"]["input_files"],
            invoke_next=configuration["InvokeNext"],
            action_container=faasr_json["ActionContainers"][name],
        )
        function_list.append(function)
        if name == faasr_json["FunctionInvoke"]:
            start_function = function

    return SyntheticFaaSrWorkflow(
        compute_server=compute_server,
        data_store=data_store_name,
        data_endpoint=faasr_json["DataStores"][data_store_name]["Endpoint"],
        bucket=faasr_json["DataStores"][data_store_name]["Bucket"],
        region=faasr_json["DataStores"][data_store_name]["Region"],
        writable=faasr_json["DataStores"][data_store_name]["Writable"],
        files=file_json["files"],
        function_list=function_list,
        start_function=start_function,
    )
