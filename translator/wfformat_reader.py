import json

from workflow import *


def wfformat_to_workflow_obj(data):
    """
    Takes WfFormat JSON and uses it to initialize a WfFormatWorkflow object
    :param_data: JSON instance
    """

    task_runtimes = {}
    tasks = []
    files = {}

    # create mapping of file name to size
    for f in data["workflow"]["specification"]["files"]:
        file_name = f["id"].rsplit("/", 1)[-1]
        files[file_name] = f["sizeInBytes"]

    # create mapping of task id to runtime
    for e in data["workflow"]["execution"]["tasks"]:
        task_runtimes[e["id"]] = e["runtimeInSeconds"]

    # create tasks
    for t in data["workflow"]["specification"]["tasks"]:
        name = t["name"].rsplit("/", 1)[-1]
        input_files = [file.rsplit("/", 1)[-1] for file in t["inputFiles"]]
        output_files = [file.rsplit("/", 1)[-1] for file in t["outputFiles"]]
        tasks.append(
            Task(
                runtime=task_runtimes[t["id"]],
                name=name,
                id=t["id"],
                children=t["children"],
                parents=t["parents"],
                input_files=input_files,
                output_files=output_files,
            )
        )

    return WfFormatWorkflow(files, tasks)
