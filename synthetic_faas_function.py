import os
import time
from datetime import datetime


def synthetic_faas_function(
    folder,
    execution_time,
    input_files,
    input_size_in_bytes,
    output_size_in_bytes,
    actionid,
):
    def timestamp():
        return datetime.now().strftime("%Y%m%d%H%M%S%f")

    # Log download start
    time_stamp1 = f"LOG DOWNLOAD[{actionid}][{input_size_in_bytes}][START]: began downloading files from S3 at time: {timestamp()}"
    faasr_log(time_stamp1)

    # Download input files from S3
    for file in input_files:
        try:
            faasr_log(f"Downloading file: {file}")
            local_file = f"{timestamp()}-{file}"
            faasr_get_file(
                remote_folder=folder, remote_file=file, local_file=local_file
            )
        except Exception as e:
            faasr_log(f"ERROR: Failed to download: {file}")
            raise e

    # Log download finish
    time_stamp2 = f"LOG DOWNLOAD[{actionid}][{input_size_in_bytes}][FINISH]: finished downloading files from S3 at time: {timestamp()}"
    faasr_log(time_stamp2)

    # Simulate function execution
    time_stamp3 = f"LOG SLEEP[{actionid}][{execution_time}][START]: began sleeping at time: {timestamp()}"
    faasr_log(time_stamp3)
    time.sleep(execution_time)
    time_stamp4 = f"LOG SLEEP[{actionid}][{execution_time}][FINISH]: finished sleeping at time: {timestamp()}"
    faasr_log(time_stamp4)

    # Create binary file for output
    output_file = f"output_{timestamp()}.bin"
    with open(output_file, "wb") as f:
        if output_size_in_bytes > 0:
            f.seek(output_size_in_bytes - 1)
            f.write(b"\0")

    # Store output file in S3
    time_stamp5 = f"LOG OUTPUT[{actionid}][{output_size_in_bytes}][START]: began transferring output file to S3 at time: {timestamp()}"
    faasr_log(time_stamp5)
    faasr_put_file(
        local_file=output_file, remote_folder=folder, remote_file=output_file
    )
    time_stamp6 = f"LOG OUTPUT[{actionid}][{output_size_in_bytes}][FINISH]: finished transferring output file to S3 at time: {timestamp()}"
    faasr_log(time_stamp6)

    # Log function completion
    log_msg = f"Function synthetic_faas_function finished; output written to {folder}/{output_file} in default S3 bucket"
    faasr_log(log_msg)
    faasr_log(time_stamp1)
    faasr_log(time_stamp2)
    faasr_log(time_stamp3)
    faasr_log(time_stamp4)
    faasr_log(time_stamp5)
