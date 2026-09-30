import os
from socket import gethostname

from prefect import flow, get_run_logger

# Coiled sets these in the container of each batch job.  They identify the job
# and the cluster, so a flow run can be matched to the Coiled account it ran in.
COILED_VARIABLES = ["COILED_JOB_ID", "COILED_CLUSTER_ID", "COILED_CLUSTER_HOSTNAME"]


@flow
def on_coiled() -> dict[str, str | None]:
    logger = get_run_logger()

    logger.info(f"The flow ran on host {gethostname()}")

    coiled = {name: os.environ.get(name) for name in COILED_VARIABLES}
    for name, value in coiled.items():
        logger.info(f"{name}={value}")

    return coiled


if __name__ == "__main__":
    on_coiled()
