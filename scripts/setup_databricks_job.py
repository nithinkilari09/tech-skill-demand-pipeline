"""Creates/updates the scheduled Databricks Workflow that runs Bronze -> Silver
-> Gold end to end, daily, with no human triggering a job run.

This is the piece that was still manual: every Bronze/Silver/Gold run so far
in this project was a one-off `w.jobs.submit()` call kicked off by hand. This
script instead defines a persistent Job (by name, idempotent -- re-running
this script updates the existing job rather than creating duplicates) with a
Quartz cron schedule, so the pipeline keeps itself current without anyone
re-running anything.

Task DAG (serverless compute throughout, same as every manual run so far):
    bronze_remoteok  \\
                       -> silver_transform -> gold_aggregate
    bronze_arbeitnow /

Scheduled for 04:00 UTC -- after ingest.yml's 03:00 UTC GitHub Actions run
(pool_postings + upload_to_s3) has had time to land the day's raw postings in
S3, and before dashboard.yml's 06:00 UTC rebuild reads the resulting Gold
tables.

Uploads the three notebooks to a stable workspace path every run, so the job
definition always points at whatever's currently in notebooks/ -- no separate
"remember to re-upload" step when the notebooks change.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace
from pathlib import Path

WORKSPACE_USER = "nithinkilari09@gmail.com"
WORKSPACE_DIR = f"/Workspace/Users/{WORKSPACE_USER}"
NOTEBOOKS_DIR = Path(__file__).parent.parent / "notebooks"
JOB_NAME = "tech_skill_demand_pipeline"

# Quartz cron: seconds minutes hours day-of-month month day-of-week
SCHEDULE_CRON = "0 0 4 * * ?"
SCHEDULE_TIMEZONE = "UTC"


def upload_notebook(w, local_name):
    local_path = NOTEBOOKS_DIR / local_name
    workspace_path = f"{WORKSPACE_DIR}/{local_path.stem}"
    w.workspace.upload(
        path=workspace_path,
        content=local_path.read_bytes(),
        format=workspace.ImportFormat.SOURCE,
        overwrite=True,
        language=workspace.Language.PYTHON,
    )
    print(f"Uploaded {local_name} -> {workspace_path}")
    return workspace_path


def build_job_settings(bronze_path, silver_path, gold_path):
    return jobs.JobSettings(
        name=JOB_NAME,
        tasks=[
            jobs.Task(
                task_key="bronze_remoteok",
                notebook_task=jobs.NotebookTask(
                    notebook_path=bronze_path, base_parameters={"source": "remoteok"}
                ),
            ),
            jobs.Task(
                task_key="bronze_arbeitnow",
                notebook_task=jobs.NotebookTask(
                    notebook_path=bronze_path, base_parameters={"source": "arbeitnow"}
                ),
            ),
            jobs.Task(
                task_key="silver_transform",
                depends_on=[
                    jobs.TaskDependency(task_key="bronze_remoteok"),
                    jobs.TaskDependency(task_key="bronze_arbeitnow"),
                ],
                notebook_task=jobs.NotebookTask(notebook_path=silver_path),
            ),
            jobs.Task(
                task_key="gold_aggregate",
                depends_on=[jobs.TaskDependency(task_key="silver_transform")],
                notebook_task=jobs.NotebookTask(notebook_path=gold_path),
            ),
        ],
        schedule=jobs.CronSchedule(
            quartz_cron_expression=SCHEDULE_CRON,
            timezone_id=SCHEDULE_TIMEZONE,
            pause_status=jobs.PauseStatus.UNPAUSED,
        ),
        max_concurrent_runs=1,
    )


def main():
    w = WorkspaceClient()

    bronze_path = upload_notebook(w, "bronze_ingest.py")
    silver_path = upload_notebook(w, "silver_transform.py")
    gold_path = upload_notebook(w, "gold_aggregate.py")

    settings = build_job_settings(bronze_path, silver_path, gold_path)

    existing = list(w.jobs.list(name=JOB_NAME))
    if existing:
        job_id = existing[0].job_id
        w.jobs.reset(job_id=job_id, new_settings=settings)
        print(f"Updated existing job '{JOB_NAME}' (job_id={job_id})")
    else:
        created = w.jobs.create(
            name=settings.name,
            tasks=settings.tasks,
            schedule=settings.schedule,
            max_concurrent_runs=settings.max_concurrent_runs,
        )
        job_id = created.job_id
        print(f"Created job '{JOB_NAME}' (job_id={job_id})")

    print(f"Schedule: '{SCHEDULE_CRON}' ({SCHEDULE_TIMEZONE}), unpaused")
    print(f"View at: {w.config.host}/jobs/{job_id}")


if __name__ == "__main__":
    main()
