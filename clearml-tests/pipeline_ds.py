from clearml import Task
from clearml.automation.controller import PipelineController


# Connecting ClearML with the current process,
# from here on everything is logged automatically
task = Task.init(
    project_name="kedro_tutorial_clearml",
    task_name="DE pipeline",
    task_type=Task.TaskTypes.controller,
    reuse_last_task_id=False,
)

pipe = PipelineController(default_execution_queue="default")
pipe.add_step(
    name="companies",
    base_task_project="kedro_tutorial_clearml/de",
    base_task_name="preprocess_companies",
)
pipe.add_step(
    name="shuttles",
    base_task_project="kedro_tutorial_clearml/de",
    base_task_name="preprocess_shuttles",
)
pipe.add_step(
    name="input_table",
    parents=["companies", "shuttles"],
    base_task_project="kedro_tutorial_clearml/de",
    base_task_name="create_model_input_table",
)

# Starting the pipeline (in the background)
pipe.start()
# Wait until pipeline terminates
pipe.wait()
# cleanup everything
pipe.stop()

print("done")
