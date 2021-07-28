from clearml import Task
from clearml.automation.controller import PipelineController


# Connecting ClearML with the current process,
# from here on everything is logged automatically
task = Task.init(
    project_name="kedro_tutorial_clearml",
    task_name="DS pipeline",
    task_type=Task.TaskTypes.controller,
    reuse_last_task_id=False,
)

pipe = PipelineController(default_execution_queue="default")
pipe.add_step(
    name="split",
    base_task_project="kedro_tutorial_clearml/ds",
    base_task_name="split_data",
)
pipe.add_step(
    name="train",
    base_task_project="kedro_tutorial_clearml/ds",
    base_task_name="train_model",
)

# Starting the pipeline (in the background)
pipe.start()
# Wait until pipeline terminates
pipe.wait()
# cleanup everything
pipe.stop()

print("done")
