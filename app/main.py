import asyncio

from apscheduler.triggers.interval import IntervalTrigger
from plombery import task, get_logger, Trigger, register_pipeline
from pydantic import BaseModel, conint


class InputParams(BaseModel):
  some_value: conint(gt=0)

@task
async def task1(params: InputParams = InputParams(some_value=1)):
    logger = get_logger()
    logger.debug(f"got {params}")

    await asyncio.sleep(1)
    # raise Exception('my exception')

    return [params.some_value]

@task
async def task2(params: InputParams = InputParams(some_value=1)):
    logger = get_logger()
    logger.debug(f"got {params}")

    return [params.some_value]



register_pipeline(
    id="pipeline",
    description="description for pipeline",
    tasks = [task1, task2],
    params=InputParams,
    triggers = [
        Trigger(
            id="every 10 sec",
            name="trigger name",
            schedule=IntervalTrigger(seconds=10),
            # schedule=DateTrigger(run_date=datetime.now() + timedelta(seconds=20)),
            params=InputParams(some_value=2),
        )
    ],
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("plombery:get_app", reload=True, factory=True)