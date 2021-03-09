import os
import argparse
import requests
from sanic import Sanic
from sanic.response import json
from osf_pigeon.pigeon import main, sync_metadata, get_id
from concurrent.futures import ThreadPoolExecutor
from sanic.log import logger
from asyncio import events


def run(main):
    loop = events.new_event_loop()
    try:
        events.set_event_loop(loop)
        return loop.run_until_complete(main)
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            events.set_event_loop(None)
            loop.close()


app = Sanic("osf_pigeon")
pigeon_jobs = ThreadPoolExecutor(max_workers=10, thread_name_prefix="pigeon_jobs")


def task_done(future):
    if future._exception:
        exception = future._exception
        exception = str(exception)

        logger.debug(f"ERROR:{exception}")
    if future._result:
        guid, url = future._result
        resp = requests.post(
            f"{settings.OSF_API_URL}_/ia/{guid}/done/", json={"IA_url": url}
        )
        logger.debug(f"DONE:{future._result} Response:{resp}")


@app.route("/")
async def index(request):
    return json({"🐦": "👍"})


@app.route("/archive/<guid>", methods=["GET", "POST"])
async def archive(request, guid):
    future = pigeon_jobs.submit(run, main(guid))
    future.add_done_callback(task_done)
    return json({guid: future._state})


@app.route("/metadata/<guid>", methods=["POST"])
async def metadata(request, guid):
    item_name = get_id(guid)
    future = pigeon_jobs.submit(sync_metadata, item_name, request.json)
    future.add_done_callback(task_done)
    return json({guid: future._state})


parser = argparse.ArgumentParser(
    description="Set the environment to run OSF pigeon in."
)
parser.add_argument(
    "--env", dest="env", help="what environment are you running this for"
)


if __name__ == "__main__":
    args = parser.parse_args()
    if args.env:
        os.environ["ENV"] = args.env

    from osf_pigeon import settings

    if args.env == "production":
        app.run(host=settings.HOST, port=settings.PORT)
    else:
        app.run(host=settings.HOST, port=settings.PORT, auto_reload=True, debug=True)
