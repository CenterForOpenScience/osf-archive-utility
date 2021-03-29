import os
import argparse
import requests
from sanic import Sanic
from sanic.response import json
from osf_pigeon import pigeon
from concurrent.futures import ThreadPoolExecutor
from sanic.log import logger


app = Sanic("osf_pigeon")
pigeon_jobs = ThreadPoolExecutor(max_workers=10, thread_name_prefix="pigeon_jobs")


def task_done(future):
    if future.exception():
        exception = future.exception()
        exception = str(exception)
        logger.debug(f"ERROR:{exception}")
    if future.result():
        guid, url = future.result()
        resp = requests.post(
            f"{settings.OSF_API_URL}_/ia/{guid}/done/", json={"IA_url": url}
        )
        logger.debug(f"DONE:{future._result} Response:{resp}")


@app.route("/")
async def index(request):
    return json({"🐦": "👍"})


@app.route("/archive/<guid>", methods=["GET", "POST"])
async def archive(request, guid):
    future = pigeon_jobs.submit(pigeon.run, pigeon.archive(guid))
    future.add_done_callback(task_done)
    return json({guid: future._state})


@app.route("/metadata/<guid>", methods=["POST"])
async def set_metadata(request, guid):
    item_name = pigeon.REG_ID_TEMPLATE.format(guid=guid)
    future = pigeon_jobs.submit(pigeon.sync_metadata, item_name, request.json)
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
