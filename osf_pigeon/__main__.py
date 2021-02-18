import os
import sys
import requests
from osf_pigeon import settings
from sanic import Sanic
from sanic.response import json
from osf_pigeon.pigeon import main, sync_metadata, get_id

from concurrent.futures import ThreadPoolExecutor
from sanic.log import logger


app = Sanic("osf_pigeon")
pigeon_jobs = ThreadPoolExecutor(max_workers=3, thread_name_prefix="pigeon_jobs")


def task_done(future):
    exception = None
    if future._exception:
        exception = future._exception
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.info(exc_type, fname, exc_tb.tb_lineno)
        exception = str(exception)
    if future._result:
        guid, url = future._result
        requests.post(f"{settings.OSF_API_URL}_/ia/{guid}/done/", data={"IA_url": url})

    logger.info(f"DONE: {future._result}, {exception}")


@app.route("/")
async def index(request):
    return json({"🐦": "👍"})


@app.route("/archive/<guid>", methods=["GET"])
async def archive(request, guid):
    future = pigeon_jobs.submit(main, guid)
    future.add_done_callback(task_done)
    return json({guid: future._state})


@app.route("/metadata/<guid>", methods=["POST"])
async def metadata(request, guid):
    item_name = get_id(guid)
    future = pigeon_jobs.submit(sync_metadata, item_name, request.json)
    future.add_done_callback(task_done)
    return json({guid: future._state})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8001, auto_reload=True, debug=True)
