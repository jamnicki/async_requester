import os
import json
import asyncio
import aiohttp
import aiofiles
import logging
from urllib.parse import quote
from pathlib import Path
from typing import NewType, Set, Iterable, List

from config import endpoints

FileId = NewType("FileId", str)
TaskId = NewType("TaskId", str)
TaskStatus = NewType("TaskStatus", str)
Lpmn = NewType("Lpmn", str)


class AsyncRequester:

    def __init__(
                self,
                user: str,
                sleep: float = 0.5,
                max_sleep: float = 2,
                timeout: float = 0
            ) -> None:
        self.user = user
        self.session = aiohttp.ClientSession()
        self.sleep = sleep
        self.max_sleep = max_sleep
        self.timeout = timeout

    async def _wait_status(self, task_id: TaskId) -> Set[FileId] | None:
        sleep = self.sleep
        url = endpoints.TASK_STATUS.format(task_id=task_id)
        while True:
            async with self.session.get(url=url) as response:
                if not response.ok:
                    return None, response.status
                res_data = await response.text()
                res_data = json.loads(res_data)
                status = res_data["status"]
                match status:
                    case "ERROR":
                        raise Exception(f"{task_id=} {status=}, {res_data=}")
                    case "QUEUE":
                        logging.debug(f"Task {task_id} in queue...")
                    case "PROCESSING":
                        logging.debug(f"Processing {task_id}...")
                    case "CANCEL":
                        logging.warning(f"Task {task_id} has been canceled!")
                    case "DONE":
                        logging.debug(f"Task {task_id} done.")
                        file_ids = set()
                        for processed_file in res_data["value"]:
                            file_id = processed_file["fileID"]
                            file_ids.add(file_id)
                        return file_ids

            await asyncio.sleep(sleep)
            if self.timeout != 0 and self.sleep >= self.timeout:
                return None
            if sleep < self.max_sleep:
                sleep *= 1.2

    async def upload_single(self, filepath: str) -> FileId | None:
        headers = {"Content-Type": "binary/octet-stream"}
        url = endpoints.UPLOAD
        async with aiofiles.open(filepath, "rb") as file:
            content = await file.read()
        async with self.session.post(
                url=url, data=content, headers=headers) as response:
            if not response.ok:
                return None, response.status
            logging.debug(f"Uploading {filepath}...")
            return await response.text()

    async def upload(self, dir: str) -> List[FileId] | None:
        upload_tasks = [
            self.upload_single(os.path.join(dir, filename))
            for filename in os.listdir(dir)
        ]
        return await asyncio.gather(*upload_tasks)

    async def start_task(self, file_id: FileId, lpmn: Lpmn) -> TaskId | None:
        headers = {"Content-Type": "application/json"}
        url = endpoints.START_TASK
        prefix = f"file({file_id})"
        lpmn = "|".join((prefix, lpmn))
        data = {
            "lpmn": lpmn,
            "user": self.user,
            "application": "async_requester"
        }
        async with self.session.post(
                url=url, json=data, headers=headers) as response:
            if not response.ok:
                return None, response.status
            return await response.text()

    async def run_single(
            self, file_id: FileId, lpmn: Lpmn) -> Set[FileId] | None:
        task_id = await self.start_task(file_id, lpmn)
        file_ids = await self._wait_status(task_id)
        return file_ids

    async def run(
            self, file_ids: Iterable[FileId],
            lpmn: Lpmn) -> List[FileId] | None:
        run_tasks = [self.run_single(file_id, lpmn) for file_id in file_ids]
        return await asyncio.gather(*run_tasks)

    async def download_single(self, file_id: FileId, dst_path: str) -> None:
        Path(dst_path).mkdir(parents=True, exist_ok=True)
        url = endpoints.DOWNLOAD.format(file_id=quote(file_id))
        async with self.session.get(url=url) as response:
            if not response.ok:
                return None, response.status
            file_content = await response.read()
            filename = file_id.rsplit("/", maxsplit=1)[-1]
            filepath = os.path.join(dst_path, filename)
            async with aiofiles.open(filepath, "wb") as file:
                await file.write(file_content)
                logging.debug(f"Downloading {file_id}...")

    async def download(
            self, file_ids: Iterable[FileId], dst_path: str) -> None:
        Path(dst_path).mkdir(parents=True, exist_ok=True)
        download_tasks = [
            self.download_single(*_file_ids, dst_path)
            for _file_ids in file_ids
        ]
        await asyncio.gather(*download_tasks)

    async def close_session(self) -> None:
        await self.session.close()
        logging.debug("Closing client session.")
