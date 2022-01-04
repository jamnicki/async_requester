import os
import asyncio
import logging
from requester import AsyncRequester

logging.basicConfig(level=logging.INFO)


async def process(requester, filepath, lpmn, i):
    logging.info(f"Process #{i}: Uploading...")
    file_id = await requester.upload_single(filepath)
    logging.info(f"Process #{i}: Task started...")
    processed_files = await requester.run_single(file_id, lpmn)
    logging.info(f"Process #{i}: Downloading...")
    await requester.download_single(*processed_files, "./downloads")


async def main():
    FILES = "/media/jamnicki/HDD/__Test_input/test_requester/"
    LPMN = 'any2txt|wcrft2|liner2({"model":"top9"})'

    requester = AsyncRequester(
        user="jedrzej.jamnicki@pwr.edu.pl"
    )

    try:
        process_tasks = [
            process(requester, os.path.join(FILES, filename), LPMN, i)
            for i, filename in enumerate(os.listdir(FILES))
        ]
        await asyncio.gather(*process_tasks)
    except Exception as e:
        logging.exception(e)
    finally:
        await requester.close_session()


if __name__ == "__main__":
    asyncio.run(main())
