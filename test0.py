import asyncio
import logging
from requester import AsyncRequester

logging.basicConfig(level=logging.INFO)


async def process(requester, dir, lpmn):
    logging.info("Uploading...")
    uploaded_files = await requester.upload(dir)
    logging.info("Starting tasks...")
    processed_files = await requester.run(uploaded_files, lpmn)
    logging.info("Downloading...")
    await requester.download(processed_files, dst_path="./downloads")


async def main():
    FILES = "/media/jamnicki/HDD/__Test_input/test_requester/"
    LPMN = 'any2txt|wcrft2|liner2({"model":"top9"})'

    requester = AsyncRequester(
        user="jedrzej.jamnicki@pwr.edu.pl"
    )

    try:
        await process(requester, FILES, LPMN)
    except Exception as e:
        logging.exception(e)
    finally:
        await requester.close_session()


if __name__ == "__main__":
    asyncio.run(main())
