import json
import aiofiles
import io


async def asy_read_json(path: str):
    async with aiofiles.open(path, mode='r') as f:
        return json.loads(await f.read(), strict=False)


async def asy_write_json(path: str, obj):
    async with aiofiles.open(path, mode='w') as f:
        await f.write(json.dumps(obj, indent=2))


def read_json_from(path: str):
    with io.open(path, 'r', encoding='utf-8-sig') as f:
        return json.load(f)
