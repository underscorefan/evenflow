from asyncio import Queue
from evenflow.dbops import select_errors, DatabaseCredentials
from evenflow.streams.messages import DataKeeper


async def restore_errors(send: Queue, db_cred: DatabaseCredentials):
    dk = DataKeeper()

    for error in await db_cred.do_with_connection(lambda conn: select_errors(conn, fake=True)):
        dk.append_link(mark_as_fake=error["from_fake"], source=error["source_article"], url=error["url"])

    await send.put(dk)
