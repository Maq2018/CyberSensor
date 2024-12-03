import traceback
import logging


logger = logging.getLogger('database.services')


def _bulk_load(_table, _ops):
    if not _ops:
        return

    _ops = [_op for _op in _ops if _op is not None]

    try:
        _table.bulk_write(_ops, ordered=False)

    except Exception as e:
        logger.error(f"Failed to bulk write for {_table} with len={len(_ops)},"
                     f" err: {e}, stack: {traceback.format_exc()}")


async def _async_bulk_load(_table, _ops):
    if not _ops:
        return

    _ops = [_op for _op in _ops if _op is not None]

    try:
        await _table.bulk_write(_ops, ordered=False)

    except Exception as e:
        logger.error(f"Failed to bulk write for {_table} with len={len(_ops)},"
                     f" err: {e}, stack: {traceback.format_exc()}")
