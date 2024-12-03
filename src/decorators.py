import time


def time_cost(threshold=None, logger=None):
    if not logger:
        import logging
        logger = logging.getLogger('decorators')

    def w(fn):

        def _w(*args, **kwargs):
            t0 = time.time()
            ret = fn(*args, **kwargs)
            t1 = time.time()

            if threshold is None or t1 - t0 > threshold:
                logger.info('function %s cost %.6fs', fn.__name__, t1 - t0)
            return ret

        return _w

    return w
