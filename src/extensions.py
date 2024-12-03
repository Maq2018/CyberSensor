from database import (
    CacheConnection,
    MongoConnection,
    # ClickhouseConnection,
    # S3Connection
)


cache = CacheConnection()
mongo = MongoConnection()
# ch = ClickhouseConnection()
# s3 = S3Connection()
