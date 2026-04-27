from loguru import logger

# NOTE: the default logging to stderr is still there, first logger sink
logger.add('app.log')  # a second logger sink
logger.add('app_serialized.log', serialize=True)  # a third logger sink

logger.debug("That's it, beautiful and simple logging!")
logger.error("That's an error!")
logger.warning("That's a warning!")
logger.info("That's info!")
