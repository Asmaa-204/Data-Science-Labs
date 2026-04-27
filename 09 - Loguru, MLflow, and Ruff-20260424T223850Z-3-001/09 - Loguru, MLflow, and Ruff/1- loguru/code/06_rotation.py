from loguru import logger

logger.add('app.log', rotation='2 seconds')

x = 10000
while x > 0:
    logger.debug("That's it, beautiful and simple logging!")
    logger.error("That's an error!")
    logger.warning("That's a warning!")
    logger.info("That's info!")
    logger.critical("That's CRITICAL!")
    x -= 1
