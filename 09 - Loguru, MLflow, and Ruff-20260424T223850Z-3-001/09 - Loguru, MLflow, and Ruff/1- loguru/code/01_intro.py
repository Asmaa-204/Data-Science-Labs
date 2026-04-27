from loguru import logger

# default sink: stderr
logger.debug("That's it, beautiful and simple logging!")
logger.error("That's an error!")
logger.warning("That's a warning!")
logger.info("That's info!")

logger.info(
    "If you're using Python {}, prefer {feature} of course!", 3.6, feature="f-strings"
)
