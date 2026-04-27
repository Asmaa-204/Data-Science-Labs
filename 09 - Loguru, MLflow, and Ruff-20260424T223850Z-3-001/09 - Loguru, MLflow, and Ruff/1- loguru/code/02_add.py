import sys
from loguru import logger

# === Remove the default colored output to stderr ===
logger.remove()

# === Add a new sink stdout ===
# logger.add(sys.stdout, format="{time} {level} --- {message}")
# logger.add(sys.stdout, format="{time:MMMM D, YYYY - HH:mm:ss} {level} --- {message}")
# logger.add(
#     sys.stdout,
#     format="{time:MMMM D, YYYY - HH:mm:ss} {level} --- <green>{message}</green>",
# )
# logger.add(
#     sys.stdout,
#     format="{time:MMMM D, YYYY - HH:mm:ss} {level} --- <level>{message}</level>",
# )
# logger.add(
#     sys.stdout,
#     format="{time:MMMM D, YYYY - HH:mm:ss} {level} --- <level>{message}</level>",
#     serialize=True,  # to output in a json format
# )
logger.add(
    sys.stdout,
    format="{time:MMMM D, YYYY - HH:mm:ss} {level} --- <level>{message}</level>",
    level='WARNING',  # only warning and more severity are displayed
)

logger.info('info message')
logger.error('error message')
logger.success('success message')
logger.warning('warning message')
