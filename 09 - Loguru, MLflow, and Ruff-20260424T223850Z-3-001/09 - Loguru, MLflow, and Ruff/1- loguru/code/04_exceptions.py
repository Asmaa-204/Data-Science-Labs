from loguru import logger

logger.add('app.log')
logger.add('app_serialized.log', serialize=True)


def read_file(filename):
    with open(filename, 'r') as f:
        return f.read()


# read_file('input.txt')

# 1- Using Context Manager:
with logger.catch():
    read_file('input.txt')
