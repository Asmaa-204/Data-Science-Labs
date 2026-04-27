from loguru import logger

logger.add('app.log')
logger.add('app_serialized.log', serialize=True)

# 2- Using Catch() Decorator:


# @logger.catch(level='CRITICAL')
@logger.catch()
def read_file(filename):
    with open(filename) as f:
        return f.read()


# with logger.catch():
#     read_file('input.txt')

read_file('input.txt')
