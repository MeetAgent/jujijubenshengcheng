from loguru import logger
logger.add("debug.log", level="DEBUG", rotation="500 MB", encoding="utf-8")
log = logger
if __name__ == '__main__':
    log.debug(f'debug是绿色，说明是调试的，代码ok ')
    log.info('info是天蓝色，日志正常 ')
    log.warning('黄色yello，有警告了 ')
    log.error('粉红色说明代码有错误 ')
    log.critical('血红色，说明发生了严重错误 ')