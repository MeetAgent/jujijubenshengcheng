from nb_log import get_logger

log = get_logger(__name__)

if __name__ == '__main__':
    log.debug(f'debug是绿色，说明是调试的，代码ok ')
    log.info('info是天蓝色，日志正常 ')
    log.warning('黄色yello，有警告了 ')
    log.error('粉红色说明代码有错误 ')
    log.critical('血红色，说明发生了严重错误 ')