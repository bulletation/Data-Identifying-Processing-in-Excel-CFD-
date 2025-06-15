import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import LOG_FORMAT, LOG_LEVEL, LOG_DATE_FORMAT

class Logger:
    def __init__(self, name: str):
        """
        初始化日志记录器
        Args:
            name: 日志记录器名称
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, LOG_LEVEL))

        # 创建控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, LOG_LEVEL))

        # 设置日志格式
        formatter = logging.Formatter(
            fmt=LOG_FORMAT,
            datefmt=LOG_DATE_FORMAT
        )
        console_handler.setFormatter(formatter)

        # 添加处理器到日志记录器
        if not self.logger.handlers:
            self.logger.addHandler(console_handler)

    def debug(self, message: str):
        """记录调试级别的日志"""
        self.logger.debug(message)

    def info(self, message: str):
        """记录信息级别的日志"""
        self.logger.info(message)

    def warning(self, message: str):
        """记录警告级别的日志"""
        self.logger.warning(message)

    def error(self, message: str):
        """记录错误级别的日志"""
        self.logger.error(message)

    def critical(self, message: str):
        """记录严重错误级别的日志"""
        self.logger.critical(message)