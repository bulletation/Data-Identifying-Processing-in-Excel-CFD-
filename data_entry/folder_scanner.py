import os
from typing import List, Tuple
from utils.logger import Logger

class FolderScanner:
    def __init__(self, root_path: str):
        """
        初始化文件夹扫描器
        Args:
            root_path: 要扫描的根目录路径
        """
        self.root_path = root_path
        self.logger = Logger('folder_scanner')
        
    def scan_folders(self) -> List[Tuple[str, str]]:
        """
        扫描文件夹，返回最内层文件夹的路径和名称
        Returns:
            List[Tuple[str, str]]: 包含(文件夹完整路径, 文件夹名称)的列表
        """
        self.logger.info(f"开始扫描文件夹: {self.root_path}")
        inner_folders = []
        
        try:
            for root, dirs, files in os.walk(self.root_path):
                # 如果当前文件夹没有子文件夹，说明是最内层文件夹
                if not dirs:
                    folder_path = root
                    folder_name = os.path.basename(root)
                    inner_folders.append((folder_path, folder_name))
                    self.logger.debug(f"找到最内层文件夹: {folder_name}, 路径: {folder_path}")
            
            self.logger.info(f"扫描完成，共找到 {len(inner_folders)} 个最内层文件夹")
            return inner_folders
            
        except Exception as e:
            self.logger.error(f"扫描文件夹时发生错误: {str(e)}")
            raise
    
    def validate_folder(self, folder_path: str) -> bool:
        """
        验证文件夹是否存在且可访问
        Args:
            folder_path: 要验证的文件夹路径
        Returns:
            bool: 文件夹是否有效
        """
        try:
            return os.path.isdir(folder_path) and os.access(folder_path, os.R_OK)
        except Exception as e:
            self.logger.error(f"验证文件夹时发生错误: {str(e)}")
            return False