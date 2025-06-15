import os
from typing import List, Tuple, Dict
from utils.logger import Logger

class FolderScanner:
    def __init__(self, root_path: str):
        """初始化文件夹扫描器
        Args:
            root_path: 要扫描的根目录路径
        """
        self.root_path = root_path
        self.logger = Logger('folder_scanner')
        
    def scan_folders(self) -> List[Tuple[str, str, Dict[str, List[str]]]]:
        """扫描文件夹，返回最内层文件夹的路径、名称和文件类型信息
        Returns:
            List[Tuple[str, str, Dict[str, List[str]]]]: 包含(文件夹完整路径, 文件夹名称, 文件类型信息)的列表
        """
        self.logger.info(f"开始扫描文件夹: {self.root_path}")
        inner_folders = []
        
        try:
            for root, dirs, files in os.walk(self.root_path):
                # 如果当前文件夹没有子文件夹，说明是最内层文件夹
                if not dirs:
                    folder_path = root
                    folder_name = os.path.basename(root)
                    
                    # 获取文件类型信息
                    files_by_type = self._get_files_by_type(folder_path)
                    
                    inner_folders.append((folder_path, folder_name, files_by_type))
                    self.logger.debug(f"找到最内层文件夹: {folder_name}, 路径: {folder_path}, 文件类型: {files_by_type}")
            
            self.logger.info(f"扫描完成，共找到 {len(inner_folders)} 个最内层文件夹")
            return inner_folders
            
        except Exception as e:
            self.logger.error(f"扫描文件夹时发生错误: {str(e)}")
            raise
    
    def _get_files_by_type(self, folder_path: str) -> Dict[str, List[str]]:
        """获取文件夹中的文件，并按类型分类
        Args:
            folder_path: 文件夹路径
        Returns:
            Dict[str, List[str]]: 按文件类型分类的文件列表，如 {'pdf': [...], 'excel': [...]}
        """
        files_by_type = {'pdf': [], 'excel': []}
        try:
            for file in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file)
                if not os.path.isfile(file_path):
                    continue
                    
                ext = os.path.splitext(file)[1].lower()
                if ext == '.pdf':
                    files_by_type['pdf'].append(file_path)
                elif ext in ['.xlsx', '.xls']:
                    files_by_type['excel'].append(file_path)
                    
            return files_by_type
        except Exception as e:
            self.logger.error(f"获取文件列表时发生错误: {str(e)}")
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