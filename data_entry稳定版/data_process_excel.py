import os
import pandas as pd
from typing import Dict, List, Optional
from utils.logger import Logger
from openai import OpenAI
import re
from tqdm import tqdm
from config import OPENAI_API_KEY, OPENAI_BASE_URL, OPENAI_MODEL, MAX_TEXT_LENGTH

class DataProcessor:
    def __init__(self, target_items: List[str]):
        """初始化数据处理器
        Args:
            target_items: 需要提取的目标指标列表
        """
        self.target_items = target_items
        self.logger = Logger('excel_processor')
        self.keywords = {
            "一般公共预算收入决算", "税收收入", "增值税", "营业税", "企业所得税",
            "个人所得税", "资源税", "城市维护建设税", "房产税", "印花税",
            "城镇土地使用税", "土地增值税", "车船税", "耕地占用税", "契税",
            "烟叶税", "环境保护税", "非税收入", "专项收入", "行政事业性收费收入",
            "罚没收入", "国有资本经营收入", "国有资源(资产)有偿使用收入",
            "一般公共预算支出决算", "一般公共服务支出",
            "上级补助收入", "返还性收入", "一般性转移支付收入", "专项转移支付收入",
            "公共预算上年结余资金", "公共预算调入资金", "公共预算债务转贷收入",
            "一般公共预算总收入", "公共预算债务还本支出", "一般公共预算总支出",
            "政府性基金收入决算", "政府性基金支出决算", "政府性基金预算收入决算",
            "政府性基金预算支出决算", "政府性基金预算收支决算",
            "政府债务限额", "一般债务限额", "专项债务限额",
            "政府债务余额", "一般债务余额", "专项债务余额"
        }

    def process_file(self, excel_path: str) -> Dict[str, Dict[str, str]]:
        """处理单个Excel文件"""
        try:
            results = []
            # 读取所有工作表
            xls = pd.ExcelFile(excel_path)
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                # 转换为文本格式
                sheet_text = []
                for _, row in df.iterrows():
                    clean_values = [
                        str(cell)
                        .replace('\n', '')    # 删除换行符
                        .replace('\\n', '')   # 删除转义符
                        .replace(' ', '')     # 删除空格
                        if cell is not None
                        else ""
                        for cell in row.values
                    ]
                    sheet_text.append(" | ".join(clean_values))
                
                # 检查是否包含关键词
                full_text = "\n".join(sheet_text)
                if any(keyword in full_text for keyword in self.keywords):
                    results.append(f"=== {sheet_name} ===\n")
                    results.append(full_text)
                    results.append("\n--------------------------------------\n")
            
            if not results:
                self.logger.warning(f"未在文件中找到相关数据: {excel_path}")
                return {"全市": {}}
                
            # 使用AI提取所有工作表的数据
            return self._extract_financial_data("".join(results))
        except Exception as e:
            self.logger.error(f"处理Excel文件时发生错误: {str(e)}")
            raise

    def _extract_financial_data(self, text: str) -> Dict[str, Dict[str, str]]:
        """使用AI提取财政数据"""
        try:
            client = OpenAI(
                api_key=OPENAI_API_KEY,
                base_url=OPENAI_BASE_URL
            )
            
            prompt = f"""
    输入的文本数据，请你按照表格样式来理解，如果遇到“--------------------------------------”，说明到下一个表格了；注意指标与列对应的关系！！！
    其他要求：
    1. 不区分市级和市本级，统一当作市级处理
    2. 必须确保输出格式是输出格式为：
       全市-指标名称: 数值
    3. 一般公共预算总收入和一般公共预算总支出其实就是对应表格里的总收入和总支出，可以提取
    4. 数值可以是负数，如“专项转移支付收入”对应的数字是负数，请提取！
            
            目标指标：
            {"; ".join(self.target_items)}
            
            表格内容：
            {text[:MAX_TEXT_LENGTH]}
            """
            
            response = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1
            )
            
            return self._parse_ai_response(response.choices[0].message.content)
            
        except Exception as e:
            self.logger.error(f"AI提取数据时发生错误: {str(e)}")
            raise

    def _parse_ai_response(self, response: str) -> Dict[str, Dict[str, str]]:
        """解析AI响应数据"""
        result = {"全市": {}}  # 保持使用"全市"作为key以保持与其他代码的一致性
        pattern = re.compile(r"^(市级|全市)-(.+?):\s*([\d.,]+)")
        
        for line in response.split("\n"):
            match = pattern.search(line)
            if match:
                region, item, value = match.groups()
                item = item.strip()
                
                try:
                    num = float(value.replace(",", ""))
                    formatted = f"{num:.2f}"  # 只保留两位小数，不添加单位
                    
                    if item in self.target_items:
                        current = result["全市"].get(item)
                        if not current or num > float(current):
                            result["全市"][item] = formatted
                except Exception as e:
                    self.logger.error(f"解析数值失败: {line} → {str(e)}")
                    
        return result

    def process_folder(self, folder_path: str, output_csv: str) -> None:
        """处理指定文件夹中的所有Excel文件
        Args:
            folder_path: 要处理的文件夹路径
            output_csv: 输出CSV文件的路径
        """
        self.logger.info(f"开始处理文件夹: {folder_path}")
        try:
            # 获取所有Excel文件
            excel_files = self._get_excel_files(folder_path)
            if not excel_files:
                self.logger.warning(f"文件夹中没有找到Excel文件: {folder_path}")
                return
            
            # 批量处理Excel文件
            results = self._batch_process_excels(excel_files)
            
            # 保存结果到CSV
            self._save_to_csv(results, output_csv)
            
            self.logger.info(f"处理完成，结果已保存到: {output_csv}")
            
        except Exception as e:
            self.logger.error(f"处理文件夹时发生错误: {str(e)}")
            raise

    def _get_excel_files(self, folder_path: str) -> List[str]:
        """获取文件夹中的所有Excel文件"""
        excel_files = []
        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.lower().endswith(('.xlsx', '.xls')):
                    excel_files.append(os.path.join(root, file))
        return excel_files

    def _batch_process_excels(self, excel_files: List[str]) -> List[Dict]:
        """批量处理Excel文件"""
        results = []
        for excel_path in tqdm(excel_files, desc="处理进度"):
            try:
                data = self.process_file(excel_path)
                results.append({
                    "文件名": os.path.basename(excel_path),
                    "数据": data
                })
            except Exception as e:
                self.logger.error(f"处理Excel失败: {os.path.basename(excel_path)} → {str(e)}")
                
        return results

    def _save_to_csv(self, data: List[Dict], output_csv: str) -> None:
        """保存结果到CSV文件"""
        import csv
        try:
            with open(output_csv, "w", newline="", encoding="gbk") as f:
                writer = csv.writer(f)
                headers = ["文件夹名称"] + self.target_items
                writer.writerow(headers)
                
                for file_data in data:
                    folder_name = os.path.basename(os.path.dirname(file_data["文件名"]))
                    row = [folder_name]
                    region_data = file_data["数据"].get("全市", {})
                    row.extend([region_data.get(item, "") for item in self.target_items])
                    writer.writerow(row)
                    
        except Exception as e:
            self.logger.error(f"保存CSV文件时发生错误: {str(e)}")
            raise