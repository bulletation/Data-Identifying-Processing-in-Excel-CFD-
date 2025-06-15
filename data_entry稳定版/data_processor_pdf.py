import os
import pdfplumber
from typing import Dict, List, Optional
from utils.logger import Logger
from openai import OpenAI
import re
from tqdm import tqdm
from config import OPENAI_API_KEY, OPENAI_BASE_URL, OPENAI_MODEL, MAX_TEXT_LENGTH

class DataProcessor:
    def __init__(self, target_items: List[str]):
        """
        初始化数据处理器
        Args:
            target_items: 需要提取的目标指标列表
        """
        self.target_items = target_items
        self.logger = Logger('data_processor')
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
        
    def process_folder(self, folder_path: str, output_csv: str) -> None:
        """
        处理指定文件夹中的所有PDF文件
        Args:
            folder_path: 要处理的文件夹路径
            output_csv: 输出CSV文件的路径
        """
        self.logger.info(f"开始处理文件夹: {folder_path}")
        try:
            # 获取所有PDF文件
            pdf_files = self._get_pdf_files(folder_path)
            if not pdf_files:
                self.logger.warning(f"文件夹中没有找到PDF文件: {folder_path}")
                return
            
            # 批量处理PDF文件
            results = self._batch_process_pdfs(pdf_files)
            
            # 保存结果到CSV
            self._save_to_csv(results, output_csv)
            
            self.logger.info(f"处理完成，结果已保存到: {output_csv}")
            
        except Exception as e:
            self.logger.error(f"处理文件夹时发生错误: {str(e)}")
            raise

    def _get_pdf_files(self, folder_path: str) -> List[str]:
        """获取文件夹中的所有PDF文件"""
        pdf_files = []
        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.lower().endswith('.pdf'):
                    pdf_files.append(os.path.join(root, file))
        return pdf_files

    def _extract_text_from_pdf(self, pdf_path: str) -> str:
        """从PDF中提取文本"""
        results = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if any(keyword in page_text for keyword in self.keywords) and "功能分类" not in page_text:
                        table = page.extract_table()
                        if table:
                            results.append(page_text)
                            results.append("--------------------------------------\n")
            return "".join(results)
        except Exception as e:
            self.logger.error(f"提取PDF文本时发生错误: {str(e)}")
            raise

    def _extract_financial_data(self, text: str) -> Dict[str, Dict[str, str]]:
        """使用AI提取财政数据"""
        try:
            client = OpenAI(
                api_key=OPENAI_API_KEY,
                base_url=OPENAI_BASE_URL
            )
            
            prompt = f""" 输入的文本数据，请你按照表格样式来理解，如果遇到"--------------------------------------"，说明到下一个表格了，如果该表格里出现了政府性基金字样，表明是政府性基金相关的数据，在提取诸如 政府性基金上级补助收入  金额时，如没有"政府性基金上级补助收入"字样，可提取 政府性基金数据下的上级补助收入 ，二者等价，其他的同理；
            一定要给"上级补助收入（政府性基金中出现的）"赋值（可以是负数），如果你识别不出来，就将其赋值为0
            其他要求：
            1.不区分市级和市本级，市级和市本级统一当作市级来处理
            2. 部分目标指标的名称是"政府性基金上级补助收入"，形如这样的目标指标是提取政府性基金相关表格中的上级补助收入，在政府性基金相关表格中搜索"上级补助收入"对应的金额即可
                -比如，在政府性基金段落中，出现了 上级补助收入，则对应的数字即为"政府性基金上级补助收入"，提取该数据，上级补助收入也可以是负数，可以提取
            3. 必须确保输出格式是输出格式为：
               全市-指标名称: 数值
                不允许输出"### 市级
                - 一般公共预算收入: 1611792
                - 税收收入: 797890
                - 增值税: 18774"
                相反，应该输出"
                市级-一般公共预算收入: 1611792
                市级-税收收入: 797890
                市级-增值税: 18774"
            目标指标：
            {"; ".join(self.target_items)}
            
            文本内容：
            {text[:MAX_TEXT_LENGTH]}  # 控制输入长度
            """
            
            response = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1  # 降低随机性
            )
            
            return self._parse_ai_response(response.choices[0].message.content)
            
        except Exception as e:
            self.logger.error(f"AI提取数据时发生错误: {str(e)}")
            raise


    def _parse_ai_response(self, response: str) -> Dict[str, Dict[str, str]]:
        """解析AI响应数据"""
        result = {"全市": {}}  # 保持使用"全市"作为key以保持与其他代码的一致性
        pattern = re.compile(r"^(市级|全市)-(.+?):\s*([\d.,]+)")  # 同时匹配"市级"和"全市"
        
        for line in response.split("\n"):
            match = pattern.search(line)
            if match:
                region, item, value = match.groups()
                item = item.strip()
                
                try:
                    num = float(value.replace(",", ""))
                    formatted = f"{num:.2f}"  # 只保留两位小数，不添加单位
                    
                    if item in self.target_items:
                        current = result["全市"].get(item)  # 统一使用"全市"作为key
                        if not current or num > float(current):  # 直接比较数值
                            result["全市"][item] = formatted
                except Exception as e:
                    self.logger.error(f"解析数值失败: {line} → {str(e)}")
                    
        return result

    def _batch_process_pdfs(self, pdf_files: List[str]) -> List[Dict]:
        """批量处理PDF文件"""
        results = []
        for pdf_path in tqdm(pdf_files, desc="处理进度"):
            try:
                text = self._extract_text_from_pdf(pdf_path)
                ai_data = self._extract_financial_data(text)
                results.append({
                    "文件名": os.path.basename(pdf_path),
                    "数据": ai_data
                })
            except Exception as e:
                self.logger.error(f"处理PDF失败: {os.path.basename(pdf_path)} → {str(e)}")
                
        return results

    def _save_to_csv(self, data: List[Dict], output_csv: str) -> None:
        """保存结果到CSV文件"""
        import csv
        try:
            with open(output_csv, "w", newline="", encoding="gbk") as f:
                writer = csv.writer(f)
                headers = ["文件夹名称"] + self.target_items  # 修改首格标题为"文件夹名称"
                writer.writerow(headers)
                
                for file_data in data:
                    # 从文件路径中提取最内层文件夹名称
                    folder_name = os.path.basename(os.path.dirname(file_data["文件名"]))
                    row = [folder_name]  # 使用文件夹名称替代文件名
                    region_data = file_data["数据"].get("全市", {})
                    row.extend([region_data.get(item, "") for item in self.target_items])
                    writer.writerow(row)
                    
        except Exception as e:
            self.logger.error(f"保存CSV文件时发生错误: {str(e)}")
            raise