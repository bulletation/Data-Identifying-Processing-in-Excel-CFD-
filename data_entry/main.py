from folder_scanner import FolderScanner
from data_processor import DataProcessor
import os
import csv

# 定义目标指标列表
TARGET_ITEMS = [
    "一般公共预算收入",
    "税收收入",
    "增值税",
    "营业税",
    "企业所得税",
    "个人所得税",
    "资源税",
    "城市维护建设税",
    "房产税",
    "印花税",
    "城镇土地使用税",
    "土地增值税",
    "车船税",
    "耕地占用税",
    "契税",
    "烟叶税",
    "环境保护税",
    "非税收入",
    "专项收入",
    "行政事业性收费收入",
    "罚没收入",
    "国有资本经营收入",
    "国有资源(资产)有偿使用收入",
    "一般公共预算支出",
    "一般公共服务支出",
    "外交支出",
    "国防支出",
    "公共安全支出",
    "教育支出",
    "科学技术支出",
    "文化体育与传媒支出",
    "社会保障和就业支出",
    "医疗卫生与计划生育支出",
    "节能环保支出",
    "城乡社区支出",
    "农林水支出",
    "交通运输支出",
    "资源勘探信息等支出",
    "商业服务业等支出",
    "金融支出",
    "援助其他地区支出",
    "国土海洋气象等支出",
    "住房保障支出",
    "粮油物资储备支出",
    "债务付息支出",
    "上级补助收入",
    "返还性收入",
    "一般性转移支付收入",
    "专项转移支付收入",
    "上年结余资金",
    "调入资金",
    "债务转贷收入",
    "一般公共预算总收入",
    "债务还本支出",
    "一般公共预算总支出",
    "政府性基金收入",
    "政府性基金预算收入",
    "国有土地使用权出让收入",
    "上级补助收入（政府性基金中出现的）",
    "政府性基金债务转贷收入",
    "政府性基金总收入",
    "政府性基金支出",
    "政府性基金债务付息支出",
    "政府性基金债务还本支出",
    "政府性基金调出资金",
    "政府性基金总支出",
    "政府债务限额",
    "一般债务限额",
    "专项债务限额",
    "政府债务余额",
    "一般债务余额",
    "专项债务余额"
]

def main():
    # 设置根目录和输出文件路径
    root_path = r"C:\CFD项目自动化尝试\广东省\潮州市"  # 您可以根据实际情况修改路径
    
    # 获取最后一个文件夹名称（城市名）
    city_name = os.path.basename(root_path)
    # 构建输出文件名
    output_csv = os.path.join(os.path.dirname(root_path), f"财政数据汇总{city_name}.csv")
    
    try:
        # 初始化文件夹扫描器和数据处理器
        scanner = FolderScanner(root_path)
        processor = DataProcessor(TARGET_ITEMS)
        
        # 扫描所有最内层文件夹
        folders = scanner.scan_folders()
        
        # 创建汇总CSV文件并写入表头
        with open(output_csv, "w", newline="", encoding="gbk") as f:
            writer = csv.writer(f)
            headers = ["文件夹名称"] + TARGET_ITEMS
            writer.writerow(headers)
            
            # 处理每个文件夹中的PDF文件
            for folder_path, folder_name in folders:
                if scanner.validate_folder(folder_path):
                    print(f"正在处理文件夹: {folder_name}")
                    try:
                        # 获取该文件夹中的所有PDF文件
                        pdf_files = []
                        for file in os.listdir(folder_path):
                            if file.lower().endswith('.pdf'):
                                pdf_files.append(os.path.join(folder_path, file))
                        
                        # 无论是否有PDF文件，都创建一行数据
                        row = [folder_name]
                        
                        if not pdf_files:
                            print(f"警告：文件夹 {folder_name} 中没有找到PDF文件")
                            # 填充空值
                            row.extend(["" for _ in TARGET_ITEMS])
                        else:
                            # 处理PDF文件并获取数据
                            text = ""
                            for pdf_file in pdf_files:
                                text += processor._extract_text_from_pdf(pdf_file)
                            
                            if text:
                                # 提取财政数据
                                data = processor._extract_financial_data(text)
                                region_data = data.get("全市", {})
                                row.extend([region_data.get(item, "") for item in TARGET_ITEMS])
                            else:
                                # 如果没有提取到文本，填充空值
                                row.extend(["" for _ in TARGET_ITEMS])
                        
                        # 写入CSV
                        writer.writerow(row)
                        print(f"已完成文件夹 {folder_name} 的处理并写入CSV")
                        
                    except Exception as e:
                        print(f"处理文件夹 {folder_name} 时发生错误: {str(e)}")
                        continue
                else:
                    print(f"无法访问文件夹: {folder_path}")
                
        print(f"所有文件夹处理完成！结果已保存到: {output_csv}")
        
    except Exception as e:
        print(f"处理过程中发生错误: {str(e)}")

if __name__ == "__main__":
    main()
