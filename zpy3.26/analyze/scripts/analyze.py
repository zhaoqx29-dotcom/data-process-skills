"""
Analyze - 数据分析子 skill
负责数据加载、数据质量评估和生成分析报告

使用方式：
    from analyze import load_file, load_csv, load_excel, assess, generate_analysis_report

    # 加载数据
    df = load_file('data.csv')

    # 评估数据
    assess_result = assess(df)

    # 生成分析报告
    analysis_report = generate_analysis_report(df, assess_result, target_columns)
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional


def load_csv(path: str, encoding: str = "utf-8", **kwargs) -> pd.DataFrame:
    """加载 CSV 文件

    Args:
        path: CSV 文件路径
        encoding: 编码格式，默认 utf-8
        **kwargs: pandas.read_csv 的其他参数

    Returns:
        DataFrame
    """
    return pd.read_csv(path, encoding=encoding, **kwargs)


def load_excel(path: str, sheet_name: str = 0, **kwargs) -> pd.DataFrame:
    """加载 Excel 文件（支持 .xlsx, .xls）

    Args:
        path: Excel 文件路径
        sheet_name: 工作表名称或索引，默认第一个
        **kwargs: pandas.read_excel 的其他参数

    Returns:
        DataFrame
    """
    try:
        return pd.read_excel(path, sheet_name=sheet_name, **kwargs)
    except ImportError:
        raise ImportError(
            "openpyxl or xlrd is required for reading Excel files. "
            "Install with: pip install openpyxl"
        )


def load_file(path: str, **kwargs) -> pd.DataFrame:
    """智能加载文件，自动识别文件类型

    根据文件扩展名自动选择加载方式：
    - .csv -> load_csv
    - .xlsx/.xls -> load_excel

    Args:
        path: 文件路径
        **kwargs: 传递给底层加载函数的参数

    Returns:
        DataFrame
    """
    if path.endswith('.csv'):
        return load_csv(path, **kwargs)
    elif path.endswith('.xlsx') or path.endswith('.xls'):
        return load_excel(path, **kwargs)
    else:
        raise ValueError(f"Unsupported file format: {path}. Supported formats: .csv, .xlsx, .xls")


def assess(df: pd.DataFrame) -> Dict:
    """数据质量评估

    Args:
        df: 输入 DataFrame

    Returns:
        评估报告字典
    """
    report = {
        "shape": df.shape,
        "columns": list(df.columns),
        "dtypes": df.dtypes.astype(str).to_dict(),
        "missing": {},
        "duplicates": int(df.duplicated().sum()),
        "column_analysis": {},
        "missing_analysis": {},
    }

    for col in df.columns:
        # 缺失值统计
        missing_count = int(df[col].isna().sum())
        missing_pct = round(missing_count / len(df) * 100, 2)
        report["missing"][col] = {
            "count": missing_count,
            "percentage": missing_pct
        }

        # 缺失值模式分析
        if missing_count > 0:
            missing_mask = df[col].isna()
            start_missing = missing_mask.iloc[:min(5, len(df))].any()

            max_consecutive_missing = 0
            current_consecutive = 0
            for is_missing in missing_mask:
                if is_missing:
                    current_consecutive += 1
                    max_consecutive_missing = max(max_consecutive_missing, current_consecutive)
                else:
                    current_consecutive = 0

            is_continuous_small_missing = max_consecutive_missing <= 3 and missing_pct <= 5
            is_continuous_large_missing = max_consecutive_missing > 3

            missing_indices = missing_mask[missing_mask].index
            if len(missing_indices) > 0:
                first_missing_idx = missing_indices[0]
                missing_count_ratio = len(missing_indices) / len(df)
                is_at_start = (first_missing_idx <= 2 and
                              missing_count_ratio <= 0.05 and
                              max_consecutive_missing <= 5)
            else:
                is_at_start = False

            report["missing_analysis"][col] = {
                "max_consecutive_missing": max_consecutive_missing,
                "is_continuous_small_missing": is_continuous_small_missing,
                "is_continuous_large_missing": is_continuous_large_missing,
                "is_at_start": is_at_start,
                "start_missing": bool(start_missing),
            }
        else:
            report["missing_analysis"][col] = {
                "max_consecutive_missing": 0,
                "is_continuous_small_missing": False,
                "is_continuous_large_missing": False,
                "is_at_start": False,
                "start_missing": False,
            }

        # 列详细分析
        col_data = df[col].dropna()
        if len(col_data) > 0:
            unique_count = col_data.nunique()
            dtype_str = str(df[col].dtype)

            characteristics = []
            if dtype_str in ["int64", "float64", "int32", "float32"]:
                characteristics.append("数值型")
                skewness = col_data.skew()
                if abs(skewness) > 1:
                    characteristics.append("偏态分布" if skewness > 0 else "左")
                    characteristics.append(f"偏度:{skewness:.2f}")
                elif abs(skewness) < 0.3:
                    characteristics.append("正态分布")

                Q1 = col_data.quantile(0.25)
                Q3 = col_data.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                outliers = ((col_data < lower_bound) | (col_data > upper_bound)).sum()
                outlier_ratio = outliers / len(col_data)
                if outlier_ratio > 0.05:
                    characteristics.append("存在显著异常值")

                if len(col_data) > 2:
                    diff = col_data.diff().abs()
                    mean_diff = diff.mean()
                    if mean_diff > 0 and (diff.std() / mean_diff) < 0.5:
                        characteristics.append("平滑趋势")

                if len(col_data) > 5:
                    if col_data.mean() != 0:
                        cv = col_data.std() / abs(col_data.mean())
                        if cv < 0.1:
                            characteristics.append("平稳数据")
                        elif cv < 0.3:
                            characteristics.append("中等波动")

                if len(col_data) > 10:
                    diff = col_data.diff().dropna()
                    if len(diff) > 0:
                        if diff.std() / diff.mean() < 0.5:
                            characteristics.append("周期性数据")
            elif dtype_str == "object" or dtype_str == "str":
                characteristics.append("文本型")
                if unique_count < len(col_data) * 0.1:
                    characteristics.append("类别型")
                mode_val = col_data.mode()
                if len(mode_val) > 0:
                    mode_count = (col_data == mode_val[0]).sum()
                    mode_ratio = mode_count / len(col_data)
                    if mode_ratio > 0.4:
                        characteristics.append(f"众数占比{mode_ratio*100:.0f}%")
                if any(keyword in col.lower() for keyword in ["date", "time", "timestamp"]):
                    characteristics.append("时间序列")
            else:
                characteristics.append("其他类型")

            if dtype_str in ["int64", "float64", "int32", "float32"] and len(col_data) > 1:
                if "平滑趋势" in characteristics or "正态分布" in characteristics:
                    characteristics.append("随时间缓慢变化")

            report["column_analysis"][col] = {
                "unique_count": int(unique_count),
                "characteristics": characteristics,
                "dtype": dtype_str
            }
        else:
            report["column_analysis"][col] = {
                "unique_count": 0,
                "characteristics": ["全缺失"],
                "dtype": str(df[col].dtype)
            }

    return report


def generate_analysis_report(df: pd.DataFrame, assess_result: Dict, target_columns: List[str],
                              file_path: str = "", save_path: str = "") -> str:
    """生成文字版数据分析报告

    Args:
        df: DataFrame
        assess_result: 评估结果
        target_columns: 目标列列表
        file_path: 文件路径
        save_path: 保存路径，如果提供则自动保存为 Markdown 文件

    Returns:
        Markdown 格式的分析报告
    """
    rows, cols = assess_result["shape"]
    target_count = len(target_columns)

    report_lines = [
        "📊 数据分析报告",
        "",
        "### 数据概览",
        f"• 数据文件: {file_path}",
        f"• 数据形状: {rows} 行 × {cols} 列",
        f"• 处理涉及列: {target_count} 列",
        "",
        "---",
        "",
        "### 指定列分析结果",
        ""
    ]

    for col in target_columns:
        if col not in df.columns:
            continue

        missing_info = assess_result["missing"].get(col, {"count": 0, "percentage": 0})
        col_info = assess_result["column_analysis"].get(col, {"unique_count": 0, "characteristics": [], "dtype": "unknown"})

        report_lines.extend([
            f"【列名：{col}】",
            f"• 数据类型: {col_info.get('dtype', 'unknown')}",
            f"• 缺失值数量: {missing_info['count']}",
            f"• 缺失率: {missing_info['percentage']}%",
            f"• 取值数量: {col_info.get('unique_count', 0)}",
            f"• 数据特征: {', '.join(col_info.get('characteristics', []))}",
            ""
        ])

    # 数据质量评估
    report_lines.extend([
        "---",
        "",
        "### 数据质量评估",
    ])

    # 生成完整性评估
    missing_cols = [col for col in target_columns
                   if assess_result["missing"].get(col, {}).get("count", 0) > 0]
    if not missing_cols:
        completeness = "所有指定列无缺失值"
    else:
        missing_rates = [assess_result["missing"][col]["percentage"] for col in missing_cols]
        avg_missing = sum(missing_rates) / len(missing_rates)
        if avg_missing < 5:
            completeness = f"存在 {len(missing_cols)} 列有少量缺失，完整性良好"
        elif avg_missing < 30:
            completeness = f"存在 {len(missing_cols)} 列有中等程度缺失，建议处理"
        else:
            completeness = f"存在 {len(missing_cols)} 列缺失率较高，需重点关注"

    # 潜在问题
    issues = []
    for col in target_columns:
        col_info = assess_result["column_analysis"].get(col, {})
        characteristics = col_info.get("characteristics", [])
        if "存在显著异常值" in characteristics:
            issues.append(f"{col} 存在显著异常值")
        if "偏态分布" in characteristics:
            issues.append(f"{col} 呈偏态分布")

    report_lines.append(f"• 完整性评估: {completeness}")
    if issues:
        report_lines.append(f"• 潜在问题: {', '.join(issues)}")
    else:
        report_lines.append("• 潜在问题: 无明显问题")

    report_content = "\n".join(report_lines)

    # 如果提供了保存路径，自动保存为 Markdown 文件
    if save_path:
        with open(save_path, 'w', encoding='utf-8') as f:
            f.write(report_content)

    return report_content


if __name__ == "__main__":
    print("Analyze - 数据分析子 skill")
    print("\n支持的函数：")
    print("  - load_file: 智能加载数据文件")
    print("  - load_csv: 加载 CSV 文件")
    print("  - load_excel: 加载 Excel 文件")
    print("  - assess: 数据质量评估")
    print("  - generate_analysis_report: 生成分析报告")
