ta Engineering Operations - 原子能力
数据加载、评估、清洗（缺失值填充、滤波降噪、标准化）

使用方式：
    from data_ops import (
        # 数据加载与保存
        load_csv, load_excel, load_file,
        save_csv, save_excel, save_file,
        # 数据评估
        assess,
        # 缺失值处理
        fill_missing_ffill, fill_missing_bfill,
        fill_missing_mean, fill_missing_median,
        fill_missing_mode, fill_missing_value,
        fill_missing_interpolate,
        # 滤波降噪
        filter_median, filter_moving_avg, filter_fourier,
        # 标准化/归一化
        normalize_minmax, normalize_custom_range, normalize_log
    )

    # 加载数据
    df = load_csv('data.csv')

    # 评估数据
    report = assess(df)

    # 填充缺失值
    df = fill_missing_mean(df, ['amount', 'price'])
    df = fill_missing_ffill(df, ['temperature'])
    df = fill_missing_bfill(df, ['temperature'])
    df = fill_missing_mode(df, ['category'])
    df = fill_missing_interpolate(df, ['sensor_value'])

    # 滤波降噪
    df = filter_median(df, ['signal'], window_size=5)
    df = filter_moving_avg(df, ['signal'], window_size=10)
    df = filter_fourier(df, ['signal'], cutoff_freq=0.1)

    # 标准化
    df = normalize_minmax(df, ['feature1', 'feature2'])
    df = normalize_custom_range(df, ['value'], min_val=0, max_val=100)
    df = normalize_log(df, ['price', 'income'])

    # 保存数据
    save_csv(df, 'output.csv')
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Union
from scipy import signal
from scipy.fft import fft, ifft, fftfreq


def load_csv(path: str, encoding: str = "utf-8", **kwargs) -> pd.DataFrame:
    """加载 CSV 文件

    Args:
        path: CSV 文件路径
        encoding: 编码格式，默认 utf-8
        **kwargs: pandas.read_csv 的其他参数

    Returns:
        DataFrame

    Example:
        df = load_csv('data/sales.csv')
        df = load_csv('data.csv', sep=';', nrows=100)
    """
    return pd.read_csv(path, encoding=encoding, **kwargs)


def save_csv(df: pd.DataFrame, path: str, encoding: str = "utf-8", index: bool = False) -> None:
    """保存为 CSV 文件

    Args:
        df: 要保存的 DataFrame
        path: 保存路径
        encoding: 编码格式，默认 utf-8
        index: 是否保存索引

    Example:
        save_csv(df, 'output.csv')
        save_csv(df, 'data.csv', index=True)
    """
    df.to_csv(path, encoding=encoding, index=index)


def load_excel(path: str, sheet_name: str = 0, **kwargs) -> pd.DataFrame:
    """加载 Excel 文件（支持 .xlsx, .xls）

    Args:
        path: Excel 文件路径
        sheet_name: 工作表名称或索引，默认第一个
        **kwargs: pandas.read_excel 的其他参数

    Returns:
        DataFrame

    Example:
        df = load_excel('data.xlsx', sheet_name='Sheet1')
        df = load_excel('data.xlsx', sheet_name=0)
    """
    try:
        return pd.read_excel(path, sheet_name=sheet_name, **kwargs)
    except ImportError:
        raise ImportError(
            "openpyxl or xlrd is required for reading Excel files. "
            "Install with: pip install openpyxl"
        )


def save_excel(df: pd.DataFrame, path: str, sheet_name: str = 'Sheet1', index: bool = False) -> None:
    """保存为 Excel 文件（支持 .xlsx, .xls）

    Args:
        df: 要保存的 DataFrame
        path: 保存路径
        sheet_name: 工作表名称，默认 'Sheet1'
        index: 是否保存索引

    Example:
        save_excel(df, 'output.xlsx')
        save_excel(df, 'data.xlsx', sheet_name='Sheet2', index=True)
    """
    try:
        df.to_excel(path, sheet_name=sheet_name, index=index)
    except ImportError:
        raise ImportError(
            "openpyxl is required for writing Excel files. "
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

    Example:
        df = load_file('data.csv')
        df = load_file('data.xlsx', sheet_name='Sheet1')
    """
    if path.endswith('.csv'):
        return load_csv(path, **kwargs)
    elif path.endswith('.xlsx') or path.endswith('.xls'):
        return load_excel(path, **kwargs)
    else:
        raise ValueError(f"Unsupported file format: {path}. Supported formats: .csv, .xlsx, .xls")


def save_file(df: pd.DataFrame, path: str, **kwargs) -> None:
    """智能保存文件，自动识别文件类型

    根据文件扩展名自动选择保存方式：
    - .csv -> save_csv
    - .xlsx/.xls -> save_excel

    Args:
        df: 要保存的 DataFrame
        path: 保存路径
        **kwargs: 传递给底层保存函数的参数

    Example:
        save_file(df, 'output.csv')
        save_file(df, 'output.xlsx', sheet_name='Sheet1')
    """
    if path.endswith('.csv'):
        return save_csv(df, path, **kwargs)
    elif path.endswith('.xlsx') or path.endswith('.xls'):
        return save_excel(df, path, **kwargs)
    else:
        raise ValueError(f"Unsupported file format: {path}. Supported formats: .csv, .xlsx, .xls")


def assess(df: pd.DataFrame) -> Dict:
    """数据质量评估

    Args:
        df: 输入 DataFrame

    Returns:
        评估报告字典，包含：
        - shape: 数据形状
        - columns: 列名列表
        - dtypes: 数据类型
        - missing: 缺失值统计（每列的缺失数和缺失率）
        - duplicates: 重复行数
        - column_analysis: 每列的详细分析（取值数量、数据特征等）
        - missing_analysis: 缺失值模式分析（连续缺失、开头缺失等）

    Example:
        report = assess(df)
        print(report['missing'])
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

            # 检测是否在开头出现缺失（前5行内）
            start_missing = missing_mask.iloc[:min(5, len(df))].any()

            # 检测连续缺失的最大长度
            max_consecutive_missing = 0
            current_consecutive = 0
            for is_missing in missing_mask:
                if is_missing:
                    current_consecutive += 1
                    max_consecutive_missing = max(max_consecutive_missing, current_consecutive)
                else:
                    current_consecutive = 0

            # 判断是否为连续少量缺失（最大连续缺失 <= 3 且 缺失率 <= 5%）
            is_continuous_small_missing = max_consecutive_missing <= 3 and missing_pct <= 5

            # 判断是否为连续多行缺失（最大连续缺失 > 3）
            is_continuous_large_missing = max_consecutive_missing > 3

            # 判断缺失位置（开头、中间、结尾）
            missing_indices = missing_mask[missing_mask].index
            if len(missing_indices) > 0:
                first_missing_idx = missing_indices[0]
                last_missing_idx = missing_indices[-1]
                # 开头缺失：
                # 1. 缺失出现在前3行内（更严格的判断）
                # 2. 且缺失总数较少（不超过总行数的5%，避免随机缺失误判）
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

            # 识别数据特征
            characteristics = []
            if dtype_str in ["int64", "float64", "int32", "float32"]:
                characteristics.append("数值型")
                # 检测分布偏度
                skewness = col_data.skew()
                if abs(skewness) > 1:
                    characteristics.append("偏态分布" if skewness > 0 else "左")
                    characteristics.append(f"偏度:{skewness:.2f}")
                elif abs(skewness) < 0.3:
                    characteristics.append("正态分布")

                # 检测异常值（使用IQR方法）
                Q1 = col_data.quantile(0.25)
                Q3 = col_data.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                outliers = ((col_data < lower_bound) | (col_data > upper_bound)).sum()
                outlier_ratio = outliers / len(col_data)
                if outlier_ratio > 0.05:  # 超过5%为异常值比例高
                    characteristics.append("存在显著异常值")

                # 检测线性或平滑趋势（相邻行差异的稳定性）
                if len(col_data) > 2:
                    diff = col_data.diff().abs()
                    mean_diff = diff.mean()
                    # 如果差异较小（< 均值的20%），认为有平滑趋势
                    if mean_diff > 0 and (diff.std() / mean_diff) < 0.5:
                        characteristics.append("平滑趋势")

                # 检测波动程度（判断随机小幅波动噪声）
                if len(col_data) > 5:
                    # 使用变异系数（标准差/均值）判断波动程度
                    if col_data.mean() != 0:
                        cv = col_data.std() / abs(col_data.mean())
                        if cv < 0.1:  # 变异系数小于0.1，数据较平稳
                            characteristics.append("平稳数据")
                        elif cv < 0.3:  # 变异系数0.1-0.3，中等波动
                            characteristics.append("中等波动")

                # 检测是否适合周期性分析（周期性强的数据）
                if len(col_data) > 10:
                    # 简单的周期性检测：检查相邻变化的自相关性
                    diff = col_data.diff().dropna()
                    if len(diff) > 0:
                        # 使用差分的标准差判断周期性
                        if diff.std() / diff.mean() < 0.5:
                            characteristics.append("周期性数据")
            elif dtype_str == "object" or dtype_str == "str":
                characteristics.append("文本型")
                if unique_count < len(col_data) * 0.1:
                    characteristics.append("类别型")
                # 检测众数占比
                mode_val = col_data.mode()
                if len(mode_val) > 0:
                    mode_count = (col_data == mode_val[0]).sum()
                    mode_ratio = mode_count / len(col_data)
                    if mode_ratio > 0.4:
                        characteristics.append(f"众数占比{mode_ratio*100:.0f}%")
                # 检测是否为日期/时间
                if any(keyword in col.lower() for keyword in ["date", "time", "timestamp"]):
                    characteristics.append("时间序列")
            else:
                characteristics.append("其他类型")

            # 判断是否随时间缓慢变化（数值型数据，变化率低）
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


def recommend_strategy(df: pd.DataFrame, columns: List[str], business_context: str = "general",
                     process_type: str = "all") -> Dict:
    """智能推荐数据处理策略

    根据数据特征和业务场景自动推荐最佳处理方法

    Args:
        df: 输入 DataFrame
        columns: 需要分析的列名列表
        business_context: 业务场景描述，如 "sensor_data"、"time_series"、"general"
        process_type: 处理类型，"all"(全部), "missing"(缺失值), "filter"(滤波降噪), "normalize"(标准化)

    Returns:
        推荐策略字典，每列包含推荐方法和说明

    Example:
        strategies = recommend_strategy(df, ['temperature', 'humidity'], 'sensor_data')
        print(strategies['temperature']['recommended_method'])
        # 仅推荐滤波降噪策略
        strategies = recommend_strategy(df, ['signal'], 'sensor_data', process_type='filter')
    """
    strategies = {}
    report = assess(df)

    for col in columns:
        if col not in df.columns:
            continue

        col_info = {
            "recommended_method": None,
            "reason": "",
            "missing_rate": report["missing"][col]["percentage"],
            "data_type": report["column_analysis"][col]["dtype"],
            "characteristics": report["column_analysis"][col]["characteristics"],
            "missing_analysis": report["missing_analysis"].get(col, {})
        }

        missing_rate = col_info["missing_rate"]
        characteristics = col_info["characteristics"]
        dtype_str = col_info["data_type"]
        missing_analysis = col_info["missing_analysis"]

        # 检查是否为数值类型
        is_numeric = dtype_str in ["int64", "float64", "int32", "float32"]

        # ===== 滤波降噪推荐 =====
        if process_type in ["all", "filter"]:
            # 只对数值型数据进行滤波推荐
            if is_numeric:
                has_significant_outliers = "存在显著异常值" in characteristics
                is_stable = "平稳数据" in characteristics
                is_moderate_fluctuation = "中等波动" in characteristics
                is_smooth_trend = "平滑趋势" in characteristics
                is_periodic = "周期性数据" in characteristics

                # 1. 中值滤波 - 条件：专治孤立尖峰、脉冲噪声 + 适配离散数据 + 不模糊数据边缘
                if has_significant_outliers:
                    col_info["recommended_method"] = "filter_median"
                    col_info["reason"] = "检测到显著异常值（孤立尖峰/脉冲噪声），推荐使用中值滤波，专治脉冲噪声且不模糊数据边缘"
                    if process_type == "filter":
                        strategies[col] = col_info
                        continue

                # 2. 移动平均滤波 - 条件：抑制随机小幅波动噪声 + 适配平稳/周期性数据
                elif is_moderate_fluctuation or (is_stable and is_smooth_trend):
                    window_size = 5 if is_moderate_fluctuation else 10
                    col_info["recommended_method"] = "filter_moving_avg"
                    col_info["reason"] = f"存在随机小幅波动噪声，推荐使用移动平均滤波（窗口大小={window_size}），窗口越大平滑越强"
                    if process_type == "filter":
                        strategies[col] = col_info
                        continue

                # 3. 移动平均滤波（平稳数据）
                elif is_stable:
                    col_info["recommended_method"] = "filter_moving_avg"
                    col_info["reason"] = "数据较平稳，推荐使用移动平均滤波抑制随机小幅波动噪声"
                    if process_type == "filter":
                        strategies[col] = col_info
                        continue

                # 4. 傅里叶变换滤波 - 条件：从频率维度分离噪声/信号 + 适配周期性强的数据
                elif is_periodic or "时间序列" in characteristics:
                    col_info["recommended_method"] = "filter_fourier"
                    col_info["reason"] = "数据周期性较强，推荐使用傅里叶变换滤波从频率维度分离噪声与信号"
                    if process_type == "filter":
                        strategies[col] = col_info
                        continue

                # 5. 默认滤波策略 - 使用移动平均
                elif not col_info["recommended_method"]:
                    col_info["recommended_method"] = "filter_moving_avg"
                    col_info["reason"] = "数值型数据，推荐使用移动平均滤波抑制随机小幅波动噪声"

        # ===== 缺失值处理推荐 =====
        if process_type in ["all", "missing"]:
            # 如果已经有滤波推荐且不是"all"模式，跳过缺失值推荐
            if process_type == "all" and col_info["recommended_method"]:
                # all模式需要同时返回缺失值和滤波推荐，这里继续处理缺失值
                pass

            # 检查是否有缺失值需要处理
            if missing_rate == 0:
                if not col_info["recommended_method"]:
                    col_info["recommended_method"] = "no_action"
                    col_info["reason"] = "该列无缺失值"
                strategies[col] = col_info
                continue

            # 检查是否为类别列
            is_categorical = dtype_str in ["object", "str"] or "类别型" in characteristics

            # 检查是否为时间序列
            # 注意：时间序列判断不包括对象类型（类别数据不应被视为时间序列）
            is_time_series = (not is_categorical and
                            ("时间序列" in characteristics or
                             any(keyword in col.lower() for keyword in ["date", "time", "timestamp"]) or
                             "sensor_data" in business_context or "time_series" in business_context))

            # 提取缺失值模式分析特征
            is_continuous_small_missing = missing_analysis.get("is_continuous_small_missing", False)
            is_continuous_large_missing = missing_analysis.get("is_continuous_large_missing", False)
            is_at_start = missing_analysis.get("is_at_start", False)
            start_missing = missing_analysis.get("start_missing", False)
            max_consecutive_missing = missing_analysis.get("max_consecutive_missing", 0)

            # ===== 缺失值处理推荐（优先级顺序）=====
            # 注意：优先级从高到低，一旦匹配就不再判断后续条件

            # 0. 高缺失率检查（最高优先级）
            if missing_rate > 30:
                col_info["recommended_method"] = "ask_user"
                col_info["reason"] = f"缺失率过高({missing_rate}%)，建议由用户确认处理方式或考虑删除该列"

            # 1. 类别型数据优先判断（类别数据有特定处理方式）
            elif is_categorical:
                # 检查众数占比
                mode_ratio_check = any("众数占比" in c and "%" in c for c in characteristics)
                if mode_ratio_check:
                    col_info["recommended_method"] = "fill_missing_mode"
                    col_info["reason"] = "类别型数据，众数占比超过40%，推荐使用众数填充"
                else:
                    # 类别数据但众数占比不高，仍用众数（作为类别数据的默认方法）
                    col_info["recommended_method"] = "fill_missing_mode"
                    col_info["reason"] = "类别型数据，推荐使用众数填充（最频繁值）"

            # 2. 后向填充 (bfill) - 条件：缺失值出现在数据开头 + 缺失块很短（<=3行）
            #    优先级高于长时间缺失的插值判断，因为短开头缺失用后向填充很简单有效
            elif is_at_start and start_missing and max_consecutive_missing <= 3:
                col_info["recommended_method"] = "fill_missing_bfill"
                col_info["reason"] = "缺失值出现在数据开头且缺失块很短，推荐使用后向填充"

            # 3. 线性插值 (interpolate) - 条件：连续多行缺失（>3行）+ 明显的线性或平滑趋势
            #    注意：对于开头连续缺失>3行的情况，如果数据有明显趋势，插值比bfill更合理
            elif is_continuous_large_missing and ("平滑趋势" in characteristics or
                                              ("随时间缓慢变化" in characteristics) or
                                              is_time_series):
                col_info["recommended_method"] = "fill_missing_interpolate"
                col_info["reason"] = "存在连续多行缺失且数据有明显线性或平滑趋势，推荐使用线性插值"

            # 4. 后向填充 (bfill) - 条件：缺失值出现在数据开头 + 缺失块较短（4-5行）
            #    对于4-5行的开头缺失，如果没有更明显的趋势特征，使用bfill
            elif is_at_start and start_missing and max_consecutive_missing <= 5:
                col_info["recommended_method"] = "fill_missing_bfill"
                col_info["reason"] = "缺失值出现在数据开头且缺失块较短，推荐使用后向填充"

            # 3. 前向填充 (ffill) - 条件：连续少量缺失 + 随时间缓慢变化
            elif is_continuous_small_missing and ("随时间缓慢变化" in characteristics or
                                              "平滑趋势" in characteristics or
                                              is_time_series):
                col_info["recommended_method"] = "fill_missing_ffill"
                col_info["reason"] = "缺失值为连续少量缺失且数据随时间缓慢变化，推荐使用前向填充保留时间趋势"

            # 5. 存在异常值或偏态分布 -> 中位数填充
            #    优先级高于常规的分布判断，优先处理异常值问题
            elif not is_categorical:
                has_outliers = "存在显著异常值" in characteristics
                is_skewed = "偏态分布" in characteristics

                if has_outliers or is_skewed:
                    col_info["recommended_method"] = "fill_missing_median"
                    if has_outliers:
                        col_info["reason"] = "数值型数据存在显著异常值，推荐使用对离群值不敏感的中位数填充"
                    else:
                        col_info["reason"] = "数值型数据呈偏态分布，推荐使用中位数填充"

            # 6. 正态分布 -> 均值填充
            if "正态分布" in characteristics and not col_info["recommended_method"]:
                col_info["recommended_method"] = "fill_missing_mean"
                col_info["reason"] = "数值型数据接近正态分布，推荐使用均值填充"

            # 7. 默认策略：中位数填充（更稳健）
            if not is_categorical and not col_info["recommended_method"]:
                col_info["recommended_method"] = "fill_missing_median"
                col_info["reason"] = f"数值型数据，缺失率{missing_rate}%，推荐使用中位数填充"

            # 8. 其他情况默认使用众数填充
            if not col_info["recommended_method"]:
                col_info["recommended_method"] = "fill_missing_mode"
                col_info["reason"] = "推荐使用众数填充"

        # 如果没有在 missing 分支中设置，且还没有设置推荐方法，则跳过
        if not col_info["recommended_method"] and process_type == "missing":
            continue

        strategies[col] = col_info

    return strategies


def fill_missing_ffill(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """用前向填充（forward fill）填充指定列的缺失值

    适合时间序列数据，用前一个有效值填充缺失值

    Args:
        df: 输入 DataFrame
        columns: 需要填充的列名列表

    Returns:
        填充后的 DataFrame

    Example:
        df = fill_missing_ffill(df, ['temperature', 'humidity'])
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].ffill()
    return df


def fill_missing_bfill(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """用后向填充（backward fill）填充指定列的缺失值

    适合时间序列数据，用后一个有效值填充缺失值

    Args:
        df: 输入 DataFrame
        columns: 需要填充的列名列表

    Returns:
        填充后的 DataFrame

    Example:
        df = fill_missing_bfill(df, ['temperature', 'humidity'])
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].bfill()
    return df


def fill_missing_mean(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """用均值填充指定列的缺失值

    适合数值列、正态分布的数据

    Args:
        df: 输入 DataFrame
        columns: 需要填充的列名列表

    Returns:
        填充后的 DataFrame

    Example:
        df = fill_missing_mean(df, ['price', 'score'])
    """
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            df[col] = df[col].fillna(df[col].mean())
    return df


def fill_missing_median(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """用中位数填充指定列的缺失值

    适合数值列、偏态分布的数据

    Args:
        df: 输入 DataFrame
        columns: 需要填充的列名列表

    Returns:
        填充后的 DataFrame

    Example:
        df = fill_missing_median(df, ['income', 'age'])
    """
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            df[col] = df[col].fillna(df[col].median())
    return df


def fill_missing_mode(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """用众数填充指定列的缺失值

    适合类别列，使用最频繁的值填充

    Args:
        df: 输入 DataFrame
        columns: 需要填充的列名列表

    Returns:
        填充后的 DataFrame

    Example:
        df = fill_missing_mode(df, ['category', 'city'])
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            mode_val = df[col].mode()
            if len(mode_val) > 0:
                df[col] = df[col].fillna(mode_val[0])
    return df


def fill_missing_value(df: pd.DataFrame, column: str, value: any) -> pd.DataFrame:
    """用指定值填充某列的缺失值

    适合特定业务含义的填充，如 "未知"、0 等

    Args:
        df: 输入 DataFrame
        column: 列名
        value: 填充值

    Returns:
        填充后的 DataFrame

    Example:
        df = fill_missing_value(df, 'city', '未知')
        df = fill_missing_value(df, 'count', 0)
    """
    df = df.copy()
    if column in df.columns:
        df[column] = df[column].fillna(value)
    return df


def get_fill_summary(df_before: pd.DataFrame, df_after: pd.DataFrame, fill_dict: Dict[str, str]) -> List[Dict]:
    """获取填充操作的摘要信息

    Args:
        df_before: 填充前的 DataFrame
        df_after: 填充后的 DataFrame
        fill_dict: 列名到填充方法的映射，如 {'column1': 'mean', 'column2': 'ffill'}

    Returns:
        填充摘要列表，每项包含：列名、填充方法、填充数量
    """
    summary = []
    for col, method in fill_dict.items():
        if col in df_before.columns:
            missing_before = df_before[col].isna().sum()
            missing_after = df_after[col].isna().sum()
            filled = int(missing_before - missing_after)
            summary.append({
                "column": col,
                "method": method,
                "filled_count": filled
            })
    return summary


# ==================== 缺失值处理 - 线性插值 ====================

def fill_missing_interpolate(df: pd.DataFrame, columns: List[str], method: str = 'linear',
                           limit_direction: str = 'both') -> pd.DataFrame:
    """使用线性插值填充指定列的缺失值

    适合数值型数据的线性趋势插值，支持多种插值方法

    Args:
        df: 输入 DataFrame
        columns: 需要填充的列名列表
        method: 插值方法，可选: 'linear', 'nearest', 'zero', 'slinear', 'quadratic', 'cubic' 等
        limit_direction: 插值方向，'forward', 'backward', 或 'both'

    Returns:
        填充后的 DataFrame

    Example:
        df = fill_missing_interpolate(df, ['temperature', 'pressure'], method='linear')
        df = fill_missing_interpolate(df, ['sensor_value'], method='cubic')
    """
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            df[col] = df[col].interpolate(method=method, limit_direction=limit_direction)
    return df


# ==================== 滤波降噪 ====================

def filter_median(df: pd.DataFrame, columns: List[str], window_size: int = 3) -> pd.DataFrame:
    """中值滤波，专治孤立尖峰和脉冲噪声

    核心适用特征：
    1. 专治孤立尖峰、脉冲噪声：对异常值（离群点）非常有效，是非线性滤波器
    2. 适配离散数据：适合处理离散数据，不会引入虚假值
    3. 不模糊数据边缘：能够较好地保留数据边缘和突变特征

    Args:
        df: 输入 DataFrame
        columns: 需要滤波的列名列表
        window_size: 滑动窗口大小，必须为奇数，默认 3

    Returns:
        滤波后的 DataFrame

    Example:
        # 去除传感器数据中的脉冲噪声
        df = filter_median(df, ['pressure_sensor'], window_size=5)
        # 大窗口去除更强的尖峰噪声
        df = filter_median(df, ['signal'], window_size=7)
    """
    if window_size % 2 != 1:
        raise ValueError("window_size must be an odd number")
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            df[col] = signal.medfilt(df[col].values, kernel_size=window_size)
    return df


def filter_moving_avg(df: pd.DataFrame, columns: List[str], window_size: int = 5,
                      center: bool = False) -> pd.DataFrame:
    """移动平均滤波，抑制随机小幅波动噪声

    核心适用特征：
    1. 抑制随机小幅波动噪声：通过平均值平滑数据，减少随机抖动
    2. 适配平稳/周期性数据：适合处理平稳数据或具有明显周期性的数据
    3. 窗口越大平滑越强：窗口大小控制平滑程度，大窗口平滑强但可能丢失细节

    Args:
        df: 输入 DataFrame
        columns: 需要滤波的列名列表
        window_size: 滑动窗口大小，默认 5
        center: 是否使用居中窗口，默认 False

    Returns:
        滤波后的 DataFrame

    Example:
        # 轻度平滑随机噪声
        df = filter_moving_avg(df, ['temperature'], window_size=5)
        # 中度平滑，居中窗口避免相位偏移
        df = filter_moving_avg(df, ['flow_rate'], window_size=10, center=True)
        # 强度平滑，适合高噪声数据
        df = filter_moving_avg(df, ['sensor_value'], window_size=20)
    """
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            df[col] = df[col].rolling(window=window_size, center=center, min_periods=1).mean()
    return df


def filter_fourier(df: pd.DataFrame, columns: List[str], cutoff_freq: float = 0.1,
                   sampling_rate: Optional[float] = None) -> pd.DataFrame:
    """傅里叶变换滤波，从频率维度分离噪声与信号

    核心适用特征：
    1. 从频率维度分离噪声/信号：通过频域分析识别噪声频率段，精准滤除
    2. 适配周期性强的数据：适合处理具有明显周期性结构的数据（如振动信号、音频）
    3. 需先识别噪声对应的频率段：需要分析频谱，确定噪声所在的频率范围

    Args:
        df: 输入 DataFrame
        columns: 需要滤波的列名列表
        cutoff_freq: 截止频率（归一化频率，0-0.5 代表奈奎斯特频率），默认 0.1
        sampling_rate: 采样率（Hz），如果为 None，则使用归一化频率

    Returns:
        滤波后的 DataFrame

    Example:
        # 低通滤波，去除高频噪声
        df = filter_fourier(df, ['vibration_signal'], cutoff_freq=0.1)
        # 指定采样率，滤波音频数据中的高频噪声
        df = filter_fourier(df, ['audio'], cutoff_freq=1000, sampling_rate=48000)
        # 更低截止频率，去除更多高频成分
        df = filter_fourier(df, ['sensor_wave'], cutoff_freq=0.05)
    """
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            # 提取非空数据
            data = df[col].fillna(0).values
            n = len(data)

            # FFT 变换
            fft_values = fft(data)
            freqs = fftfreq(n)

            # 归一化截止频率
            if sampling_rate is not None:
                cutoff = cutoff_freq / sampling_rate
            else:
                cutoff = cutoff_freq

            # 滤波：设置高于截止频率的傅里叶系数为0
            mask = np.abs(freqs) > cutoff
            fft_values_filtered = fft_values.copy()
            fft_values_filtered[mask] = 0

            # 逆 FFT 变换
            filtered_data = np.real(ifft(fft_values_filtered))

            # 填回数据
            df[col] = filtered_data

    return df


# ==================== 标准化/归一化 ====================

def normalize_minmax(df: pd.DataFrame, columns: List[str],
                    min_val: float = 0.0, max_val: float = 1.0) -> pd.DataFrame:
    """Min-Max 归一化，将数据缩放到指定范围

    公式：x_norm = (x - x_min) / (x_max - x_min) * (max_val - min_val) + min_val

    Args:
        df: 输入 DataFrame
        columns: 需要归一化的列名列表
        min_val: 归一化后的最小值，默认 0
        max_val: 归一化后的最大值，默认 1

    Returns:
        归一化后的 DataFrame

    Example:
        df = normalize_minmax(df, ['feature1', 'feature2'])
        df = normalize_minmax(df, ['score'], min_val=-1, max_val=1)
    """
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            col_min = df[col].min()
            col_max = df[col].max()
            if col_max - col_min == 0:
                df[col] = min_val
            else:
                df[col] = (df[col] - col_min) / (col_max - col_min) * (max_val - min_val) + min_val
    return df


def normalize_custom_range(df: pd.DataFrame, columns: List[str],
                         target_min: float, target_max: float,
                         source_min: Optional[float] = None, source_max: Optional[float] = None) -> pd.DataFrame:
    """指定区间缩放，将数据从源区间（默认为数据实际范围）缩放到目标区间

    公式：x_scaled = (x - source_min) / (source_max - source_min) * (target_max - target_min) + target_min

    Args:
        df: 输入 DataFrame
        columns: 需要缩放的列名列表
        target_min: 目标区间的最小值
        target_max: 目标区间的最大值
        source_min: 源区间的最小值，如果为 None 则使用列的实际最小值
        source_max: 源区间的最大值，如果为 None 则使用列的实际最大值

    Returns:
        缩放后的 DataFrame

    Example:
        df = normalize_custom_range(df, ['temperature'], target_min=20, target_max=30)
        df = normalize_custom_range(df, ['value'], target_min=0, target_max=100, source_min=0, source_max=1)
    """
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            s_min = source_min if source_min is not None else df[col].min()
            s_max = source_max if source_max is not None else df[col].max()

            if s_max - s_min == 0:
                df[col] = target_min
            else:
                df[col] = (df[col] - s_min) / (s_max - s_min) * (target_max - target_min) + target_min
    return df


def normalize_log(df: pd.DataFrame, columns: List[str],
                 offset: Optional[float] = None, base: str = 'e') -> pd.DataFrame:
    """对数变换，用于处理偏态分布的右偏数据

    适用于正偏态（右偏）数据，可以使数据分布更接近正态分布
    支持自然对数和常用对数

    Args:
        df: 输入 DataFrame
        columns: 需要对数变换的列名列表
        offset: 偏移量，用于处理非正数。如果为 None，当数据中有 ≤0 的值时自动设置为最小正数的绝对值
        base: 对数底数，'e' 为自然对数，'10' 为常用对数

    Returns:
        对数变换后的 DataFrame

    Example:
        # 自然对数
        df = normalize_log(df, ['price', 'income'])
        # 常用对数
        df = normalize_log(df, ['sales'], base='10')
        # 指定偏移量
        df = normalize_log(df, ['amount'], offset=1)
    """
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            # 计算偏移量
            if offset is None:
                min_val = df[col].min()
                if min_val <= 0:
                    offset = 1 - min_val
                else:
                    offset = 0

            # 应用偏移量并确保为正数
            shifted_data = df[col] + offset

            # 根据底数选择对数
            if base == '10':
                df[col] = np.log10(shifted_data)
            else:
                df[col] = np.log(shifted_data)

    return df


# ==================== 链式执行 ====================

def chain_execute(
    df: pd.DataFrame,
    plan: List[Dict],
    verbose: bool = True
) -> pd.DataFrame:
    """链式执行多个数据处理操作

    Args:
        df: 输入 DataFrame
        plan: 处理计划列表，每个元素包含:
            - step: 步骤序号
            - step_name: 步骤名称（可选）
            - columns: 目标列名列表
            - func: 函数类别 ('fill_missing', 'filter_noise', 'normalize')
            - method 或 type: 具体方法名
            - 其他参数如 window_size, min_val, max_val 等
        verbose: 是否打印执行详情

    Returns:
        处理后的 DataFrame

    Example:
        # 预定义方法示例
        plan = [
            {"step": 1, "step_name": "缺失值处理",
             "columns": ["temp"], "func": "fill_missing", "method": "ffill"},
            {"step": 2, "step_name": "滤波降噪",
             "columns": ["signal"], "func": "filter_noise", "type": "median", "window_size": 5}
        ]

        # 自定义策略示例
        plan = [
            {"step": 1, "step_name": "缺失值处理",
             "columns": ["target"], "func": "fill_missing", "method": "custom_combine",
             "mode_of": ["col_a", "col_b", "col_c"]},  # 取三列的众数填入
            {"step": 2, "step_name": "缺失值处理",
             "columns": ["flag"], "func": "fill_missing", "method": "custom_value",
             "value": "未知"},  # 填充指定值
        ]

        df_result = chain_execute(df, plan)
    """
    df_processed = df.copy()

    # 处理函数映射
    func_map = {
        "fill_missing": {
            "ffill": fill_missing_ffill,
            "bfill": fill_missing_bfill,
            "mean": fill_missing_mean,
            "median": fill_missing_median,
            "mode": fill_missing_mode,
            "interpolate": fill_missing_interpolate
        },
        "filter_noise": {
            "median": filter_median,
            "moving_avg": filter_moving_avg,
            "fourier": filter_fourier
        },
        "normalize": {
            "minmax": normalize_minmax,
            "custom_range": normalize_custom_range,
            "log": normalize_log
        }
    }

    for step_plan in plan:
        step_num = step_plan.get("step", 0)
        step_name = step_plan.get("step_name", "")
        columns = step_plan.get("columns", [])
        func_type = step_plan.get("func")
        method = step_plan.get("method") or step_plan.get("type")

        # 跳过处理
        if method == "skip" or func_type is None:
            if verbose:
                print(f"步骤{step_num}: {step_name} - 跳过")
            continue

        # 处理自定义策略
        if method == "custom_combine":
            if verbose:
                print(f"步骤{step_num}: {step_name} - {', '.join(columns)} (自定义功能组合: {step_plan.get('description', '')})")
            # 自定义功能组合：取多个列的众数填入目标列
            mode_source_cols = step_plan.get("mode_of", [])
            for target_col in columns:
                if target_col not in df_processed.columns:
                    continue
                # 收集所有源列的非空值
                all_values = []
                for src_col in mode_source_cols:
                    if src_col in df_processed.columns:
                        all_values.extend(df_processed[src_col].dropna().tolist())
                # 计算众数并填充
                if all_values:
                    from collections import Counter
                    counter = Counter(all_values)
                    mode_val = counter.most_common(1)[0][0]
                    df_processed[target_col] = df_processed[target_col].fillna(mode_val)
            continue

        elif method == "custom_value":
            fill_value = step_plan.get("value", 0)
            if verbose:
                print(f"步骤{step_num}: {step_name} - {', '.join(columns)} (填充指定值: {fill_value})")
            for col in columns:
                if col in df_processed.columns:
                    df_processed[col] = df_processed[col].fillna(fill_value)
            continue

        elif method == "custom_expression":
            # 自定义计算表达式（需要根据具体表达式解析实现）
            if verbose:
                print(f"步骤{step_num}: {step_name} - {', '.join(columns)} (自定义表达式: {step_plan.get('expression', '')})")
            # 这里可以根据实际需求解析表达式，例如 "mean(col_a)" 等
            # 为简化实现，暂时跳过，实际使用时可根据需求扩展
            continue

        # 预定义方法处理
        func = func_map.get(func_type, {}).get(method)
        if not func:
            if verbose:
                print(f"步骤{step_num}: {step_name} - 未知方法 {method}，跳过")
            continue

        # 准备参数
        params = {"df": df_processed, "columns": columns}
        # 跳过已使用的参数，添加其他参数
        reserved_keys = ["step", "step_name", "columns", "func", "method", "type",
                        "description", "mode_of", "value", "expression"]
        extra_params = {k: v for k, v in step_plan.items() if k not in reserved_keys}
        params.update(extra_params)

        # 执行处理
        if verbose:
            param_str = ", ".join(f"{k}={v}" for k, v in extra_params.items())
            param_str = f" ({param_str})" if param_str else ""
            print(f"步骤{step_num}: {step_name} - {', '.join(columns)} ({method}){param_str}")

        df_processed = func(**params)

    return df_processed


if __name__ == "__main__":
    print("Data Engineering Operations - 原子能力")
    print("\n支持的缺失值填充方法：")
    print("  - fill_missing_ffill: 前向填充")
    print("  - fill_missing_bfill: 后向填充")
    print("  - fill_missing_mean: 均值填充")
    print("  - fill_missing_median: 中位数填充")
    print("  - fill_missing_mode: 众数填充")
    print("  - fill_missing_value: 指定值填充")
    print("  - fill_missing_interpolate: 线性插值")
    print("")
    print("滤波降噪：")
    print("  - filter_median: 中值滤波（去除脉冲噪声）")
    print("  - filter_moving_avg: 移动平均滤波（平滑数据）")
    print("  - filter_fourier: 傅里叶变换滤波（频域去噪）")
    print("")
    print("标准化/归一化：")
    print("  - normalize_minmax: Min-Max 归一化")
    print("  - normalize_custom_range: 指定区间缩放")
    print("  - normalize_log: 对数变换（处理右偏数据）")
    print("")
    print("链式执行：")
    print("  - chain_execute(df, plan, verbose): 链式执行多种处理操作")
    print("    支持预定义方法、自定义功能组合、指定值填充、跳过处理")

