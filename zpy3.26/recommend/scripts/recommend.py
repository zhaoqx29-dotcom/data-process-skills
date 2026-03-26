"""
Recommend - 策略推荐子 skill
负责分析数据特征、推荐处理策略并生成调用链表

使用方式：
    from recommend import recommend_strategy, generate_strategy_table, generate_plan, parse_user_strategy_doc, match_user_strategy

    # 解析用户策略文档
    user_strategy = parse_user_strategy_doc('数据预处理建议.md')

    # 推荐策略
    strategies = recommend_strategy(df, target_columns, business_context='sensor_data')

    # 匹配用户文档策略
    matched = match_user_strategy(df, strategies, user_strategy, parsed_columns)

    # 生成策略表
    strategy_table = generate_strategy_table(strategies, parsed_columns)

    # 生成调用链表
    plan = generate_plan(strategies, parsed_columns)
"""

import pandas as pd
import numpy as np
import re
from typing import Dict, List, Optional, Tuple


# 需要从 analyze 导入 assess 函数
def assess(df: pd.DataFrame) -> Dict:
    """数据质量评估 - 从 analyze 导入

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
        missing_count = int(df[col].isna().sum())
        missing_pct = round(missing_count / len(df) * 100, 2)
        report["missing"][col] = {"count": missing_count, "percentage": missing_pct}

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

        col_data = df[col].dropna()
        if len(col_data) > 0:
            unique_count = col_data.nunique()
            dtype_str = str(df[col].dtype)

            characteristics = []
            if dtype_str in ["int64", "float64", "int32", "float32"]:
                characteristics.append("数值型")
                skewness = col_data.skew()
                if abs(skewness) > 1:
                    characteristics.append("偏态分布")
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
                    if len(diff) > 0 and diff.mean() != 0:
                        if diff.std() / abs(diff.mean()) < 0.5:
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


def parse_target_columns(user_input: str) -> List[tuple]:
    """解析用户输入的列和需求

    支持格式：
    - 列名:处理类型1,处理类型2
    - 列名 处理类型1 处理类型2
    - 多个列用分号或换行分隔

    Args:
        user_input: 用户输入的字符串

    Returns:
        解析后的列表 [(col, [needs]), ...]

    Example:
        输入: "temperature:缺失值填充,滤波降噪;humidity:缺失值填充"
        输出: [('temperature', ['缺失值填充', '滤波降噪']), ('humidity', ['缺失值填充'])]
    """
    parsed = []

    # 按分号或换行分割不同的列
    lines = user_input.replace(';', '\n').split('\n')

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # 按冒号分割列名和需求
        if ':' in line:
            parts = line.split(':', 1)
            col = parts[0].strip()
            needs_str = parts[1].strip() if len(parts) > 1 else ''
        else:
            # 没有冒号，整行作为列名
            col = line.strip()
            needs_str = ''

        # 解析需求
        needs = []
        if needs_str:
            # 按逗号分割
            needs = [n.strip() for n in needs_str.split(',') if n.strip()]

        if col:
            parsed.append((col, needs))

    return parsed


def recommend_strategy(df: pd.DataFrame, columns: List[str], business_context: str = "general",
                     process_type: str = "all") -> Dict:
    """智能推荐数据处理策略

    Args:
        df: 输入 DataFrame
        columns: 需要分析的列名列表
        business_context: 业务场景描述
        process_type: 处理类型，"all", "missing", "filter", "normalize"

    Returns:
        推荐策略字典
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

        is_numeric = dtype_str in ["int64", "float64", "int32", "float32"]

        # 滤波降噪推荐
        if process_type in ["all", "filter"]:
            if is_numeric:
                has_significant_outliers = "存在显著异常值" in characteristics
                is_stable = "平稳数据" in characteristics
                is_moderate_fluctuation = "中等波动" in characteristics
                is_smooth_trend = "平滑趋势" in characteristics
                is_periodic = "周期性数据" in characteristics

                if has_significant_outliers:
                    col_info["filter_method"] = "filter_median"
                    col_info["filter_reason"] = "检测到显著异常值（孤立尖峰/脉冲噪声），推荐使用中值滤波"
                elif is_moderate_fluctuation or (is_stable and is_smooth_trend):
                    col_info["filter_method"] = "filter_moving_avg"
                    col_info["filter_reason"] = "存在随机小幅波动噪声，推荐使用移动平均滤波"
                elif is_stable:
                    col_info["filter_method"] = "filter_moving_avg"
                    col_info["filter_reason"] = "数据较平稳，推荐使用移动平均滤波"
                elif is_periodic or "时间序列" in characteristics:
                    col_info["filter_method"] = "filter_fourier"
                    col_info["filter_reason"] = "数据周期性较强，推荐使用傅里叶变换滤波"
                elif not col_info.get("filter_method"):
                    col_info["filter_method"] = "filter_moving_avg"
                    col_info["filter_reason"] = "数值型数据，推荐使用移动平均滤波"

        # 缺失值处理推荐
        if process_type in ["all", "missing"]:
            if missing_rate == 0:
                col_info["missing_method"] = "no_action"
                col_info["missing_reason"] = "该列无缺失值"
                strategies[col] = col_info
                continue

            is_categorical = dtype_str in ["object", "str"] or "类别型" in characteristics
            is_time_series = (not is_categorical and
                            ("时间序列" in characteristics or
                             any(keyword in col.lower() for keyword in ["date", "time", "timestamp"]) or
                             "sensor_data" in business_context or "time_series" in business_context))

            is_continuous_small_missing = missing_analysis.get("is_continuous_small_missing", False)
            is_continuous_large_missing = missing_analysis.get("is_continuous_large_missing", False)
            is_at_start = missing_analysis.get("is_at_start", False)
            start_missing = missing_analysis.get("start_missing", False)
            max_consecutive_missing = missing_analysis.get("max_consecutive_missing", 0)

            # 高缺失率
            if missing_rate > 30:
                col_info["missing_method"] = "ask_user"
                col_info["missing_reason"] = f"缺失率过高({missing_rate}%)，建议由用户确认"

            # 类别型数据
            elif is_categorical:
                col_info["missing_method"] = "fill_missing_mode"
                col_info["missing_reason"] = "类别型数据，推荐使用众数填充"

            # 后向填充 - 开头缺失
            elif is_at_start and start_missing and max_consecutive_missing <= 3:
                col_info["missing_method"] = "fill_missing_bfill"
                col_info["missing_reason"] = "缺失值出现在数据开头且缺失块很短，推荐使用后向填充"

            # 线性插值 - 连续多行缺失
            elif is_continuous_large_missing and ("平滑趋势" in characteristics or
                                              "随时间缓慢变化" in characteristics or
                                              is_time_series):
                col_info["missing_method"] = "fill_missing_interpolate"
                col_info["missing_reason"] = "存在连续多行缺失且数据有明显线性趋势，推荐使用线性插值"

            # 后向填充 - 较短开头缺失
            elif is_at_start and start_missing and max_consecutive_missing <= 5:
                col_info["missing_method"] = "fill_missing_bfill"
                col_info["missing_reason"] = "缺失值出现在数据开头且缺失块较短，推荐使用后向填充"

            # 前向填充 - 连续少量缺失
            elif is_continuous_small_missing and ("随时间缓慢变化" in characteristics or
                                              "平滑趋势" in characteristics or
                                              is_time_series):
                col_info["missing_method"] = "fill_missing_ffill"
                col_info["missing_reason"] = "缺失值为连续少量缺失且数据随时间缓慢变化，推荐使用前向填充"

            # 中位数填充 - 异常值或偏态
            elif not is_categorical:
                has_outliers = "存在显著异常值" in characteristics
                is_skewed = "偏态分布" in characteristics

                if has_outliers or is_skewed:
                    col_info["missing_method"] = "fill_missing_median"
                    if has_outliers:
                        col_info["missing_reason"] = "数值型数据存在显著异常值，推荐使用中位数填充"
                    else:
                        col_info["missing_reason"] = "数值型数据呈偏态分布，推荐使用中位数填充"

            # 均值填充 - 正态分布
            if "正态分布" in characteristics and not col_info.get("missing_method"):
                col_info["missing_method"] = "fill_missing_mean"
                col_info["missing_reason"] = "数值型数据接近正态分布，推荐使用均值填充"

            # 默认 - 中位数填充
            if not is_categorical and not col_info.get("missing_method"):
                col_info["missing_method"] = "fill_missing_median"
                col_info["missing_reason"] = f"数值型数据，缺失率{missing_rate}%，推荐使用中位数填充"

            if not col_info.get("missing_method"):
                col_info["missing_method"] = "fill_missing_mode"
                col_info["missing_reason"] = "推荐使用众数填充"

        strategies[col] = col_info

    return strategies


def generate_strategy_table(strategies: Dict, parsed_columns: List[tuple]) -> str:
    """生成推荐策略表

    Args:
        strategies: 策略字典
        parsed_columns: 解析后的列和需求列表 [(col, [needs]), ...]

    Returns:
        Markdown 格式的策略表
    """
    lines = [
        "| 列名 | 处理类型 | 推荐方法 | 备选方法 | 推荐理由 |",
        "|------|----------|----------|----------|----------|"
    ]

    for col, needs in parsed_columns:
        strategy = strategies.get(col, {})

        # 缺失值处理
        if "缺失值填充" in needs or "缺失值" in needs:
            missing_method = strategy.get("missing_method", "N/A")
            missing_reason = strategy.get("missing_reason", "")
            if missing_method and missing_method != "no_action" and missing_method != "ask_user":
                lines.append(f"| {col} | 缺失值填充 | {missing_method} | bfill/mean/median/interpolate | {missing_reason} |")

        # 滤波降噪
        if "滤波降噪" in needs or "降噪" in needs:
            filter_method = strategy.get("filter_method", "N/A")
            filter_reason = strategy.get("filter_reason", "")
            if filter_method:
                lines.append(f"| {col} | 滤波降噪 | {filter_method} | median/fourier | {filter_reason} |")

        # 标准化归一化
        if "标准化归一化" in needs or "标准化" in needs or "归一化" in needs:
            lines.append(f"| {col} | 标准化归一化 | normalize_minmax | custom_range/log | 量纲差异，归一化到[0,1] |")

    return "\n".join(lines)


def generate_plan(strategies: Dict, parsed_columns: List[tuple]) -> List[Dict]:
    """生成调用链表

    Args:
        strategies: 策略字典
        parsed_columns: 解析后的列和需求列表

    Returns:
        步骤列表
    """
    plan = []
    step = 1

    # 步骤1: 缺失值处理
    missing_cols = []
    for col, needs in parsed_columns:
        if "缺失值填充" in needs or "缺失值" in needs:
            strategy = strategies.get(col, {})
            method = strategy.get("missing_method")
            if method and method != "no_action" and method != "ask_user":
                missing_cols.append(col)

    if missing_cols:
        # 使用第一个有方法的列的方法作为默认方法
        default_method = "ffill"
        for col in missing_cols:
            strategy = strategies.get(col, {})
            method = strategy.get("missing_method")
            if method:
                default_method = method.replace("fill_missing_", "")
                break

        plan.append({
            "step": step,
            "step_name": "缺失值处理",
            "columns": missing_cols,
            "func": "fill_missing",
            "method": default_method
        })
        step += 1

    # 步骤2: 滤波降噪
    filter_cols = []
    for col, needs in parsed_columns:
        if "滤波降噪" in needs or "降噪" in needs:
            strategy = strategies.get(col, {})
            method = strategy.get("filter_method")
            if method:
                filter_cols.append(col)

    if filter_cols:
        # 使用第一个有方法的列的方法作为默认方法
        default_type = "moving_avg"
        for col in filter_cols:
            strategy = strategies.get(col, {})
            method = strategy.get("filter_method")
            if method:
                default_type = method.replace("filter_", "")
                break

        plan.append({
            "step": step,
            "step_name": "滤波降噪",
            "columns": filter_cols,
            "func": "filter_noise",
            "type": default_type,
            "window_size": 10
        })
        step += 1

    # 步骤3: 标准化归一化
    normalize_cols = []
    for col, needs in parsed_columns:
        if "标准化归一化" in needs or "标准化" in needs or "归一化" in needs:
            normalize_cols.append(col)

    if normalize_cols:
        plan.append({
            "step": step,
            "step_name": "标准化归一化",
            "columns": normalize_cols,
            "func": "normalize",
            "type": "minmax"
        })

    return plan


def parse_user_strategy_doc(doc_path: str) -> Dict:
    """解析用户策略文档

    Args:
        doc_path: 文档路径

    Returns:
        解析后的策略字典
    """
    try:
        with open(doc_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

    strategy = {
        "missing": {
            "light": [],     # 轻度缺失 <5%
            "medium": [],    # 中度缺失 5-20%
            "heavy": []     # 重度缺失 >20%
        },
        "outlier": {
            "methods": [],   # 检测方法
            "treatments": [] # 处理方法
        },
        "normalize": {
            "methods": []   # 标准化方法
        }
    }

    # 解析缺失值处理策略
    missing_pattern = r'##\s*1\.?\s*缺失值处理[\s\S]*?(?=##|\Z)'
    missing_match = re.search(missing_pattern, content)
    if missing_match:
        missing_text = missing_match.group()
        # 轻度缺失
        if '轻度' in missing_text or '<5%' in missing_text:
            if '均值' in missing_text:
                strategy["missing"]["light"].append("mean")
            if '中位数' in missing_text:
                strategy["missing"]["light"].append("median")
            if '众数' in missing_text:
                strategy["missing"]["light"].append("mode")
        # 中度缺失
        if '中度' in missing_text or '5-20%' in missing_text:
            if 'KNN' in missing_text:
                strategy["missing"]["medium"].append("knn")
            if '随机森林' in missing_text:
                strategy["missing"]["medium"].append("random_forest")
        # 重度缺失
        if '重度' in missing_text or '>20%' in missing_text:
            if '删除' in missing_text:
                strategy["missing"]["heavy"].append("drop")
            if '领域知识' in missing_text:
                strategy["missing"]["heavy"].append("domain_knowledge")

    # 解析异常值处理策略
    outlier_pattern = r'##\s*2\.?\s*异常值[\s\S]*?(?=##|\Z)'
    outlier_match = re.search(outlier_pattern, content)
    if outlier_match:
        outlier_text = outlier_match.group()
        if '3σ' in outlier_text or '3σ' in outlier_text:
            strategy["outlier"]["methods"].append("3sigma")
        if 'IQR' in outlier_text or '箱线图' in outlier_text:
            strategy["outlier"]["methods"].append("iqr")
        if 'DBSCAN' in outlier_text:
            strategy["outlier"]["methods"].append("dbscan")
        if '截断' in outlier_text:
            strategy["outlier"]["treatments"].append("clip")
        if '删除' in outlier_text:
            strategy["outlier"]["treatments"].append("delete")

    # 解析标准化策略
    normalize_pattern = r'##\s*3\.?\s*(?:数据)?标准化[/归一化]*[\s\S]*?(?=##|\Z)'
    normalize_match = re.search(normalize_pattern, content)
    if normalize_match:
        normalize_text = normalize_match.group()
        if 'StandardScaler' in normalize_text or '标准化' in normalize_text:
            strategy["normalize"]["methods"].append("standard")
        if 'MinMaxScaler' in normalize_text or '归一化' in normalize_text:
            strategy["normalize"]["methods"].append("minmax")

    return strategy


def match_user_strategy(df: pd.DataFrame, strategies: Dict, user_strategy: Dict,
                        parsed_columns: List[tuple]) -> List[Dict]:
    """根据数据特征匹配用户文档策略

    Args:
        df: DataFrame
        strategies: skill 内置推荐策略
        user_strategy: 用户策略文档解析结果
        parsed_columns: 解析后的列和需求列表

    Returns:
        匹配结果列表
    """
    matched = []
    report = assess(df)

    for col, needs in parsed_columns:
        if col not in df.columns:
            continue

        missing_rate = report["missing"][col]["percentage"]
        characteristics = report["column_analysis"][col]["characteristics"]
        has_outliers = "存在显著异常值" in characteristics

        # 缺失值处理匹配
        if "缺失值填充" in needs or "缺失值" in needs:
            method = "N/A"
            user_method = "N/A"

            # Skill 内置推荐
            skill_strategy = strategies.get(col, {})
            missing_method = skill_strategy.get("missing_method", "")
            if missing_method:
                method = missing_method.replace("fill_missing_", "")

            # 匹配用户文档
            if missing_rate < 5 and user_strategy.get("missing", {}).get("light"):
                user_method = "/".join(user_strategy["missing"]["light"])
            elif 5 <= missing_rate <= 20 and user_strategy.get("missing", {}).get("medium"):
                user_method = "/".join(user_strategy["missing"]["medium"])
            elif missing_rate > 20 and user_strategy.get("missing", {}).get("heavy"):
                user_method = "/".join(user_strategy["missing"]["heavy"])

            matched.append({
                "col": col,
                "process_type": "缺失值处理",
                "data_feature": f"缺失率{missing_rate}%({'轻度' if missing_rate < 5 else '中度' if missing_rate <= 20 else '重度'})",
                "skill_method": method,
                "user_method": user_method,
                "user_strategy_available": user_method != "N/A"
            })

        # 异常值处理匹配
        if "异常值" in needs or "滤波降噪" in needs:
            method = "N/A"
            user_method = "N/A"

            skill_strategy = strategies.get(col, {})
            filter_method = skill_strategy.get("filter_method", "")
            if filter_method:
                method = filter_method.replace("filter_", "")

            if has_outliers and user_strategy.get("outlier", {}).get("methods"):
                user_method = "/".join(user_strategy["outlier"]["methods"])

            matched.append({
                "col": col,
                "process_type": "异常值处理",
                "data_feature": "存在显著异常值" if has_outliers else "无显著异常值",
                "skill_method": method,
                "user_method": user_method,
                "user_strategy_available": user_method != "N/A"
            })

        # 标准化匹配
        if "标准化" in str(needs) or "归一化" in str(needs) or "标准化归一化" in str(needs):
            method = "N/A"
            user_method = "N/A"

            if user_strategy.get("normalize", {}).get("methods"):
                user_method = "/".join(user_strategy["normalize"]["methods"])

            matched.append({
                "col": col,
                "process_type": "标准化/归一化",
                "data_feature": "数值型特征",
                "skill_method": "minmax/log",
                "user_method": user_method,
                "user_strategy_available": user_method != "N/A"
            })

    return matched


def generate_comparison_table(matched: List[Dict]) -> str:
    """生成策略对比表格

    Args:
        matched: 匹配结果列表

    Returns:
        Markdown 格式的对比表格
    """
    if not matched:
        return ""

    lines = [
        "| 列名 | 处理类型 | 数据特征 | Skill内置推荐 | 用户文档匹配策略 | 选择 |",
        "|------|----------|----------|--------------|-----------------|------|"
    ]

    for m in matched:
        user_method = m["user_method"] if m["user_method"] != "N/A" else "无对应策略"
        lines.append(
            f"| {m['col']} | {m['process_type']} | {m['data_feature']} | "
            f"{m['skill_method']} | {user_method} | [1]/[2]/[3] |"
        )

    return "\n".join(lines)


if __name__ == "__main__":
    print("Recommend - 策略推荐子 skill")
    print("\n支持的函数：")
    print("  - recommend_strategy: 智能推荐处理策略")
    print("  - generate_strategy_table: 生成策略表")
    print("  - generate_plan: 生成调用链表")
    print("  - parse_user_strategy_doc: 解析用户策略文档")
    print("  - match_user_strategy: 匹配用户文档策略")
    print("  - generate_comparison_table: 生成对比表格")
