"""
Process - 数据处理子 skill
负责执行数据处理、生成评价报告

使用方式：
    from process import chain_execute, generate_evaluation_report, save_processed_data

    # 执行链式处理
    df_result = chain_execute(df, plan, verbose=True)

    # 生成评价报告
    evaluation_report = generate_evaluation_report(df, df_result, plan)

    # 保存处理后的数据
    saved_path = save_processed_data(df_result, 'data.csv')
    # 保存为: data-处理后.csv
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from scipy import signal
from scipy.fft import fft, ifft, fftfreq


# ==================== 缺失值处理 ====================

def fill_missing_ffill(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """前向填充"""
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].ffill()
    return df


def fill_missing_bfill(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """后向填充"""
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].bfill()
    return df


def fill_missing_mean(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """均值填充"""
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            df[col] = df[col].fillna(df[col].mean())
    return df


def fill_missing_median(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """中位数填充"""
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            df[col] = df[col].fillna(df[col].median())
    return df


def fill_missing_mode(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """众数填充"""
    df = df.copy()
    for col in columns:
        if col in df.columns:
            mode_val = df[col].mode()
            if len(mode_val) > 0:
                df[col] = df[col].fillna(mode_val[0])
    return df


def fill_missing_interpolate(df: pd.DataFrame, columns: List[str], method: str = 'linear',
                           limit_direction: str = 'both') -> pd.DataFrame:
    """线性插值"""
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            df[col] = df[col].interpolate(method=method, limit_direction=limit_direction)
    return df


def fill_missing_knn(df: pd.DataFrame, columns: List[str], n_neighbors: int = 5) -> pd.DataFrame:
    """KNN 插补 - 用户文档策略支持"""
    try:
        from sklearn.impute import KNNImputer
        df = df.copy()
        # 只对数值型列进行 KNN 插补
        numeric_cols = [col for col in columns if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]]
        if numeric_cols:
            imputer = KNNImputer(n_neighbors=n_neighbors)
            df[numeric_cols] = imputer.fit_transform(df[numeric_cols])
        return df
    except ImportError:
        # 如果没有 sklearn，使用中位数填充作为后备
        return fill_missing_median(df, columns)


def fill_missing_random_forest(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """随机森林插补 - 用户文档策略支持"""
    # 简化实现：使用迭代插补
    try:
        from sklearn.impute import IterativeImputer
        from sklearn.ensemble import RandomForestRegressor
        df = df.copy()
        numeric_cols = [col for col in columns if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]]
        if numeric_cols:
            imputer = IterativeImputer(estimator=RandomForestRegressor(n_estimators=10, random_state=42), max_iter=10)
            df[numeric_cols] = imputer.fit_transform(df[numeric_cols])
        return df
    except ImportError:
        return fill_missing_median(df, columns)


# ==================== 异常值处理 ====================

def handle_outliers_iqr(df: pd.DataFrame, columns: List[str], method: str = 'clip') -> pd.DataFrame:
    """IQR 方法处理异常值 - 用户文档策略支持

    Args:
        df: DataFrame
        columns: 列名列表
        method: 处理方法，'clip'（截断）或 'delete'（删除）

    Returns:
        处理后的 DataFrame
    """
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            if method == 'clip':
                df[col] = df[col].clip(lower=lower_bound, upper=upper_bound)
            elif method == 'delete':
                df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]

    return df


def handle_outliers_3sigma(df: pd.DataFrame, columns: List[str], method: str = 'clip') -> pd.DataFrame:
    """3σ 原则处理异常值 - 用户文档策略支持

    Args:
        df: DataFrame
        columns: 列名列表
        method: 处理方法，'clip'（截断）或 'delete'（删除）

    Returns:
        处理后的 DataFrame
    """
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            mean = df[col].mean()
            std = df[col].std()
            lower_bound = mean - 3 * std
            upper_bound = mean + 3 * std

            if method == 'clip':
                df[col] = df[col].clip(lower=lower_bound, upper=upper_bound)
            elif method == 'delete':
                df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]

    return df

def filter_median(df: pd.DataFrame, columns: List[str], window_size: int = 3) -> pd.DataFrame:
    """中值滤波"""
    if window_size % 2 != 1:
        raise ValueError("window_size must be an odd number")
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            df[col] = signal.medfilt(df[col].values, kernel_size=window_size)
    return df


def filter_moving_avg(df: pd.DataFrame, columns: List[str], window_size: int = 5,
                      center: bool = False) -> pd.DataFrame:
    """移动平均滤波"""
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            df[col] = df[col].rolling(window=window_size, center=center, min_periods=1).mean()
    return df


def filter_fourier(df: pd.DataFrame, columns: List[str], cutoff_freq: float = 0.1,
                   sampling_rate: Optional[float] = None) -> pd.DataFrame:
    """傅里叶变换滤波"""
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            data = df[col].fillna(0).values
            n = len(data)

            fft_values = fft(data)
            freqs = fftfreq(n)

            if sampling_rate is not None:
                cutoff = cutoff_freq / sampling_rate
            else:
                cutoff = cutoff_freq

            mask = np.abs(freqs) > cutoff
            fft_values_filtered = fft_values.copy()
            fft_values_filtered[mask] = 0

            filtered_data = np.real(ifft(fft_values_filtered))
            df[col] = filtered_data

    return df


# ==================== 标准化/归一化 ====================

def normalize_minmax(df: pd.DataFrame, columns: List[str],
                    min_val: float = 0.0, max_val: float = 1.0) -> pd.DataFrame:
    """Min-Max 归一化"""
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
    """指定区间缩放"""
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
    """对数变换"""
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            if offset is None:
                min_val = df[col].min()
                if min_val <= 0:
                    offset = 1 - min_val
                else:
                    offset = 0

            shifted_data = df[col] + offset

            if base == '10':
                df[col] = np.log10(shifted_data)
            elif base == '2':
                df[col] = np.log2(shifted_data)
            else:
                df[col] = np.log(shifted_data)

    return df


def normalize_standard(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """StandardScaler 标准化 (均值0，方差1) - 用户文档策略支持"""
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            mean = df[col].mean()
            std = df[col].std()
            if std != 0:
                df[col] = (df[col] - mean) / std
    return df


# ==================== 链式执行 ====================

def chain_execute(df: pd.DataFrame, plan: List[Dict], verbose: bool = True) -> pd.DataFrame:
    """链式执行多个数据处理操作

    Args:
        df: 输入 DataFrame
        plan: 处理计划列表
        verbose: 是否打印执行详情

    Returns:
        处理后的 DataFrame
    """
    df_processed = df.copy()

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

        if method == "skip" or func_type is None:
            if verbose:
                print(f"步骤{step_num}: {step_name} - 跳过")
            continue

        # 自定义策略处理
        if method == "custom_combine":
            if verbose:
                print(f"步骤{step_num}: {step_name} - {', '.join(columns)} (自定义功能组合)")
            mode_source_cols = step_plan.get("mode_of", [])
            for target_col in columns:
                if target_col not in df_processed.columns:
                    continue
                all_values = []
                for src_col in mode_source_cols:
                    if src_col in df_processed.columns:
                        all_values.extend(df_processed[src_col].dropna().tolist())
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

        # 预定义方法处理
        func = func_map.get(func_type, {}).get(method)
        if not func:
            if verbose:
                print(f"步骤{step_num}: {step_name} - 未知方法 {method}，跳过")
            continue

        params = {"df": df_processed, "columns": columns}
        reserved_keys = ["step", "step_name", "columns", "func", "method", "type",
                        "description", "mode_of", "value", "expression"]
        extra_params = {k: v for k, v in step_plan.items() if k not in reserved_keys}
        params.update(extra_params)

        if verbose:
            param_str = ", ".join(f"{k}={v}" for k, v in extra_params.items())
            param_str = f" ({param_str})" if param_str else ""
            print(f"步骤{step_num}: {step_name} - {', '.join(columns)} ({method}){param_str}")

        df_processed = func(**params)

    return df_processed


def save_processed_data(df: pd.DataFrame, original_file_path: str = "") -> str:
    """保存处理后的数据

    Args:
        df: 处理后的 DataFrame
        original_file_path: 原始文件路径

    Returns:
        保存的文件路径
    """
    if original_file_path:
        # 从原文件路径生成新文件名
        if original_file_path.endswith('.csv'):
            new_path = original_file_path.replace('.csv', '-处理后.csv')
        elif original_file_path.endswith('.xlsx'):
            new_path = original_file_path.replace('.xlsx', '-处理后.xlsx')
        elif original_file_path.endswith('.xls'):
            new_path = original_file_path.replace('.xls', '-处理后.xls')
        else:
            new_path = original_file_path + '-处理后'
    else:
        new_path = "处理后数据.csv"

    if new_path.endswith('.csv'):
        df.to_csv(new_path, index=False, encoding='utf-8-sig')
    elif new_path.endswith('.xlsx') or new_path.endswith('.xls'):
        df.to_excel(new_path, index=False)

    return new_path


def save_report(content: str, file_name: str) -> str:
    """保存报告为 Markdown 文件

    Args:
        content: 报告内容
        file_name: 文件名

    Returns:
        保存的文件路径
    """
    save_path = file_name if file_name.endswith('.md') else f"{file_name}.md"
    with open(save_path, 'w', encoding='utf-8') as f:
        f.write(content)
    return save_path


# ==================== 评价与图表 ====================

def generate_evaluation_report(df_before: pd.DataFrame, df_after: pd.DataFrame,
                                plan: List[Dict], file_path: str = "",
                                business_scene: str = "", target_columns: List[str] = None,
                                save_path: str = "") -> str:
    """生成评价结果报告

    Args:
        df_before: 处理前 DataFrame
        df_after: 处理后 DataFrame
        plan: 处理计划
        file_path: 文件路径
        business_scene: 业务场景
        target_columns: 处理列
        save_path: 保存路径，如果提供则自动保存为 Markdown 文件

    Returns:
        Markdown 格式评价报告
    """
    if target_columns is None:
        target_columns = []

    lines = [
        "## 数据预处理结果评价报告",
        "",
        "### 原始数据信息"
    ]

    if file_path:
        lines.append(f"- 文件: {file_path}")
    if business_scene:
        lines.append(f"- 业务场景: {business_scene}")
    if target_columns:
        lines.append(f"- 处理列: {', '.join(target_columns)}")

    lines.extend([
        "",
        "### 执行结果（链式处理）",
        "| 步骤 | 操作 | 目标列 | 方法 | 结果 |",
        "|------|------|--------|------|------|"
    ])

    for step_plan in plan:
        step = step_plan.get("step", "")
        step_name = step_plan.get("step_name", "")
        columns = step_plan.get("columns", [])
        method = step_plan.get("method") or step_plan.get("type", "N/A")

        # 计算结果
        result = ""
        if step_plan.get("func") == "fill_missing":
            filled_count = 0
            for col in columns:
                if col in df_before.columns and col in df_after.columns:
                    before_missing = df_before[col].isna().sum()
                    after_missing = df_after[col].isna().sum()
                    filled_count += int(before_missing - after_missing)
            result = f"填充{filled_count}个缺失值" if filled_count > 0 else "无缺失值"
        elif step_plan.get("func") == "filter_noise":
            result = "数据平滑"
        elif step_plan.get("func") == "normalize":
            result = f"缩放至[{step_plan.get('min_val', 0)}, {step_plan.get('max_val', 1)}]"

        lines.append(f"| {step} | {step_name} | {', '.join(columns)} | {method} | {result} |")

    # 统计指标变化
    lines.extend([
        "",
        "### 统计指标变化",
        "| 列名 | 处理前均值 | 处理后均值 | 处理前标准差 | 处理后标准差 |",
        "|------|------------|------------|--------------|--------------|"
    ])

    all_cols = set()
    for step_plan in plan:
        all_cols.update(step_plan.get("columns", []))

    for col in all_cols:
        if col in df_before.columns and col in df_after.columns:
            # 只对数值型列计算统计指标
            if df_before[col].dtype in ["int64", "float64", "int32", "float32"]:
                before_mean = df_before[col].mean()
                after_mean = df_after[col].mean()
                before_std = df_before[col].std()
                after_std = df_after[col].std()
                lines.append(f"| {col} | {before_mean:.4f} | {after_mean:.4f} | {before_std:.4f} | {after_std:.4f} |")
            else:
                # 类别列显示填充前后缺失值变化
                before_missing = df_before[col].isna().sum()
                after_missing = df_after[col].isna().sum()
                lines.append(f"| {col} | 缺失{before_missing}个 | 缺失{after_missing}个 | - | - |")

    # 整体效果评价
    lines.extend([
        "",
        "### 整体效果评价",
        f"- 数据质量提升: 处理完成，数据已按照推荐策略进行处理",
        f"- 业务适用性: 处理后的数据适用于{business_scene or '后续分析'}",
        "- 建议后续操作: 可根据业务需求进行进一步分析或建模"
    ])

    report_content = "\n".join(lines)

    # 如果提供了保存路径，自动保存为 Markdown 文件
    if save_path:
        with open(save_path, 'w', encoding='utf-8') as f:
            f.write(report_content)

    return report_content


def generate_comparison_chart(df_before: pd.DataFrame, df_after: pd.DataFrame,
                              plan: List[Dict], save_path: str = "数据处理前后图表对比.png") -> str:
    """生成对比图表

    Args:
        df_before: 处理前 DataFrame
        df_after: 处理后 DataFrame
        plan: 处理计划
        save_path: 保存路径

    Returns:
        图表保存路径
    """
    try:
        import matplotlib.pyplot as plt
        import matplotlib
        matplotlib.use('Agg')

        # 收集所有处理的列
        all_cols = set()
        for step_plan in plan:
            all_cols.update(step_plan.get("columns", []))

        if not all_cols:
            return ""

        # 创建图表
        n_cols = len(all_cols)
        fig, axes = plt.subplots(n_cols, 2, figsize=(12, 4 * n_cols))

        if n_cols == 1:
            axes = axes.reshape(1, -1)

        for idx, col in enumerate(all_cols):
            if col not in df_before.columns or col not in df_after.columns:
                continue

            # 处理前数据分布
            axes[idx, 0].hist(df_before[col].dropna(), bins=30, alpha=0.7, color='blue', edgecolor='black')
            axes[idx, 0].set_title(f'{col} - 处理前')
            axes[idx, 0].set_xlabel('值')
            axes[idx, 0].set_ylabel('频数')

            # 处理后数据分布
            axes[idx, 1].hist(df_after[col].dropna(), bins=30, alpha=0.7, color='green', edgecolor='black')
            axes[idx, 1].set_title(f'{col} - 处理后')
            axes[idx, 1].set_xlabel('值')
            axes[idx, 1].set_ylabel('频数')

        plt.tight_layout()
        plt.savefig(save_path, dpi=150)
        plt.close()

        return save_path
    except Exception as e:
        return f"图表生成失败: {str(e)}"


if __name__ == "__main__":
    print("Process - 数据处理子 skill")
    print("\n支持的函数：")
    print("  - 缺失值处理: fill_missing_ffill, fill_missing_bfill, fill_missing_mean,")
    print("               fill_missing_median, fill_missing_mode, fill_missing_interpolate")
    print("  - 滤波降噪: filter_median, filter_moving_avg, filter_fourier")
    print("  - 标准化: normalize_minmax, normalize_custom_range, normalize_log")
    print("  - 链式执行: chain_execute")
    print("  - 评价报告: generate_evaluation_report")
    print("  - 对比图表: generate_comparison_chart")
