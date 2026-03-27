"""
标准化归一化模块
提供数据标准化/归一化方法
"""

import pandas as pd
import numpy as np
from typing import List, Optional


def normalize_minmax(df: pd.DataFrame, columns: List[str],
                    min_val: float = 0.0, max_val: float = 1.0) -> pd.DataFrame:
    """Min-Max 归一化：将数据缩放到指定范围（默认[0,1]），消除量纲影响"""
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
    """指定区间缩放：将数据从原始范围缩放到目标范围（适合业务特定范围要求）"""
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
    """对数变换：使用对数压缩数据范围（适合右偏分布/长尾数据）"""
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
            else:
                df[col] = np.log(shifted_data)

    return df


def normalize_standardize(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """标准化(Z-score)：将数据转换为均值为0、标准差为1的分布"""
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            mean_val = df[col].mean()
            std_val = df[col].std()
            if std_val != 0:
                df[col] = (df[col] - mean_val) / std_val
    return df


# 导出所有函数
__all__ = [
    'normalize_minmax',
    'normalize_custom_range',
    'normalize_log',
    'normalize_standardize'
]