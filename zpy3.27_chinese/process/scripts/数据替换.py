"""
数据替换模块
提供对数变换、差分变换、类别编码等方法
"""

import pandas as pd
import numpy as np
from typing import List, Optional
from sklearn.preprocessing import LabelEncoder, OneHotEncoder


# ==================== 对数变换 ====================

def transform_log(df: pd.DataFrame, columns: List[str],
                  base: str = 'e', offset: Optional[float] = None) -> pd.DataFrame:
    """对数变换：将非线性/指数增长数据转换为接近线性/正态分布

    适用于：右偏分布、长尾数据、经济增长数据等

    Args:
        df: 输入 DataFrame
        columns: 需要变换的列
        base: 对数底数 ('e', '10', '2')
        offset: 偏移量，处理非正值

    Returns:
        变换后的 DataFrame
    """
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            if offset is None:
                min_val = df[col].min()
                offset = 1 - min_val if min_val <= 0 else 0

            shifted_data = df[col] + offset

            if base == '10':
                df[col] = np.log10(shifted_data)
            elif base == '2':
                df[col] = np.log2(shifted_data)
            else:
                df[col] = np.log(shifted_data)

    return df


def transform_log1p(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """log(1+x) 变换：适合数据包含0的情况，避免log(0)问题"""
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            df[col] = np.log1p(df[col])
    return df


# ==================== 差分变换 ====================

def transform_diff(df: pd.DataFrame, columns: List[str], periods: int = 1) -> pd.DataFrame:
    """一阶差分变换：用当前值减去前一个值

    适用于：消除趋势、平稳化时间序列

    Args:
        df: 输入 DataFrame
        columns: 需要差分的列
        periods: 差分阶数（默认1）

    Returns:
        变换后的 DataFrame（第一行会变为NaN）
    """
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            df[col] = df[col].diff(periods)
    return df


def transform_diff2(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """二阶差分：消除趋势趋势（加速度）"""
    return transform_diff(df, columns, periods=2)


def transform_pct_change(df: pd.DataFrame, columns: List[str], periods: int = 1) -> pd.DataFrame:
    """百分比变化：用 (当前值 - 前一个值) / 前一个值

    适用于：增长率、环比变化等
    """
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            df[col] = df[col].pct_change(periods)
    return df


# ==================== 类别编码 ====================

def encode_label(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """标签编码：将类别转换为 0 到 n-1 的整数

    适用于：有序类别、树模型输入
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col].astype(str))
    return df


def encode_onehot(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """独热编码：将类别转换为二进制列

    适用于：无序类别、线性模型输入
    返回包含独热列的新 DataFrame
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            dummies = pd.get_dummies(df[col], prefix=col, dtype=int)
            df = pd.concat([df, dummies], axis=1)
            df = df.drop(columns=[col])
    return df


def encode_target(df: pd.DataFrame, columns: List[str], target_col: str) -> pd.DataFrame:
    """目标编码：用目标变量的均值替换类别

    适用于：高基数类别、但需要防止数据泄露

    Args:
        df: 输入 DataFrame
        columns: 需要编码的列
        target_col: 目标变量列名
    """
    df = df.copy()
    for col in columns:
        if col in df.columns and target_col in df.columns:
            target_means = df.groupby(col)[target_col].mean()
            df[col + '_encoded'] = df[col].map(target_means)
    return df


def encode_ordinal(df: pd.DataFrame, columns: List[str], mapping: dict) -> pd.DataFrame:
    """顺序编码：根据顺序关系映射为整数

    适用于：有序类别（低/中/高 → 0/1/2）

    Args:
        df: 输入 DataFrame
        columns: 需要编码的列
        mapping: {列名: {类别: 数值}}
    """
    df = df.copy()
    for col in columns:
        if col in df.columns and col in mapping:
            df[col] = df[col].map(mapping[col])
    return df


# ==================== 导出 ====================

__all__ = [
    'transform_log',
    'transform_log1p',
    'transform_diff',
    'transform_diff2',
    'transform_pct_change',
    'encode_label',
    'encode_onehot',
    'encode_target',
    'encode_ordinal'
]