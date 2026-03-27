"""
缺失值填充模块
提供各种缺失值填充方法
"""

import pandas as pd
from typing import List


def fill_missing_ffill(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """前向填充：用前一个非空值填充"""
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].ffill()
    return df


def fill_missing_bfill(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """后向填充：用后一个非空值填充"""
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].bfill()
    return df


def fill_missing_mean(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """均值填充：用列的均值填充（适合正态分布数据）"""
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            df[col] = df[col].fillna(df[col].mean())
    return df


def fill_missing_median(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """中位数填充：用列的中位数填充（适合偏态分布/有异常值的数据）"""
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            df[col] = df[col].fillna(df[col].median())
    return df


def fill_missing_mode(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """众数填充：用列的众数填充（适合类别列）"""
    df = df.copy()
    for col in columns:
        if col in df.columns:
            mode_val = df[col].mode()
            if len(mode_val) > 0:
                df[col] = df[col].fillna(mode_val[0])
    return df


def fill_missing_interpolate(df: pd.DataFrame, columns: List[str], method: str = 'linear',
                             limit_direction: str = 'both') -> pd.DataFrame:
    """线性插值：用相邻值的线性插值填充（适合时间序列/线性趋势数据）"""
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            df[col] = df[col].interpolate(method=method, limit_direction=limit_direction)
    return df


# 导出所有函数
__all__ = [
    'fill_missing_ffill',
    'fill_missing_bfill',
    'fill_missing_mean',
    'fill_missing_median',
    'fill_missing_mode',
    'fill_missing_interpolate'
]