"""
异常值处理模块
提供多种异常值检测和处理方法
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional


# ==================== 3-Sigma 方法 ====================

def outlier_3sigma_detect(df: pd.DataFrame, columns: List[str], threshold: float = 3.0) -> Dict:
    """3-Sigma 异常值检测：超出均值±3倍标准差的值视为异常

    适用于：近似正态分布的数据

    Args:
        df: 输入 DataFrame
        columns: 需要检测的列
        threshold: 倍数（默认3）

    Returns:
        异常值字典: {列名: [(索引, 值), ...]}
    """
    outliers = {}
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            mean = df[col].mean()
            std = df[col].std()
            if std == 0:
                continue

            lower_bound = mean - threshold * std
            upper_bound = mean + threshold * std

            mask = (df[col] < lower_bound) | (df[col] > upper_bound)
            outlier_indices = df[mask].index.tolist()
            outlier_values = df.loc[outlier_indices, col].tolist()

            if outlier_indices:
                outliers[col] = {
                    'count': len(outlier_indices),
                    'lower_bound': lower_bound,
                    'upper_bound': upper_bound,
                    'outliers': list(zip(outlier_indices, outlier_values))
                }
    return outliers


def outlier_3sigma_clip(df: pd.DataFrame, columns: List[str], threshold: float = 3.0) -> pd.DataFrame:
    """3-Sigma 裁剪：将异常值所在的整行删除"""
    df = df.copy()
    mask = pd.Series([False] * len(df), index=df.index)
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            mean = df[col].mean()
            std = df[col].std()
            if std == 0:
                continue
            lower = mean - threshold * std
            upper = mean + threshold * std
            col_mask = (df[col] < lower) | (df[col] > upper)
            mask = mask | col_mask
    # 删除包含异常值的行
    return df[~mask].reset_index(drop=True)


def outlier_3sigma_remove(df: pd.DataFrame, columns: List[str], threshold: float = 3.0) -> pd.DataFrame:
    """3-Sigma 移除：将异常值所在的整行删除"""
    return outlier_3sigma_clip(df, columns, threshold)


# ==================== IQR 方法 ====================

def outlier_iqr_detect(df: pd.DataFrame, columns: List[str], k: float = 1.5) -> Dict:
    """四分位距法(IQR) 异常值检测：低于 Q1-k*IQR 或高于 Q3+k*IQR 视为异常

    适用于：非正态分布、有明显离群点

    Args:
        df: 输入 DataFrame
        columns: 需要检测的列
        k: IQR倍数（常用1.5或3）

    Returns:
        异常值字典
    """
    outliers = {}
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - k * IQR
            upper_bound = Q3 + k * IQR

            mask = (df[col] < lower_bound) | (df[col] > upper_bound)
            outlier_indices = df[mask].index.tolist()
            outlier_values = df.loc[outlier_indices, col].tolist()

            if outlier_indices:
                outliers[col] = {
                    'count': len(outlier_indices),
                    'Q1': Q1,
                    'Q3': Q3,
                    'IQR': IQR,
                    'lower_bound': lower_bound,
                    'upper_bound': upper_bound,
                    'outliers': list(zip(outlier_indices, outlier_values))
                }
    return outliers


def outlier_iqr_clip(df: pd.DataFrame, columns: List[str], k: float = 1.5) -> pd.DataFrame:
    """IQR 裁剪：将异常值所在的整行删除"""
    df = df.copy()
    mask = pd.Series([False] * len(df), index=df.index)
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower = Q1 - k * IQR
            upper = Q3 + k * IQR
            col_mask = (df[col] < lower) | (df[col] > upper)
            mask = mask | col_mask
    # 删除包含异常值的行
    return df[~mask].reset_index(drop=True)


# ==================== Z-Score 方法 ====================

def outlier_zscore_detect(df: pd.DataFrame, columns: List[str], threshold: float = 3.0) -> Dict:
    """Z-score 异常值检测

    相当于标准化后的 3-Sigma
    """
    outliers = {}
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            mean = df[col].mean()
            std = df[col].std()
            if std == 0:
                continue

            z_scores = np.abs((df[col] - mean) / std)
            mask = z_scores > threshold
            outlier_indices = df[mask].index.tolist()
            outlier_values = df.loc[outlier_indices, col].tolist()

            if outlier_indices:
                outliers[col] = {
                    'count': len(outlier_indices),
                    'threshold': threshold,
                    'outliers': list(zip(outlier_indices, outlier_values))
                }
    return outliers


def outlier_zscore_clip(df: pd.DataFrame, columns: List[str], threshold: float = 3.0) -> pd.DataFrame:
    """Z-score 裁剪：将异常值所在的整行删除"""
    return outlier_3sigma_remove(df, columns, threshold)


# ==================== 移动标准差法 ====================

def outlier_moving_std_detect(df: pd.DataFrame, columns: List[str],
                              window: int = 10, threshold: float = 3.0) -> Dict:
    """移动标准差法 异常值检测

    在滑动窗口内计算局部均值和标准差，判断当前点是否偏离

    适用于：时间序列数据
    """
    outliers = {}
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            rolling_mean = df[col].rolling(window=window, center=True, min_periods=1).mean()
            rolling_std = df[col].rolling(window=window, center=True, min_periods=1).std()

            z_scores = np.abs((df[col] - rolling_mean) / rolling_std.replace(0, 1))
            mask = z_scores > threshold
            outlier_indices = df[mask].index.tolist()
            outlier_values = df.loc[outlier_indices, col].tolist()

            if outlier_indices:
                outliers[col] = {
                    'count': len(outlier_indices),
                    'window': window,
                    'threshold': threshold,
                    'outliers': list(zip(outlier_indices, outlier_values))
                }
    return outliers


def outlier_moving_std_clip(df: pd.DataFrame, columns: List[str],
                             window: int = 10, threshold: float = 3.0) -> pd.DataFrame:
    """移动标准差法 裁剪：将异常值所在的整行删除"""
    df = df.copy()
    mask = pd.Series([False] * len(df), index=df.index)
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            rolling_mean = df[col].rolling(window=window, center=True, min_periods=1).mean()
            rolling_std = df[col].rolling(window=window, center=True, min_periods=1).std()
            rolling_std = rolling_std.replace(0, 1)

            z_scores = np.abs((df[col] - rolling_mean) / rolling_std)
            col_mask = z_scores > threshold
            mask = mask | col_mask
    # 删除包含异常值的行
    return df[~mask].reset_index(drop=True)


# ==================== 聚类检测 (DBSCAN) ====================

def outlier_dbscan_detect(df: pd.DataFrame, columns: List[str],
                          eps: float = 0.5, min_samples: int = 5) -> Dict:
    """DBSCAN 聚类异常值检测

    不属于任何密集簇的点视为异常

    适用于：复杂分布、未知异常模式
    """
    from sklearn.cluster import DBSCAN

    outliers = {}
    data = df[columns].copy()

    # 处理缺失值
    data = data.fillna(data.mean())

    clustering = DBSCAN(eps=eps, min_samples=min_samples).fit(data)
    labels = clustering.labels_

    for col in columns:
        noise_mask = labels == -1
        noise_indices = df[noise_mask].index.tolist()
        noise_values = df.loc[noise_indices, col].tolist()

        if noise_indices:
            outliers[col] = {
                'count': len(noise_indices),
                'eps': eps,
                'min_samples': min_samples,
                'outliers': list(zip(noise_indices, noise_values))
            }
    return outliers


def outlier_dbscan_remove(df: pd.DataFrame, columns: List[str],
                          eps: float = 0.5, min_samples: int = 5) -> pd.DataFrame:
    """DBSCAN 移除异常值"""
    from sklearn.cluster import DBSCAN

    data = df[columns].copy().fillna(0)
    clustering = DBSCAN(eps=eps, min_samples=min_samples).fit(data)
    mask = clustering.labels_ != -1
    return df[mask].copy()


# ==================== 统一处理接口 ====================

def detect_outliers(df: pd.DataFrame, columns: List[str],
                    method: str = 'iqr', **kwargs) -> Dict:
    """统一的异常值检测接口

    Args:
        df: 输入 DataFrame
        columns: 需要检测的列
        method: 检测方法 ('3sigma', 'iqr', 'zscore', 'moving_std', 'dbscan')
        **kwargs: 方法对应的参数

    Returns:
        异常值字典
    """
    if method == '3sigma':
        return outlier_3sigma_detect(df, columns, kwargs.get('threshold', 3.0))
    elif method == 'iqr':
        return outlier_iqr_detect(df, columns, kwargs.get('k', 1.5))
    elif method == 'zscore':
        return outlier_zscore_detect(df, columns, kwargs.get('threshold', 3.0))
    elif method == 'moving_std':
        return outlier_moving_std_detect(df, columns, kwargs.get('window', 10), kwargs.get('threshold', 3.0))
    elif method == 'dbscan':
        return outlier_dbscan_detect(df, columns, kwargs.get('eps', 0.5), kwargs.get('min_samples', 5))
    else:
        return {}


def handle_outliers(df: pd.DataFrame, columns: List[str],
                   method: str = 'iqr', action: str = 'clip', **kwargs) -> pd.DataFrame:
    """统一的异常值处理接口

    Args:
        df: 输入 DataFrame
        columns: 需要处理的列
        method: 检测方法
        action: 处理方式 ('clip', 'remove', 'nan')
        **kwargs: 方法对应的参数

    Returns:
        处理后的 DataFrame
    """
    if method == '3sigma':
        return outlier_3sigma_clip(df, columns, kwargs.get('threshold', 3.0))
    elif method == 'iqr':
        return outlier_iqr_clip(df, columns, kwargs.get('k', 1.5))
    elif method == 'zscore':
        return outlier_zscore_clip(df, columns, kwargs.get('threshold', 3.0))
    elif method == 'moving_std':
        return outlier_moving_std_clip(df, columns, kwargs.get('window', 10), kwargs.get('threshold', 3.0))
    elif method == 'dbscan':
        if action == 'remove':
            return outlier_dbscan_remove(df, columns, kwargs.get('eps', 0.5), kwargs.get('min_samples', 5))
        else:
            # DBSCAN 不支持裁剪，默认返回原数据
            return df
    else:
        return df


# ==================== 导出 ====================

__all__ = [
    'outlier_3sigma_detect',
    'outlier_3sigma_clip',
    'outlier_3sigma_remove',
    'outlier_iqr_detect',
    'outlier_iqr_clip',
    'outlier_zscore_detect',
    'outlier_zscore_clip',
    'outlier_moving_std_detect',
    'outlier_moving_std_clip',
    'outlier_dbscan_detect',
    'outlier_dbscan_remove',
    'detect_outliers',
    'handle_outliers'
]