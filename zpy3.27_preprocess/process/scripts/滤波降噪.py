"""
滤波降噪模块
提供各种数据平滑/降噪方法
"""

import pandas as pd
import numpy as np
from typing import List, Optional
from scipy import signal
from scipy.fft import fft, ifft, fftfreq


def filter_median(df: pd.DataFrame, columns: List[str], window_size: int = 3) -> pd.DataFrame:
    """中值滤波：用窗口内中值替换当前值（适合去除脉冲噪声/孤立尖峰）"""
    if window_size % 2 != 1:
        raise ValueError("window_size must be an odd number")
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            df[col] = signal.medfilt(df[col].values, kernel_size=window_size)
    return df


def filter_moving_avg(df: pd.DataFrame, columns: List[str], window_size: int = 5,
                     center: bool = False) -> pd.DataFrame:
    """移动平均滤波：用窗口内均值替换当前值（适合平滑随机波动）"""
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype in ["int64", "float64", "int32", "float32"]:
            df[col] = df[col].rolling(window=window_size, center=center, min_periods=1).mean()
    return df


def filter_fourier(df: pd.DataFrame, columns: List[str], cutoff_freq: float = 0.1,
                   sampling_rate: Optional[float] = None) -> pd.DataFrame:
    """傅里叶变换滤波：在频域滤除高频噪声（适合周期性数据）"""
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


# 导出所有函数
__all__ = [
    'filter_median',
    'filter_moving_avg',
    'filter_fourier'
]