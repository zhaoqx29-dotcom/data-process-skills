"""
Process 数据处理模块
统一导出所有处理函数

使用方式：
    from process import chain_execute, generate_evaluation_report, generate_comparison_chart

    # 或导入所有模块
    from process import 缺失值填充, 滤波降噪, 标准化归一化, 数据替换, 异常值处理, 诊断与执行
"""

# 导入各个子模块
from . import 缺失值填充
from . import 滤波降噪
from . import 标准化归一化
from . import 数据替换
from . import 异常值处理
from . import 诊断与执行

# 从诊断与执行模块导出主要函数
from .诊断与执行 import (
    diagnose_data,
    print_diagnosis_report,
    interactive_confirm_and_process,
    modify_recommendations,
    apply_recommendations,
    chain_execute,
    generate_evaluation_report,
    generate_comparison_chart
)

# 导出缺失值填充函数
from .缺失值填充 import (
    fill_missing_ffill,
    fill_missing_bfill,
    fill_missing_mean,
    fill_missing_median,
    fill_missing_mode,
    fill_missing_interpolate
)

# 导出滤波降噪函数
from .滤波降噪 import (
    filter_median,
    filter_moving_avg,
    filter_fourier
)

# 导出标准化归一化函数
from .标准化归一化 import (
    normalize_minmax,
    normalize_custom_range,
    normalize_log,
    normalize_standardize
)

# 导出数据替换函数
from .数据替换 import (
    transform_log,
    transform_log1p,
    transform_diff,
    transform_diff2,
    transform_pct_change,
    encode_label,
    encode_onehot,
    encode_target,
    encode_ordinal
)

# 导出异常值处理函数
from .异常值处理 import (
    outlier_3sigma_detect,
    outlier_3sigma_clip,
    outlier_iqr_detect,
    outlier_iqr_clip,
    outlier_zscore_detect,
    outlier_zscore_clip,
    outlier_moving_std_detect,
    outlier_moving_std_clip,
    outlier_dbscan_detect,
    outlier_dbscan_remove,
    detect_outliers,
    handle_outliers
)

__all__ = [
    # 诊断与执行
    'diagnose_data',
    'print_diagnosis_report',
    'interactive_confirm_and_process',
    'modify_recommendations',
    'apply_recommendations',
    'chain_execute',
    'generate_evaluation_report',
    'generate_comparison_chart',
    # 缺失值填充
    'fill_missing_ffill',
    'fill_missing_bfill',
    'fill_missing_mean',
    'fill_missing_median',
    'fill_missing_mode',
    'fill_missing_interpolate',
    # 滤波降噪
    'filter_median',
    'filter_moving_avg',
    'filter_fourier',
    # 标准化归一化
    'normalize_minmax',
    'normalize_custom_range',
    'normalize_log',
    'normalize_standardize',
    # 数据替换
    'transform_log',
    'transform_log1p',
    'transform_diff',
    'transform_diff2',
    'transform_pct_change',
    'encode_label',
    'encode_onehot',
    'encode_target',
    'encode_ordinal',
    # 异常值处理
    'outlier_3sigma_detect',
    'outlier_3sigma_clip',
    'outlier_iqr_detect',
    'outlier_iqr_clip',
    'outlier_zscore_detect',
    'outlier_zscore_clip',
    'outlier_moving_std_detect',
    'outlier_moving_std_clip',
    'outlier_dbscan_detect',
    'outlier_dbscan_remove',
    'detect_outliers',
    'handle_outliers',
    # 子模块（可直接导入使用）
    '缺失值填充',
    '滤波降噪',
    '标准化归一化',
    '数据替换',
    '异常值处理',
    '诊断与执行'
]