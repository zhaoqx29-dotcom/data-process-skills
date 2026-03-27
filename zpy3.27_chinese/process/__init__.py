"""
Process 数据处理模块（包）
"""

from .scripts import (
    缺失值填充,
    滤波降噪,
    标准化归一化,
    数据替换,
    异常值处理,
    诊断与执行
)

from .scripts.诊断与执行 import (
    diagnose_data,
    print_diagnosis_report,
    interactive_confirm_and_process,
    modify_recommendations,
    apply_recommendations,
    chain_execute,
    generate_evaluation_report,
    generate_comparison_chart
)

from .scripts.缺失值填充 import (
    fill_missing_ffill,
    fill_missing_bfill,
    fill_missing_mean,
    fill_missing_median,
    fill_missing_mode,
    fill_missing_interpolate
)

from .scripts.滤波降噪 import (
    filter_median,
    filter_moving_avg,
    filter_fourier
)

from .scripts.标准化归一化 import (
    normalize_minmax,
    normalize_custom_range,
    normalize_log,
    normalize_standardize
)

from .scripts.数据替换 import (
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

from .scripts.异常值处理 import (
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
    'diagnose_data',
    'print_diagnosis_report',
    'interactive_confirm_and_process',
    'modify_recommendations',
    'apply_recommendations',
    'chain_execute',
    'generate_evaluation_report',
    'generate_comparison_chart',
    'fill_missing_ffill',
    'fill_missing_bfill',
    'fill_missing_mean',
    'fill_missing_median',
    'fill_missing_mode',
    'fill_missing_interpolate',
    'filter_median',
    'filter_moving_avg',
    'filter_fourier',
    'normalize_minmax',
    'normalize_custom_range',
    'normalize_log',
    'normalize_standardize',
    'transform_log',
    'transform_log1p',
    'transform_diff',
    'transform_diff2',
    'transform_pct_change',
    'encode_label',
    'encode_onehot',
    'encode_target',
    'encode_ordinal',
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
    '缺失值填充',
    '滤波降噪',
    '标准化归一化',
    '数据替换',
    '异常值处理',
    '诊断与执行'
]