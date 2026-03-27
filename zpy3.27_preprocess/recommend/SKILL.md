---
name: recommend
description: 策略推荐子 skill，负责分析数据特征、推荐处理策略并生成调用链表。当主 skill 需要生成处理策略时调用此 skill。输入：assess_result、analysis_report、business_scene、target_columns输出：strategy_table（推荐策略表）、plan（调用链表）
---

# Recommend - 策略推荐

策略推荐子 skill，负责分析数据特征、推荐处理策略并生成调用链表。

## 触发条件

由主 skill time_data_process 调用，执行策略推荐任务。

## 输入

| 参数 | 类型 | 说明 |
|------|------|------|
| assess_result | Dict | 数据质量评估结果（来自 analyze） |
| analysis_report | str | 数据分析报告 |
| business_scene | str | 业务场景描述 |
| target_columns | List[str] | 用户指定需要处理的列和需求 |

## 输出

| 参数 | 类型 | 说明 |
|------|------|------|
| strategy_table | str | 推荐策略表（Markdown 格式） |
| plan | List[Dict] | 调用链表，用于 process 执行 |

## 原子函数清单

### 策略推荐
| 函数 | 说明 | 典型场景 |
|------|------|----------|
| `parse_target_columns(user_input)` | 解析用户输入 | 解析"列名:处理类型"格式 |
| `recommend_strategy(df, columns, business_context)` | 综合推荐处理策略 | 根据数据特征和业务场景自动推荐最佳处理方法 |

### 策略表生成
| 函数 | 说明 | 典型场景 |
|------|------|----------|
| `generate_strategy_table(strategies, target_columns)` | 生成策略表 | 展示推荐策略给用户确认 |
| `generate_plan(strategies, target_columns)` | 生成调用链表 | 构建执行计划 |

## 使用示例

```python
import sys
sys.path.insert(0, 'scripts')
from recommend import parse_target_columns, recommend_strategy, generate_strategy_table, generate_plan

# 用户输入（字符串）
user_input = "temperature:缺失值填充,滤波降噪;humidity:缺失值填充"

# 解析用户输入
parsed_columns = parse_target_columns(user_input)
# parsed_columns = [('temperature', ['缺失值填充', '滤波降噪']), ('humidity', ['缺失值填充'])]

# 获取需要处理的列名列表
columns = [col for col, needs in parsed_columns]

# 为每列推荐策略
strategies = {}
for col in columns:
    col_strategy = recommend_strategy(df, [col], business_context='sensor_data', process_type='all')
    strategies[col] = col_strategy.get(col, {})

# 为每列推荐策略
strategies = {}
for col, needs in parsed_columns:
    col_strategies = recommend_strategy(df, [col], business_scene='sensor_data', process_type='all')
    strategies[col] = col_strategies.get(col, {})

# 生成策略表
strategy_table = generate_strategy_table(strategies, parsed_columns)

# 生成调用链表
plan = generate_plan(strategies, parsed_columns)

# 输出
# strategy_table: Markdown 格式的策略表
# plan: 步骤列表，用于 process 执行
```

## 输出格式

### strategy_table 格式：
```markdown
| 列名 | 处理类型 | 推荐方法 | 备选方法 | 推荐理由 |
|------|----------|----------|----------|----------|
| temperature | 缺失值填充 | ffill | bfill/mean/median/interpolate | 时间序列数据，前向填充 |
| temperature | 滤波降噪 | moving_avg | median/fourier | 随机噪声，移动平均平滑 |
| humidity | 缺失值填充 | ffill | bfill/mean/median/interpolate | 时间序列数据，前向填充 |
```

### plan 格式：
```python
[
    {
        "step": 1,
        "step_name": "缺失值处理",
        "columns": ["temperature", "humidity"],
        "func": "fill_missing",
        "method": "ffill"
    },
    {
        "step": 2,
        "step_name": "滤波降噪",
        "columns": ["temperature"],
        "func": "filter_noise",
        "type": "moving_avg",
        "window_size": 10
    }
]
```

## 智能策略推荐规则

### 缺失值处理推荐
| 数据特征 | 推荐方法 | 说明 |
|---------|----------|------|
| 缺失值在数据开头 + 缺失块较短 | fill_missing_bfill | 后向填充 |
| 类别型数据 + 众数占比高 | fill_missing_mode | 众数填充 |
| 连续少量缺失 + 时间序列 | fill_missing_ffill | 前向填充 |
| 连续多行缺失 + 线性趋势 | fill_missing_interpolate | 线性插值 |
| 数值型 + 异常值/偏态 | fill_missing_median | 中位数填充 |
| 数值型 + 正态分布 | fill_missing_mean | 均值填充 |

### 滤波降噪推荐
| 数据特征 | 推荐方法 | 说明 |
|---------|----------|------|
| 存在显著异常值/脉冲噪声 | filter_median | 中值滤波 |
| 随机波动/平稳数据 | filter_moving_avg | 移动平均滤波 |
| 周期性数据 | filter_fourier | 傅里叶变换滤波 |

### 标准化归一化推荐
| 数据特征 | 推荐方法 | 说明 |
|---------|----------|------|
| 需消除量纲影响 | normalize_minmax | Min-Max 归一化 |
| 需特定业务范围 | normalize_custom_range | 指定区间缩放 |
| 右偏分布/长尾数据 | normalize_log | 对数变换 |
| 数据接近正态分布 | normalize_standardize | Z-score标准化 |

### 数据替换推荐
| 数据特征 | 推荐方法 | 说明 |
|---------|----------|------|
| 右偏分布/指数增长 | transform_log | 对数变换 |
| 数据包含0 | transform_log1p | log(1+x)变换 |
| 时间序列去趋势 | transform_diff | 一阶差分 |
| 增长率/环比变化 | transform_pct_change | 百分比变化 |
| 无序类别/线性模型 | encode_onehot | 独热编码 |
| 有序类别/树模型 | encode_label | 标签编码 |

### 异常值处理推荐
| 数据特征 | 推荐方法 | 说明 |
|---------|----------|------|
| 正态分布数据 | outlier_3sigma_clip | 3-Sigma裁剪 |
| 非正态分布 | outlier_iqr_clip | IQR裁剪(默认1.5倍) |
| 高频检测(时间序列) | outlier_moving_std_clip | 移动标准差裁剪 |
| 复杂分布/未知模式 | outlier_dbscan_remove | DBSCAN聚类移除 |
| 快速标准化检测 | outlier_zscore_clip | Z-score裁剪 |
