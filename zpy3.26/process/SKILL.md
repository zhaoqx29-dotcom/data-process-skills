---
name: process
description:数据处理子 skill，负责执行数据处理、生成评价报告和对比图表。当主 skill 需要执行数据处理操作时调用此 skill。输入：df（原始数据）、plan（调用链表）输出：df_result（处理后数据）、evaluation_report、chart
---

# Process - 数据处理

数据处理子 skill，负责执行数据处理、生成评价报告和对比图表。

## 触发条件

由主 skill time_data_process 调用，执行数据处理任务。

## 输入

| 参数 | 类型 | 说明 |
|------|------|------|
| df | pd.DataFrame | 原始数据 |
| plan | List[Dict] | 调用链表（来自 recommend） |

## 输出

| 参数 | 类型 | 说明 |
|------|------|------|
| df_result | pd.DataFrame | 处理后的数据 |
| evaluation_report | str | 评价结果报告（Markdown 格式） |
| chart | str | 对比图表路径 |

## 原子函数清单

### 链式执行
| 函数 | 说明 | 典型场景 |
|------|------|----------|
| `chain_execute(df, plan, verbose)` | 链式执行多种处理操作 | 多列多种处理类型组合执行 |

### 缺失值处理
| 函数 | 说明 | 典型场景 |
|------|------|----------|
| `fill_missing_ffill(df, columns)` | 前向填充 | 时间序列数据 |
| `fill_missing_bfill(df, columns)` | 后向填充 | 数据开头缺失 |
| `fill_missing_mean(df, columns)` | 均值填充 | 正态分布数值 |
| `fill_missing_median(df, columns)` | 中位数填充 | 偏态分布/离群值 |
| `fill_missing_mode(df, columns)` | 众数填充 | 类别列 |
| `fill_missing_interpolate(df, columns)` | 线性插值 | 线性趋势数据 |

### 滤波降噪
| 函数 | 说明 | 典型场景 |
|------|------|----------|
| `filter_median(df, columns, window_size)` | 中值滤波 | 去除脉冲噪声 |
| `filter_moving_avg(df, columns, window_size)` | 移动平均滤波 | 平滑数据 |
| `filter_fourier(df, columns, cutoff_freq)` | 傅里叶变换滤波 | 频域去噪 |

### 标准化/归一化
| 函数 | 说明 | 典型场景 |
|------|------|----------|
| `normalize_minmax(df, columns, min_val, max_val)` | Min-Max 归一化 | 消除量纲 |
| `normalize_custom_range(df, columns, target_min, target_max)` | 指定区间缩放 | 业务特定范围 |
| `normalize_log(df, columns, offset, base)` | 对数变换 | 右偏分布 |

### 评价与图表
| 函数 | 说明 | 典型场景 |
|------|------|----------|
| `generate_evaluation_report(df_before, df_after, plan)` | 生成评价报告 | 展示处理效果 |
| `generate_comparison_chart(df_before, df_after, plan)` | 生成对比图表 | 可视化处理效果 |

## 使用示例

```python
import sys
sys.path.insert(0, 'scripts')
from process import chain_execute, generate_evaluation_report, generate_comparison_chart

# 假设已有 df 和 plan
# plan = [
#     {"step": 1, "step_name": "缺失值处理", "columns": ["temp"], "func": "fill_missing", "method": "ffill"},
#     {"step": 2, "step_name": "滤波降噪", "columns": ["signal"], "func": "filter_noise", "type": "moving_avg", "window_size": 10}
# ]

# 执行链式处理
df_result = chain_execute(df, plan, verbose=True)

# 生成评价报告
evaluation_report = generate_evaluation_report(df, df_result, plan)

# 生成对比图表
chart_path = generate_comparison_chart(df, df_result, plan)

# 输出
# df_result: 处理后的 DataFrame
# evaluation_report: Markdown 格式评价报告
# chart_path: 图表保存路径
```

## 输出格式

### evaluation_report 格式：
```markdown
## 数据预处理结果评价报告

### 原始数据信息
- 文件: {path}
- 业务场景: {场景}
- 处理列: {列列表}

### 执行结果
| 步骤 | 操作 | 目标列 | 方法 | 结果 |
|------|------|--------|------|------|
| 1 | 缺失值处理 | temperature | ffill | 填充3个缺失值 |
| 2 | 滤波降噪 | signal | moving_avg | 数据平滑 |

### 统计指标变化
| 列名 | 处理前均值 | 处理后均值 | 处理前标准差 | 处理后标准差 |
|------|------------|------------|--------------|--------------|
| temperature | 25.3 | 25.3 | 3.2 | 2.8 |

### 整体效果评价
- 数据质量提升: {描述}
- 业务适用性: {描述}
- 建议后续操作: {建议}
```

### plan 结构：
```python
[
    {
        "step": 1,
        "step_name": "缺失值处理",
        "columns": ["col1", "col2"],
        "func": "fill_missing",
        "method": "ffill"
    },
    {
        "step": 2,
        "step_name": "滤波降噪",
        "columns": ["col1"],
        "func": "filter_noise",
        "type": "median",
        "window_size": 5
    }
]
```
