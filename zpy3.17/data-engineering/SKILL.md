me: data-engineering
description: |
  数据工程智能工具层，提供结构化表格数据的加载、评估、智能策略推荐与数据清洗。
  支持缺失值处理、滤波降噪、标准化归一化等多种数据工程能力。
  当用户需要加载数据、评估质量、处理缺失值、滤波降噪、标准化等时触发。
  Agent 可根据数据特征自动推荐最佳处理策略，也可按照用户手动指定的策略执行。
  执行完成后必须展示详细的处理报告，包括：处理了哪些数据、做了哪些操作、操作前后数据状态对比等。
---

# Data Engineering

数据加载、评估、数据清洗模块，支持：
- 缺失值处理（前/后向填充、统计填充、插值）
- 滤波降噪（中值滤波、移动平均、傅里叶变换）
- 标准化/归一化（Min-Max、指定区间、对数变换）

## 触发条件

- 用户提到"加载数据"、"读取数据"、"导入数据"
- 用户提到"数据评估"、"数据质量"、"查看缺失值"、"数据检查"
- 用户提到"填充缺失值"、"处理空值"、"缺失值处理"
- 用户提到"滤波"、"降噪"、"去噪"、"平滑数据"
- 用户提到"标准化"、"归一化"、"Min-Max"、"缩放"、"对数变换"
- 用户提到"前向填充"、"后向填充"、"均值填充"、"中位数填充"、"众数填充"

## 原子函数清单

### 数据加载
| 函数 | 说明 | 典型场景 |
|------|------|----------|
| `load_csv(path, encoding, **kwargs)` | 加载 CSV | CSV 文件 |
| `load_excel(path, sheet_name, **kwargs)` | 加载 Excel | .xlsx/.xls 文件 |
| `save_csv(df, path, encoding, index)` | 保存 CSV | 导出 CSV |
| `save_excel(df, path, sheet_name, index)` | 保存 Excel | 导出 .xlsx/.xls |
| `load_file(path, **kwargs)` | 智能加载 | 根据扩展名自动选择（推荐）|
| `save_file(df, path, **kwargs)` | 智能保存 | 根据扩展名自动保存（推荐）|

**支持的文件格式：** .csv, .xlsx, .xls

**编码说明：**
- 默认编码：`utf-8`
- 中文数据常见编码：`gbk`、`gb2312`
- 如果加载失败，尝试使用 `encoding='gbk'` 或 `encoding='utf-8-sig'`
- Excel 文件（.xlsx/.xls）通常不需要指定编码

### 数据评估
| 函数 | 说明 | 典型场景 |
|------|------|----------|
| `assess(df)` | 全面评估数据质量 | 了解数据基本情况、缺失值统计 |

**assess() 返回格式：**
```python
{
    "shape": (rows, cols),
    "columns": ["col1", "col2", ...],
    "dtypes": {"col1": "int64", "col2": "float64", ...},
    "missing": {
        "col1": {"count": 5, "percentage": 5.0},
        "col2": {"count": 0, "percentage": 0.0}
    },
    "duplicates": 2,
    "column_analysis": {
        "col1": {
            "unique_count": 10,
            "characteristics": ["数值型", "偏态分布"],
            "dtype": "float64"
        }
    }
}
```

### 智能策略推荐
| 函数 | 说明 | 典型场景 |
|------|------|----------|
| `recommend_strategy(df, columns, business_context)` | 综合推荐处理策略 | 根据数据特征和业务场景自动推荐最佳处理方法 |

**参数说明：**
- `df`: 数据框 (pandas.DataFrame)
- `columns`: 需要分析的列名列表 (List[str])
- `business_context`: 业务场景描述，如 "sensor_data"（传感器数据）、"time_series"（时间序列）、"general"（通用）

**返回格式：**
```python
{
    "column_name": {
        "recommended_method": "fill_missing_median",
        "reason": "数值列，缺失率5-30%，推荐使用对离群值不敏感的中位数填充",
        "missing_rate": 8.5,
        "data_type": "float64",
        "characteristics": ["数值型", "偏态分布"]
    }
}
```

### 缺失值处理 - 前后填充
| 函数 | 说明 | 典型场景 |
|------|------|----------|
| `fill_missing_ffill(df, columns)` | 前向填充 | 时间序列数据，用前一个值填充 |
| `fill_missing_bfill(df, columns)` | 后向填充 | 时间序列数据，用后一个值填充 |

### 缺失值处理 - 统计填充
| 函数 | 说明 | 典型场景 |
|------|------|----------|
| `fill_missing_mean(df, columns)` | 均值填充 | 数值列、正态分布 |
| `fill_missing_median(df, columns)` | 中位数填充 | 数值列、偏态分布 |
| `fill_missing_mode(df, columns)` | 众数填充 | 类别列，用最频繁的值 |
| `fill_missing_value(df, column, value)` | 指定值填充 | 特定业务含义（如"未知"、0） |
| `fill_missing_interpolate(df, columns)` | 线性插值 | 数值型数据的线性趋势插值 |

### 滤波降噪
| 函数 | 说明 | 典型场景 |
|------|------|----------|
| `filter_median(df, columns, window_size)` | 中值滤波 | 去除脉冲噪声、离群点 |
| `filter_moving_avg(df, columns, window_size)` | 移动平均滤波 | 平滑数据、减少随机噪声 |
| `filter_fourier(df, columns, cutoff_freq)` | 傅里叶变换滤波 | 频域去噪、去除特定频率成分 |

### 标准化/归一化
| 函数 | 说明 | 典型场景 |
|------|------|----------|
| `normalize_minmax(df, columns, min_val, max_val)` | Min-Max 归一化 | 将数据缩放到 [0,1] 或指定范围 |
| `normalize_custom_range(df, columns, target_min, target_max)` | 指定区间缩放 | 将数据缩放到任意目标区间 |
| `normalize_log(df, columns, offset, base)` | 对数变换 | 处理右偏数据，使分布接近正态 |

### 工具函数
| 函数 | 说明 | 典型场景 |
|------|------|----------|
| `get_fill_summary(df_before, df_after, fill_dict)` | 获取填充摘要 | 生成处理报告 |

### 链式执行（Chain Execute）
| 函数 | 说明 | 典型场景 |
|------|------|----------|
| `chain_execute(df, plan, verbose)` | 链式执行多种处理操作 | 多列多种处理类型组合执行 |

**chain_execute() 参数说明：**
- `df`: 输入数据框 (pandas.DataFrame)
- `plan`: 处理计划列表 (List[Dict])，每个元素包含操作描述
- `verbose`: 是否打印执行详情 (bool, 默认 True)

**plan 结构示例：**
```python
plan = [
    # 步骤1：缺失值处理（预定义方法）
    {
        "step": 1,
        "step_name": "缺失值处理",
        "columns": ["temperature", "humidity"],
        "func": "fill_missing",
        "method": "ffill"
    },
    # 步骤2：缺失值处理（自定义功能组合）
    {
        "step": 2,
        "step_name": "缺失值处理",
        "columns": ["target_column"],
        "func": "fill_missing",
        "method": "custom_combine",
        "mode_of": ["col_a", "col_b", "col_c"],  # 从这三列取众数
        "description": "取col_a、col_b、col_c三列的众数填入target_column"
    },
    # 步骤3：滤波降噪
    {
        "step": 3,
        "step_name": "滤波降噪",
        "columns": ["signal"],
        "func": "filter_noise",
        "type": "median",
        "window_size": 5
    },
    # 步骤4：标准化归一化
    {
        "step": 4,
        "step_name": "标准化归一化",
        "columns": ["feature1", "feature2"],
        "func": "normalize",
        "type": "minmax"
    }
]
```

**支持的自定义策略类型：**

| 策略类型 | method/type | 参数说明 | 示例 |
|----------|------------|----------|------|
| 预定义方法 | ffill/bfill/mean/median/mode/interpolate - fill_missing<br>median/moving_avg/fourier - filter_noise<br>minmax/custom_range/log - normalize | 按对应函数的参数传入 | 同单独调用现有函数 |
| 自定义功能组合 | custom_combine | `mode_of`: 列名列表，取这些列的众数填入目标列 | "取A、B、C三列的众数填入D列" |
| 填充指定值 | custom_value | `value`: 指定填充值（数值或字符串） | 填充为 0、-1、"未知" |
| 自定义计算表达式 | custom_expression | `expression`: 计算表达式（需解析） | "用列1的均值填充列2" |
| 跳过处理 | skip | 标记跳过该列该类型处理 | sensor_value 列跳过标准化 |

## 智能策略推荐规则

### 缺失值处理自动推荐规则
| 数据特征 | 推荐方法 | 触发条件 |
|---------|----------|---------|
| fill_missing_ffill | 前向填充 | 检测到时间序列列（列名含 date/time/timestamp）或业务场景为传感器/时间序列 |
| fill_missing_bfill | 后向填充 | 时间序列数据且首行缺失 |
| fill_missing_mean | 均值填充 | 数值列、缺失率 < 5%、分布接近正态 |
| fill_missing_median | 中位数填充 | 数值列、缺失率 5%-30% 或分布偏态 |
| fill_missing_mode | 众数填充 | 类别列（object类型）或取值数量 < 20 |
| fill_missing_interpolate | 线性插值 | 数值列、数据有明显线性趋势 |
| 谨慎处理 | 不推荐自动填充 | 缺失率 > 30%，需用户确认 |

### 滤波降噪自动推荐规则
| 数据特征 | 推荐方法 | 触发条件 |
|---------|----------|---------|
| filter_median | 中值滤波 | 检测到脉冲噪声（大量离群点） |
| filter_moving_avg | 移动平均 | 检测到随机噪声（数据波动较大） |
| filter_fourier | 傅里叶变换 | 检测到周期性噪声（频谱有明显峰值） |

### 标准化归一化自动推荐规则
| 数据特征 | 推荐方法 | 触发条件 |
|---------|----------|---------|
| normalize_minmax | Min-Max 归一化 | 数值列、需要消除量纲影响 |
| normalize_log | 对数变换 | 数值列、右偏分布（偏度 > 1） |
| normalize_custom_range | 指定区间缩放 | 数值列、业务需要特定范围（如评分0-100） |

## 智能推荐使用场景

### 场景 A：由 strategy3.9 调用自动推荐
```
1. strategy3.9 传递：df、target_columns、business_context
2. data-engineering 调用 recommend_strategy() 获取推荐策略
3. 返回策略列表给 strategy3.9
4. strategy3.9 展示给用户确认
```

### 场景 B：由 strategy3.9 调用执行指定策略
```
1. strategy3.9 收集用户确认后的策略
2. 传递给 data-engineering 执行对应的 fill_missing_* 等函数
3. 返回执行结果（处理后的数据 + 填充摘要）
```

## Agent 内部编排决策

### 执行后必须展示详细报告

填充完成后，必须展示以下格式的报告：

```markdown
## 缺失值填充处理报告

### 原始数据
- 文件: {path}
- 形状: {rows} 行 × {cols} 列

### 数据评估结果
| 列名 | 数据类型 | 缺失值数量 | 缺失率 |
|------|----------|------------|---------|
| {column} | {dtype} | {count} | {percentage}% |

### 填充操作记录
| 列名 | 填充方法 | 填充数量 |
|------|----------|----------|
| {column} | {method} | {count} |

### 填充后数据
- 保存路径: {path}
- 形状: {rows} 行 × {cols} 列
- 剩余缺失值: {count}
```

### 场景 1：首次加载数据并评估
```
1. load_csv() 加载数据
2. assess() 评估数据质量
3. 展示缺失值统计表
4. 分析缺失率，判断是否需要处理
```

### 场景 2：时间序列数据填充
```
1. 查看缺失情况（assess 结果）
2. 检查是否为时间序列数据
3. 优先使用 fill_missing_ffill() 前向填充
4. 首行缺失可使用 fill_missing_bfill() 后向填充
```

### 场景 3：数值列统计填充
```
1. 查看缺失率和数据分布
2. 正态分布 → fill_missing_mean()
3. 偏态分布（有离群值）→ fill_missing_median()
4. 记录填充前后的统计变化
```

### 场景 4：类别列填充
```
1. 查看类别列的缺失情况
2. 使用 fill_missing_mode() 用众数填充
3. 或使用 fill_missing_value() 填充为"未知"等特定值
```

### 场景 5：基于缺失率的策略选择
```
1. 检查各列缺失率（assess 结果）
2. 缺失率 < 5% → 直接填充（数值用 median，类别用 mode）
3. 缺失率 5-30% → 考虑是否填充或删除
4. 缺失率 > 30% → 考虑删除该列或特殊处理
```

### 场景 6：插值填充（数据有趋势）
```
1. 查看缺失情况（assess 结果）
2. 检查数据是否有线性趋势（查看图表或分析）
3. 使用 fill_missing_interpolate() 进行线性插值
4. 可选择不同插值方法：'linear', 'cubic', 'quadratic' 等
```

### 场景 7：滤波降噪（传感器数据）
```
1. 评估数据质量，查看噪声特征（assess 结果）
2. 脉冲噪声 → filter_median() 中值滤波
3. 随机噪声 → filter_moving_avg() 移动平均
4. 周期性噪声 → filter_fourier() 傅里叶变换滤波
```

### 场景 8：标准化/归一化
```
1. 检查数据分布特征
2. 数据需要统一尺度 → normalize_minmax() Min-Max 归一化
3. 需要特定业务范围 → normalize_custom_range() 指定区间缩放
4. 右偏数据（如收入、价格）→ normalize_log() 对数变换
```

## 使用示例

### 示例 1：智能推荐策略（由 strategy3.9 调用）

```python
import sys
sys.path.insert(0, '.claude/skills/data-engineering/scripts')
from data_ops import load_file, assess, recommend_strategy

# 加载数据
df = load_file('sensor_data.csv')

# 指定需要处理的列
target_columns = ['temperature', 'humidity', 'pressure']

# 获取自动推荐策略（业务场景：传感器数据）
strategies = recommend_strategy(df, target_columns, business_context='sensor_data')

"""
strategies 返回格式：
{
    'temperature': {
        'recommended_method': 'fill_missing_ffill',
        'reason': '传感器数据，推荐使用前向填充保留时间趋势',
        'missing_rate': 3.2,
        'data_type': 'float64',
        'characteristics': ['数值型', '时间序列', '连续变化']
    },
    'humidity': {
        'recommended_method': 'fill_missing_ffill',
        'reason': '传感器数据，推荐使用前向填充保留时间趋势',
        'missing_rate': 5.1,
        'data_type': 'float64',
        'characteristics': ['数值型', '时间序列']
    },
    'pressure': {
        'recommended_method': 'fill_missing_ffill',
        'reason': '传感器数据，推荐使用前向填充保留时间趋势',
        'missing_rate': 2.8,
        'data_type': 'float64',
        'characteristics': ['数值型', '时间序列']
    }
}
"""
```

### 示例 2：执行指定策略（由 strategy3.9 调用）

```python
import sys
sys.path.insert(0, '.claude/skills/data-engineering/scripts')
from data_ops import (
    load_file, save_file, assess,
    # 缺失值处理
    fill_missing_ffill, fill_missing_bfill,
    fill_missing_mean, fill_missing_median,
    fill_missing_mode, fill_missing_value,
    fill_missing_interpolate,
    # 滤波降噪
    filter_median, filter_moving_avg, filter_fourier,
    # 标准化/归一化
    normalize_minmax, normalize_custom_range, normalize_log
)

# 智能加载数据（自动识别 CSV 或 Excel）
df = load_file('sales_data.csv')
# 或
df = load_file('sales_data.xlsx', sheet_name='Sheet1')

# 评估数据
report = assess(df)
# report 包含: shape, columns, dtypes, missing (每列的缺失数和缺失率)

# 根据用户确认的策略执行缺失值处理
df = fill_missing_ffill(df, ['temperature'])  # 时间序列数据前向填充
df = fill_missing_mean(df, ['amount', 'price'])  # 数值列均值填充
df = fill_missing_mode(df, ['category', 'city'])  # 类别列众数填充
df = fill_missing_interpolate(df, ['sensor_value'])  # 线性插值

# 滤波降噪
df = filter_median(df, ['signal'], window_size=5)  # 中值滤波去除脉冲噪声
df = filter_moving_avg(df, ['temperature'], window_size=10)  # 移动平均平滑数据
df = filter_fourier(df, ['audio'], cutoff_freq=0.1)  # 傅里叶变换滤波

# 标准化/归一化
df = normalize_minmax(df, ['feature1', 'feature2'])  # Min-Max 归一化到 [0,1]
df = normalize_custom_range(df, ['value'], target_min=0, target_max=100)  # 指定区间缩放
df = normalize_log(df, ['price', 'income'])  # 对数变换处理右偏数据

# 智能保存结果
save_file(df, 'cleaned_data.csv')
# 或保存为 Excel
save_file(df, 'cleaned_data.xlsx', sheet_name='Result')
```

### 示例 3：完整工作流程（strategy3.9 调用 data-engineering）

```python
# ==== strategy3.9 部分 ====

# 1. 加载数据
df = load_file('sensor_data.csv')

# 2. 询问用户业务语义（已收集）
#    - 业务场景：传感器温度数据
#    - 处理列：temperature, humidity
#    - 处理需求：缺失值填充

# 3. 调用 assess 进行数据分析
report = assess(df)

# 4. 调用 recommend_strategy 获取自动推荐策略
strategies = recommend_strategy(df, ['temperature', 'humidity'], business_context='sensor_data')

# 5. 展示推荐策略给用户确认
# 策略显示：
# temperature: fill_missing_ffill - 传感器数据，前向填充保留时间趋势
# humidity: fill_missing_ffill - 传感器数据，前向填充保留时间趋势

# 6. 询问用户是否需要自定义策略
# 用户选择：n（使用推荐策略）

# === 执行处理 ===

# 7. 根据推荐策略执行处理
df_filled = fill_missing_ffill(df, ['temperature'])
df_filled = fill_missing_ffill(df_filled, ['humidity'])

# 8. 生成处理结果评价报告
final_report = assess(df_filled)
```

### 示例 4：链式执行多种处理（支持自定义策略）

```python
import sys
sys.path.insert(0, '.claude/skills/data-engineering/scripts')
from data_ops import load_file, chain_execute

# 加载数据
df = load_file('sensor_data.csv')

# 构建处理计划（由 strategy3.9 生成）
plan = [
    # 步骤1：缺失值处理（预定义方法）
    {
        "step": 1,
        "step_name": "缺失值处理",
        "columns": ["temperature", "humidity"],
        "func": "fill_missing",
        "method": "ffill"
    },
    # 步骤2：缺失值处理（自定义功能组合）
    {
        "step": 2,
        "step_name": "缺失值处理",
        "columns": ["target_column"],
        "func": "fill_missing",
        "method": "custom_combine",
        "mode_of": ["col_a", "col_b", "col_c"],
        "description": "取col_a、col_b、col_c三列的众数填入target_column"
    },
    # 步骤3：滤波降噪
    {
        "step": 3,
        "step_name": "滤波降噪",
        "columns": ["signal"],
        "func": "filter_noise",
        "type": "median",
        "window_size": 5
    },
    # 步骤4：标准化归一化
    {
        "step": 4,
        "step_name": "标准化归一化",
        "columns": ["feature1", "feature2"],
        "func": "normalize",
        "type": "minmax"
    }
]

# 执行链式处理
df_result = chain_execute(df, plan, verbose=True)

# 输出示例：
# 步骤1: 缺失值处理 - temperature, humidity (ffill)
# 步骤2: 缺失值处理 - target_column (custom_combine)
# 步骤3: 滤波降噪 - signal (median)
# 步骤4: 标准化归一化 - feature1, feature2 (minmax)
```

## 填充策略参考

详细策略见 [references/cleaning-strategies.md](references/cleaning-strategies.md)

### 滤波降噪策略

| 噪声类型 | 推荐方法 | 说明 |
|----------|----------|------|
| 脉冲噪声/离群点 | 中值滤波 | 中值滤波对异常值不敏感，最适合去除脉冲噪声 |
| 随机噪声 | 移动平均 | 通过取窗口内平均值平滑数据，减少随机波动 |
| 频率特定噪声 | 傅里叶变换 | 在频域中过滤特定频率成分 |

### 标准化/归一化策略

| 数据特征 | 推荐方法 | 说明 |
|---------|----------|------|
| 需消除量纲影响 | Min-Max 归一化 | 统一缩放到 [0,1]，适合距离类算法 |
| 需要特定 业务范围 | 指定区间缩放 | 将数据映射到任意目标区间，如 [0,100] |
| 右偏分布/长尾数据 | 对数变换 | 使数据分布更接近正态，减少倾斜影响 |
