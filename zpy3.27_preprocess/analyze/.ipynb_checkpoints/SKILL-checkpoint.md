---
name: analyze
description:数据分析子 skill，负责数据加载、数据质量评估和生成分析报告。当主 skill 需要了解数据的基本情况时调用此 skill。输入：文件路径、用户指定列输出：df、assess_result、analysis_report
---

# Analyze - 数据分析

数据分析子 skill，负责数据加载、评估和生成报告。

## 触发条件

由主 skill time_data_process 调用，执行数据分析任务。

## 输入

| 参数 | 类型 | 说明 |
|------|------|------|
| file_path | str | 数据文件路径 |
| target_columns | List[str] | 用户指定需要处理的列 |
| encoding | str | 文件编码，默认 utf-8 |
| sheet_name | str | Excel 文件工作表名 |

## 输出

| 参数 | 类型 | 说明 |
|------|------|------|
| df | pd.DataFrame | 加载的 DataFrame |
| assess_result | Dict | 数据质量评估结果 |
| analysis_report | str | 文字版分析报告 |

## 原子函数清单

### 数据加载
| 函数 | 说明 | 典型场景 |
|------|------|----------|
| `load_csv(path, encoding, **kwargs)` | 加载 CSV | CSV 文件 |
| `load_excel(path, sheet_name, **kwargs)` | 加载 Excel | .xlsx/.xls 文件 |
| `load_file(path, **kwargs)` | 智能加载 | 根据扩展名自动选择（推荐）|

### 数据评估
| 函数 | 说明 | 典型场景 |
|------|------|----------|
| `assess(df)` | 全面评估数据质量 | 了解数据基本情况、缺失值统计 |

## 使用示例

```python
import sys
sys.path.insert(0, 'scripts')
from analyze import load_file, assess, generate_analysis_report

# 加载数据
df = load_file('sensor_data.csv')

# 评估数据质量
assess_result = assess(df)

# 生成分析报告
target_columns = ['temperature', 'humidity']
analysis_report = generate_analysis_report(df, assess_result, target_columns)

# 输出
# df: 加载的 DataFrame
# assess_result: 评估结果字典
# analysis_report: Markdown 格式分析报告
```

## 输出格式

### assess() 返回格式：
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
    },
    "missing_analysis": {
        "col1": {
            "max_consecutive_missing": 3,
            "is_continuous_small_missing": True,
            "is_at_start": False
        }
    }
}
```

### analysis_report 格式：
```markdown
📊 数据分析报告

### 数据概览
• 数据文件: {path}
• 数据形状: {rows} 行 × {cols} 列
• 处理涉及列: {count} 列

---

### 指定列分析结果

【列名：{column_name}】
• 数据类型: {dtype}
• 缺失值数量: {count}
• 缺失率: {percentage}%
• 取值数量: {unique_count}
• 数据特征: {characteristics}

---

### 数据质量评估
• 完整性评估: {描述}
• 潜在问题: {问题和建议}
```
