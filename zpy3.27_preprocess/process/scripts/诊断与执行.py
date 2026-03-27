"""
诊断与执行模块
数据诊断、交互确认、链式执行、评价报告
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional

# 尝试导入子模块（作为包时使用相对导入，独立运行时使用绝对导入）
try:
    from . import 缺失值填充, 滤波降噪, 标准化归一化, 数据替换, 异常值处理
except ImportError:
    import 缺失值填充
    import 滤波降噪
    import 标准化归一化
    import 标准化归一化


# ==================== 数据诊断 ====================

def diagnose_data(df: pd.DataFrame) -> Dict:
    """诊断数据质量，检测四个指标：缺失值、异常值、重复行、噪声

    Args:
        df: 输入 DataFrame

    Returns:
        诊断报告字典，包含检测结果和处理建议
    """
    diagnosis = {
        "missing": {},      # 缺失值: {列名: 缺失数量}
        "outliers": {},    # 异常值: {列名: 异常值数量}
        "duplicates": 0,   # 重复行数量
        "noise": {},       # 噪声: {列名: CV值}
        "recommendations": []  # 处理建议列表
    }

    # 1. 检测缺失值
    for col in df.columns:
        missing_count = df[col].isna().sum()
        if missing_count > 0:
            missing_ratio = missing_count / len(df)
            diagnosis["missing"][col] = {
                "count": missing_count,
                "ratio": missing_ratio,
                "method": "median" if missing_ratio >= 0.5 else "mean"
            }

    # 2. 检测异常值 (IQR方法)
    for col in df.columns:
        if df[col].dtype in ["int64", "float64", "int32", "float32"]:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 3 * IQR
            upper_bound = Q3 + 3 * IQR

            outlier_mask = (df[col] < lower_bound) | (df[col] > upper_bound)
            outlier_count = outlier_mask.sum()

            if outlier_count > 0:
                diagnosis["outliers"][col] = {
                    "count": outlier_count,
                    "lower_bound": lower_bound,
                    "upper_bound": upper_bound
                }

    # 3. 检测重复行
    diagnosis["duplicates"] = df.duplicated().sum()

    # 4. 检测噪声 (变异系数 CV = std/mean)
    for col in df.columns:
        if df[col].dtype in ["int64", "float64", "int32", "float32"]:
            mean_val = df[col].mean()
            std_val = df[col].std()
            if mean_val != 0:
                cv = abs(std_val / mean_val)
                if cv > 0.5:
                    diagnosis["noise"][col] = {
                        "cv": cv,
                        "method": "moving_avg",
                        "window_size": 5
                    }

    # 生成处理建议
    recommendations = []

    # 缺失值建议
    for col, info in diagnosis["missing"].items():
        recommendations.append({
            "issue": "missing",
            "column": col,
            "description": f"缺失值: {info['count']}个 ({info['ratio']*100:.1f}%)",
            "method": info["method"],
            "action": "fill_missing"
        })

    # 异常值建议
    for col, info in diagnosis["outliers"].items():
        recommendations.append({
            "issue": "outliers",
            "column": col,
            "description": f"异常值: {info['count']}个",
            "method": "clip",
            "action": "clip_outliers",
            "lower_bound": info["lower_bound"],
            "upper_bound": info["upper_bound"]
        })

    # 重复行建议
    if diagnosis["duplicates"] > 0:
        recommendations.append({
            "issue": "duplicates",
            "column": "all",
            "description": f"重复行: {diagnosis['duplicates']}行",
            "method": "drop",
            "action": "remove_duplicates"
        })

    # 噪声建议
    for col, info in diagnosis["noise"].items():
        recommendations.append({
            "issue": "noise",
            "column": col,
            "description": f"噪声(变异系数CV): {info['cv']:.2f} > 0.5",
            "method": info["method"],
            "action": "filter_noise"
        })

    diagnosis["recommendations"] = recommendations

    return diagnosis


def print_diagnosis_report(diagnosis: Dict) -> str:
    """打印诊断报告并返回格式化字符串"""
    lines = ["=" * 60, "数据诊断报告", "=" * 60, ""]

    # 缺失值
    if diagnosis["missing"]:
        lines.append("【缺失值】")
        for col, info in diagnosis["missing"].items():
            lines.append(f"  - {col}: {info['count']}个 ({info['ratio']*100:.1f}%) → 建议用{info['method']}填充")
        lines.append("")

    # 异常值
    if diagnosis["outliers"]:
        lines.append("【异常值】(IQR方法, 3倍IQR)")
        for col, info in diagnosis["outliers"].items():
            lines.append(f"  - {col}: {info['count']}个 → 建议裁剪到边界")
        lines.append("")

    # 重复行
    if diagnosis["duplicates"] > 0:
        lines.append(f"【重复行】: {diagnosis['duplicates']}行 → 建议删除")
        lines.append("")

    # 噪声
    if diagnosis["noise"]:
        lines.append("【噪声】(变异系数CV > 0.5)")
        for col, info in diagnosis["noise"].items():
            lines.append(f"  - {col}: CV={info['cv']:.2f} → 建议用{info['method']}平滑")
        lines.append("")

    # 总结
    total_issues = len(diagnosis["recommendations"])
    lines.append(f"共检测到 {total_issues} 个问题，需要处理。")

    return "\n".join(lines)


def interactive_confirm_and_process(df: pd.DataFrame, auto_confirm: bool = False) -> tuple:
    """交互式确认处理：诊断数据 → 打印报告 → 询问用户 → 处理

    Args:
        df: 输入 DataFrame
        auto_confirm: 是否自动确认（跳过交互）

    Returns:
        (处理后的DataFrame, 处理计划plan, 诊断报告)
    """
    # 1. 诊断数据
    diagnosis = diagnose_data(df)

    # 2. 打印诊断报告
    report = print_diagnosis_report(diagnosis)
    print(report)
    print()

    # 如果没有问题，直接返回
    if not diagnosis["recommendations"]:
        print("数据质量良好，无需处理。")
        return df, [], report

    # 3. 询问用户确认
    if auto_confirm:
        print("（自动模式：直接按建议处理）")
        user_input = ""
    else:
        print("请确认处理方式：")
        print("  y         - 按上述建议处理")
        print("  n         - 取消，保持原数据")
        print("  m         - 修改某些处理建议")
        user_input = input("> ").strip().lower()

    if user_input == 'n':
        print("已取消处理，数据保持原样。")
        return df.copy(), [], report

    # 4. 如果用户选择修改
    if user_input == 'm':
        recommendations = modify_recommendations(diagnosis["recommendations"])
    else:
        recommendations = diagnosis["recommendations"]

    # 5. 执行处理
    print("\n开始处理...")
    df_processed, plan = apply_recommendations(df, recommendations)
    print(f"处理完成！共执行 {len(plan)} 个步骤。")

    return df_processed, plan, report


def modify_recommendations(recommendations: List[Dict]) -> List[Dict]:
    """交互式修改处理建议"""
    recommendations = [r.copy() for r in recommendations]

    print("\n" + "=" * 50)
    print("修改处理建议")
    print("=" * 50)

    print("\n当前处理建议：")
    for i, r in enumerate(recommendations):
        print(f"  [{i+1}] {r['description']} → {r['method']}")

    print("\n请输入要修改的建议编号（多个用逗号分隔，如 1,3），或直接回车跳过：")
    user_input = input("> ").strip()

    if not user_input:
        print("未修改任何建议。")
        return recommendations

    try:
        indices = [int(x.strip()) - 1 for x in user_input.split(',') if x.strip()]
        indices = [i for i in indices if 0 <= i < len(recommendations)]
    except ValueError:
        print("输入格式有误，未修改任何建议。")
        return recommendations

    for idx in indices:
        r = recommendations[idx]
        print(f"\n修改建议 [{idx+1}]: {r['description']}")
        print(f"当前处理方式: {r['method']}")

        if r["issue"] == "missing":
            print("请选择填充方式：")
            print("  1) mean   - 均值填充")
            print("  2) median - 中位数填充")
            print("  3) ffill  - 前向填充")
            print("  4) bfill  - 后向填充")
            print("  5) interpolate - 线性插值")
            print("  6) skip   - 跳过（不处理）")
            choice = input("> ").strip()
            if choice in ["1", "2", "3", "4", "5", "6"]:
                method_map = {"1": "mean", "2": "median", "3": "ffill", "4": "bfill", "5": "interpolate", "6": "skip"}
                r["method"] = method_map[choice]
                if choice == "6":
                    r["action"] = "skip"

        elif r["issue"] == "outliers":
            print("请选择处理方式：")
            print("  1) clip   - 裁剪到边界")
            print("  2) skip   - 跳过（不处理）")
            choice = input("> ").strip()
            if choice in ["1", "2"]:
                r["method"] = "clip" if choice == "1" else "skip"
                if choice == "2":
                    r["action"] = "skip"

        elif r["issue"] == "duplicates":
            print("请选择处理方式：")
            print("  1) drop   - 删除重复行")
            print("  2) skip   - 跳过（不处理）")
            choice = input("> ").strip()
            if choice in ["1", "2"]:
                r["method"] = "drop" if choice == "1" else "skip"
                if choice == "2":
                    r["action"] = "skip"

        elif r["issue"] == "noise":
            print("请选择降噪方式：")
            print("  1) moving_avg - 移动平均 (默认window=5)")
            print("  2) median     - 中值滤波")
            print("  3) skip       - 跳过（不处理）")
            choice = input("> ").strip()
            if choice in ["1", "2", "3"]:
                if choice == "1":
                    r["method"] = "moving_avg"
                    window_input = input("请输入窗口大小（直接回车使用默认值5）: ").strip()
                    if window_input.isdigit():
                        r["window_size"] = int(window_input)
                elif choice == "2":
                    r["method"] = "median"
                    window_input = input("请输入窗口大小（直接回车使用默认值3）: ").strip()
                    if window_input.isdigit():
                        r["window_size"] = int(window_input)
                else:
                    r["method"] = "skip"
                    r["action"] = "skip"

    recommendations = [r for r in recommendations if r.get("action") != "skip"]

    print("\n修改后的处理建议：")
    for i, r in enumerate(recommendations):
        extra = f", window={r.get('window_size', '')}" if r.get("window_size") else ""
        print(f"  [{i+1}] {r['description']} → {r['method']}{extra}")

    return recommendations


def apply_recommendations(df: pd.DataFrame, recommendations: List[Dict]) -> tuple:
    """根据处理建议自动处理数据"""
    df_processed = df.copy()
    plan = []
    step_num = 1

    # 处理重复行
    dup_recommendations = [r for r in recommendations if r["issue"] == "duplicates"]
    if dup_recommendations:
        df_processed = df_processed.drop_duplicates()
        plan.append({
            "step": step_num,
            "step_name": "删除重复行",
            "columns": ["all"],
            "func": "remove_duplicates",
            "method": "drop",
            "description": f"删除{dup_recommendations[0]['description']}"
        })
        step_num += 1

    # 处理缺失值
    missing_recommendations = [r for r in recommendations if r["issue"] == "missing"]
    if missing_recommendations:
        cols_by_method = {}
        for r in missing_recommendations:
            method = r["method"]
            if method not in cols_by_method:
                cols_by_method[method] = []
            cols_by_method[method].append(r["column"])

        for method, cols in cols_by_method.items():
            if method == "mean":
                df_processed = 缺失值填充.fill_missing_mean(df_processed, cols)
            elif method == "median":
                df_processed = 缺失值填充.fill_missing_median(df_processed, cols)
            elif method == "ffill":
                df_processed = 缺失值填充.fill_missing_ffill(df_processed, cols)
            elif method == "bfill":
                df_processed = 缺失值填充.fill_missing_bfill(df_processed, cols)
            elif method == "interpolate":
                df_processed = 缺失值填充.fill_missing_interpolate(df_processed, cols)

            plan.append({
                "step": step_num,
                "step_name": "填充缺失值",
                "columns": cols,
                "func": "fill_missing",
                "method": method,
                "description": f"用{method}填充缺失值"
            })
            step_num += 1

    # 处理异常值
    outlier_recommendations = [r for r in recommendations if r["issue"] == "outliers"]
    for r in outlier_recommendations:
        col = r["column"]
        if col in df_processed.columns:
            df_processed[col] = df_processed[col].clip(
                r.get("lower_bound", -np.inf),
                r.get("upper_bound", np.inf)
            )
            plan.append({
                "step": step_num,
                "step_name": "裁剪异常值",
                "columns": [col],
                "func": "clip_outliers",
                "method": "clip",
                "description": "裁剪异常值"
            })
            step_num += 1

    # 处理噪声
    noise_recommendations = [r for r in recommendations if r["issue"] == "noise"]
    for r in noise_recommendations:
        col = r["column"]
        window_size = r.get("window_size", 5)

        if r["method"] == "moving_avg":
            df_processed = 滤波降噪.filter_moving_avg(df_processed, [col], window_size=window_size)
        elif r["method"] == "median":
            df_processed = 滤波降噪.filter_median(df_processed, [col], window_size=window_size)

        plan.append({
            "step": step_num,
            "step_name": "降噪平滑",
            "columns": [col],
            "func": "filter_noise",
            "method": r["method"],
            "window_size": window_size,
            "description": f"{r['method']}平滑(window={window_size})"
        })
        step_num += 1

    return df_processed, plan


# ==================== 链式执行 ====================

def chain_execute(df: pd.DataFrame, plan: List[Dict], verbose: bool = True) -> pd.DataFrame:
    """链式执行多个数据处理操作"""
    df_processed = df.copy()

    func_map = {
        "fill_missing": {
            "ffill": 缺失值填充.fill_missing_ffill,
            "bfill": 缺失值填充.fill_missing_bfill,
            "mean": 缺失值填充.fill_missing_mean,
            "median": 缺失值填充.fill_missing_median,
            "mode": 缺失值填充.fill_missing_mode,
            "interpolate": 缺失值填充.fill_missing_interpolate
        },
        "filter_noise": {
            "median": 滤波降噪.filter_median,
            "moving_avg": 滤波降噪.filter_moving_avg,
            "fourier": 滤波降噪.filter_fourier
        },
        "normalize": {
            "minmax": 标准化归一化.normalize_minmax,
            "custom_range": 标准化归一化.normalize_custom_range,
            "log": 标准化归一化.normalize_log,
            "standardize": 标准化归一化.normalize_standardize
        }
    }

    for step_plan in plan:
        step_num = step_plan.get("step", 0)
        step_name = step_plan.get("step_name", "")
        columns = step_plan.get("columns", [])
        func_type = step_plan.get("func")
        method = step_plan.get("method") or step_plan.get("type")

        if method == "skip" or func_type is None:
            if verbose:
                print(f"步骤{step_num}: {step_name} - 跳过")
            continue

        func = func_map.get(func_type, {}).get(method)
        if not func:
            if verbose:
                print(f"步骤{step_num}: {step_name} - 未知方法 {method}，跳过")
            continue

        params = {"df": df_processed, "columns": columns}
        reserved_keys = ["step", "step_name", "columns", "func", "method", "type", "description"]
        extra_params = {k: v for k, v in step_plan.items() if k not in reserved_keys}
        params.update(extra_params)

        if verbose:
            param_str = ", ".join(f"{k}={v}" for k, v in extra_params.items())
            param_str = f" ({param_str})" if param_str else ""
            print(f"步骤{step_num}: {step_name} - {', '.join(columns)} ({method}){param_str}")

        df_processed = func(**params)

    return df_processed


# ==================== 评价与图表 ====================

def generate_evaluation_report(df_before: pd.DataFrame, df_after: pd.DataFrame,
                                plan: List[Dict], file_path: str = "",
                                business_scene: str = "", target_columns: List[str] = None,
                                save_path: str = "") -> str:
    """生成评价结果报告"""
    if target_columns is None:
        target_columns = []

    lines = [
        "## 数据预处理结果评价报告",
        "",
        "### 原始数据信息"
    ]

    if file_path:
        lines.append(f"- 文件: {file_path}")
    if business_scene:
        lines.append(f"- 业务场景: {business_scene}")
    if target_columns:
        lines.append(f"- 处理列: {', '.join(target_columns)}")

    lines.extend([
        "",
        "### 执行结果（链式处理）",
        "| 步骤 | 操作 | 目标列 | 方法 | 结果 |",
        "|------|------|--------|------|------|"
    ])

    for step_plan in plan:
        step = step_plan.get("step", "")
        step_name = step_plan.get("step_name", "")
        columns = step_plan.get("columns", [])
        method = step_plan.get("method") or step_plan.get("type", "N/A")

        result = ""
        if step_plan.get("func") == "fill_missing":
            filled_count = 0
            for col in columns:
                if col in df_before.columns and col in df_after.columns:
                    before_missing = df_before[col].isna().sum()
                    after_missing = df_after[col].isna().sum()
                    filled_count += int(before_missing - after_missing)
            result = f"填充{filled_count}个缺失值" if filled_count > 0 else "无缺失值"
        elif step_plan.get("func") == "filter_noise":
            result = "数据平滑"
        elif step_plan.get("func") == "normalize":
            result = f"缩放至[{step_plan.get('min_val', 0)}, {step_plan.get('max_val', 1)}]"

        lines.append(f"| {step} | {step_name} | {', '.join(columns)} | {method} | {result} |")

    # 统计指标变化
    lines.extend([
        "",
        "### 统计指标变化",
        "| 列名 | 处理前均值 | 处理后均值 | 处理前标准差 | 处理后标准差 |",
        "|------|------------|------------|--------------|--------------|"
    ])

    all_cols = set()
    for step_plan in plan:
        all_cols.update(step_plan.get("columns", []))

    for col in all_cols:
        if col in df_before.columns and col in df_after.columns:
            if df_before[col].dtype in ["int64", "float64", "int32", "float32"]:
                before_mean = df_before[col].mean()
                after_mean = df_after[col].mean()
                before_std = df_before[col].std()
                after_std = df_after[col].std()
                lines.append(f"| {col} | {before_mean:.4f} | {after_mean:.4f} | {before_std:.4f} | {after_std:.4f} |")

    lines.extend([
        "",
        "### 整体效果评价",
        f"- 数据质量提升: 处理完成，数据已按照推荐策略进行处理",
        f"- 业务适用性: 处理后的数据适用于{business_scene or '后续分析'}",
        "- 建议后续操作: 可根据业务需求进行进一步分析或建模"
    ])

    report_content = "\n".join(lines)

    if save_path:
        with open(save_path, 'w', encoding='utf-8') as f:
            f.write(report_content)

    return report_content


def generate_comparison_chart(df_before: pd.DataFrame, df_after: pd.DataFrame,
                              plan: List[Dict], save_path: str = "数据处理前后图表对比.png") -> str:
    """生成对比图表"""
    try:
        import matplotlib.pyplot as plt
        import matplotlib
        matplotlib.use('Agg')
        plt.rcParams['font.sans-serif'] = ['WenQuanYi Micro Hei', 'SimHei', 'Microsoft YaHei', 'Arial']
        plt.rcParams['axes.unicode_minus'] = False

        all_cols = set()
        for step_plan in plan:
            all_cols.update(step_plan.get("columns", []))

        if not all_cols:
            return ""

        n_cols = len(all_cols)
        fig, axes = plt.subplots(n_cols, 2, figsize=(12, 4 * n_cols))

        if n_cols == 1:
            axes = axes.reshape(1, -1)

        for idx, col in enumerate(all_cols):
            if col not in df_before.columns or col not in df_after.columns:
                continue

            axes[idx, 0].hist(df_before[col].dropna(), bins=30, alpha=0.7, color='blue', edgecolor='black')
            axes[idx, 0].set_title(f'{col} - 处理前')
            axes[idx, 0].set_xlabel('值')
            axes[idx, 0].set_ylabel('频数')

            axes[idx, 1].hist(df_after[col].dropna(), bins=30, alpha=0.7, color='green', edgecolor='black')
            axes[idx, 1].set_title(f'{col} - 处理后')
            axes[idx, 1].set_xlabel('值')
            axes[idx, 1].set_ylabel('频数')

        plt.tight_layout()
        plt.savefig(save_path, dpi=150)
        plt.close()

        return save_path
    except Exception as e:
        return f"图表生成失败: {str(e)}"


# 导出所有函数
__all__ = [
    'diagnose_data',
    'print_diagnosis_report',
    'interactive_confirm_and_process',
    'modify_recommendations',
    'apply_recommendations',
    'chain_execute',
    'generate_evaluation_report',
    'generate_comparison_chart'
]