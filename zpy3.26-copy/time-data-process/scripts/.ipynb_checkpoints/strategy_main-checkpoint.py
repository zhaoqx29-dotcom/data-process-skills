import sys
import os
import io

# 设置输出编码为 UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 添加 data-engineering 脚本路径
data_engineering_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data-engineering', 'scripts'))
if data_engineering_path not in sys.path:
    sys.path.insert(0, data_engineering_path)

try:
    from data_ops import load_file, assess, recommend_strategy, chain_execute
except ImportError:
    print(f"无法导入 data_ops 模块，当前路径: {sys.path}")
    print(f"data_engineering_path: {data_engineering_path}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
import pandas as pd

def main():
    # ==================== 阶段一：数据加载与业务语义理解 ====================

    print("=" * 60)
    print("阶段一：数据加载与业务语义理解")
    print("=" * 60)

    # 数据文件路径
    file_path = r"C:\Users\z00951953\test_dataset.csv"

    print(f"\n正在加载数据文件: {file_path}")
    df = load_file(file_path)
    print(f"✓ 数据加载成功：{df.shape[0]} 行 × {df.shape[1]} 列")
    print(f"  列名: {', '.join(df.columns)}")

    # 业务情境（模拟用户输入）
    business_scene = "员工绩效分析"
    target_columns = ["age", "salary", "score"]  # 需要处理的列
    processing_type = "缺失值填充"  # 处理类型

    print(f"\n业务场景: {business_scene}")
    print(f"处理列: {', '.join(target_columns)}")
    print(f"处理需求: {processing_type}")

    # ==================== 阶段二：数据分析与处理推荐 ====================

    print("\n" + "=" * 60)
    print("阶段二：数据分析与处理推荐")
    print("=" * 60)

    # 评估数据质量
    print("\n正在评估数据质量...")
    report = assess(df)

    # 生成数据分析报告（仅展示用户指定列）
    print("\n📊 数据分析报告\n")
    print("### 数据概览")
    print(f"• 数据文件: {file_path}")
    print(f"• 数据形状: {report['shape'][0]} 行 × {report['shape'][1]} 列")
    print(f"• 处理涉及列: {len(target_columns)} 列\n")

    print("---\n")
    print("### 指定列分析结果\n")

    for col in target_columns:
        if col in report['missing']:
            col_data = report['column_analysis'][col]
            print(f"【列名：{col}】")
            print(f"• 数据类型: {col_data['dtype']}")
            print(f"• 缺失值数量: {report['missing'][col]['count']}")
            print(f"• 缺失率: {report['missing'][col]['percentage']}%")
            print(f"• 取值数量: {col_data['unique_count']}")
            char_str = "、".join(col_data['characteristics'])
            print(f"• 数据特征: {char_str}\n")

    # 数据质量评估
    print("---\n")
    print("### 数据质量评估\n")
    total_missing = sum([m['count'] for col, m in report['missing'].items() if col in target_columns])
    if total_missing == 0:
        print("• 完整性评估: 所有处理列均无缺失值，数据质量良好。")
    else:
        print(f"• 完整性评估: 处理列中存在 {total_missing} 个缺失值，需要处理。")

    # 获取推荐策略
    print("\n" + "=" * 60)
    print("生成处理推荐策略...")
    print("=" * 60 + "\n")

    strategies = recommend_strategy(df, target_columns, business_context=business_scene)

    # 处理任务列表
    print("### 处理任务表\n")
    print("| 列名 | 处理类型 | 推荐方法 | 推荐理由 |")
    print("|------|----------|----------|---------|")
    for col in target_columns:
        if col in strategies:
            s = strategies[col]
            method = s['recommended_method']
            if method == "no_action":
                reason = "该列无缺失值"
            else:
                reason = s['reason']
            print(f"| {col} | 缺失值填充 | {method} | {reason} |")

    # 初始执行顺序
    print("\n### 初始执行顺序\n")
    print("**步骤1：缺失值处理**")
    for col in target_columns:
        if col in strategies:
            method = strategies[col]['recommended_method']
            if method != "no_action":
                print(f"- {col}: {method}")

    # ==================== 阶段三：执行处理 ====================

    print("\n" + "=" * 60)
    print("阶段三：执行数据预处理")
    print("=" * 60 + "\n")

    # 构建处理计划
    plan = []
    step = 1
    for col in target_columns:
        if col in strategies:
            method = strategies[col]['recommended_method']
            if method != "no_action":
                plan.append({
                    "step": step,
                    "step_name": "缺失值处理",
                    "columns": [col],
                    "func": "fill_missing",
                    "method": method.split('_')[-1]  # 取最后一部分，如 fill_missing_ffill -> ffill
                })
                step += 1

    # 执行链式处理
    print("开始执行处理...\n")
    df_result = chain_execute(df, plan, verbose=True)

    # ==================== 阶段四：生成处理结果评价报告 ====================

    print("\n" + "=" * 60)
    print("阶段四：生成处理结果评价报告")
    print("=" * 60 + "\n")

    print("## 数据预处理结果评价报告\n")

    print("### 原始数据信息")
    print(f"- 文件: {file_path}")
    print(f"- 业务场景: {business_scene}")
    print(f"- 处理列: {', '.join(target_columns)}\n")

    print("### 数据分析报告")
    print("（同上阶段二输出，此处略）\n")

    print("### 处理策略")
    print("| 列名 | 策略来源 | 推荐策略 | 实际使用策略 | 策略说明 |")
    print("|------|----------|----------|---------------|---------|")
    for col in target_columns:
        if col in strategies:
            s = strategies[col]
            method = s['recommended_method']
            if method == "no_action":
                actual_method = "skip"
                reason = "该列无缺失值"
            else:
                actual_method = method
                reason = s['reason']
            print(f"| {col} | 自动推荐 | {method} | {actual_method} | {reason} |")

    print("\n### 执行结果（链式处理）")
    print("| 步骤 | 操作 | 目标列 | 方法 | 结果 |")
    print("|------|------|--------|------|------|")
    for p in plan:
        # 计算填充数量
        col = p['columns'][0]
        before = int(df[col].isna().sum())
        after = int(df_result[col].isna().sum())
        filled = before - after
        result = f"填充 {filled} 个缺失值" if filled > 0 else "无缺失值"
        print(f"| {p['step']} | 缺失值处理 | {col} | {p['method']} | {result} |")

    # 统计指标变化
    print("\n### 统计指标变化")
    print("| 列名 | 处理前均值 | 处理后均值 | 处理前中位数 | 处理后中位数 | 处理前标准差 | 处理后标准差 |")
    print("|------|------------|------------|----------------|----------------|--------------|--------------|")
    for col in target_columns:
        if col in df.columns and df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
            before_mean = round(df[col].mean(), 2)
            after_mean = round(df_result[col].mean(), 2)
            before_median = round(df[col].median(), 2)
            after_median = round(df_result[col].median(), 2)
            before_std = round(df[col].std(), 2)
            after_std = round(df_result[col].std(), 2)
            print(f"| {col} | {before_mean} | {after_mean} | {before_median} | {after_median} | {before_std} | {after_std} |")

    # 计算填充的缺失值总数
    total_before_missing = 0
    total_after_missing = 0
    for col in target_columns:
        if col in df.columns:
            total_before_missing += int(df[col].isna().sum())
        if col in df_result.columns:
            total_after_missing += int(df_result[col].isna().sum())
    filled_count = total_before_missing - total_after_missing

    print("\n### 整体效果评价")
    if total_after_missing == 0:
        print(f"- 数据质量提升: 已填充 {filled_count} 个缺失值，数据完整性 100%")
    else:
        print(f"- 数据质量提升: 已填充 {filled_count} 个缺失值，剩余 {total_after_missing} 个缺失值待处理")
    print("- 业务适用性: 使用中位数填充策略，适合员工绩效分析场景，能有效处理年龄、薪资、评分等数值型数据的缺失值")
    print("- 建议后续操作: 可考虑进行异常值检测和离群值分析，进一步优化数据质量")

    print("\n" + "=" * 60)
    print("✓ 数据预处理流程执行完成")
    print("=" * 60)

if __name__ == "__main__":
    main()
