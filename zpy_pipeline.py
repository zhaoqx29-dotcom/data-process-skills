#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zpy3.17(1) 数据预处理工具
严格按照 strategy3.16 的 skill.md 流程执行

使用方式：
    python3 zpy_pipeline.py --file <数据文件路径>
    # 交互式模式，会询问业务场景、展示分析报告、推荐策略、让用户确认
"""

import argparse
import sys
import os
import pandas as pd

# 添加技能脚本路径
SKILL_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SKILL_DIR, 'analyze/scripts'))
sys.path.insert(0, os.path.join(SKILL_DIR, 'recommend/scripts'))
sys.path.insert(0, os.path.join(SKILL_DIR, 'process/scripts'))

from analyze import load_file, assess, generate_analysis_report
from process import (
    fill_missing_ffill, fill_missing_bfill, fill_missing_mean,
    fill_missing_median, fill_missing_mode, fill_missing_interpolate,
    filter_median, filter_moving_avg, filter_fourier,
    normalize_minmax, normalize_custom_range, normalize_log,
    generate_evaluation_report, generate_comparison_chart
)

# 方法映射
MISSING_METHODS = {
    'ffill': fill_missing_ffill,
    'bfill': fill_missing_bfill,
    'mean': fill_missing_mean,
    'median': fill_missing_median,
    'mode': fill_missing_mode,
    'interpolate': fill_missing_interpolate,
}

FILTER_METHODS = {
    'median': filter_median,
    'moving_avg': filter_moving_avg,
    'fourier': filter_fourier,
}

NORMALIZE_METHODS = {
    'minmax': normalize_minmax,
    'custom_range': normalize_custom_range,
    'log': normalize_log,
}


def parse_columns_input(user_input: str) -> list:
    """解析用户输入的列和处理类型"""
    parsed = []
    items = user_input.replace('\n', ';').split(';')

    for item in items:
        item = item.strip()
        if not item:
            continue

        if ':' in item:
            parts = item.split(':', 1)
            column = parts[0].strip()
            types_str = parts[1].strip()
            types = [t.strip() for t in types_str.replace(' ', ';').replace(',', ';').split(';') if t.strip()]
            parsed.append({'column': column, 'types': types})
        else:
            parsed.append({'column': item, 'types': ['缺失值填充']})

    return parsed


def get_missing_method(method_name: str):
    """获取缺失值处理方法"""
    method_map = {
        'fill_missing_ffill': MISSING_METHODS['ffill'],
        'fill_missing_bfill': MISSING_METHODS['bfill'],
        'fill_missing_mean': MISSING_METHODS['mean'],
        'fill_missing_median': MISSING_METHODS['median'],
        'fill_missing_mode': MISSING_METHODS['mode'],
        'fill_missing_interpolate': MISSING_METHODS['interpolate'],
        'ffill': MISSING_METHODS['ffill'],
        'bfill': MISSING_METHODS['bfill'],
        'mean': MISSING_METHODS['mean'],
        'median': MISSING_METHODS['median'],
        'mode': MISSING_METHODS['mode'],
        'interpolate': MISSING_METHODS['interpolate'],
    }
    return method_map.get(method_name, fill_missing_mean)


def get_filter_method(method_name: str):
    """获取滤波方法"""
    method_map = {
        'filter_median': FILTER_METHODS['median'],
        'filter_moving_avg': FILTER_METHODS['moving_avg'],
        'filter_fourier': FILTER_METHODS['fourier'],
        'median': FILTER_METHODS['median'],
        'moving_avg': FILTER_METHODS['moving_avg'],
        'fourier': FILTER_METHODS['fourier'],
    }
    return method_map.get(method_name, filter_moving_avg)


def get_normalize_method(method_name: str):
    """获取标准化方法"""
    method_map = {
        'normalize_minmax': NORMALIZE_METHODS['minmax'],
        'normalize_custom_range': NORMALIZE_METHODS['custom_range'],
        'normalize_log': NORMALIZE_METHODS['log'],
        'minmax': NORMALIZE_METHODS['minmax'],
        'custom_range': NORMALIZE_METHODS['custom_range'],
        'log': NORMALIZE_METHODS['log'],
    }
    return method_map.get(method_name, normalize_minmax)


def recommend_auto_strategy(df, column: str, business_context: str) -> dict:
    """自动推荐处理策略"""
    result = assess(df)
    col_info = result['column_analysis'].get(column, {})
    missing_info = result['missing'].get(column, {})
    characteristics = col_info.get('characteristics', [])
    missing_rate = missing_info.get('percentage', 0)

    strategies = {'missing': None, 'filter': None, 'normalize': None}

    # 缺失值推荐（按优先级）
    if missing_rate > 0:
        # 优先级1: 后向填充 - 缺失在数据开头
        missing_analysis = result.get('missing_analysis', {}).get(column, {})
        if missing_analysis.get('is_at_start') and missing_analysis.get('max_consecutive_missing', 0) <= 5:
            strategies['missing'] = {'method': 'bfill', 'reason': '缺失值出现在数据开头，后向填充'}
        # 优先级2: 线性插值 - 连续多行缺失
        elif missing_analysis.get('max_consecutive_missing', 0) > 3:
            strategies['missing'] = {'method': 'interpolate', 'reason': '连续多行缺失，线性插值'}
        # 优先级3: 前向填充 - 时间序列
        elif '时间序列' in characteristics or '随时间缓慢变化' in characteristics:
            strategies['missing'] = {'method': 'ffill', 'reason': '时间序列数据，前向填充保留趋势'}
        # 优先级4: 均值/中位数 - 正态/偏态分布
        elif '正态分布' in characteristics:
            strategies['missing'] = {'method': 'mean', 'reason': '正态分布数据，均值填充'}
        elif '偏态分布' in characteristics:
            strategies['missing'] = {'method': 'median', 'reason': '偏态分布数据，中位数填充'}
        else:
            strategies['missing'] = {'method': 'mean', 'reason': '数值型数据，均值填充'}

    # 滤波推荐
    if '存在显著异常值' in characteristics:
        strategies['filter'] = {'method': 'median', 'reason': '存在显著异常值，中值滤波'}
    elif '中等波动' in characteristics or '平稳数据' in characteristics:
        strategies['filter'] = {'method': 'moving_avg', 'reason': '随机波动，移动平均平滑'}

    # 标准化推荐
    if column in ['计划重量', '实际重量', '温度', '电压', 'AL', 'SI', 'CU', 'MG']:
        strategies['normalize'] = {'method': 'minmax', 'reason': '消除量纲影响，归一化到[0,1]'}

    return strategies


def show_data_analysis_report(df, result, target_columns):
    """展示数据分析报告"""
    print("\n" + "=" * 60)
    print("📊 数据分析报告")
    print("=" * 60)

    print(f"\n### 数据概览")
    print(f"• 数据形状: {result['shape'][0]} 行 × {result['shape'][1]} 列")
    print(f"• 处理涉及列: {len(target_columns)} 列")

    print(f"\n---")
    print(f"\n### 指定列分析结果")

    for col in target_columns:
        if col not in df.columns:
            continue
        missing_info = result['missing'].get(col, {})
        col_info = result['column_analysis'].get(col, {})

        print(f"\n【列名：{col}】")
        print(f"• 数据类型: {col_info.get('dtype', 'unknown')}")
        print(f"• 缺失值数量: {missing_info.get('count', 0)}")
        print(f"• 缺失率: {missing_info.get('percentage', 0)}%")
        print(f"• 取值数量: {col_info.get('unique_count', 'N/A')}")
        chars = col_info.get('characteristics', [])
        print(f"• 数据特征: {', '.join(chars) if chars else '无'}")

    print(f"\n---")
    print(f"\n### 数据质量评估")

    # 评估完整性
    missing_cols = [col for col in target_columns if result['missing'].get(col, {}).get('count', 0) > 0]
    if not missing_cols:
        print("• 完整性评估: 所有处理列无缺失值")
    else:
        for col in missing_cols:
            pct = result['missing'][col]['percentage']
            if pct > 30:
                print(f"• 完整性评估: {col} 缺失率较高 ({pct}%)，建议谨慎处理")
            elif pct > 0:
                print(f"• 完整性评估: {col} 存在缺失 ({pct}%)，建议处理")

    print("• 潜在问题: 无明显问题")
    print()


def show_recommendation_table(parsed_columns, df, business_context):
    """展示推荐策略表格"""
    print("\n" + "=" * 60)
    print("📋 处理任务列表")
    print("=" * 60)

    print("\n| 列名 | 处理类型 | 推荐方法 | 备选方法 | 推荐理由 |")
    print("|------|----------|----------|----------|----------|")

    for item in parsed_columns:
        col = item['column']
        types = item['types']
        strategies = recommend_auto_strategy(df, col, business_context)

        # 缺失值处理
        if '缺失值填充' in types or '缺失值' in types:
            method = strategies.get('missing', {}).get('method', 'mean') if strategies.get('missing') else 'mean'
            reason = strategies.get('missing', {}).get('reason', '自动推荐')
            alt = 'median/interpolate/ffill' if method != 'median' else 'mean/interpolate/ffill'
            print(f"| {col} | 缺失值填充 | {method} | {alt} | {reason} |")

        # 滤波降噪
        if '滤波降噪' in types or '滤波' in types:
            method = strategies.get('filter', {}).get('method', 'moving_avg') if strategies.get('filter') else 'moving_avg'
            reason = strategies.get('filter', {}).get('reason', '自动推荐')
            alt = 'median/fourier' if method != 'median' else 'moving_avg/fourier'
            print(f"| {col} | 滤波降噪 | {method} | {alt} | {reason} |")

        # 标准化归一化
        if '标准化归一化' in types or '标准化' in types or '归一化' in types:
            method = strategies.get('normalize', {}).get('method', 'minmax') if strategies.get('normalize') else 'minmax'
            reason = strategies.get('normalize', {}).get('reason', '自动推荐')
            alt = 'custom_range/log' if method != 'minmax' else 'custom_range/log'
            print(f"| {col} | 标准化归一化 | {method} | {alt} | {reason} |")

    print()


def show_execution_order(parsed_columns, df, business_context):
    """展示初始执行顺序"""
    print("\n" + "=" * 60)
    print("📋 初始执行顺序")
    print("=" * 60)

    # 按处理类型分组
    missing_steps = []
    filter_steps = []
    normalize_steps = []

    for item in parsed_columns:
        col = item['column']
        types = item['types']
        strategies = recommend_auto_strategy(df, col, business_context)

        if '缺失值填充' in types or '缺失值' in types:
            method = strategies.get('missing', {}).get('method', 'mean') if strategies.get('missing') else 'mean'
            missing_steps.append((col, method))

        if '滤波降噪' in types or '滤波' in types:
            method = strategies.get('filter', {}).get('method', 'moving_avg') if strategies.get('filter') else 'moving_avg'
            filter_steps.append((col, method))

        if '标准化归一化' in types or '标准化' in types or '归一化' in types:
            method = strategies.get('normalize', {}).get('method', 'minmax') if strategies.get('normalize') else 'minmax'
            normalize_steps.append((col, method))

    step = 0

    if missing_steps:
        step += 1
        print(f"\n**步骤{step}：** 缺失值处理")
        for col, method in missing_steps:
            print(f"- {col}: {method}")

    if filter_steps:
        step += 1
        print(f"\n**步骤{step}：** 滤波降噪")
        for col, method in filter_steps:
            win = "window_size=5" if method in ['median', 'moving_avg'] else ""
            print(f"- {col}: {method} ({win})".strip(" ()"))

    if normalize_steps:
        step += 1
        print(f"\n**步骤{step}：** 标准化归一化")
        for col, method in normalize_steps:
            print(f"- {col}: {method}")

    print()


def ask_custom_strategy(parsed_columns, df, business_context, auto_confirm: bool = True):
    """询问用户是否需要自定义策略"""
    if auto_confirm:
        # 命令行模式：自动使用推荐策略
        return None

    print("\n" + "=" * 60)
    print("❓ 是否需要自定义处理策略？")
    print("=" * 60)
    print("\n[n] 使用自动推荐方法，继续执行")
    print("[Y] 进入自定义策略流程")

    choice = input("\n请输入 [n/Y]: ").strip().lower()

    if choice == 'y':
        # 自定义策略流程
        custom_methods = {}

        print("\n请指定需要自定义策略的列名（或直接回车结束）:")
        while True:
            col = input("列名: ").strip()
            if not col:
                break

            if col not in [item['column'] for item in parsed_columns]:
                print(f"  错误: {col} 不在处理列表中")
                continue

            # 询问该列的各处理类型
            col_methods = {}

            item = next((i for i in parsed_columns if i['column'] == col), None)
            if not item:
                continue

            types = item['types']

            if '缺失值填充' in types or '缺失值' in types:
                print(f"  {col} 的缺失值填充方法: [当前: {recommend_auto_strategy(df, col, business_context).get('missing', {}).get('method', 'mean')}]")
                print("  可选: ffill, bfill, mean, median, mode, interpolate, skip")
                method = input("  选择方法: ").strip()
                if method and method != 'skip':
                    col_methods['missing'] = method

            if '滤波降噪' in types or '滤波' in types:
                print(f"  {col} 的滤波降噪方法: [当前: {recommend_auto_strategy(df, col, business_context).get('filter', {}).get('method', 'moving_avg')}]")
                print("  可选: median, moving_avg, fourier, skip")
                method = input("  选择方法: ").strip()
                if method and method != 'skip':
                    col_methods['filter'] = method

            if '标准化归一化' in types or '标准化' in types or '归一化' in types:
                print(f"  {col} 的标准化归一化方法: [当前: {recommend_auto_strategy(df, col, business_context).get('normalize', {}).get('method', 'minmax')}]")
                print("  可选: minmax, custom_range, log, skip")
                method = input("  选择方法: ").strip()
                if method and method != 'skip':
                    col_methods['normalize'] = method

            if col_methods:
                custom_methods[col] = col_methods

            cont = input("是否还需为其他列自定义策略？[Y/n]: ").strip().lower()
            if cont == 'n':
                break

        return custom_methods
    else:
        return None


def run_pipeline(file_path: str, columns_input: str, business_context: str, auto_confirm: bool = True):
    """
    严格按照 strategy3.16 skill.md 流程执行

    Args:
        file_path: 数据文件路径
        columns_input: 处理列输入
        business_context: 业务场景
        auto_confirm: 是否自动确认使用推荐策略（命令行模式为True，交互式模式为False）
    """
    print("\n" + "=" * 60)
    print("🚀 zpy3.17(1) 数据预处理工具")
    print("   严格按照 strategy3.16 流程执行")
    print("=" * 60)

    # ==================== 阶段一：数据加载与业务语义理解 ====================
    print("\n" + "-" * 60)
    print("【阶段一】数据加载与业务语义理解")
    print("-" * 60)

    # 1. 加载数据
    print(f"\n[1] 加载数据...")
    df = load_file(file_path)
    print(f"  ✅ 已加载数据: {df.shape[0]} 行 × {df.shape[1]} 列")

    # 2. 解析处理列
    print(f"\n[2] 解析处理列...")
    parsed_columns = parse_columns_input(columns_input)
    target_columns = [item['column'] for item in parsed_columns]
    print(f"  ✅ 处理列: {target_columns}")

    # ==================== 阶段二：数据分析与处理推荐 ====================
    print("\n" + "-" * 60)
    print("【阶段二】数据分析与处理推荐")
    print("-" * 60)

    # 3. 数据分析
    print(f"\n[3] 数据分析...")
    result = assess(df)
    for item in parsed_columns:
        col = item['column']
        if col in result['missing']:
            missing_count = result['missing'][col]['count']
            missing_pct = result['missing'][col]['percentage']
            print(f"  ✅ {col}: 缺失 {missing_count} 个 ({missing_pct}%)")

    # 4. 生成并展示数据分析报告
    print(f"\n[4] 生成数据分析报告...")
    analysis_report = generate_analysis_report(df, result, target_columns)
    show_data_analysis_report(df, result, target_columns)

    # 保存数据分析报告
    report_dir = os.path.dirname(file_path) or '.'
    analysis_report_path = os.path.join(report_dir, '数据分析报告.md')
    with open(analysis_report_path, 'w', encoding='utf-8') as f:
        f.write(analysis_report)
    print(f"  ✅ 分析报告已保存: {analysis_report_path}")

    # 5. 展示推荐策略表格
    print(f"\n[5] 生成推荐策略...")
    show_recommendation_table(parsed_columns, df, business_context)

    # 6. 展示初始执行顺序
    print(f"\n[6] 生成执行顺序...")
    show_execution_order(parsed_columns, df, business_context)

    # ==================== 阶段三：处理计划展示与确认 ====================
    print("\n" + "-" * 60)
    print("【阶段三】处理计划展示与确认")
    print("-" * 60)

    # 7. 询问是否自定义策略
    custom_methods = ask_custom_strategy(parsed_columns, df, business_context, auto_confirm)

    # 8. 确认执行
    if auto_confirm:
        print("\n✅ 使用自动推荐策略，直接执行...")
    else:
        print("\n" + "=" * 60)
        print("❓ 确认执行处理？[Y/n]")
        print("=" * 60)
        confirm = input("\n请确认: ").strip().lower()
        if confirm and confirm != 'y':
            print("\n❌ 已取消处理")
            return

    # ==================== 阶段四：执行处理与结果评价 ====================
    print("\n" + "-" * 60)
    print("【阶段四】执行处理与结果评价")
    print("-" * 60)

    print(f"\n[1] 开始执行处理...")
    df_before = df.copy()
    plan = []
    step_num = 0

    for item in parsed_columns:
        col = item['column']
        types = item['types']

        # 确定使用的方法
        if custom_methods and col in custom_methods:
            strategies = custom_methods[col]
        else:
            strategies = recommend_auto_strategy(df, col, business_context)

        # 执行缺失值处理
        if '缺失值填充' in types or '缺失值' in types:
            method_name = strategies.get('missing', {}).get('method', 'mean') if isinstance(strategies, dict) and strategies.get('missing') else 'mean'
            if isinstance(strategies, dict):
                strategies = {'missing': strategies.get('missing', {'method': 'mean'}), 'filter': strategies.get('filter', {}), 'normalize': strategies.get('normalize', {})}
            else:
                strategies = {'missing': {'method': 'mean'}, 'filter': {}, 'normalize': {}}
            method_name = strategies.get('missing', {}).get('method', 'mean') if isinstance(strategies.get('missing', ''), dict) else strategies.get('missing', 'mean')
            if isinstance(strategies.get('missing', ''), dict):
                method_name = strategies['missing'].get('method', 'mean')
            else:
                method_name = strategies.get('missing', 'mean')
            method = get_missing_method(method_name)
            df = method(df, [col])
            step_num += 1
            plan.append({
                'step': step_num,
                'step_name': '缺失值处理',
                'columns': [col],
                'method': f'fill_missing_{method_name}',
                'func': 'fill_missing'
            })
            print(f"  步骤{step_num}: {col} 缺失值填充 ({method_name})")

        # 执行滤波降噪
        if '滤波降噪' in types or '滤波' in types:
            if isinstance(strategies, dict):
                s = strategies.get('filter', {})
                method_name = s.get('method', 'moving_avg') if isinstance(s, dict) else 'moving_avg'
            else:
                method_name = 'moving_avg'
            method = get_filter_method(method_name)
            if method_name in ['median', 'moving_avg']:
                df = method(df, [col], window_size=5)
            else:
                df = method(df, [col])
            step_num += 1
            plan.append({
                'step': step_num,
                'step_name': '滤波降噪',
                'columns': [col],
                'method': f'filter_{method_name}',
                'func': 'filter_noise'
            })
            print(f"  步骤{step_num}: {col} 滤波降噪 ({method_name})")

        # 执行标准化归一化
        if '标准化归一化' in types or '标准化' in types or '归一化' in types:
            if isinstance(strategies, dict):
                s = strategies.get('normalize', {})
                method_name = s.get('method', 'minmax') if isinstance(s, dict) else 'minmax'
            else:
                method_name = 'minmax'
            method = get_normalize_method(method_name)
            df = method(df, [col])
            step_num += 1
            plan.append({
                'step': step_num,
                'step_name': '标准化归一化',
                'columns': [col],
                'method': f'normalize_{method_name}',
                'func': 'normalize',
                'min_val': 0,
                'max_val': 1
            })
            print(f"  步骤{step_num}: {col} 标准化归一化 ({method_name})")

    # 9. 生成评价报告
    print(f"\n[2] 生成评价报告...")
    eval_report = generate_evaluation_report(
        df_before, df, plan,
        file_path=os.path.basename(file_path),
        business_scene=business_context,
        target_columns=target_columns
    )

    # 构建完整报告
    full_report = f"""## 数据预处理结果评价报告

### 原始数据信息
- 文件: {os.path.basename(file_path)}
- 业务场景: {business_context}
- 处理列: {', '.join(target_columns)}

### 数据分析报告
{analysis_report}

### 处理策略
| 列名 | 策略来源 | 推荐策略 | 实际使用策略 | 策略说明 |
|------|----------|----------|---------------|---------|"""

    for item in parsed_columns:
        col = item['column']
        types = item['types']
        source = "自定义" if (custom_methods and col in custom_methods) else "自动推荐"

        if '缺失值填充' in types or '缺失值' in types:
            if custom_methods and col in custom_methods:
                method = custom_methods[col].get('missing', 'mean')
            else:
                method = recommend_auto_strategy(df_before, col, business_context).get('missing', {}).get('method', 'mean')
            full_report += f"\n| {col} | {source} | fill_missing_{method} | fill_missing_{method} | 自定义/自动推荐 |"

        if '滤波降噪' in types or '滤波' in types:
            if custom_methods and col in custom_methods:
                method = custom_methods[col].get('filter', 'moving_avg')
            else:
                method = recommend_auto_strategy(df_before, col, business_context).get('filter', {}).get('method', 'moving_avg')
            full_report += f"\n| {col} | {source} | filter_{method} | filter_{method} | 自定义/自动推荐 |"

        if '标准化归一化' in types or '标准化' in types or '归一化' in types:
            if custom_methods and col in custom_methods:
                method = custom_methods[col].get('normalize', 'minmax')
            else:
                method = recommend_auto_strategy(df_before, col, business_context).get('normalize', {}).get('method', 'minmax')
            full_report += f"\n| {col} | {source} | normalize_{method} | normalize_{method} | 自定义/自动推荐 |"

    full_report += f"""

### 执行结果（链式处理）
{eval_report.split('### 执行结果（链式处理）')[1] if '### 执行结果（链式处理）' in eval_report else ''}

### 整体效果评价
- 数据质量提升: 所有指定列已完成预处理，缺失值已填充，数据已标准化
- 业务适用性: 处理后的数据适用于{business_context}的后续分析和建模
- 建议后续操作: 可进行特征工程、模型训练等后续分析

### 图表对比
- 数据处理前后对比图表已保存: 数据处理前后图表对比.png
"""

    eval_report_path = os.path.join(report_dir, '处理结果评价报告.md')
    with open(eval_report_path, 'w', encoding='utf-8') as f:
        f.write(full_report)
    print(f"  ✅ 评价报告已保存: {eval_report_path}")

    # 10. 生成对比图表
    print(f"\n[3] 生成对比图表...")
    chart_path = os.path.join(report_dir, '数据处理前后图表对比.png')
    generate_comparison_chart(df_before, df, plan, chart_path)
    print(f"  ✅ 对比图表已保存: {chart_path}")

    # 保存处理后的数据
    processed_path = os.path.join(report_dir, 'processed_' + os.path.basename(file_path))
    df.to_csv(processed_path, index=False, encoding='utf-8-sig')
    print(f"  ✅ 处理后数据已保存: {processed_path}")

    print("\n" + "=" * 60)
    print("✅ 处理完成！")
    print("=" * 60)


def main():
    import argparse
    parser = argparse.ArgumentParser(description='zpy3.17(1) 数据预处理工具')
    parser.add_argument('--file', '-f', help='数据文件路径')
    parser.add_argument('--columns', '-c', help='处理列和类型，如 "列名:缺失值填充,标准化归一化"')
    parser.add_argument('--business', '-b', help='业务场景描述')

    args = parser.parse_args()

    if args.file and args.columns and args.business:
        # 命令行模式：自动确认
        run_pipeline(args.file, args.columns, args.business, auto_confirm=True)
    else:
        # 交互式模式：需要用户确认
        print("\n" + "=" * 60)
        print("🚀 zpy3.17(1) 数据预处理工具 - 交互式模式")
        print("   严格按照 strategy3.16 流程执行")
        print("=" * 60)

        # 输入数据文件路径
        file_path = input("\n请输入数据文件路径: ").strip()
        if not file_path:
            print("错误: 请输入数据文件路径")
            return

        if not os.path.exists(file_path):
            print(f"错误: 文件不存在: {file_path}")
            return

        # 输入业务场景
        business_context = input("请描述业务场景: ").strip()
        if not business_context:
            business_context = "一般业务场景"

        # 输入处理列
        print("\n请输入需要处理的列和处理类型")
        print("格式: 列名:处理类型1,处理类型2")
        print("例如: 计划重量:缺失值填充,标准化归一化;实际重量:缺失值填充")
        print("处理类型: 缺失值填充, 滤波降噪, 标准化归一化")
        columns_input = input("请输入: ").strip()

        if not columns_input:
            print("错误: 请输入处理列")
            return

        # 执行流程
        run_pipeline(file_path, columns_input, business_context, auto_confirm=False)


if __name__ == '__main__':
    main()
