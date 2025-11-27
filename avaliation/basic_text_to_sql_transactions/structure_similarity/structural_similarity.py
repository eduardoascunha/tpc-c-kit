#!/usr/bin/env python3
import re
import sys
from collections import Counter

# SQL/PL/pgSQL keywords with weighted importance
KEYWORDS = {
    # Core DML operations (high weight - essential structure)
    'SELECT': 3, 'INSERT': 3, 'UPDATE': 3, 'DELETE': 3,
    
    # Control flow (high weight - logic structure)
    'FOR': 2, 'WHILE': 2, 'LOOP': 2, 'IF': 2, 'ELSE': 2, 'ELSIF': 2,
    'CASE': 2, 'WHEN': 2, 'EXIT': 2, 'CONTINUE': 2,
    
    # PL/pgSQL specific
    'DECLARE': 1, 'BEGIN': 1, 'END': 1, 'RETURN': 2, 'RAISE': 1,
    
    # Transaction/concurrency (important for TPC-C)
    'COMMIT': 2, 'ROLLBACK': 2, 'EXCEPTION': 2,
    
    # Advanced SQL features
    'WITH': 2, 'CTE': 2, 'RETURNING': 2, 'COALESCE': 1,
    
    # Operators
    ':=': 1, 'INTO': 1,
}

# Patterns that indicate structural complexity
PATTERNS = {
    'nested_query': (r'\(\s*SELECT', 2),  # Subqueries
    'join': (r'\b(INNER|LEFT|RIGHT|FULL|CROSS)\s+JOIN\b', 2),
    'for_update': (r'\bFOR\s+UPDATE\b', 3),  # Locking strategy
    'skip_locked': (r'\bSKIP\s+LOCKED\b', 3),  # Concurrency optimization
    'limit': (r'\bLIMIT\b', 1),
    'order_by': (r'\bORDER\s+BY\b', 1),
    'group_by': (r'\bGROUP\s+BY\b', 1),
    'aggregate': (r'\b(SUM|COUNT|AVG|MAX|MIN)\s*\(', 2),
    'found_check': (r'\b(NOT\s+)?FOUND\b', 2),  # Error handling pattern
    'exception_block': (r'\bEXCEPTION\s+WHEN\b', 3),  # Error handling
}

def preprocess(sql_text):
    """Remove comments and strings, normalize for structural analysis"""
    # Remove -- comments
    sql_text = re.sub(r'--.*', '', sql_text)
    # Remove /* */ comments
    sql_text = re.sub(r'/\*[\s\S]*?\*/', '', sql_text)
    # Remove strings (preserve structure only)
    sql_text = re.sub(r"'(?:''|[^'])*'", '', sql_text)
    # Normalize to uppercase
    sql_text = sql_text.upper()
    return sql_text

def get_weighted_features(sql_text):
    """Extract weighted keyword frequencies and pattern counts"""
    sql_text = preprocess(sql_text)
    features = Counter()
    
    # Count keywords with weights
    for kw, weight in KEYWORDS.items():
        if kw in [':=', 'INTO']:
            count = len(re.findall(re.escape(kw), sql_text))
        else:
            count = len(re.findall(r'\b' + re.escape(kw) + r'\b', sql_text))
        if count > 0:
            features[kw] = count * weight
    
    # Count structural patterns
    for pattern_name, (pattern_re, weight) in PATTERNS.items():
        count = len(re.findall(pattern_re, sql_text, re.IGNORECASE))
        if count > 0:
            features[f'pattern_{pattern_name}'] = count * weight
    
    return features

def calculate_control_flow_similarity(sql1, sql2):
    """Compare control flow structures specifically"""
    sql1_clean = preprocess(sql1)
    sql2_clean = preprocess(sql2)
    
    # Count control structures
    control_keywords = ['FOR', 'WHILE', 'LOOP', 'IF', 'CASE']
    scores = []
    
    for kw in control_keywords:
        count1 = len(re.findall(r'\b' + kw + r'\b', sql1_clean))
        count2 = len(re.findall(r'\b' + kw + r'\b', sql2_clean))
        
        if count1 == 0 and count2 == 0:
            continue
        
        max_count = max(count1, count2)
        min_count = min(count1, count2)
        scores.append(min_count / max_count if max_count > 0 else 1.0)
    
    return sum(scores) / len(scores) if scores else 1.0

def calculate_operation_similarity(sql1, sql2):
    """Compare DML operations (SELECT, INSERT, UPDATE, DELETE)"""
    sql1_clean = preprocess(sql1)
    sql2_clean = preprocess(sql2)
    
    operations = ['SELECT', 'INSERT', 'UPDATE', 'DELETE']
    scores = []
    
    for op in operations:
        count1 = len(re.findall(r'\b' + op + r'\b', sql1_clean))
        count2 = len(re.findall(r'\b' + op + r'\b', sql2_clean))
        
        if count1 == 0 and count2 == 0:
            continue
        
        max_count = max(count1, count2)
        min_count = min(count1, count2)
        scores.append(min_count / max_count if max_count > 0 else 1.0)
    
    return sum(scores) / len(scores) if scores else 1.0

def structural_similarity(sql1_text, sql2_text):
    """Calculate comprehensive structural similarity"""
    feat1 = get_weighted_features(sql1_text)
    feat2 = get_weighted_features(sql2_text)
    
    all_keys = set(feat1.keys()) | set(feat2.keys())
    
    if not all_keys:
        return 1.0
    
    # Weighted feature similarity
    numerator = sum(abs(feat1.get(k, 0) - feat2.get(k, 0)) for k in all_keys)
    denominator = sum(max(feat1.get(k, 0), feat2.get(k, 0)) for k in all_keys)
    
    feature_sim = 1 - (numerator / denominator) if denominator > 0 else 1.0
    
    # Control flow similarity
    control_sim = calculate_control_flow_similarity(sql1_text, sql2_text)
    
    # Operation similarity
    operation_sim = calculate_operation_similarity(sql1_text, sql2_text)
    
    # Weighted combination (features are most important, then operations, then control flow)
    overall_sim = (0.5 * feature_sim) + (0.3 * operation_sim) + (0.2 * control_sim)
    
    return overall_sim, feature_sim, control_sim, operation_sim, feat1, feat2

def print_detailed_comparison(feat1, feat2):
    """Print detailed feature comparison"""
    all_features = sorted(set(feat1.keys()) | set(feat2.keys()))
    
    print("\n=== Detailed Feature Comparison ===")
    print(f"{'Feature':<25} {'File 1':>10} {'File 2':>10} {'Diff':>10}")
    print("-" * 57)
    
    for feature in all_features:
        v1 = feat1.get(feature, 0)
        v2 = feat2.get(feature, 0)
        diff = abs(v1 - v2)
        print(f"{feature:<25} {v1:>10} {v2:>10} {diff:>10}")

def main(file1, file2):
    with open(file1, 'r', encoding='utf-8') as f:
        sql1 = f.read()
    with open(file2, 'r', encoding='utf-8') as f:
        sql2 = f.read()

    overall, feature, control, operation, feat1, feat2 = structural_similarity(sql1, sql2)
    
    print("\n" + "="*60)
    print("  SQL Structural Similarity Analysis")
    print("="*60)
    print(f"\nOverall Similarity:      {overall:.4f} ({overall*100:.2f}%)")
    print(f"  - Feature Similarity:  {feature:.4f} ({feature*100:.2f}%)")
    print(f"  - Operation Similarity:{operation:.4f} ({operation*100:.2f}%)")
    print(f"  - Control Flow Simil.: {control:.4f} ({control*100:.2f}%)")
    
    # Interpretation
    print("\n" + "="*60)
    print("  Interpretation:")
    print("="*60)
    if overall >= 0.85:
        print("EXCELLENT - Very similar structure, likely a good substitute")
    elif overall >= 0.70:
        print("GOOD - Similar structure with some differences")
    elif overall >= 0.50:
        print("MODERATE - Noticeable structural differences")
    else:
        print("LOW - Significantly different approaches")
    
    print_detailed_comparison(feat1, feat2)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} file1.sql file2.sql")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
