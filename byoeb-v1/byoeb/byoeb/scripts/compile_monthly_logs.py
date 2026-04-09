"""Compile monthly user-bot interaction logs for analysis.
Fetches data using the same logic as ASHA Logs page and generates Excel output.
"""

import argparse
import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
from zoneinfo import ZoneInfo
import pandas as pd
from dotenv import load_dotenv
import os
import json
import tiktoken
from openai import OpenAI

# Optional imports for chart generation
try:
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

# Add parent directory to path to import byoeb modules
script_dir = Path(__file__).parent
byoeb_package_dir = script_dir.parent
byoeb_parent_dir = byoeb_package_dir.parent
sys.path.insert(0, str(byoeb_parent_dir))

from byoeb.background_jobs.daily_logs.asha_logs import fetch_daily_logs
# Reuse existing Azure Blob client configuration
from byoeb.kb_app.configuration.dependency_setup import amedia_storage, amedia_storage_analysis
# Mongo factory / repos
from byoeb.factory.mongo_db import MongoDBFactory, Scope
from byoeb.repositories.repository_factory import RepositoryFactory
from byoeb.chat_app.configuration.config import app_config

# Constants
IST = ZoneInfo("Asia/Kolkata")
DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_DATE_FORMAT = "%d-%m-%Y"
SEPARATOR_LENGTH = 60


def locate_keys() -> Path: 
    candidates = [
        Path("keys.env"),
        Path("../../keys.env"),
        Path("byoeb-v1/byoeb/keys.env"),
    ]
    for path in candidates:
        if path.exists():
            return path.resolve()
    raise FileNotFoundError("keys.env not found. Set APP_PATH or run from repo root.")


def get_month_range(year: int, month: int) -> Tuple[datetime, datetime]:
    """Get start and end datetime for a given month in IST timezone."""
    start = datetime(year, month, 1, 0, 0, 0, tzinfo=IST)
    if month == 12:
        end = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=IST)
    else:
        end = datetime(year, month + 1, 1, 0, 0, 0, tzinfo=IST)
    return start, end


def month_range(args) -> Tuple[datetime, datetime]:
    """Parse month range from arguments or use defaults."""
    if args.month and args.year:
        return get_month_range(args.year, args.month)
    elif args.start and args.end:
        try:
            start = datetime.strptime(args.start, DATE_FORMAT).replace(tzinfo=IST)
            end = datetime.strptime(args.end, DATE_FORMAT).replace(tzinfo=IST)
        except ValueError as e:
            raise ValueError(f"Invalid date format. Use {DATE_FORMAT}. Error: {e}")
        
        if start >= end:
            raise ValueError("Start date must be before end date.")
        
        return start, end
    elif args.start or args.end:
        raise ValueError("Provide both --start and --end, or neither.")
    else:
        now = datetime.now(IST)
        end = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if end.month == 1:
            start = end.replace(year=end.year - 1, month=12)
        else:
            start = end.replace(month=end.month - 1)
    return start, end


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compile monthly ASHA bot interaction logs for analysis.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze last month (default)
  python compile_monthly_logs.py
  
  # Analyze specific month
  python compile_monthly_logs.py --year 2024 --month 11
  
  # Analyze custom date range
  python compile_monthly_logs.py --start 2024-11-01 --end 2024-12-01
        """
    )
    parser.add_argument(
        "--year", 
        type=int, 
        help="Year for monthly analysis (e.g., 2024). Use with --month."
    )
    parser.add_argument(
        "--month", 
        type=int, 
        choices=range(1, 13), 
        metavar="[1-12]",
        help="Month for analysis (1-12). Use with --year."
    )
    parser.add_argument(
        "--start", 
        type=str, 
        help="Start date (inclusive) YYYY-MM-DD. Use with --end."
    )
    parser.add_argument(
        "--end", 
        type=str, 
        help="End date (exclusive) YYYY-MM-DD. Use with --start."
    )
    parser.add_argument(
        "--output", 
        type=str, 
        help="Output Excel file path (default: monthly_logs_YYYY-MM.xlsx)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Output directory for all files (default: analysis/YYYY-MM/). All outputs (markdown, charts, Excel) will be saved here."
    )
    parser.add_argument(
        "--save-excel",
        action="store_true",
        help="Save Excel files (idk_queries and response_times). Default: only markdown is saved."
    )
    parser.add_argument(
        "--prev-month-summary",
        type=str,
        help="Path to previous month's summary markdown file for MoM comparison."
    )
    parser.add_argument(
        "--llm-report",
        action="store_true",
        help="Generate LLM-based executive summary with token/cost estimates."
    )
    parser.add_argument(
        "--llm-model",
        type=str,
        default="gpt-4o-mini",
        help="Model for LLM summary (default: gpt-4o-mini)."
    )
    parser.add_argument(
        "--llm-max-tokens",
        type=int,
        default=300,
        help="Max completion tokens for LLM summary (default: 300)."
    )
    parser.add_argument(
        "--llm-temperature",
        type=float,
        default=0.2,
        help="Temperature for LLM summary (default: 0.2)."
    )
    parser.add_argument(
        "--upload-azure",
        action="store_true",
        help="Upload the generated analysis folder to Azure Blob Storage (uses configured amedia_storage)."
    )
    parser.add_argument(
        "--azure-prefix",
        type=str,
        default=None,
        help="Optional prefix in Azure Blob (default: analysis/<YYYY-MM>/)."
    )
    return parser.parse_args()


async def fetch_monthly_logs(start: datetime, end: datetime) -> pd.DataFrame:
    """Fetch logs for the specified date range using the same logic as ASHA Logs page."""
    start_ts = str(start.timestamp())
    end_ts = str(end.timestamp())
    
    print(f"Fetching logs from {start.strftime(DATETIME_FORMAT)} to {end.strftime(DATETIME_FORMAT)} (IST)...")
    
    try:
        rows = [row async for row in fetch_daily_logs(start_timestamp=start_ts, end_timestamp=end_ts)]
        df = pd.DataFrame(rows)
    except Exception as e:
        print(f"Error fetching logs: {e}")
        raise
    
    if df.empty:
        print("No messages found in the selected range.")
        return df
    
    print(f"Fetched {len(df)} log entries.")
    return df


def get_previous_month_range(current_start: datetime) -> Tuple[datetime, datetime]:
    """Calculate the previous month's date range."""
    # Get first day of current month
    first_day_current = current_start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # Get last day of previous month
    if first_day_current.month == 1:
        # January -> previous month is December of previous year
        prev_month = 12
        prev_year = first_day_current.year - 1
    else:
        prev_month = first_day_current.month - 1
        prev_year = first_day_current.year
    
    # First day of previous month
    prev_start = first_day_current.replace(year=prev_year, month=prev_month)
    
    # Last day of previous month (which is day before first day of current month)
    prev_end = first_day_current
    
    return prev_start, prev_end


async def compute_previous_month_metrics(start: datetime, end: datetime, user_repo=None) -> Optional[dict]:
    """Fetch previous month's data and compute metrics needed for MoM comparison."""
    try:
        prev_start, prev_end = get_previous_month_range(start)
        
        print(f"\n{'='*SEPARATOR_LENGTH}")
        print(f"Fetching Previous Month Data for MoM Comparison")
        print(f"{'='*SEPARATOR_LENGTH}")
        print(f"Previous Month Range: {prev_start.strftime(DATE_FORMAT)} to {prev_end.strftime(DATE_FORMAT)} (IST)")
        
        # Fetch previous month's data
        prev_df = await fetch_monthly_logs(prev_start, prev_end)
        
        if prev_df.empty:
            print("No previous month data found. MoM comparison will be skipped.")
            return None
        
        # Filter previous month's data
        prev_df = filter_test_users(prev_df)
        prev_df = filter_by_log_date(prev_df, prev_start, prev_end)
        
        if prev_df.empty:
            print("No previous month data remaining after filtering. MoM comparison will be skipped.")
            return None
        
        print(f"Previous month data: {len(prev_df)} entries")
        
        # Compute IDK metrics for previous month
        prev_idk_analysis = analyze_idk(prev_df)
        prev_total_interactions = prev_idk_analysis.get('total_queries', 0)
        prev_idk_percentage = prev_idk_analysis.get('idk_percentage', 0)
        prev_success_percentage = 100 - prev_idk_percentage
        
        # Compute response time metrics for previous month
        prev_rt_analysis = analyze_response_times(prev_df)
        prev_rt_stats = prev_rt_analysis.get('statistics', {}) or {}
        prev_avg_rt = prev_rt_stats.get('mean', 0)
        
        # Active users (from logs) for previous month
        prev_active_count = prev_df['user_id'].nunique() if 'user_id' in prev_df.columns else 0

        # Onboarded users for previous month (from DB)
        prev_onboarded_count = None
        if user_repo:
            prev_onboarded_count = await count_onboarded_asha(prev_start, prev_end, user_repo)

        prev_metrics = {
            'total_interactions': prev_total_interactions,
            'success_percentage': prev_success_percentage,
            'idk_percentage': prev_idk_percentage,
            'avg_response_time': prev_avg_rt,
            'user_stats': {
                "active_count": prev_active_count,
                "onboarded_count": prev_onboarded_count
            }
        }
        
        print(f"Previous month metrics:")
        print(f"  - Total interactions: {prev_total_interactions:,}")
        print(f"  - Success %: {prev_success_percentage:.1f}%")
        print(f"  - IDK %: {prev_idk_percentage:.1f}%")
        print(f"  - Avg response time: {prev_avg_rt:.1f}s")
        
        return prev_metrics
        
    except Exception as e:
        print(f"Warning: Failed to fetch previous month data: {e}")
        print("MoM comparison will be skipped.")
        return None


async def upload_folder_to_azure(local_folder: Path, month_str: str, prefix: Optional[str] = None) -> None:
    """Upload all files in local_folder to Azure Blob Storage under the given prefix."""
    if prefix:
        blob_prefix = prefix.rstrip("/") + "/"
    else:
        blob_prefix = f"analysis/{month_str}/"

    local_folder = local_folder.resolve()
    if not local_folder.exists():
        print(f"Warning: Output folder does not exist: {local_folder}")
        return

    # Collect files
    files = [p for p in local_folder.rglob("*") if p.is_file()]
    if not files:
        print(f"Warning: No files to upload in {local_folder}")
        return

    print(f"\nUploading {len(files)} files to Azure Blob Storage...")
    print(f"Local folder : {local_folder}")
    print(f"Blob prefix  : {blob_prefix}")

    for file_path in files:
        rel_path = file_path.relative_to(local_folder).as_posix()
        blob_name = blob_prefix + rel_path
        try:
            if amedia_storage_analysis is None:
                print("⚠️  Analysis storage client not configured. Skipping Azure upload.")
                return
            await amedia_storage_analysis.aupload_file(file_path=str(file_path), file_name=blob_name)
            print(f"  ✅ {blob_name}")
        except Exception as e:
            print(f"  ⚠️  Failed to upload {blob_name}: {e}")

    try:
        if amedia_storage_analysis:
            await amedia_storage_analysis._close()
    except Exception:
        pass


async def get_user_repository(mongo_factory: MongoDBFactory):
    repo_factory = RepositoryFactory(mongo_factory)
    return await repo_factory.get_user_repository()


async def count_onboarded_asha(start: datetime, end: datetime, user_repo) -> int:
    """Count onboarded ASHA users by created_timestamp in [start, end)."""
    filter_dict = {
        "User.user_type": "asha",
        "User.test_user": {"$ne": True},
        "User.created_timestamp": {"$gte": start, "$lt": end}
    }
    try:
        return await user_repo.count(filter_dict)
    except Exception as e:
        print(f"Warning: Failed to count onboarded ASHA users: {e}")
        return 0


async def compute_user_stats(df: pd.DataFrame, start: datetime, end: datetime, user_repo=None) -> dict:
    """Compute onboarding and active ASHA counts for a given month."""
    # Active ASHA: unique users with at least one query in the month
    active_count = df['user_id'].nunique() if 'user_id' in df.columns else 0

    # Onboarded ASHA: via DB created_timestamp; fallback to None if repo missing
    onboarded_count = None
    if user_repo:
        onboarded_count = await count_onboarded_asha(start, end, user_repo)

    return {
        "active_count": active_count,
        "onboarded_count": onboarded_count
    }


def filter_test_users(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out test users from the dataframe."""
    if df.empty:
        return df
    
    if 'test_user' not in df.columns:
        print("Warning: 'test_user' column not found. Skipping test user filter.")
        return df
    
    initial_count = len(df)
    df_filtered = df[df['test_user'] != True].copy()
    filtered_count = len(df_filtered)
    removed_count = initial_count - filtered_count
    
    if removed_count > 0:
        print(f"Filtered out {removed_count} test user entries. Remaining: {filtered_count}")
    else:
        print(f"No test users found. Total entries: {filtered_count}")
    
    return df_filtered


def filter_by_log_date(df: pd.DataFrame, start: datetime, end: datetime) -> pd.DataFrame:
    """Filter dataframe to only include logs from the specified month based on log_date."""
    if df.empty:
        return df
    
    if 'log_date' not in df.columns:
        print("Warning: 'log_date' column not found. Skipping log_date filter.")
        return df
    
    def parse_log_date(date_str) -> Optional[datetime]:
        """Parse log_date string to datetime object."""
        if pd.isna(date_str) or date_str is None:
            return None
        try:
            return datetime.strptime(str(date_str), LOG_DATE_FORMAT).replace(tzinfo=IST)
        except (ValueError, TypeError):
            return None
    
    df['log_date_parsed'] = df['log_date'].apply(parse_log_date)
    mask = (df['log_date_parsed'] >= start) & (df['log_date_parsed'] < end)
    df_filtered = df[mask].copy()
    df_filtered = df_filtered.drop(columns=['log_date_parsed'])
    
    initial_count = len(df)
    filtered_count = len(df_filtered)
    removed_count = initial_count - filtered_count
    
    if removed_count > 0:
        print(f"Filtered by log_date: removed {removed_count} entries outside month range. Remaining: {filtered_count}")
    
    return df_filtered


def categorize_idk_cause(row: pd.Series) -> str:
    """Categorize the cause of an IDK response.
    
    Categories:
    - Missing details: Incomplete input (short queries, missing context)
    - Query misinterpretation: Bot categorized wrongly
    - Domain knowledge gaps: Valid ASHA queries not answered
    """
    query_text = str(row.get('query_en', '') or '').lower().strip()
    query_source = str(row.get('query_source', '') or '').lower().strip()
    query_type = str(row.get('query_type', '') or '').lower()
    rewritten_query = str(row.get('rewritten_query', '') or '').lower().strip()
    
    # Use the most complete query text available
    primary_query = query_text if query_text and len(query_text) > len(query_source) else query_source
    if not primary_query or primary_query == 'nan':
        primary_query = rewritten_query
    
    # Missing details: very short queries (< 5 chars or < 2 words) or single words
    if len(primary_query) < 5:
        return "Missing details"
    
    word_count = len(primary_query.split())
    if word_count < 2:
        return "Missing details"
    
    # Missing details: queries that are just names or single unclear words
    if word_count == 1 and primary_query not in ['what', 'how', 'when', 'where', 'why', 'who']:
        # Check if it's a common name or unclear term
        unclear_terms = ['chotte', 'vashmita', 'pakil', 'satul', 'kan', 'kai', 'tan', 'niji']
        if primary_query in unclear_terms:
            return "Missing details"
    
    # Query misinterpretation: query_type doesn't match query content or rewritten query differs significantly
    if query_type and rewritten_query and primary_query:
        if query_type not in primary_query and query_type not in rewritten_query:
            if len(rewritten_query) > len(primary_query) * 1.5:  # Significant rewriting suggests misinterpretation
                return "Query misinterpretation"
    
    # Default to domain knowledge gaps (valid ASHA queries not answered)
    return "Domain knowledge gaps"


def categorize_asha_theme(row: pd.Series) -> str:
    """Categorize ASHA-related IDK queries into themes.
    
    Themes: maternal care, child health, payments, workload, supplies, training, 
    technical/administrative, general health, other
    """
    # Get all query text fields and combine
    query_text = str(row.get('query_en', '') or '').lower()
    query_source = str(row.get('query_source', '') or '').lower()
    rewritten_query = str(row.get('rewritten_query', '') or '').lower()
    combined_text = f"{query_text} {query_source} {rewritten_query}"
    
    # Check for very short or unclear queries first (likely missing details)
    if len(query_text.strip()) < 5 or len(query_source.strip()) < 5:
        if not any(word in combined_text for word in ['weight', 'height', 'age', 'blood', 'pressure', 'sugar']):
            return "Other"
    
    # Maternal care keywords (expanded)
    maternal_keywords = ['pregnancy', 'pregnant', 'delivery', 'childbirth', 'antenatal', 
                        'postnatal', 'post-natal', 'maternal', 'breastfeeding', 'breast feeding',
                        'lactation', 'prenatal', 'pre-natal', 'maternity', 'obstetric',
                        'gynecolog', 'gynaecolog', 'menstrual', 'period', 'contraception']
    if any(keyword in combined_text for keyword in maternal_keywords):
        return "Maternal care"
    
    # Child health keywords (new category - weight, height, growth, development)
    child_health_keywords = ['child weight', 'child height', 'child age', 'child growth',
                            'weight of child', 'height of child', 'age of child',
                            'normal weight', 'normal height', 'child development',
                            'vaccination', 'immunization', 'vaccine', 'immunize',
                            'newborn', 'infant', 'toddler', 'baby weight', 'baby height',
                            'teenager weight', 'teenager height', 'adolescent']
    if any(keyword in combined_text for keyword in child_health_keywords):
        return "Child health"
    
    # General health keywords (new category - blood pressure, sugar, vitals, etc.)
    general_health_keywords = ['blood pressure', 'bp', 'sugar level', 'blood sugar', 'glucose',
                              'breathing rate', 'respiratory rate', 'pulse', 'heart rate',
                              'blood volume', 'normal range', 'vital sign', 'vitals',
                              'vitamin', 'deficiency', 'health check', 'medical check']
    if any(keyword in combined_text for keyword in general_health_keywords):
        return "General health"
    
    # Technical/Administrative keywords (new category - IDs, registration, app issues)
    technical_keywords = ['abha', 'aura id', 'aadhaar', 'registration', 'register',
                         'nct card', 'nct', 'onboard', 'onboarding', 'app', 'application',
                         'eshuchi', 'stopped working', 'not working', 'technical',
                         'account', 'id creation', 'create id', 'family head', 'survey']
    if any(keyword in combined_text for keyword in technical_keywords):
        return "Technical/Administrative"
    
    # Payments keywords (expanded)
    payment_keywords = ['payment', 'salary', 'wage', 'money', 'incentive', 'remuneration', 
                       'compensation', 'allowance', 'fund', 'financial', 'rupee', 'rs.',
                       'rs ', 'rupees', 'income', 'earn', 'paid']
    if any(keyword in combined_text for keyword in payment_keywords):
        return "Payments"
    
    # Workload keywords (expanded)
    workload_keywords = ['workload', 'work load', 'too much', 'overwork', 'busy', 
                        'schedule', 'hours', 'duty', 'responsibility', 'task',
                        'too many', 'excessive', 'overload', 'stress', 'pressure at work']
    if any(keyword in combined_text for keyword in workload_keywords):
        return "Workload"
    
    # Supplies keywords (expanded)
    supply_keywords = ['supply', 'supplies', 'equipment', 'material', 'stock', 'inventory',
                       'medicine', 'drug', 'tablet', 'kit', 'tool', 'resource',
                       'cuff', 'blood pressure cuff', 'medical equipment', 'device']
    if any(keyword in combined_text for keyword in supply_keywords):
        return "Supplies"
    
    # Training keywords (expanded)
    training_keywords = ['training', 'learn', 'course', 'workshop', 'education', 'skill',
                        'knowledge', 'guidance', 'instruction', 'teach', 'how to',
                        'learn how', 'training program', 'capacity building']
    if any(keyword in combined_text for keyword in training_keywords):
        return "Training"
    
    return "Other"


def analyze_response_times(df: pd.DataFrame) -> dict:
    """Analyze response times: calculate statistics and identify patterns."""
    if df.empty:
        return {
            'total_queries': 0,
            'valid_responses': 0,
            'statistics': {},
            'patterns': {},
            'response_time_data': pd.DataFrame()
        }
    
    # Create a copy for analysis
    analysis_df = df.copy()
    
    # Calculate response time in seconds
    # Filter out rows with missing timestamps
    valid_mask = (
        analysis_df['incoming_timestamp'].notna() & 
        analysis_df['outgoing_timestamp'].notna() &
        (analysis_df['incoming_timestamp'] > 0) &
        (analysis_df['outgoing_timestamp'] > 0)
    )
    
    valid_df = analysis_df[valid_mask].copy()
    
    if valid_df.empty:
        return {
            'total_queries': len(df),
            'valid_responses': 0,
            'statistics': {},
            'patterns': {},
            'response_time_data': pd.DataFrame()
        }
    
    # Calculate response time (outgoing - incoming) in seconds
    valid_df['response_time_seconds'] = valid_df['outgoing_timestamp'] - valid_df['incoming_timestamp']
    
    # Filter out negative or unrealistic response times (more than 1 hour = 3600 seconds)
    # Negative times indicate data issues, very long times might be outliers
    valid_df = valid_df[
        (valid_df['response_time_seconds'] >= 0) & 
        (valid_df['response_time_seconds'] <= 3600)
    ].copy()
    
    if valid_df.empty:
        return {
            'total_queries': len(df),
            'valid_responses': 0,
            'statistics': {},
            'patterns': {},
            'response_time_data': pd.DataFrame()
        }
    
    # Calculate statistics
    response_times = valid_df['response_time_seconds']
    mean_rt = response_times.mean()
    median_rt = response_times.median()
    p95_rt = response_times.quantile(0.95)
    p90_rt = response_times.quantile(0.90)
    p75_rt = response_times.quantile(0.75)
    min_rt = response_times.min()
    max_rt = response_times.max()
    
    statistics = {
        'mean': round(mean_rt, 2),
        'median': round(median_rt, 2),
        'p95': round(p95_rt, 2),
        'p90': round(p90_rt, 2),
        'p75': round(p75_rt, 2),
        'min': round(min_rt, 2),
        'max': round(max_rt, 2),
        'count': len(valid_df)
    }
    
    # Analyze patterns by different dimensions
    patterns = {}
    
    # Helper function for p95 calculation
    def calc_p95(series):
        return series.quantile(0.95)
    
    # Pattern by query_type
    if 'query_type' in valid_df.columns:
        query_type_stats = valid_df.groupby('query_type')['response_time_seconds'].agg([
            'count', 'mean', 'median', calc_p95
        ]).round(2)
        query_type_stats.columns = ['count', 'mean', 'median', 'p95']
        query_type_stats = query_type_stats.sort_values('mean', ascending=False)
        patterns['by_query_type'] = query_type_stats.to_dict('index')
    
    # Pattern by message_category
    if 'message_category' in valid_df.columns:
        category_stats = valid_df.groupby('message_category')['response_time_seconds'].agg([
            'count', 'mean', 'median', calc_p95
        ]).round(2)
        category_stats.columns = ['count', 'mean', 'median', 'p95']
        category_stats = category_stats.sort_values('mean', ascending=False)
        patterns['by_message_category'] = category_stats.to_dict('index')
    
    # Pattern by user_language
    if 'user_language' in valid_df.columns:
        language_stats = valid_df.groupby('user_language')['response_time_seconds'].agg([
            'count', 'mean', 'median', calc_p95
        ]).round(2)
        language_stats.columns = ['count', 'mean', 'median', 'p95']
        language_stats = language_stats.sort_values('mean', ascending=False)
        patterns['by_language'] = language_stats.to_dict('index')
    
    # Pattern by district (only if meaningful - at least 10 queries per district)
    if 'district' in valid_df.columns:
        district_counts = valid_df['district'].value_counts()
        significant_districts = district_counts[district_counts >= 10].index
        if len(significant_districts) > 0:
            district_df = valid_df[valid_df['district'].isin(significant_districts)]
            district_stats = district_df.groupby('district')['response_time_seconds'].agg([
                'count', 'mean', 'median', calc_p95
            ]).round(2)
            district_stats.columns = ['count', 'mean', 'median', 'p95']
            district_stats = district_stats.sort_values('mean', ascending=False)
            patterns['by_district'] = district_stats.to_dict('index')
    
    # Pattern by message_type
    if 'message_type' in valid_df.columns:
        message_type_stats = valid_df.groupby('message_type')['response_time_seconds'].agg([
            'count', 'mean', 'median', calc_p95
        ]).round(2)
        message_type_stats.columns = ['count', 'mean', 'median', 'p95']
        message_type_stats = message_type_stats.sort_values('mean', ascending=False)
        patterns['by_message_type'] = message_type_stats.to_dict('index')
    
    # Create response time distribution data for export
    response_time_data = valid_df[[
        'user_id', 'phone_number_id', 'log_date', 'message_category', 
        'query_type', 'user_language', 'district', 'message_type',
        'incoming_timestamp', 'outgoing_timestamp', 'response_time_seconds'
    ]].copy()
    
    return {
        'total_queries': len(df),
        'valid_responses': len(valid_df),
        'statistics': statistics,
        'patterns': patterns,
        'response_time_data': response_time_data
    }


def analyze_idk(df: pd.DataFrame) -> dict:
    """Analyze IDK queries: count percentage, categorize causes, and bucket ASHA themes."""
    if df.empty:
        return {
            'total_queries': 0,
            'idk_count': 0,
            'idk_percentage': 0.0,
            'text_idk_count': 0,
            'audio_idk_count': 0,
            'text_idk_percentage': 0.0,
            'audio_idk_percentage': 0.0,
            'causes': {},
            'asha_themes': {},
            'breakdowns': {},
            'idk_queries': pd.DataFrame()
        }
    
    total_queries = len(df)
    
    # Identify IDK queries (audio_idk or text_idk)
    idk_mask = df['message_category'].isin(['audio_idk', 'text_idk'])
    idk_df = df[idk_mask].copy()
    idk_count = len(idk_df)
    
    if idk_count == 0:
        return {
            'total_queries': total_queries,
            'idk_count': 0,
            'idk_percentage': 0.0,
            'text_idk_count': 0,
            'audio_idk_count': 0,
            'text_idk_percentage': 0.0,
            'audio_idk_percentage': 0.0,
            'causes': {},
            'asha_themes': {},
            'breakdowns': {},
            'idk_queries': pd.DataFrame()
        }
    
    idk_percentage = (idk_count / total_queries) * 100
    
    # Breakdown by text/audio IDK
    text_idk_count = len(idk_df[idk_df['message_category'] == 'text_idk'])
    audio_idk_count = len(idk_df[idk_df['message_category'] == 'audio_idk'])
    text_idk_pct = (text_idk_count / idk_count * 100) if idk_count > 0 else 0
    audio_idk_pct = (audio_idk_count / idk_count * 100) if idk_count > 0 else 0
    
    # Categorize causes
    idk_df['idk_cause'] = idk_df.apply(categorize_idk_cause, axis=1)
    causes = idk_df['idk_cause'].value_counts().to_dict()
    causes_percentage = {cause: (count / idk_count) * 100 for cause, count in causes.items()}
    
    # Categorize ASHA themes (only for domain knowledge gaps)
    domain_gaps_df = idk_df[idk_df['idk_cause'] == 'Domain knowledge gaps'].copy()
    if not domain_gaps_df.empty:
        domain_gaps_df['asha_theme'] = domain_gaps_df.apply(categorize_asha_theme, axis=1)
        asha_themes = domain_gaps_df['asha_theme'].value_counts().to_dict()
        asha_themes_percentage = {theme: (count / len(domain_gaps_df)) * 100 for theme, count in asha_themes.items()}
    else:
        asha_themes = {}
        asha_themes_percentage = {}
    
    # Add theme column to all IDK queries
    idk_df['asha_theme'] = idk_df.apply(categorize_asha_theme, axis=1)
    
    # Create IDK queries dump with relevant columns
    idk_queries_dump = idk_df[[
        'user_id', 'phone_number_id', 'log_date', 'message_category',
        'query_source', 'query_en', 'rewritten_query', 'idk_cause', 'asha_theme'
    ]].copy()
    
    # Breakdowns by language and geography (only if meaningful differences)
    breakdowns = {}
    
    # Breakdown by language
    if 'user_language' in df.columns:
        lang_breakdown = []
        for lang in df['user_language'].dropna().unique():
            lang_df = df[df['user_language'] == lang]
            lang_total = len(lang_df)
            lang_idk = len(lang_df[lang_df['message_category'].isin(['audio_idk', 'text_idk'])])
            if lang_total > 0:
                lang_idk_pct = (lang_idk / lang_total) * 100
                lang_breakdown.append({
                    'language': lang,
                    'total_queries': lang_total,
                    'idk_count': lang_idk,
                    'idk_percentage': round(lang_idk_pct, 2)
                })
        
        if lang_breakdown:
            # Check for meaningful differences (>10% variation)
            idk_percentages = [b['idk_percentage'] for b in lang_breakdown]
            if len(idk_percentages) > 1:
                max_pct = max(idk_percentages)
                min_pct = min(idk_percentages)
                if max_pct > 0 and (max_pct - min_pct) / max_pct > 0.1:  # >10% relative difference
                    breakdowns['by_language'] = sorted(lang_breakdown, key=lambda x: x['idk_percentage'], reverse=True)
    
    # Breakdown by district
    if 'district' in df.columns:
        district_breakdown = []
        district_counts = df['district'].dropna().value_counts()
        # Only include districts with at least 50 queries for statistical significance
        significant_districts = district_counts[district_counts >= 50].index
        
        for district in significant_districts:
            district_df = df[df['district'] == district]
            district_total = len(district_df)
            district_idk = len(district_df[district_df['message_category'].isin(['audio_idk', 'text_idk'])])
            if district_total > 0:
                district_idk_pct = (district_idk / district_total) * 100
                district_breakdown.append({
                    'district': district,
                    'total_queries': district_total,
                    'idk_count': district_idk,
                    'idk_percentage': round(district_idk_pct, 2)
                })
        
        if district_breakdown:
            # Check for meaningful differences (>10% variation)
            idk_percentages = [b['idk_percentage'] for b in district_breakdown]
            if len(idk_percentages) > 1:
                max_pct = max(idk_percentages)
                min_pct = min(idk_percentages)
                if max_pct > 0 and (max_pct - min_pct) / max_pct > 0.1:  # >10% relative difference
                    breakdowns['by_district'] = sorted(district_breakdown, key=lambda x: x['idk_percentage'], reverse=True)
    
    # Breakdown by block
    if 'block' in df.columns:
        block_breakdown = []
        block_counts = df['block'].dropna().value_counts()
        # Only include blocks with at least 30 queries for statistical significance
        significant_blocks = block_counts[block_counts >= 30].index
        
        for block in significant_blocks:
            block_df = df[df['block'] == block]
            block_total = len(block_df)
            block_idk = len(block_df[block_df['message_category'].isin(['audio_idk', 'text_idk'])])
            if block_total > 0:
                block_idk_pct = (block_idk / block_total) * 100
                block_breakdown.append({
                    'block': block,
                    'total_queries': block_total,
                    'idk_count': block_idk,
                    'idk_percentage': round(block_idk_pct, 2)
                })
        
        if block_breakdown:
            # Check for meaningful differences (>10% variation)
            idk_percentages = [b['idk_percentage'] for b in block_breakdown]
            if len(idk_percentages) > 1:
                max_pct = max(idk_percentages)
                min_pct = min(idk_percentages)
                if max_pct > 0 and (max_pct - min_pct) / max_pct > 0.1:  # >10% relative difference
                    breakdowns['by_block'] = sorted(block_breakdown, key=lambda x: x['idk_percentage'], reverse=True)
    
    # Breakdown by sector
    if 'sector' in df.columns:
        sector_breakdown = []
        sector_counts = df['sector'].dropna().value_counts()
        # Only include sectors with at least 20 queries for statistical significance
        significant_sectors = sector_counts[sector_counts >= 20].index
        
        for sector in significant_sectors:
            sector_df = df[df['sector'] == sector]
            sector_total = len(sector_df)
            sector_idk = len(sector_df[sector_df['message_category'].isin(['audio_idk', 'text_idk'])])
            if sector_total > 0:
                sector_idk_pct = (sector_idk / sector_total) * 100
                sector_breakdown.append({
                    'sector': sector,
                    'total_queries': sector_total,
                    'idk_count': sector_idk,
                    'idk_percentage': round(sector_idk_pct, 2)
                })
        
        if sector_breakdown:
            # Check for meaningful differences (>10% variation)
            idk_percentages = [b['idk_percentage'] for b in sector_breakdown]
            if len(idk_percentages) > 1:
                max_pct = max(idk_percentages)
                min_pct = min(idk_percentages)
                if max_pct > 0 and (max_pct - min_pct) / max_pct > 0.1:  # >10% relative difference
                    breakdowns['by_sector'] = sorted(sector_breakdown, key=lambda x: x['idk_percentage'], reverse=True)
    
    return {
        'total_queries': total_queries,
        'idk_count': idk_count,
        'idk_percentage': round(idk_percentage, 2),
        'text_idk_count': text_idk_count,
        'audio_idk_count': audio_idk_count,
        'text_idk_percentage': round(text_idk_pct, 2),
        'audio_idk_percentage': round(audio_idk_pct, 2),
        'causes': causes,
        'causes_percentage': causes_percentage,
        'asha_themes': asha_themes,
        'asha_themes_percentage': asha_themes_percentage,
        'breakdowns': breakdowns,
        'idk_queries': idk_queries_dump
    }


def generate_summary_insights(idk_analysis: dict, response_time_analysis: dict, month_str: str) -> str:
    """Generate executive summary insights (≤100 words) with key findings."""
    insights = []
    
    # Insight 1: Overall IDK rate
    idk_pct = idk_analysis['idk_percentage']
    total_queries = idk_analysis['total_queries']
    idk_count = idk_analysis['idk_count']
    
    if idk_count > 0:
        insights.append(f"IDK rate: {idk_pct}% ({idk_count:,}/{total_queries:,} queries).")
    
    # Insight 2: Top IDK cause
    if idk_analysis['causes']:
        top_cause = max(idk_analysis['causes'].items(), key=lambda x: x[1])
        cause_pct = idk_analysis['causes_percentage'][top_cause[0]]
        insights.append(f"Primary cause: {top_cause[0]} ({cause_pct:.1f}% of IDKs).")
    
    # Insight 3: Top ASHA IDK theme (if domain knowledge gaps exist)
    if idk_analysis['asha_themes']:
        top_theme = max(idk_analysis['asha_themes'].items(), key=lambda x: x[1])
        theme_pct = idk_analysis['asha_themes_percentage'][top_theme[0]]
        insights.append(f"Top ASHA theme: {top_theme[0]} ({theme_pct:.1f}% of domain gaps).")
    
    # Insight 4: Language or geographic pattern (if significant)
    breakdowns = idk_analysis.get('breakdowns', {})
    if 'by_language' in breakdowns and len(breakdowns['by_language']) > 1:
        highest_lang = breakdowns['by_language'][0]
        lowest_lang = breakdowns['by_language'][-1]
        if highest_lang['idk_percentage'] > lowest_lang['idk_percentage'] * 1.2:  # >20% difference
            insights.append(f"Language gap: {highest_lang['language']} has {highest_lang['idk_percentage']:.1f}% IDK vs {lowest_lang['language']} at {lowest_lang['idk_percentage']:.1f}%.")
    
    if 'by_district' in breakdowns and len(breakdowns['by_district']) > 0:
        highest_district = breakdowns['by_district'][0]
        insights.append(f"Highest IDK district: {highest_district['district']} ({highest_district['idk_percentage']:.1f}%).")
    
    # Insight 5: Response time (if available)
    if response_time_analysis.get('valid_responses', 0) > 0:
        stats = response_time_analysis['statistics']
        insights.append(f"Avg response time: {stats['mean']:.1f}s (median: {stats['median']:.1f}s, P95: {stats['p95']:.1f}s).")
    
    # Combine insights into executive summary
    summary = " ".join(insights)
    
    # Ensure it's ≤100 words
    words = summary.split()
    if len(words) > 100:
        summary = " ".join(words[:100]) + "..."
    
    return summary


def format_asha_idk_buckets(idk_analysis: dict) -> str:
    """Format ASHA IDK bucket distribution as a readable string."""
    if not idk_analysis.get('asha_themes'):
        return "No ASHA IDK themes identified."
    
    lines = []
    for theme, count in sorted(idk_analysis['asha_themes'].items(), key=lambda x: x[1], reverse=True):
        percentage = idk_analysis['asha_themes_percentage'][theme]
        lines.append(f"  • {theme}: {percentage:.1f}% ({count:,} queries)")
    
    return "\n".join(lines)


def format_markdown_table(headers: list, rows: list[dict]) -> str:
    """Render a simple markdown table."""
    if not rows:
        return "No data available."
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |"
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(h, "")) for h in headers) + " |")
    return "\n".join(lines)


def normalize_language_name(lang_code: str) -> Optional[str]:
    """Map language codes to full names."""
    lang_map = {
        'hi': 'Hindi',
        'mr': 'Marathi',
        'en': 'English',
        'hindi': 'Hindi',
        'marathi': 'Marathi',
        'english': 'English',
        'Hindi': 'Hindi',
        'Marathi': 'Marathi',
        'English': 'English'
    }
    if pd.isna(lang_code) or lang_code is None:
        return None
    lang_str = str(lang_code).strip().lower()
    return lang_map.get(lang_str, lang_code)  # Return original if not found


def calculate_language_percentages(df: pd.DataFrame) -> dict:
    """Calculate percentage distribution of queries by language."""
    if df.empty or 'user_language' not in df.columns:
        return {}
    
    lang_counts = df['user_language'].value_counts()
    total = len(df)
    if total == 0:
        return {}
    
    lang_pct = {}
    for lang, count in lang_counts.items():
        if pd.notna(lang):
            normalized_lang = normalize_language_name(lang)
            if normalized_lang:
                # Aggregate counts if multiple codes map to same language
                if normalized_lang in lang_pct:
                    lang_pct[normalized_lang] += count
                else:
                    lang_pct[normalized_lang] = count
    
    # Convert counts to percentages
    for lang in lang_pct:
        lang_pct[lang] = round((lang_pct[lang] / total) * 100, 1)
    
    return lang_pct


def parse_prev_month_summary(prev_summary_path: str) -> Optional[dict]:
    """Parse previous month's summary markdown to extract metrics for MoM comparison."""
    try:
        content = Path(prev_summary_path).read_text(encoding="utf-8")
        prev_data = {}
        
        # Try to find the main metrics table
        lines = content.split('\n')
        in_table = False
        headers = []
        
        for line in lines:
            # Find table header
            if '| Month' in line or '| Total Interactions' in line:
                in_table = True
                headers = [h.strip() for h in line.split('|')[1:-1]]
                continue
            
            # Skip separator line
            if in_table and line.startswith('|---'):
                continue
            
            # Parse data row
            if in_table and line.startswith('|') and headers:
                parts = [p.strip() for p in line.split('|')[1:-1]]
                if len(parts) == len(headers) and parts[0] and parts[0] != 'Month':
                    # Map values to headers
                    for i, header in enumerate(headers):
                        if i < len(parts):
                            value = parts[i]
                            try:
                                if header == 'Total Interactions':
                                    prev_data['total_interactions'] = int(value.replace(',', ''))
                                elif header == 'IDK %':
                                    prev_data['idk_percentage'] = float(value.replace('%', ''))
                                elif header == 'Success %':
                                    prev_data['success_percentage'] = float(value.replace('%', ''))
                                elif header == 'Avg Response Time (s)':
                                    prev_data['avg_response_time'] = float(value)
                            except (ValueError, AttributeError):
                                pass
                    break  # Only parse first data row
        
        # Fallback: try to extract from text patterns if table parsing failed
        if not prev_data:
            import re
            total_match = re.search(r'Total Interactions[:\s]+(\d+(?:,\d+)*)', content)
            if total_match:
                prev_data['total_interactions'] = int(total_match.group(1).replace(',', ''))
            
            idk_match = re.search(r'IDK %[:\s]+(\d+\.?\d*)%', content)
            if idk_match:
                prev_data['idk_percentage'] = float(idk_match.group(1))
            
            success_match = re.search(r'Success %[:\s]+(\d+\.?\d*)%', content)
            if success_match:
                prev_data['success_percentage'] = float(success_match.group(1))
            
            rt_match = re.search(r'Avg Response Time \(s\)[:\s]+(\d+\.?\d*)', content)
            if rt_match:
                prev_data['avg_response_time'] = float(rt_match.group(1))
        
        return prev_data if prev_data else None
    except Exception as e:
        print(f"Warning: Could not parse previous month summary: {e}")
        return None


def calculate_mom_delta(current: float, previous: Optional[float]) -> Optional[str]:
    """Calculate month-over-month delta percentage."""
    if previous is None or previous == 0:
        return None
    delta = ((current - previous) / previous) * 100
    return f"{delta:+.2f}%"


def create_mermaid_pie_chart(title: str, data: dict, max_items: int = 10) -> str:
    """Create a Mermaid pie chart from a dictionary of labels and values."""
    if not data:
        return ""
    
    # Sort by value and take top items
    sorted_data = sorted(data.items(), key=lambda x: x[1], reverse=True)[:max_items]
    
    # Build Mermaid pie chart syntax
    lines = [f"```mermaid", f"pie title {title}"]
    for label, value in sorted_data:
        # Clean label for Mermaid (remove special chars, limit length)
        clean_label = str(label).replace('"', "'").replace('\n', ' ')[:30]
        lines.append(f'    "{clean_label}" : {value}')
    
    lines.append("```")
    return "\n".join(lines)


def create_text_bar_chart(title: str, data: dict, max_items: int = 10) -> str:
    """Create a simple text-based bar chart that works without any dependencies."""
    if not data:
        return ""
    
    sorted_data = sorted(data.items(), key=lambda x: x[1], reverse=True)[:max_items]
    if not sorted_data:
        return ""
    
    max_value = max(v for _, v in sorted_data) if sorted_data else 1
    
    lines = [f"### {title}", ""]
    for label, value in sorted_data:
        bar_length = int((value / max_value) * 50) if max_value > 0 else 0
        bar = "█" * bar_length
        lines.append(f"**{str(label)[:30]}**: {value:.1f} {bar}")
    
    return "\n".join(lines)


def create_matplotlib_chart(chart_type: str, data: dict, title: str, output_path: str, 
                           x_label: str = "", y_label: str = "", max_items: int = 10,
                           extra_data: Optional[dict] = None) -> Optional[str]:
    """Generate a matplotlib chart and save as image. Returns markdown image link or None."""
    if not HAS_MATPLOTLIB or not data:
        return None
    
    try:
        sorted_data = sorted(data.items(), key=lambda x: x[1], reverse=True)[:max_items]
        if not sorted_data:
            return None
        
        labels = [str(k)[:30] for k, v in sorted_data]  # Truncate long labels
        values = [v for _, v in sorted_data]
        
        fig, ax = plt.subplots(figsize=(10, 6))
        plt.rcParams['font.size'] = 9
        
        if chart_type == 'pie':
            ax.pie(values, labels=labels, autopct='%1.1f%%', startangle=90)
            ax.axis('equal')
        elif chart_type == 'barh':  # Horizontal bar chart
            bars = ax.barh(range(len(labels)), values)
            ax.set_yticks(range(len(labels)))
            ax.set_yticklabels(labels)
            ax.set_xlabel(y_label if y_label else "Count")
            ax.invert_yaxis()
            # Add value labels on bars
            for i, (bar, val) in enumerate(zip(bars, values)):
                width = bar.get_width()
                ax.text(width, bar.get_y() + bar.get_height()/2, 
                       f'{val:.1f}', ha='left', va='center', fontsize=8)
        elif chart_type == 'bar':  # Vertical bar chart
            bars = ax.bar(range(len(labels)), values)
            ax.set_xticks(range(len(labels)))
            ax.set_xticklabels(labels, rotation=45, ha='right')
            ax.set_ylabel(y_label if y_label else "Count")
            # Add value labels on bars
            for bar, val in zip(bars, values):
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{val:.1f}', ha='center', va='bottom', fontsize=8)
        elif chart_type == 'stacked_bar':  # Stacked bar chart
            if extra_data:
                labels_list = list(data.keys())[:max_items]
                bottom = [0] * len(labels_list)
                for key, values_dict in extra_data.items():
                    values_list = [values_dict.get(l, 0) for l in labels_list]
                    ax.bar(range(len(labels_list)), values_list, label=key, bottom=bottom)
                    bottom = [b + v for b, v in zip(bottom, values_list)]
                ax.set_xticks(range(len(labels_list)))
                ax.set_xticklabels(labels_list, rotation=45, ha='right')
                ax.set_ylabel(y_label if y_label else "Count")
                ax.legend()
        elif chart_type == 'histogram':  # Histogram
            ax.hist(values, bins=min(30, len(set(values))), edgecolor='black', alpha=0.7)
            ax.set_xlabel(x_label if x_label else "Value")
            ax.set_ylabel(y_label if y_label else "Frequency")
            ax.set_title(title, fontsize=12, fontweight='bold')
            plt.tight_layout()
            plt.savefig(output_path, dpi=150, bbox_inches='tight')
            plt.close()
            return f"![{title}]({Path(output_path).name})"
        elif chart_type == 'boxplot':  # Box plot
            ax.boxplot(values, vert=True)
            ax.set_ylabel(y_label if y_label else "Value")
            ax.set_xticklabels([title])
        
        ax.set_title(title, fontsize=12, fontweight='bold')
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()
        
        # Return markdown image link
        return f"![{title}]({Path(output_path).name})"
    except Exception as e:
        print(f"Warning: Failed to generate chart {title}: {e}")
        return None


def create_response_time_distribution_chart(df: pd.DataFrame, title: str, output_path: str) -> Optional[str]:
    """Create improved response time distribution visualizations: histogram and CDF."""
    if not HAS_MATPLOTLIB or df.empty or 'response_time_seconds' not in df.columns:
        return None
    
    try:
        valid_rt = df['response_time_seconds'].dropna()
        if valid_rt.empty:
            return None
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
        
        # Histogram with percentile markers
        n, bins, patches = ax1.hist(valid_rt, bins=50, edgecolor='black', alpha=0.7, color='steelblue')
        ax1.set_xlabel('Response Time (seconds)')
        ax1.set_ylabel('Frequency')
        ax1.set_title('Response Time Distribution (Histogram)')
        
        # Add percentile lines
        p50 = valid_rt.median()
        p75 = valid_rt.quantile(0.75)
        p90 = valid_rt.quantile(0.90)
        p95 = valid_rt.quantile(0.95)
        
        ax1.axvline(p50, color='green', linestyle='--', linewidth=2, label=f'Median (P50): {p50:.1f}s')
        ax1.axvline(p75, color='orange', linestyle='--', linewidth=1.5, label=f'P75: {p75:.1f}s')
        ax1.axvline(p90, color='red', linestyle='--', linewidth=1.5, label=f'P90: {p90:.1f}s')
        ax1.axvline(p95, color='darkred', linestyle='--', linewidth=1.5, label=f'P95: {p95:.1f}s')
        ax1.legend(fontsize=8)
        ax1.grid(True, alpha=0.3)
        
        # Cumulative Distribution Function (CDF) - more insightful than box plot
        sorted_rt = valid_rt.sort_values()
        p = pd.Series(range(1, len(sorted_rt) + 1)) / len(sorted_rt) * 100
        
        ax2.plot(sorted_rt, p, linewidth=2, color='steelblue')
        ax2.set_xlabel('Response Time (seconds)')
        ax2.set_ylabel('Cumulative Percentage (%)')
        ax2.set_title('Cumulative Distribution Function (CDF)')
        ax2.grid(True, alpha=0.3)
        
        # Add reference lines for common thresholds
        thresholds = [10, 30, 60, 120, 300]
        for threshold in thresholds:
            if threshold <= sorted_rt.max():
                pct_below = (sorted_rt <= threshold).sum() / len(sorted_rt) * 100
                ax2.axvline(threshold, color='gray', linestyle=':', alpha=0.5, linewidth=1)
                ax2.text(threshold, pct_below + 2, f'{pct_below:.1f}%', 
                        ha='center', fontsize=7, color='gray')
        
        # Add percentile markers on CDF
        for percentile, value, color in [(50, p50, 'green'), (90, p90, 'red'), (95, p95, 'darkred')]:
            pct_value = (sorted_rt <= value).sum() / len(sorted_rt) * 100
            ax2.plot(value, pct_value, 'o', color=color, markersize=6)
            ax2.text(value, pct_value - 5, f'P{percentile}', 
                    ha='center', fontsize=7, color=color, fontweight='bold')
        
        plt.suptitle(title, fontsize=14, fontweight='bold', y=1.02)
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()
        
        return f"![{title}]({Path(output_path).name})"
    except Exception as e:
        print(f"Warning: Failed to generate response time distribution chart: {e}")
        return None


def create_comparison_chart(data1: dict, data2: dict, title: str, output_path: str, 
                           label1: str = "Series 1", label2: str = "Series 2") -> Optional[str]:
    """Create a side-by-side comparison bar chart."""
    if not HAS_MATPLOTLIB or not data1 or not data2:
        return None
    
    try:
        # Get common keys
        common_keys = sorted(set(data1.keys()) & set(data2.keys()))
        if not common_keys:
            return None
        
        x = range(len(common_keys))
        width = 0.35
        
        fig, ax = plt.subplots(figsize=(12, 6))
        values1 = [data1[k] for k in common_keys]
        values2 = [data2[k] for k in common_keys]
        
        ax.bar([i - width/2 for i in x], values1, width, label=label1, alpha=0.8)
        ax.bar([i + width/2 for i in x], values2, width, label=label2, alpha=0.8)
        
        ax.set_xlabel('Category')
        ax.set_ylabel('Value')
        ax.set_title(title)
        ax.set_xticks(x)
        ax.set_xticklabels([str(k)[:20] for k in common_keys], rotation=45, ha='right')
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()
        
        return f"![{title}]({Path(output_path).name})"
    except Exception as e:
        print(f"Warning: Failed to generate comparison chart: {e}")
        return None


def create_multi_metric_chart(patterns_data: dict, title: str, output_path: str, 
                              metrics: list = ['mean', 'median', 'p95']) -> Optional[str]:
    """Create a grouped bar chart showing multiple metrics (mean, median, P95) for different categories."""
    if not HAS_MATPLOTLIB or not patterns_data:
        return None
    
    try:
        categories = list(patterns_data.keys())[:10]  # Top 10
        if not categories:
            return None
        
        x = range(len(categories))
        width = 0.25
        
        fig, ax = plt.subplots(figsize=(14, 6))
        
        metric_data = {}
        for metric in metrics:
            metric_data[metric] = [patterns_data[cat].get(metric, 0) for cat in categories]
        
        positions = []
        labels_map = {'mean': 'Mean', 'median': 'Median', 'p95': 'P95'}
        colors = ['steelblue', 'lightcoral', 'lightgreen']
        
        for i, metric in enumerate(metrics):
            pos = [j + (i - len(metrics)/2 + 0.5) * width for j in x]
            positions.append(pos)
            ax.bar(pos, metric_data[metric], width, label=labels_map.get(metric, metric), 
                  alpha=0.8, color=colors[i % len(colors)])
        
        ax.set_xlabel('Category')
        ax.set_ylabel('Response Time (seconds)')
        ax.set_title(title)
        ax.set_xticks(x)
        ax.set_xticklabels([str(c)[:25] for c in categories], rotation=45, ha='right')
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()
        
        return f"![{title}]({Path(output_path).name})"
    except Exception as e:
        print(f"Warning: Failed to generate multi-metric chart: {e}")
        return None


def build_markdown_report(
    month_str: str,
    summary_insights: str,
    idk_analysis: dict,
    response_time_analysis: dict,
    df: pd.DataFrame,
    llm_usage: Optional[dict] = None,
    llm_summary: Optional[str] = None,
    prev_month_data: Optional[dict] = None,
    user_stats: Optional[dict] = None,
    prev_user_stats: Optional[dict] = None,
    output_dir: Optional[Path] = None
) -> str:
    """Build a simplified markdown report matching the required table format."""
    sections: list[str] = []
    chart_dir = output_dir if output_dir else Path(".")

    # Format month as "MMM-YY" (e.g., "Nov-25")
    try:
        month_date = datetime.strptime(month_str, "%Y-%m")
        month_display = month_date.strftime("%b-%y")
    except:
        month_display = month_str

    # Calculate metrics
    total_interactions = idk_analysis.get('total_queries', 0)
    idk_count = idk_analysis.get('idk_count', 0)
    idk_percentage = idk_analysis.get('idk_percentage', 0)
    success_percentage = 100 - idk_percentage

    # User stats (from DB and logs)
    onboarded_count = user_stats.get("onboarded_count") if user_stats else None
    active_count = user_stats.get("active_count") if user_stats else None
    prev_onboarded = prev_user_stats.get("onboarded_count") if prev_user_stats else None
    prev_active = prev_user_stats.get("active_count") if prev_user_stats else None
    mom_onboarded_delta = calculate_mom_delta(onboarded_count, prev_onboarded) if onboarded_count is not None else None
    mom_active_delta = calculate_mom_delta(active_count, prev_active) if active_count is not None else None
    
    # Language percentages
    lang_pct = calculate_language_percentages(df)
    hindi_pct = lang_pct.get('Hindi', 0)
    marathi_pct = lang_pct.get('Marathi', 0)
    english_pct = lang_pct.get('English', 0)
    
    # Text/Audio IDK breakdown
    text_idk_pct = idk_analysis.get('text_idk_percentage', 0)
    audio_idk_pct = idk_analysis.get('audio_idk_percentage', 0)
    
    # Response times
    rt_stats = response_time_analysis.get('statistics', {}) or {}
    avg_rt = rt_stats.get('mean', 0)
    p90_rt = rt_stats.get('p90', 0)
    
    # MoM deltas
    mom_volume_delta = calculate_mom_delta(
        total_interactions, 
        prev_month_data.get('total_interactions') if prev_month_data else None
    ) if prev_month_data else None
    
    mom_success_delta = calculate_mom_delta(
        success_percentage,
        prev_month_data.get('success_percentage') if prev_month_data else None
    ) if prev_month_data else None
    
    mom_idk_delta = calculate_mom_delta(
        idk_percentage,
        prev_month_data.get('idk_percentage') if prev_month_data else None
    ) if prev_month_data else None
    
    mom_rt_delta = calculate_mom_delta(
        avg_rt,
        prev_month_data.get('avg_response_time') if prev_month_data else None
    ) if prev_month_data else None

    # Main metrics table
    sections.append("# Monthly Analysis Report")
    sections.append("")
    sections.append("## Key Metrics")
    sections.append("")
    
    # Always include MoM columns (show "-" when no previous data)
    headers = ["Month", "Total Interactions", "Hindi %", "Marathi %", "English %", 
               "Success %", "IDK %", "IDK count", "Text IDK %", "Audio IDK %",
               "Avg Response Time (s)", "P90 Response Time (s)",
               "MoM Δ Volume %", "MoM Δ Success %", "MoM Δ IDK%", "MoM Δ Avg RT (s)"]
    
    row = {
        "Month": month_display,
        "Total Interactions": f"{total_interactions:,}",
        "Hindi %": f"{hindi_pct:.1f}",
        "Marathi %": f"{marathi_pct:.1f}",
        "English %": f"{english_pct:.1f}",
        "Success %": f"{success_percentage:.1f}",
        "IDK %": f"{idk_percentage:.1f}",
        "IDK count": f"{idk_count:,}",
        "Text IDK %": f"{text_idk_pct:.1f}",
        "Audio IDK %": f"{audio_idk_pct:.1f}",
        "Avg Response Time (s)": f"{avg_rt:.1f}",
        "P90 Response Time (s)": f"{p90_rt:.1f}",
        "MoM Δ Volume %": mom_volume_delta if prev_month_data and mom_volume_delta else "-",
        "MoM Δ Success %": mom_success_delta if prev_month_data and mom_success_delta else "-",
        "MoM Δ IDK%": mom_idk_delta if prev_month_data and mom_idk_delta else "-",
        "MoM Δ Avg RT (s)": mom_rt_delta if prev_month_data and mom_rt_delta else "-"
    }
    
    sections.append(format_markdown_table(headers, [row]))
    sections.append("")

    # ASHA User Metrics
    if onboarded_count is not None or active_count is not None:
        sections.append("## ASHA User Metrics (Month-on-Month)")
        sections.append("")
        user_headers = ["Metric", "Current", "Previous", "MoM Δ"]
        user_rows = []
        if onboarded_count is not None:
            user_rows.append({
                "Metric": "Onboarded ASHA (in month)",
                "Current": f"{onboarded_count:,}",
                "Previous": f"{prev_onboarded:,}" if prev_onboarded is not None else "-",
                "MoM Δ": mom_onboarded_delta or "-"
            })
        if active_count is not None:
            user_rows.append({
                "Metric": "Active ASHA (>=1 query in month)",
                "Current": f"{active_count:,}",
                "Previous": f"{prev_active:,}" if prev_active is not None else "-",
                "MoM Δ": mom_active_delta or "-"
            })
        if user_rows:
            sections.append(format_markdown_table(user_headers, user_rows))
            sections.append("")
    
    # Language distribution chart (one chart only)
    if lang_pct:
        lang_chart_data = {k: v for k, v in lang_pct.items() if v > 0}
        if lang_chart_data:
            sections.append("### Language Distribution")
            sections.append("")
            # Prefer matplotlib if available, else mermaid
            chart_added = False
            if HAS_MATPLOTLIB:
                chart_path = chart_dir / f"{month_str}_language_dist.png"
                img_link = create_matplotlib_chart('pie', lang_chart_data, "Language Distribution", 
                                                   str(chart_path), max_items=10)
                if img_link:
                    sections.append(img_link)
                    sections.append("")
                    chart_added = True
            if not chart_added:
                mermaid_chart = create_mermaid_pie_chart("Language Distribution", lang_chart_data)
                if mermaid_chart:
                    sections.append(mermaid_chart)
                    sections.append("")
    
    # Success vs IDK Comparison Chart (one chart only)
    if total_interactions > 0:
        success_idk_data = {
            'Success': success_percentage,
            'IDK': idk_percentage
        }
        sections.append("### Success vs IDK Rate")
        sections.append("")
        # Prefer matplotlib if available, else mermaid
        chart_added = False
        if HAS_MATPLOTLIB:
            chart_path = chart_dir / f"{month_str}_success_idk_comparison.png"
            img_link = create_matplotlib_chart('bar', success_idk_data, "Success vs IDK Rate", 
                                               str(chart_path), y_label="Percentage (%)", max_items=10)
            if img_link:
                sections.append(img_link)
                sections.append("")
                chart_added = True
        if not chart_added:
            mermaid_chart = create_mermaid_pie_chart("Success vs IDK Rate", success_idk_data)
            if mermaid_chart:
                sections.append(mermaid_chart)
                sections.append("")
    
    # Text vs Audio IDK Comparison (one chart only)
    if idk_count > 0:
        text_audio_data = {
            'Text IDK': text_idk_pct,
            'Audio IDK': audio_idk_pct
        }
        sections.append("### Text vs Audio IDK Distribution")
        sections.append("")
        # Prefer matplotlib if available, else mermaid
        chart_added = False
        if HAS_MATPLOTLIB:
            chart_path = chart_dir / f"{month_str}_text_audio_idk.png"
            img_link = create_matplotlib_chart('bar', text_audio_data, "Text vs Audio IDK Distribution", 
                                               str(chart_path), y_label="Percentage (%)", max_items=10)
            if img_link:
                sections.append(img_link)
                sections.append("")
                chart_added = True
        if not chart_added:
            mermaid_chart = create_mermaid_pie_chart("Text vs Audio IDK", text_audio_data)
            if mermaid_chart:
                sections.append(mermaid_chart)
                sections.append("")
    
    # Executive summary
    sections.append("## Executive Summary")
    sections.append("")
    sections.append(llm_summary or summary_insights)
    sections.append("")
    
    # IDK Buckets table (excluding "Other" category as it indicates poor categorization)
    themes = idk_analysis.get('asha_themes', {}) or {}
    themes_pct = idk_analysis.get('asha_themes_percentage', {}) or {}
    if themes:
        sections.append("## IDK Buckets by Category")
        sections.append("")
        sections.append("*Note: 'Other' category is excluded as it indicates poor categorization.*")
        sections.append("")
        
        rows = []
        for name, count in sorted(themes.items(), key=lambda x: x[1], reverse=True):
            # Skip "Other" category entirely
            if name.lower() == 'other':
                continue
            
            rows.append({
                "Category": name,
                "Count": f"{count:,}",
                "% of Domain Gaps": f"{themes_pct.get(name, 0):.1f}%"
            })
        
        if rows:
            sections.append(format_markdown_table(["Category", "Count", "% of Domain Gaps"], rows))
            sections.append("")
            
            # IDK Buckets chart (excluding Other, one chart only, no duplicate title)
            buckets_data = {name: count for name, count in sorted(themes.items(), key=lambda x: x[1], reverse=True) 
                           if name.lower() != 'other'}
            if buckets_data:
                # Prefer matplotlib if available, else mermaid
                chart_added = False
                if HAS_MATPLOTLIB:
                    chart_path = chart_dir / f"{month_str}_idk_buckets.png"
                    img_link = create_matplotlib_chart('barh', buckets_data, "IDK Buckets by Category", 
                                                       str(chart_path), y_label="Count", max_items=15)
                    if img_link:
                        sections.append(img_link)
                        sections.append("")
                        chart_added = True
                if not chart_added:
                    mermaid_chart = create_mermaid_pie_chart("IDK Buckets by Category", buckets_data)
                    if mermaid_chart:
                        sections.append(mermaid_chart)
                        sections.append("")
    
    # Detailed IDK Analysis
    sections.append("## Detailed IDK Analysis")
    sections.append("")
    
    # IDK causes
    causes = idk_analysis.get('causes', {}) or {}
    causes_pct = idk_analysis.get('causes_percentage', {}) or {}
    if causes:
        rows = [
            {
                "Cause": name,
                "Count": f"{count:,}",
                "% of IDK": f"{causes_pct.get(name, 0):.1f}%"
            }
            for name, count in sorted(causes.items(), key=lambda x: x[1], reverse=True)
        ]
        sections.append("### IDK Causes")
        sections.append(format_markdown_table(["Cause", "Count", "% of IDK"], rows))
        sections.append("")
        
        # IDK Causes chart (one chart only, no duplicate title)
        if causes:
            # Prefer matplotlib if available, else mermaid
            chart_added = False
            if HAS_MATPLOTLIB:
                chart_path = chart_dir / f"{month_str}_idk_causes.png"
                img_link = create_matplotlib_chart('pie', causes, "IDK Causes Distribution", 
                                                   str(chart_path), max_items=10)
                if img_link:
                    sections.append(img_link)
                    sections.append("")
                    chart_added = True
            if not chart_added:
                mermaid_chart = create_mermaid_pie_chart("IDK Causes Distribution", causes)
                if mermaid_chart:
                    sections.append(mermaid_chart)
                    sections.append("")
    
    # Breakdowns by language and geography
    breakdowns = idk_analysis.get('breakdowns', {}) or {}
    
    if breakdowns.get('by_language'):
        rows = [
            {
                "Language": b["language"],
                "IDK %": f"{b['idk_percentage']:.1f}%",
                "IDK/Total": f"{b['idk_count']:,}/{b['total_queries']:,}"
            }
            for b in breakdowns["by_language"]
        ]
        sections.append("### IDK Breakdown by Language")
        sections.append(format_markdown_table(["Language", "IDK %", "IDK/Total"], rows))
        sections.append("")
        
        # IDK by Language chart (one chart only, no duplicate title)
        lang_idk_data = {b["language"]: b["idk_percentage"] for b in breakdowns["by_language"]}
        if lang_idk_data:
            # Prefer matplotlib if available, else mermaid, else text
            chart_added = False
            if HAS_MATPLOTLIB:
                chart_path = chart_dir / f"{month_str}_idk_by_language.png"
                img_link = create_matplotlib_chart('barh', lang_idk_data, "IDK Percentage by Language", 
                                                   str(chart_path), y_label="IDK %", max_items=10)
                if img_link:
                    sections.append(img_link)
                    sections.append("")
                    chart_added = True
            if not chart_added:
                mermaid_chart = create_mermaid_pie_chart("IDK Percentage by Language", lang_idk_data)
                if mermaid_chart:
                    sections.append(mermaid_chart)
                    sections.append("")
                    chart_added = True
            if not chart_added:
                text_chart = create_text_bar_chart("IDK Percentage by Language", lang_idk_data)
                if text_chart:
                    sections.append(text_chart)
                    sections.append("")
    
    if breakdowns.get('by_district'):
        rows = [
            {
                "District": b["district"],
                "IDK %": f"{b['idk_percentage']:.1f}%",
                "IDK/Total": f"{b['idk_count']:,}/{b['total_queries']:,}"
            }
            for b in breakdowns["by_district"][:10]
        ]
        sections.append("### IDK Breakdown by District (top 10)")
        sections.append(format_markdown_table(["District", "IDK %", "IDK/Total"], rows))
        sections.append("")
        
        # District IDK chart (one chart only, no duplicate title)
        district_idk_data = {b["district"]: b["idk_percentage"] for b in breakdowns["by_district"][:10]}
        if district_idk_data:
            # Prefer matplotlib if available, else mermaid, else text
            chart_added = False
            if HAS_MATPLOTLIB:
                chart_path = chart_dir / f"{month_str}_idk_by_district.png"
                img_link = create_matplotlib_chart('barh', district_idk_data, "IDK Percentage by District (Top 10)", 
                                                   str(chart_path), y_label="IDK %", max_items=10)
                if img_link:
                    sections.append(img_link)
                    sections.append("")
                    chart_added = True
            if not chart_added:
                mermaid_chart = create_mermaid_pie_chart("IDK Percentage by District (Top 10)", district_idk_data)
                if mermaid_chart:
                    sections.append(mermaid_chart)
                    sections.append("")
                    chart_added = True
            if not chart_added:
                text_chart = create_text_bar_chart("IDK Percentage by District", district_idk_data)
                if text_chart:
                    sections.append(text_chart)
                    sections.append("")
    
    if breakdowns.get('by_block'):
        rows = [
            {
                "Block": b["block"],
                "IDK %": f"{b['idk_percentage']:.1f}%",
                "IDK/Total": f"{b['idk_count']:,}/{b['total_queries']:,}"
            }
            for b in breakdowns["by_block"][:10]
        ]
        sections.append("### IDK Breakdown by Block (top 10)")
        sections.append(format_markdown_table(["Block", "IDK %", "IDK/Total"], rows))
        sections.append("")
        
        # Block IDK chart (one chart only, no duplicate title)
        block_idk_data = {b["block"]: b["idk_percentage"] for b in breakdowns["by_block"][:10]}
        if block_idk_data:
            # Prefer matplotlib if available, else mermaid, else text
            chart_added = False
            if HAS_MATPLOTLIB:
                chart_path = chart_dir / f"{month_str}_idk_by_block.png"
                img_link = create_matplotlib_chart('barh', block_idk_data, "IDK Percentage by Block (Top 10)", 
                                                   str(chart_path), y_label="IDK %", max_items=10)
                if img_link:
                    sections.append(img_link)
                    sections.append("")
                    chart_added = True
            if not chart_added:
                mermaid_chart = create_mermaid_pie_chart("IDK Percentage by Block (Top 10)", block_idk_data)
                if mermaid_chart:
                    sections.append(mermaid_chart)
                    sections.append("")
                    chart_added = True
            if not chart_added:
                text_chart = create_text_bar_chart("IDK Percentage by Block", block_idk_data)
                if text_chart:
                    sections.append(text_chart)
                    sections.append("")
    
    if breakdowns.get('by_sector'):
        rows = [
            {
                "Sector": b["sector"],
                "IDK %": f"{b['idk_percentage']:.1f}%",
                "IDK/Total": f"{b['idk_count']:,}/{b['total_queries']:,}"
            }
            for b in breakdowns["by_sector"][:10]
        ]
        sections.append("### IDK Breakdown by Sector (top 10)")
        sections.append(format_markdown_table(["Sector", "IDK %", "IDK/Total"], rows))
        sections.append("")
        
        # Sector IDK chart (one chart only, no duplicate title)
        sector_idk_data = {b["sector"]: b["idk_percentage"] for b in breakdowns["by_sector"][:10]}
        if sector_idk_data:
            # Prefer matplotlib if available, else mermaid, else text
            chart_added = False
            if HAS_MATPLOTLIB:
                chart_path = chart_dir / f"{month_str}_idk_by_sector.png"
                img_link = create_matplotlib_chart('barh', sector_idk_data, "IDK Percentage by Sector (Top 10)", 
                                                   str(chart_path), y_label="IDK %", max_items=10)
                if img_link:
                    sections.append(img_link)
                    sections.append("")
                    chart_added = True
            if not chart_added:
                mermaid_chart = create_mermaid_pie_chart("IDK Percentage by Sector (Top 10)", sector_idk_data)
                if mermaid_chart:
                    sections.append(mermaid_chart)
                    sections.append("")
                    chart_added = True
            if not chart_added:
                text_chart = create_text_bar_chart("IDK Percentage by Sector", sector_idk_data)
                if text_chart:
                    sections.append(text_chart)
                    sections.append("")
    
    # Detailed Response Time Analysis
    sections.append("## Detailed Response Time Analysis")
    sections.append("")
    
    rt_stats = response_time_analysis.get('statistics', {}) or {}
    if rt_stats:
        sections.append("### Response Time Statistics")
        sections.append(
            f"- Mean: {rt_stats.get('mean', 0):.1f}s\n"
            f"- Median: {rt_stats.get('median', 0):.1f}s\n"
            f"- P95: {rt_stats.get('p95', 0):.1f}s\n"
            f"- P90: {rt_stats.get('p90', 0):.1f}s\n"
            f"- P75: {rt_stats.get('p75', 0):.1f}s\n"
            f"- Min: {rt_stats.get('min', 0):.1f}s\n"
            f"- Max: {rt_stats.get('max', 0):.1f}s\n"
            f"- Valid samples: {rt_stats.get('count', 0):,}"
        )
        sections.append("")
        
        # Response Time Distribution Chart
        rt_data = response_time_analysis.get('response_time_data', pd.DataFrame())
        if not rt_data.empty and 'response_time_seconds' in rt_data.columns:
            chart_path = chart_dir / f"{month_str}_rt_distribution.png"
            img_link = create_response_time_distribution_chart(rt_data, "Response Time Distribution", str(chart_path))
            if img_link:
                sections.append("### Response Time Distribution")
                sections.append("")
                sections.append(img_link)
                sections.append("")
        
        # Response Time Statistics Visualization (one chart only, no duplicate title)
        if rt_stats:
            stats_data = {
                'Mean': rt_stats.get('mean', 0),
                'Median': rt_stats.get('median', 0),
                'P75': rt_stats.get('p75', 0),
                'P90': rt_stats.get('p90', 0),
                'P95': rt_stats.get('p95', 0)
            }
            # Prefer matplotlib if available, else text
            chart_added = False
            if HAS_MATPLOTLIB:
                chart_path = chart_dir / f"{month_str}_rt_statistics.png"
                img_link = create_matplotlib_chart('bar', stats_data, "Response Time Statistics Comparison", 
                                                   str(chart_path), y_label="Time (seconds)", max_items=10)
                if img_link:
                    sections.append(img_link)
                    sections.append("")
                    chart_added = True
            if not chart_added:
                text_chart = create_text_bar_chart("Response Time Statistics Comparison", stats_data)
                if text_chart:
                    sections.append(text_chart)
                    sections.append("")
    else:
        sections.append("- No valid response-time samples.")
        sections.append("")
    
    # Response time patterns
    patterns = response_time_analysis.get('patterns', {}) or {}
    
    def add_pattern(title: str, key: str):
        if key in patterns and patterns[key]:
            rows = []
            chart_data = {}
            for name, stat in patterns[key].items():
                group_name = name if name else "(blank)"
                rows.append({
                    "Group": group_name,
                    "Mean": f"{stat.get('mean', 0):.1f}s",
                    "Median": f"{stat.get('median', 0):.1f}s",
                    "P95": f"{stat.get('p95', 0):.1f}s",
                    "Count": stat.get('count', 0)
                })
                chart_data[group_name] = stat.get('mean', 0)
            if rows:
                sections.append(f"### Response Time by {title}")
                sections.append(format_markdown_table(["Group", "Mean", "Median", "P95", "Count"], rows))
                sections.append("")
                
                # Add chart for response time patterns (one chart only, no duplicate title)
                if chart_data:
                    # Prefer matplotlib if available, else text
                    chart_added = False
                    if HAS_MATPLOTLIB:
                        chart_path = chart_dir / f"{month_str}_rt_{key.lower().replace(' ', '_')}_mean.png"
                        img_link = create_matplotlib_chart('barh', chart_data, f"Average Response Time by {title}", 
                                                           str(chart_path), y_label="Mean Response Time (s)", max_items=15)
                        if img_link:
                            sections.append(img_link)
                            sections.append("")
                            chart_added = True
                    if not chart_added:
                        text_chart = create_text_bar_chart(f"Average Response Time by {title}", chart_data)
                        if text_chart:
                            sections.append(text_chart)
                            sections.append("")
    
    add_pattern("Query Type", "by_query_type")
    add_pattern("Message Category", "by_message_category")
    add_pattern("Language", "by_language")
    add_pattern("District", "by_district")
    add_pattern("Message Type", "by_message_type")

    return "\n".join(sections)


def build_llm_prompt(month_str: str, idk_analysis: dict, response_time_analysis: dict) -> List[Dict[str, str]]:
    """Construct a concise prompt for the LLM executive summary."""
    idk_pct = idk_analysis.get("idk_percentage", 0)
    idk_count = idk_analysis.get("idk_count", 0)
    total_q = idk_analysis.get("total_queries", 0)
    themes_pct = idk_analysis.get("asha_themes_percentage", {}) or {}
    top_themes = sorted(themes_pct.items(), key=lambda x: x[1], reverse=True)[:5]

    rt_stats = response_time_analysis.get("statistics", {}) or {}

    def fmt_top_themes():
        if not top_themes:
            return "None"
        return ", ".join([f"{k} {v:.1f}%" for k, v in top_themes])

    return [
        {
            "role": "system",
            "content": (
                "You are a concise analytics assistant. "
                "Write an executive summary in <=100 words, 3-5 crisp insights. "
                "Focus on IDK rates, top themes, language/geo gaps, and response times. "
                "Be direct and non-repetitive."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Month: {month_str}\n"
                f"Total queries: {total_q}\n"
                f"IDK: {idk_count} ({idk_pct:.1f}%)\n"
                f"Top IDK themes: {fmt_top_themes()}\n"
                f"Response times (s): mean {rt_stats.get('mean', 0)}, median {rt_stats.get('median', 0)}, "
                f"p95 {rt_stats.get('p95', 0)}\n"
                "Provide 3-5 bullet-like sentences (but as a short paragraph) "
                "covering: IDK rate, biggest themes, any notable language/geo gaps (if known), "
                "and response-time headline."
            ),
        },
    ]


def estimate_llm_cost(model: str, prompt_tokens: int, completion_tokens: int) -> float:
    """Estimate cost based on simple per-1k pricing map."""
    pricing = {
        "gpt-4o-mini": {"in": 0.000150, "out": 0.000600},
        "gpt-4o-2024-08-06": {"in": 0.0025, "out": 0.01},
    }
    price = pricing.get(model, pricing["gpt-4o-mini"])
    return (prompt_tokens * price["in"] + completion_tokens * price["out"]) / 1000


def generate_llm_summary(
    month_str: str,
    idk_analysis: dict,
    response_time_analysis: dict,
    model: str,
    temperature: float,
    max_tokens: int
) -> Tuple[Optional[str], Optional[dict]]:
    """Generate LLM summary with token/cost usage."""
    try:
        client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        if not client.api_key:
            print("Warning: OPENAI_API_KEY not set. Skipping LLM summary.")
            return None, None
        messages = build_llm_prompt(month_str, idk_analysis, response_time_analysis)
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        text = response.choices[0].message.content.strip()
        usage = response.usage
        prompt_tokens = usage.prompt_tokens if hasattr(usage, "prompt_tokens") else 0
        completion_tokens = usage.completion_tokens if hasattr(usage, "completion_tokens") else 0
        total_tokens = usage.total_tokens if hasattr(usage, "total_tokens") else prompt_tokens + completion_tokens
        estimated_cost = estimate_llm_cost(model, prompt_tokens, completion_tokens)
        llm_usage = {
            "model": model,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
            "estimated_cost": estimated_cost,
        }
        return text, llm_usage
    except Exception as e:
        print(f"Warning: LLM summary failed: {e}")
        return None, None


def save_to_excel(df: pd.DataFrame, output_path: str) -> None:
    """Save dataframe to Excel file."""
    if df.empty:
        print("Warning: No data to save. Creating empty Excel file.")
    
    output_path_obj = Path(output_path).resolve()
    output_dir = output_path_obj.parent
    
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise OSError(f"Failed to create output directory {output_dir}: {e}")
    
    try:
        df.to_excel(str(output_path_obj), index=False)
        print(f"Saved {len(df)} rows to {output_path_obj}")
    except Exception as e:
        raise RuntimeError(f"Failed to save Excel file to {output_path_obj}: {e}")


async def main() -> None:
    """Main function to orchestrate the monthly logs compilation."""
    args = parse_args()
    
    if args.month and not args.year:
        print("Error: --month requires --year to be specified.")
        sys.exit(1)
    if args.year and not args.month:
        print("Error: --year requires --month to be specified.")
        sys.exit(1)
    
    try:
        start, end = month_range(args)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    print(f"\n{'='*SEPARATOR_LENGTH}")
    print(f"Monthly Logs Compilation")
    print(f"{'='*SEPARATOR_LENGTH}")
    print(f"Date Range: {start.strftime(DATE_FORMAT)} to {end.strftime(DATE_FORMAT)} (IST)")
    print(f"{'='*SEPARATOR_LENGTH}\n")
    
    # Init Mongo user repo for onboarding counts
    mongo_factory = MongoDBFactory(config=app_config, scope=Scope.SINGLETON.value)
    user_repo = await get_user_repository(mongo_factory)
    
    try:
        df = await fetch_monthly_logs(start, end)
    except Exception as e:
        print(f"Error fetching logs: {e}")
        sys.exit(1)
    
    if df.empty:
        print("No data to process. Exiting.")
        sys.exit(0)
    
    df = filter_test_users(df)
    df = filter_by_log_date(df, start, end)
    
    if df.empty:
        print("No data remaining after filtering. Exiting.")
        sys.exit(0)
    
    # Compute user stats for current month (active via logs, onboarded via DB)
    current_user_stats = await compute_user_stats(df, start, end, user_repo)
    
    # Perform IDK Analysis
    print(f"\n{'='*SEPARATOR_LENGTH}")
    print("IDK Analysis")
    print(f"{'='*SEPARATOR_LENGTH}")
    
    idk_analysis = analyze_idk(df)
    
    print(f"Total queries: {idk_analysis['total_queries']}")
    print(f"IDK queries: {idk_analysis['idk_count']} ({idk_analysis['idk_percentage']}%)")
    
    if idk_analysis['idk_count'] > 0:
        print(f"  - Text IDK: {idk_analysis.get('text_idk_count', 0)} ({idk_analysis.get('text_idk_percentage', 0):.1f}%)")
        print(f"  - Audio IDK: {idk_analysis.get('audio_idk_count', 0)} ({idk_analysis.get('audio_idk_percentage', 0):.1f}%)")
    
    if idk_analysis['idk_count'] > 0:
        print(f"\nIDK Causes:")
        for cause, count in idk_analysis['causes'].items():
            percentage = idk_analysis['causes_percentage'][cause]
            print(f"  - {cause}: {count} ({percentage:.1f}%)")
        
        if idk_analysis['asha_themes']:
            print(f"\nASHA IDK Themes (Domain Knowledge Gaps):")
            for theme, count in idk_analysis['asha_themes'].items():
                percentage = idk_analysis['asha_themes_percentage'][theme]
                print(f"  - {theme}: {count} ({percentage:.1f}%)")
        
        # Show breakdowns if they exist and show meaningful differences
        breakdowns = idk_analysis.get('breakdowns', {})
        
        if 'by_language' in breakdowns:
            print("\nIDK Patterns")
            print(f"\nIDK Breakdown by Language:")
            for lang_data in breakdowns['by_language']:
                print(f"  - {lang_data['language']}: {lang_data['idk_percentage']:.1f}% "
                      f"({lang_data['idk_count']}/{lang_data['total_queries']} queries)")
        
        if 'by_district' in breakdowns:
            print(f"\nIDK Breakdown by District (top 10):")
            for district_data in breakdowns['by_district'][:10]:
                print(f"  - {district_data['district']}: {district_data['idk_percentage']:.1f}% "
                      f"({district_data['idk_count']}/{district_data['total_queries']} queries)")
        
        if 'by_block' in breakdowns:
            print(f"\nIDK Breakdown by Block (top 10):")
            for block_data in breakdowns['by_block'][:10]:
                print(f"  - {block_data['block']}: {block_data['idk_percentage']:.1f}% "
                      f"({block_data['idk_count']}/{block_data['total_queries']} queries)")
        
        if 'by_sector' in breakdowns:
            print(f"\nIDK Breakdown by Sector (top 10):")
            for sector_data in breakdowns['by_sector'][:10]:
                print(f"  - {sector_data['sector']}: {sector_data['idk_percentage']:.1f}% "
                      f"({sector_data['idk_count']}/{sector_data['total_queries']} queries)")
    
    # Perform Response Time Analysis
    print(f"\n{'='*SEPARATOR_LENGTH}")
    print("Response Time Analysis")
    print(f"{'='*SEPARATOR_LENGTH}")
    
    response_time_analysis = analyze_response_times(df)
    
    print(f"Total queries: {response_time_analysis['total_queries']}")
    print(f"Valid responses (with timestamps): {response_time_analysis['valid_responses']}")
    
    if response_time_analysis['valid_responses'] > 0:
        stats = response_time_analysis['statistics']
        print(f"\nResponse Time Statistics (seconds):")
        print(f"  Mean: {stats['mean']}s")
        print(f"  Median: {stats['median']}s")
        print(f"  P95: {stats['p95']}s")
        print(f"  P90: {stats['p90']}s")
        print(f"  P75: {stats['p75']}s")
        print(f"  Min: {stats['min']}s")
        print(f"  Max: {stats['max']}s")
        
        patterns = response_time_analysis['patterns']
        
        # Show patterns by query_type
        if 'by_query_type' in patterns and patterns['by_query_type']:
            print(f"\nResponse Times by Query Type (slowest first):")
            for query_type, stats_dict in list(patterns['by_query_type'].items())[:10]:  # Top 10
                print(f"  - {query_type}: mean={stats_dict['mean']}s, median={stats_dict['median']}s, "
                      f"p95={stats_dict['p95']}s (count={stats_dict['count']})")
        
        # Show patterns by message_category
        if 'by_message_category' in patterns and patterns['by_message_category']:
            print(f"\nResponse Times by Message Category (slowest first):")
            for category, stats_dict in patterns['by_message_category'].items():
                print(f"  - {category}: mean={stats_dict['mean']}s, median={stats_dict['median']}s, "
                      f"p95={stats_dict['p95']}s (count={stats_dict['count']})")
        
        # Show patterns by language (if meaningful differences)
        if 'by_language' in patterns and patterns['by_language']:
            lang_stats = patterns['by_language']
            if len(lang_stats) > 1:
                # Check if there's meaningful difference (more than 10% variation)
                mean_times = [s['mean'] for s in lang_stats.values()]
                if max(mean_times) / min(mean_times) > 1.1:
                    print(f"\nResponse Times by Language (slowest first):")
                    for language, stats_dict in lang_stats.items():
                        print(f"  - {language}: mean={stats_dict['mean']}s, median={stats_dict['median']}s, "
                              f"p95={stats_dict['p95']}s (count={stats_dict['count']})")
        
        # Show patterns by district (if meaningful differences)
        if 'by_district' in patterns and patterns['by_district']:
            district_stats = patterns['by_district']
            if len(district_stats) > 1:
                mean_times = [s['mean'] for s in district_stats.values()]
                if max(mean_times) / min(mean_times) > 1.1:
                    print(f"\nResponse Times by District (slowest first, top 10):")
                    for district, stats_dict in list(district_stats.items())[:10]:
                        print(f"  - {district}: mean={stats_dict['mean']}s, median={stats_dict['median']}s, "
                              f"p95={stats_dict['p95']}s (count={stats_dict['count']})")
    
    # Generate and display summary insights
    print(f"\n{'='*SEPARATOR_LENGTH}")
    print("Summary Insights")
    print(f"{'='*SEPARATOR_LENGTH}")
    
    month_str = start.strftime("%Y-%m")
    summary_insights = generate_summary_insights(idk_analysis, response_time_analysis, month_str)
    asha_buckets = format_asha_idk_buckets(idk_analysis)

    llm_summary = None
    llm_usage = None
    if args.llm_report:
        llm_summary, llm_usage = generate_llm_summary(
            month_str=month_str,
            idk_analysis=idk_analysis,
            response_time_analysis=response_time_analysis,
            model=args.llm_model,
            temperature=args.llm_temperature,
            max_tokens=args.llm_max_tokens,
        )
        if llm_usage:
            print("\nLLM Summary Token/Cost Estimate:")
            print(f"  Model: {llm_usage['model']}")
            print(f"  Prompt tokens: {llm_usage['prompt_tokens']}")
            print(f"  Completion tokens: {llm_usage['completion_tokens']}")
            print(f"  Total tokens: {llm_usage['total_tokens']}")
            print(f"  Estimated cost: ${llm_usage['estimated_cost']:.4f}")

    print(f"\nExecutive Summary ({month_str}):")
    print(f"{llm_summary or summary_insights}")
    print(f"\nASHA IDK Bucket Distribution:")
    print(asha_buckets)
    
    # Automatically fetch previous month data for MoM comparison
    prev_month_data = None
    prev_user_stats = None
    try:
        prev_month_data = await compute_previous_month_metrics(start, end, user_repo)
        if prev_month_data and "user_stats" in prev_month_data:
            prev_user_stats = prev_month_data["user_stats"]
    except Exception as e:
        print(f"Warning: Failed to compute previous month metrics: {e}")
        print("MoM comparison will be skipped.")
        prev_month_data = None
        prev_user_stats = None
    
    # Fallback: If automatic fetch failed, try parsing from file if provided
    if not prev_month_data and args.prev_month_summary:
        print(f"\nTrying to load previous month data from file: {args.prev_month_summary}")
        prev_month_data = parse_prev_month_summary(args.prev_month_summary)
        if prev_month_data:
            print("Previous month data loaded from file successfully.")
        else:
            print("Warning: Could not parse previous month data from file. MoM comparison will be skipped.")
    if prev_month_data and "user_stats" in prev_month_data and not prev_user_stats:
        prev_user_stats = prev_month_data.get("user_stats")
    
    # Determine output folder structure: analysis/YYYY-MM/
    month_str = start.strftime("%Y-%m")
    
    if args.output_dir:
        # Use custom output directory if specified
        output_dir = Path(args.output_dir)
    elif args.output:
        # If output path is specified, use its parent directory / analysis / month
        base_output_path = Path(args.output)
        output_dir = base_output_path.parent / "analysis" / month_str
    else:
        # Default: create analysis/YYYY-MM/ folder
        output_dir = Path("analysis") / month_str
    
    # Create output directory
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"\nOutput directory: {output_dir.absolute()}")
    except OSError as e:
        print(f"Error: Failed to create output directory {output_dir}: {e}")
        sys.exit(1)
    
    # Define all output file paths within the folder
    if args.output:
        base_name = base_output_path.stem
        output_path = str(output_dir / f"{base_name}.xlsx")
        idk_output_path = str(output_dir / f"{base_name}_idk_queries.xlsx")
        response_time_output_path = str(output_dir / f"{base_name}_response_times.xlsx")
        summary_output_path = str(output_dir / f"{base_name}_summary.md")
    else:
        output_path = str(output_dir / f"monthly_logs_{month_str}.xlsx")
        idk_output_path = str(output_dir / f"monthly_logs_{month_str}_idk_queries.xlsx")
        response_time_output_path = str(output_dir / f"monthly_logs_{month_str}_response_times.xlsx")
        summary_output_path = str(output_dir / f"monthly_logs_{month_str}_summary.md")
    
    # Save Excel files only if --save-excel flag is set
    if args.save_excel:
        # Save main logs
        try:
            save_to_excel(df, output_path)
            print(f"Saved main logs: {output_path}")
        except (OSError, RuntimeError) as e:
            print(f"Warning: Failed to save main logs: {e}")
        
        # Save IDK queries dump
        if not idk_analysis['idk_queries'].empty:
            try:
                save_to_excel(idk_analysis['idk_queries'], idk_output_path)
                print(f"Saved IDK queries: {idk_output_path}")
            except (OSError, RuntimeError) as e:
                print(f"Warning: Failed to save IDK queries dump: {e}")
        
        # Save response time data
        if not response_time_analysis['response_time_data'].empty:
            try:
                save_to_excel(response_time_analysis['response_time_data'], response_time_output_path)
                print(f"Saved response times: {response_time_output_path}")
            except (OSError, RuntimeError) as e:
                print(f"Warning: Failed to save response time data: {e}")
    else:
        print("\nNote: Excel files not saved (use --save-excel to save them).")

    # Save markdown summary (executive + detailed tables)
    try:
        # Charts will be saved in the same output directory
        summary_md = build_markdown_report(
            month_str,
            summary_insights,
            idk_analysis,
            response_time_analysis,
            df,
            llm_usage=llm_usage,
            llm_summary=llm_summary,
            prev_month_data=prev_month_data,
            user_stats=current_user_stats,
            prev_user_stats=prev_user_stats,
            output_dir=output_dir
        )
        Path(summary_output_path).write_text(summary_md, encoding="utf-8")
        print(f"\n✅ Generated markdown report: {summary_output_path}")
        print(f"✅ All outputs saved to: {output_dir.absolute()}")
    except Exception as e:
        print(f"Warning: Failed to save summary markdown: {e}")
    
    # Count files in output directory
    output_files = list(output_dir.glob("*"))
    file_count = len(output_files)
    chart_files = list(output_dir.glob("*.png")) if HAS_MATPLOTLIB else []
    excel_files = list(output_dir.glob("*.xlsx")) if args.save_excel else []
    
    print(f"\n{'='*SEPARATOR_LENGTH}")
    print(f"Compilation complete!")
    print(f"{'='*SEPARATOR_LENGTH}")
    print(f"\n📁 Output Directory: {output_dir.absolute()}")
    print(f"📄 Files created: {file_count}")
    print(f"   - Markdown report: {Path(summary_output_path).name}")
    if HAS_MATPLOTLIB:
        print(f"   - Chart images: {len(chart_files)} PNG files")
    if args.save_excel:
        print(f"   - Excel files: {len(excel_files)} files")
    print(f"\n📤 Upload to Azure Storage:")
    print(f"   Upload ONLY this folder: {output_dir.name}/")
    print(f"   Full path: {output_dir.absolute()}")
    print(f"   Target in Azure: /analysis/{month_str}/")
    print(f"\n⚠️  Note: Upload only the '{output_dir.name}' folder, not the entire 'analysis' folder")
    print(f"   This ensures only the current month's data is uploaded.")
    print(f"\nTotal rows processed: {len(df)}")
    print(f"{'='*SEPARATOR_LENGTH}")

    # Optional: upload to Azure Blob Storage
    if args.upload_azure:
        azure_prefix = args.azure_prefix or f"analysis/{month_str}/"
        print(f"\nStarting Azure upload with prefix: {azure_prefix}")
        await upload_folder_to_azure(output_dir, month_str, prefix=azure_prefix)
        print("Azure upload complete.")


if __name__ == "__main__":
    try:
        load_dotenv(locate_keys(), override=True)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user. Exiting.")
        sys.exit(130)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

