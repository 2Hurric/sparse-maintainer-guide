#!/usr/bin/env python3
"""
Generate comprehensive monthly reports from sparse mailing list emails.
Analyzes emails from 2021-2026 and creates structured summaries.
"""

from __future__ import annotations

import os
import re
import json
import email
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Any, Optional
import textwrap


class MonthlyReportGenerator:
    def __init__(self, emails_dir: str, start_year: int = 2021, end_year: int = 2026):
        self.emails_dir = Path(emails_dir)
        self.start_year = start_year
        self.end_year = end_year
        self.monthly_emails = defaultdict(list)

    def parse_mbox_file(self, filepath: Path) -> Optional[Dict[str, Any]]:
        """Parse a single mbox file and extract metadata."""
        try:
            content = filepath.read_text(encoding='utf-8', errors='replace')
            msg = email.message_from_string(content)

            from_addr = msg.get('From', '')
            subject = msg.get('Subject', '')
            date_str = msg.get('Date', '')

            # Extract author name/email
            author_match = re.search(r'"?([^"<]+)"?\s*<([^>]+)>', from_addr)
            if author_match:
                author_name = author_match.group(1).strip()
                author_email = author_match.group(2).strip()
            else:
                author_name = from_addr.split('@')[0] if '@' in from_addr else from_addr
                author_email = from_addr

            # Clean up author name
            author_name = re.sub(r'\s*\([^)]*\)\s*', '', author_name).strip()
            if not author_name:
                author_name = author_email.split('@')[0]

            # Get body
            body = ''
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == 'text/plain':
                        body = part.get_payload(decode=True)
                        if isinstance(body, bytes):
                            body = body.decode('utf-8', errors='replace')
                        break
            else:
                body = msg.get_payload(decode=True)
                if isinstance(body, bytes):
                    body = body.decode('utf-8', errors='replace')
                elif body is None:
                    body = msg.get_payload()

            return {
                'filepath': str(filepath),
                'from': from_addr,
                'author_name': author_name,
                'author_email': author_email,
                'subject': subject,
                'date_str': date_str,
                'body': (body or '')[:3000],  # Limit body size
                'year': filepath.parent.parent.name,
                'month': filepath.parent.name
            }
        except Exception as e:
            print(f"Error parsing {filepath}: {e}")
            return None

    def load_emails(self):
        """Load all emails from the specified year range."""
        print(f"Loading emails from {self.start_year}-{self.end_year}...")

        for year in range(self.start_year, self.end_year + 1):
            year_dir = self.emails_dir / str(year)
            if not year_dir.exists():
                continue

            for month_dir in sorted(year_dir.iterdir()):
                if not month_dir.is_dir():
                    continue

                month_key = f"{year}-{month_dir.name}"
                mbox_files = list(month_dir.glob('*.mbox'))

                for filepath in mbox_files:
                    email_data = self.parse_mbox_file(filepath)
                    if email_data:
                        self.monthly_emails[month_key].append(email_data)

                print(f"  {month_key}: {len(self.monthly_emails[month_key])} emails")

    def categorize_email(self, email_data: Dict) -> List[str]:
        """Categorize an email based on subject and content."""
        categories = []
        subject = email_data['subject'].lower()
        body = email_data['body'].lower()

        # Patch related
        if re.search(r'\[patch', subject):
            if 'rfc' in subject:
                categories.append('RFC Patch')
            elif re.search(r'v\d+', subject):
                categories.append('Patch Revision')
            else:
                categories.append('New Patch')

        # Bug/Fix related
        if any(kw in subject for kw in ['fix', 'bug', 'crash', 'error', 'issue']):
            categories.append('Bug Fix')

        # Feature/Enhancement
        if any(kw in subject for kw in ['add', 'support', 'implement', 'new', 'introduce']):
            categories.append('Feature')

        # Review/Discussion
        if subject.startswith('re:'):
            categories.append('Discussion')

        # Warnings
        if 'warning' in subject or 'sparse:' in subject:
            categories.append('Warning Report')

        # Specific topics
        if 'clang' in subject or 'llvm' in subject:
            categories.append('Clang/LLVM')
        if 'gcc' in subject:
            categories.append('GCC')
        if 'kernel' in subject or 'kbuild' in subject:
            categories.append('Kernel Integration')
        if 'lock' in subject or 'mutex' in subject or 'spinlock' in subject:
            categories.append('Locking')
        if 'context' in subject:
            categories.append('Context Analysis')
        if 'type' in subject and ('check' in subject or 'cast' in subject):
            categories.append('Type Checking')
        if 'endian' in subject or '__le' in subject or '__be' in subject:
            categories.append('Endianness')
        if 'address' in subject and 'space' in subject:
            categories.append('Address Space')

        return categories if categories else ['General']

    def extract_key_topics(self, emails: List[Dict]) -> List[Dict]:
        """Extract key discussion topics from a list of emails."""
        # Group by thread (normalized subject)
        threads = defaultdict(list)
        for email_data in emails:
            # Normalize subject for thread grouping
            subject = email_data['subject']
            normalized = re.sub(r'^(Re:\s*)+', '', subject, flags=re.IGNORECASE)
            normalized = re.sub(r'\[PATCH[^\]]*\]\s*', '', normalized)
            normalized = normalized.strip()[:80]
            threads[normalized].append(email_data)

        # Sort threads by activity
        sorted_threads = sorted(threads.items(), key=lambda x: len(x[1]), reverse=True)

        topics = []
        for subject, thread_emails in sorted_threads[:10]:
            if len(thread_emails) >= 2:
                participants = list(set(e['author_name'] for e in thread_emails))
                categories = []
                for e in thread_emails:
                    categories.extend(self.categorize_email(e))

                topics.append({
                    'subject': subject,
                    'message_count': len(thread_emails),
                    'participants': participants[:5],
                    'categories': list(set(categories))
                })

        return topics

    def summarize_month(self, month_key: str, emails: List[Dict]) -> Dict:
        """Generate a summary for a single month."""
        if not emails:
            return None

        # Basic stats
        authors = defaultdict(int)
        categories = defaultdict(int)

        for email_data in emails:
            authors[email_data['author_name']] += 1
            for cat in self.categorize_email(email_data):
                categories[cat] += 1

        # Top contributors this month
        top_authors = sorted(authors.items(), key=lambda x: -x[1])[:5]

        # Key topics
        key_topics = self.extract_key_topics(emails)

        # Extract notable discussions
        notable = []
        for email_data in emails:
            subject = email_data['subject'].lower()
            # Look for significant discussions
            if any(kw in subject for kw in ['proposal', 'rfc', 'announce', 'release', 'breaking']):
                notable.append({
                    'subject': email_data['subject'],
                    'author': email_data['author_name']
                })

        return {
            'month': month_key,
            'total_emails': len(emails),
            'unique_contributors': len(authors),
            'top_contributors': [{'name': n, 'count': c} for n, c in top_authors],
            'category_breakdown': dict(categories),
            'key_topics': key_topics,
            'notable_discussions': notable[:5]
        }

    def generate_reports(self) -> Dict[str, List[Dict]]:
        """Generate all monthly reports."""
        self.load_emails()

        reports = {}
        for year in range(self.start_year, self.end_year + 1):
            year_reports = []
            for month in range(1, 13):
                month_key = f"{year}-{month:02d}"
                if month_key in self.monthly_emails:
                    summary = self.summarize_month(month_key, self.monthly_emails[month_key])
                    if summary:
                        year_reports.append(summary)

            if year_reports:
                reports[str(year)] = year_reports

        return reports

    def generate_yearly_summary(self, year: int, monthly_reports: List[Dict]) -> Dict:
        """Generate a yearly summary from monthly reports."""
        total_emails = sum(m['total_emails'] for m in monthly_reports)
        all_contributors = set()
        all_categories = defaultdict(int)
        all_topics = []

        for report in monthly_reports:
            for contrib in report['top_contributors']:
                all_contributors.add(contrib['name'])
            for cat, count in report['category_breakdown'].items():
                all_categories[cat] += count
            all_topics.extend(report['key_topics'])

        # Deduplicate and sort topics
        topic_map = {}
        for topic in all_topics:
            key = topic['subject']
            if key in topic_map:
                topic_map[key]['message_count'] += topic['message_count']
            else:
                topic_map[key] = topic.copy()

        top_topics = sorted(topic_map.values(), key=lambda x: -x['message_count'])[:10]

        return {
            'year': year,
            'total_emails': total_emails,
            'total_contributors': len(all_contributors),
            'months_active': len(monthly_reports),
            'category_breakdown': dict(all_categories),
            'top_topics': top_topics
        }


def main():
    generator = MonthlyReportGenerator('emails', start_year=2021, end_year=2026)
    reports = generator.generate_reports()

    # Add yearly summaries
    output = {
        'generated_at': datetime.now().isoformat(),
        'years': {}
    }

    for year, monthly_reports in reports.items():
        yearly_summary = generator.generate_yearly_summary(int(year), monthly_reports)
        output['years'][year] = {
            'summary': yearly_summary,
            'months': monthly_reports
        }

    # Save to JSON
    output_path = Path('monthly_reports.json')
    output_path.write_text(json.dumps(output, indent=2, default=str))
    print(f"\nReports saved to {output_path}")

    # Print summary
    print("\n=== Report Summary ===")
    for year, data in output['years'].items():
        summary = data['summary']
        print(f"\n{year}:")
        print(f"  Total emails: {summary['total_emails']}")
        print(f"  Contributors: {summary['total_contributors']}")
        print(f"  Active months: {summary['months_active']}")


if __name__ == '__main__':
    main()
