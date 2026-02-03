#!/usr/bin/env python3
"""
Analyze downloaded linux-sparse mailing list emails to extract key information
for creating a maintainer summary document.
"""

from __future__ import annotations

import os
import re
import json
import email
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime
from typing import Dict, List, Any, Optional


class EmailAnalyzer:
    def __init__(self, emails_dir: str):
        self.emails_dir = Path(emails_dir)
        self.emails = []
        self.authors = Counter()
        self.subjects = []
        self.threads = defaultdict(list)
        self.yearly_stats = defaultdict(lambda: {'count': 0, 'authors': set()})
        self.topics = defaultdict(int)

    def parse_mbox_file(self, filepath: Path) -> Optional[Dict[str, Any]]:
        """Parse a single mbox file and extract metadata."""
        try:
            content = filepath.read_text(encoding='utf-8', errors='replace')
            msg = email.message_from_string(content)

            from_addr = msg.get('From', '')
            subject = msg.get('Subject', '')
            date_str = msg.get('Date', '')
            message_id = msg.get('Message-Id', '')

            # Extract author name/email
            author_match = re.search(r'"?([^"<]+)"?\s*<([^>]+)>', from_addr)
            if author_match:
                author_name = author_match.group(1).strip()
                author_email = author_match.group(2).strip()
            else:
                author_name = from_addr
                author_email = from_addr

            # Parse date
            date = None
            if date_str:
                try:
                    # Try common formats
                    for fmt in ['%a, %d %b %Y %H:%M:%S %z', '%d %b %Y %H:%M:%S %z',
                                '%a, %d %b %Y %H:%M:%S']:
                        try:
                            date = datetime.strptime(date_str.split('(')[0].strip(), fmt)
                            break
                        except ValueError:
                            continue
                except Exception:
                    pass

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
                'date': date,
                'date_str': date_str,
                'message_id': message_id,
                'body': body or '',
                'year': filepath.parent.parent.name,
                'month': filepath.parent.name
            }
        except Exception as e:
            print(f"Error parsing {filepath}: {e}")
            return None

    def load_all_emails(self):
        """Load all emails from the directory structure."""
        print("Loading emails...")
        mbox_files = list(self.emails_dir.glob('**/*.mbox'))
        total = len(mbox_files)

        for i, filepath in enumerate(mbox_files):
            if i % 500 == 0:
                print(f"  Processing {i}/{total}...")

            email_data = self.parse_mbox_file(filepath)
            if email_data:
                self.emails.append(email_data)

        print(f"Loaded {len(self.emails)} emails")

    def analyze(self):
        """Perform analysis on loaded emails."""
        print("Analyzing emails...")

        for email_data in self.emails:
            # Count authors
            author_key = email_data['author_email'].lower()
            self.authors[author_key] += 1

            # Track subjects
            self.subjects.append(email_data['subject'])

            # Yearly stats
            year = email_data['year']
            self.yearly_stats[year]['count'] += 1
            self.yearly_stats[year]['authors'].add(author_key)

            # Categorize topics based on subject
            subject = email_data['subject'].lower()
            self._categorize_topic(subject)

    def _categorize_topic(self, subject: str):
        """Categorize email topic based on subject line."""
        if 'patch' in subject:
            self.topics['patches'] += 1
        if 'bug' in subject or 'fix' in subject:
            self.topics['bug_fixes'] += 1
        if 'rfc' in subject:
            self.topics['rfcs'] += 1
        if 'question' in subject or '?' in subject:
            self.topics['questions'] += 1
        if 'announce' in subject or 'release' in subject:
            self.topics['announcements'] += 1
        if 'warning' in subject or 'error' in subject:
            self.topics['warnings_errors'] += 1
        if 'sparse' in subject:
            self.topics['sparse_core'] += 1
        if 'check' in subject:
            self.topics['checking'] += 1

    def get_top_contributors(self, n: int = 30) -> List[tuple]:
        """Get top N contributors by email count."""
        return self.authors.most_common(n)

    def get_key_threads(self) -> List[Dict]:
        """Identify key/important threads based on various signals."""
        # Group by subject (removing Re: prefix)
        thread_groups = defaultdict(list)
        for email_data in self.emails:
            subject = re.sub(r'^(Re:\s*)+', '', email_data['subject'], flags=re.IGNORECASE)
            subject = re.sub(r'\[PATCH[^\]]*\]\s*', '', subject)  # Remove [PATCH x/y]
            subject = subject.strip()[:80]  # Truncate for grouping
            thread_groups[subject].append(email_data)

        # Sort by thread size
        sorted_threads = sorted(thread_groups.items(), key=lambda x: len(x[1]), reverse=True)

        key_threads = []
        for subject, emails in sorted_threads[:50]:
            if len(emails) >= 3:  # Only threads with 3+ messages
                dates = [e['date'] for e in emails if e['date']]
                key_threads.append({
                    'subject': subject,
                    'message_count': len(emails),
                    'participants': len(set(e['author_email'].lower() for e in emails)),
                    'first_date': min(dates).isoformat() if dates else None,
                    'last_date': max(dates).isoformat() if dates else None
                })

        return key_threads

    def extract_key_discussions(self) -> Dict[str, List[Dict]]:
        """Extract key technical discussions by category."""
        categories = {
            'sparse_features': [],
            'kernel_integration': [],
            'bug_reports': [],
            'performance': [],
            'type_checking': [],
            'warnings': [],
            'llvm_clang': [],
            'gcc': []
        }

        for email_data in self.emails:
            subject = email_data['subject'].lower()
            body = (email_data['body'] or '').lower()[:2000]

            if any(kw in subject or kw in body for kw in ['new feature', 'add support', 'implement']):
                categories['sparse_features'].append(email_data)
            if 'kernel' in subject or 'linux' in subject:
                categories['kernel_integration'].append(email_data)
            if 'bug' in subject or 'crash' in subject or 'segfault' in subject:
                categories['bug_reports'].append(email_data)
            if 'performance' in subject or 'slow' in subject or 'fast' in subject:
                categories['performance'].append(email_data)
            if 'type' in subject and ('check' in subject or 'error' in subject):
                categories['type_checking'].append(email_data)
            if 'warning' in subject:
                categories['warnings'].append(email_data)
            if 'llvm' in subject or 'clang' in subject:
                categories['llvm_clang'].append(email_data)
            if 'gcc' in subject:
                categories['gcc'].append(email_data)

        return categories

    def generate_summary(self) -> Dict[str, Any]:
        """Generate comprehensive summary data."""
        return {
            'total_emails': len(self.emails),
            'date_range': {
                'start': min((e['year'], e['month']) for e in self.emails if e['year']),
                'end': max((e['year'], e['month']) for e in self.emails if e['year'])
            },
            'total_contributors': len(self.authors),
            'top_contributors': [
                {'email': email, 'count': count}
                for email, count in self.get_top_contributors(30)
            ],
            'yearly_stats': {
                year: {'count': data['count'], 'unique_authors': len(data['authors'])}
                for year, data in sorted(self.yearly_stats.items())
            },
            'topic_distribution': dict(self.topics),
            'key_threads': self.get_key_threads()
        }

    def save_summary(self, output_path: str):
        """Save analysis summary to JSON."""
        summary = self.generate_summary()
        Path(output_path).write_text(json.dumps(summary, indent=2, default=str))
        print(f"Summary saved to {output_path}")
        return summary


def main():
    analyzer = EmailAnalyzer('emails')
    analyzer.load_all_emails()
    analyzer.analyze()
    summary = analyzer.save_summary('analysis_summary.json')

    print("\n=== Quick Summary ===")
    print(f"Total emails: {summary['total_emails']}")
    print(f"Total contributors: {summary['total_contributors']}")
    print(f"Date range: {summary['date_range']}")
    print(f"\nTop 10 contributors:")
    for i, contrib in enumerate(summary['top_contributors'][:10], 1):
        print(f"  {i}. {contrib['email']}: {contrib['count']} emails")

    print(f"\nTopic distribution:")
    for topic, count in sorted(summary['topic_distribution'].items(), key=lambda x: -x[1]):
        print(f"  {topic}: {count}")


if __name__ == '__main__':
    main()
