#!/usr/bin/env python3
"""
Build topic-organized data from sparse mailing list emails.
Groups emails into threads, filters spam, and extracts key sparse-related discussions.
"""

from __future__ import annotations

import os
import re
import json
import email
import hashlib
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple


# Keywords that indicate spam/unrelated content
SPAM_INDICATORS = [
    'loan', 'business loan', 'funding', 'lottery', 'winner', 'inheritance',
    'million dollar', 'bank transfer', 'western union', 'urgent reply',
    'dear friend', 'beneficiary', 'next of kin', 'atm card', 'covid fund',
    'charity', 'donation', 'wohltätigkeitsfonds', 'spende', 'flota',
    'servicio', 'mailbox full', 'account suspended', 'verify your',
    'click here', 'act now', 'limited time', 'hello okay', 'your mailbox'
]

# Keywords that indicate sparse-related content
SPARSE_KEYWORDS = [
    'sparse', 'checker', '__user', '__kernel', '__iomem', '__force',
    '__acquires', '__releases', '__must_hold', 'address space',
    'context', 'endian', '__le', '__be', 'bitwise', 'noderef',
    'type checking', 'static analysis', 'warning', 'annotation',
    'lock', 'mutex', 'spinlock', 'rcu', 'srcu', 'linearize',
    'symbol', 'parse', 'evaluate', 'expression', 'declaration',
    'gcc', 'clang', 'llvm', 'compiler', 'attribute', 'kbuild',
    'kernel', 'patch', 'fix', 'bug', 'error', 'overflow', 'cast',
    'pointer', 'dereference', 'null', 'undefined', 'constexpr'
]

# Major topic categories for sparse
TOPIC_CATEGORIES = {
    'context_analysis': {
        'keywords': ['context analysis', 'capability analysis', 'thread safety',
                     '__acquires', '__releases', '__must_hold', 'lockdep'],
        'title': 'Context/Lock Analysis',
        'description': 'Static verification of locking correctness'
    },
    'address_space': {
        'keywords': ['address space', '__user', '__kernel', '__iomem', 'noderef',
                     'user pointer', 'kernel pointer'],
        'title': 'Address Space Checking',
        'description': 'Detecting user/kernel pointer confusion'
    },
    'type_system': {
        'keywords': ['type check', 'bitwise', '__le16', '__be32', 'endian',
                     'cast', 'typeof', 'overflow', 'signedness'],
        'title': 'Type System & Checking',
        'description': 'Type safety, endianness, and overflow detection'
    },
    'compiler_compat': {
        'keywords': ['gcc', 'clang', 'llvm', 'attribute', '__attribute__',
                     'builtin', 'compiler.h', 'asm'],
        'title': 'Compiler Compatibility',
        'description': 'GCC/Clang compatibility and extensions'
    },
    'kernel_integration': {
        'keywords': ['kbuild', 'make c=', 'kernel build', 'linux kernel',
                     'sparse warning', 'sparse error'],
        'title': 'Kernel Integration',
        'description': 'Using sparse with the Linux kernel'
    },
    'sparse_internals': {
        'keywords': ['linearize', 'parse.c', 'evaluate', 'symbol table',
                     'expression tree', 'ssa', 'basic block', 'sparse internal'],
        'title': 'Sparse Internals',
        'description': 'Parser, linearizer, and analysis infrastructure'
    },
    'rfc_proposals': {
        'keywords': ['rfc', 'proposal', 'introduce', 'new feature', 'add support'],
        'title': 'RFCs & Proposals',
        'description': 'New features and design proposals'
    }
}


class EmailThread:
    def __init__(self, subject: str):
        self.subject = subject
        self.normalized_subject = self._normalize_subject(subject)
        self.messages: List[Dict] = []
        self.participants: set = set()
        self.categories: set = set()

    def _normalize_subject(self, subject: str) -> str:
        """Normalize subject for thread grouping."""
        s = re.sub(r'^(Re:\s*)+', '', subject, flags=re.IGNORECASE)
        s = re.sub(r'\[PATCH[^\]]*\]\s*', '', s)
        s = re.sub(r'\[RFC[^\]]*\]\s*', '', s)
        s = re.sub(r'\[GIT[^\]]*\]\s*', '', s)
        s = re.sub(r'\s+', ' ', s)
        return s.strip()[:100]

    def add_message(self, msg: Dict):
        self.messages.append(msg)
        self.participants.add(msg['author_name'])

    def get_first_date(self) -> Optional[datetime]:
        dates = [m['date'] for m in self.messages if m.get('date')]
        return min(dates) if dates else None

    def get_last_date(self) -> Optional[datetime]:
        dates = [m['date'] for m in self.messages if m.get('date')]
        return max(dates) if dates else None

    def to_dict(self) -> Dict:
        first_date = self.get_first_date()
        last_date = self.get_last_date()

        return {
            'subject': self.subject,
            'normalized_subject': self.normalized_subject,
            'message_count': len(self.messages),
            'participants': list(self.participants),
            'categories': list(self.categories),
            'first_date': first_date.isoformat() if first_date else None,
            'last_date': last_date.isoformat() if last_date else None,
            'year': first_date.year if first_date else None,
            'messages': sorted(self.messages, key=lambda m: m.get('date') or datetime.min)
        }


class TopicBuilder:
    def __init__(self, emails_dir: str):
        self.emails_dir = Path(emails_dir)
        self.threads: Dict[str, EmailThread] = {}
        self.topics: Dict[str, List[EmailThread]] = defaultdict(list)

    def parse_mbox_file(self, filepath: Path) -> Optional[Dict]:
        """Parse a single mbox file."""
        try:
            content = filepath.read_text(encoding='utf-8', errors='replace')
            msg = email.message_from_string(content)

            from_addr = msg.get('From', '')
            subject = msg.get('Subject', '')
            date_str = msg.get('Date', '')
            message_id = msg.get('Message-Id', '')

            # Skip if no subject
            if not subject or subject.strip() == '':
                return None

            # Extract author
            author_match = re.search(r'"?([^"<]+)"?\s*<([^>]+)>', from_addr)
            if author_match:
                author_name = author_match.group(1).strip()
                author_email = author_match.group(2).strip()
            else:
                author_name = from_addr.split('@')[0] if '@' in from_addr else from_addr
                author_email = from_addr

            # Clean author name
            author_name = re.sub(r'\s*\([^)]*\)\s*', '', author_name).strip()
            author_name = re.sub(r'=\?[^?]+\?[^?]+\?([^?]+)\?=', r'\1', author_name)
            if not author_name or author_name.startswith('<'):
                author_name = author_email.split('@')[0] if '@' in author_email else 'Unknown'

            # Parse date
            parsed_date = None
            if date_str:
                date_str_clean = re.sub(r'\s*\([^)]*\)\s*$', '', date_str).strip()
                for fmt in ['%a, %d %b %Y %H:%M:%S %z', '%d %b %Y %H:%M:%S %z',
                            '%a, %d %b %Y %H:%M:%S', '%d %b %Y %H:%M:%S']:
                    try:
                        parsed_date = datetime.strptime(date_str_clean, fmt)
                        break
                    except ValueError:
                        continue

            # Get body
            body = ''
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == 'text/plain':
                        payload = part.get_payload(decode=True)
                        if isinstance(payload, bytes):
                            body = payload.decode('utf-8', errors='replace')
                        break
            else:
                payload = msg.get_payload(decode=True)
                if isinstance(payload, bytes):
                    body = payload.decode('utf-8', errors='replace')
                elif payload:
                    body = str(payload)

            return {
                'filepath': str(filepath),
                'message_id': message_id,
                'author_name': author_name,
                'author_email': author_email,
                'subject': subject,
                'date': parsed_date,
                'date_str': date_str,
                'body': body[:5000] if body else '',
                'year': filepath.parent.parent.name,
                'month': filepath.parent.name
            }
        except Exception as e:
            return None

    def is_spam(self, msg: Dict) -> bool:
        """Check if message is spam."""
        subject = msg['subject'].lower()
        body = msg['body'].lower()[:1000]
        author = msg['author_name'].lower()

        for indicator in SPAM_INDICATORS:
            if indicator in subject or indicator in body:
                return True

        # Check for non-Latin subjects that are likely spam
        if re.search(r'=\?utf-8\?[bq]\?', msg['subject'].lower()):
            # Encoded subject - check if it decodes to something meaningful
            if not any(kw in subject for kw in SPARSE_KEYWORDS):
                return True

        return False

    def is_sparse_related(self, msg: Dict) -> bool:
        """Check if message is related to sparse."""
        subject = msg['subject'].lower()
        body = msg['body'].lower()[:2000]

        # Check for sparse keywords
        for kw in SPARSE_KEYWORDS:
            if kw in subject or kw in body:
                return True

        # Messages from known sparse contributors are likely relevant
        known_contributors = ['torvalds', 'luc van oostenryck', 'chris li',
                              'josh triplett', 'dan carpenter', 'sparse']
        author = msg['author_name'].lower()
        for contrib in known_contributors:
            if contrib in author:
                return True

        return False

    def categorize_message(self, msg: Dict) -> List[str]:
        """Categorize a message into topic areas."""
        categories = []
        subject = msg['subject'].lower()
        body = msg['body'].lower()[:2000]
        text = subject + ' ' + body

        for cat_id, cat_info in TOPIC_CATEGORIES.items():
            for kw in cat_info['keywords']:
                if kw in text:
                    categories.append(cat_id)
                    break

        return categories if categories else ['general']

    def load_and_process_emails(self, start_year: int = 2003, end_year: int = 2026):
        """Load all emails and organize into threads."""
        print(f"Loading emails from {start_year}-{end_year}...")

        total_loaded = 0
        spam_filtered = 0
        unrelated_filtered = 0

        for year in range(start_year, end_year + 1):
            year_dir = self.emails_dir / str(year)
            if not year_dir.exists():
                continue

            for month_dir in sorted(year_dir.iterdir()):
                if not month_dir.is_dir():
                    continue

                for filepath in month_dir.glob('*.mbox'):
                    msg = self.parse_mbox_file(filepath)
                    if not msg:
                        continue

                    total_loaded += 1

                    # Filter spam
                    if self.is_spam(msg):
                        spam_filtered += 1
                        continue

                    # Filter unrelated
                    if not self.is_sparse_related(msg):
                        unrelated_filtered += 1
                        continue

                    # Categorize
                    categories = self.categorize_message(msg)

                    # Add to thread
                    normalized = EmailThread(msg['subject'])._normalize_subject(msg['subject'])
                    thread_key = hashlib.md5(normalized.lower().encode()).hexdigest()[:16]

                    if thread_key not in self.threads:
                        self.threads[thread_key] = EmailThread(msg['subject'])

                    self.threads[thread_key].add_message(msg)
                    self.threads[thread_key].categories.update(categories)

            print(f"  {year}: processed")

        print(f"\nTotal loaded: {total_loaded}")
        print(f"Spam filtered: {spam_filtered}")
        print(f"Unrelated filtered: {unrelated_filtered}")
        print(f"Relevant threads: {len(self.threads)}")

    def organize_by_topic(self):
        """Organize threads by topic category."""
        for thread_key, thread in self.threads.items():
            if len(thread.messages) < 1:
                continue

            for cat in thread.categories:
                self.topics[cat].append(thread)

        # Sort threads within each topic by date
        for cat in self.topics:
            self.topics[cat].sort(
                key=lambda t: t.get_first_date() or datetime.min,
                reverse=True
            )

    def extract_key_decisions(self) -> List[Dict]:
        """Extract key decisions and their rationale from significant threads."""
        decisions = []

        # Find significant threads (many messages, multiple participants)
        significant_threads = []
        for thread in self.threads.values():
            if len(thread.messages) >= 5 and len(thread.participants) >= 2:
                significant_threads.append(thread)

        # Sort by message count
        significant_threads.sort(key=lambda t: len(t.messages), reverse=True)

        for thread in significant_threads[:50]:
            first_msg = min(thread.messages, key=lambda m: m.get('date') or datetime.max)

            # Try to extract decision/outcome from messages
            outcome = None
            rationale = []

            for msg in thread.messages:
                body = msg['body'].lower()

                # Look for decision indicators
                if any(phrase in body for phrase in ['merged', 'applied', 'pushed', 'committed']):
                    outcome = 'merged'
                elif any(phrase in body for phrase in ['nack', 'rejected', 'not taking', "won't work"]):
                    outcome = 'rejected'
                elif 'acked-by' in body or 'reviewed-by' in body:
                    outcome = 'accepted'

                # Extract rationale snippets
                if msg['author_name'].lower() in ['linus torvalds', 'luc van oostenryck']:
                    # Find key statements
                    sentences = re.split(r'[.!?]\s+', msg['body'][:1500])
                    for sent in sentences:
                        if len(sent) > 50 and len(sent) < 300:
                            if any(word in sent.lower() for word in ['because', 'reason', 'problem', 'issue', 'should', 'must', 'need']):
                                rationale.append({
                                    'author': msg['author_name'],
                                    'text': sent.strip()
                                })

            decisions.append({
                'subject': thread.subject,
                'normalized_subject': thread.normalized_subject,
                'message_count': len(thread.messages),
                'participants': list(thread.participants),
                'categories': list(thread.categories),
                'first_date': thread.get_first_date().isoformat() if thread.get_first_date() else None,
                'year': thread.get_first_date().year if thread.get_first_date() else None,
                'outcome': outcome,
                'rationale': rationale[:3],
                'first_message_body': first_msg['body'][:1000]
            })

        return decisions

    def build_roadmap(self) -> Dict:
        """Build a roadmap of sparse development."""
        roadmap = defaultdict(lambda: {'events': [], 'summary': ''})

        decisions = self.extract_key_decisions()

        for decision in decisions:
            if not decision['year']:
                continue

            year = decision['year']
            roadmap[year]['events'].append({
                'subject': decision['normalized_subject'],
                'categories': decision['categories'],
                'outcome': decision['outcome'],
                'participants': decision['participants'][:5],
                'message_count': decision['message_count'],
                'date': decision['first_date']
            })

        # Sort events within each year
        for year in roadmap:
            roadmap[year]['events'].sort(key=lambda e: e['date'] or '')

        return dict(roadmap)

    def generate_output(self) -> Dict:
        """Generate the final structured output."""
        self.organize_by_topic()

        output = {
            'generated_at': datetime.now().isoformat(),
            'stats': {
                'total_threads': len(self.threads),
                'total_messages': sum(len(t.messages) for t in self.threads.values()),
                'categories': {cat: len(threads) for cat, threads in self.topics.items()}
            },
            'category_info': TOPIC_CATEGORIES,
            'topics': {},
            'roadmap': self.build_roadmap(),
            'key_decisions': self.extract_key_decisions()
        }

        # Add threads by topic
        for cat, threads in self.topics.items():
            output['topics'][cat] = [t.to_dict() for t in threads[:30]]

        return output


def main():
    builder = TopicBuilder('emails')
    builder.load_and_process_emails(start_year=2003, end_year=2026)

    output = builder.generate_output()

    # Save output
    output_path = Path('topics_data.json')

    # Custom JSON encoder for datetime
    class DateEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            return super().default(obj)

    output_path.write_text(json.dumps(output, indent=2, cls=DateEncoder, default=str))
    print(f"\nOutput saved to {output_path}")

    # Print summary
    print("\n=== Topic Summary ===")
    for cat, info in TOPIC_CATEGORIES.items():
        count = len(output['topics'].get(cat, []))
        print(f"  {info['title']}: {count} threads")

    print(f"\n=== Key Decisions ===")
    for decision in output['key_decisions'][:10]:
        outcome = decision['outcome'] or 'discussed'
        print(f"  [{outcome}] {decision['normalized_subject'][:60]}...")


if __name__ == '__main__':
    main()
