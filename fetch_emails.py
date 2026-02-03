#!/usr/bin/env python3
"""
Batch fetch email threads from MARC linux-sparse mailing list archive.
https://marc.info/?l=linux-sparse
"""

from __future__ import annotations

import os
import re
import time
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple
from urllib.parse import urljoin, parse_qs, urlparse

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://marc.info/"
LIST_NAME = "linux-sparse"
DEFAULT_OUTPUT_DIR = "emails"
REQUEST_DELAY = 1.0  # seconds between requests to be polite

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MarcFetcher:
    def __init__(self, output_dir: str, delay: float = REQUEST_DELAY):
        self.output_dir = Path(output_dir)
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; email-archiver/1.0; research purposes)'
        })
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.stats = {
            'months_processed': 0,
            'messages_fetched': 0,
            'errors': 0,
            'skipped': 0
        }

    def _request(self, url: str) -> str:
        """Make a request with rate limiting."""
        time.sleep(self.delay)
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            raise

    def get_month_list(self) -> List[Tuple[str, str]]:
        """Get list of all months with messages."""
        url = f"{BASE_URL}?l={LIST_NAME}&r=1&w=2"
        logger.info(f"Fetching month list from {url}")
        html = self._request(url)
        soup = BeautifulSoup(html, 'lxml')

        months = []
        # Find all links that have b= parameter (month links)
        for link in soup.find_all('a', href=True):
            href = link['href']
            if f'l={LIST_NAME}' in href and 'b=' in href:
                # Extract the month parameter
                match = re.search(r'b=(\d{6})', href)
                if match:
                    month_code = match.group(1)
                    # Get the text description
                    text = link.get_text(strip=True)
                    if month_code not in [m[0] for m in months]:
                        months.append((month_code, text))

        # Sort by month code
        months.sort(key=lambda x: x[0])
        logger.info(f"Found {len(months)} months")
        return months

    def get_messages_for_month(self, month_code: str) -> List[str]:
        """Get all message IDs for a given month."""
        url = f"{BASE_URL}?l={LIST_NAME}&b={month_code}&w=2"
        logger.info(f"Fetching messages for month {month_code}")
        html = self._request(url)
        soup = BeautifulSoup(html, 'lxml')

        message_ids = []
        # Find all links with m= parameter (individual messages)
        for link in soup.find_all('a', href=True):
            href = link['href']
            if 'm=' in href:
                match = re.search(r'm=(\d+)', href)
                if match:
                    msg_id = match.group(1)
                    if msg_id not in message_ids:
                        message_ids.append(msg_id)

        # Also check for thread links (t= parameter) and expand them
        for link in soup.find_all('a', href=True):
            href = link['href']
            if 't=' in href:
                match = re.search(r't=(\d+)', href)
                if match:
                    thread_id = match.group(1)
                    # Fetch thread page to get individual message IDs
                    thread_msgs = self.get_thread_messages(thread_id)
                    for msg_id in thread_msgs:
                        if msg_id not in message_ids:
                            message_ids.append(msg_id)

        logger.info(f"Found {len(message_ids)} messages in month {month_code}")
        return message_ids

    def get_thread_messages(self, thread_id: str) -> List[str]:
        """Get all message IDs from a thread."""
        url = f"{BASE_URL}?t={thread_id}&r=1&w=2"
        try:
            html = self._request(url)
            soup = BeautifulSoup(html, 'lxml')

            message_ids = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'm=' in href:
                    match = re.search(r'm=(\d+)', href)
                    if match:
                        msg_id = match.group(1)
                        if msg_id not in message_ids:
                            message_ids.append(msg_id)
            return message_ids
        except Exception as e:
            logger.warning(f"Failed to fetch thread {thread_id}: {e}")
            return []

    def fetch_message_mbox(self, msg_id: str) -> Optional[str]:
        """Fetch a message in mbox format."""
        url = f"{BASE_URL}?l={LIST_NAME}&m={msg_id}&q=mbox"
        try:
            content = self._request(url)
            return content
        except Exception as e:
            logger.error(f"Failed to fetch message {msg_id}: {e}")
            return None

    def save_message(self, msg_id: str, month_code: str, content: str):
        """Save a message to disk."""
        month_dir = self.output_dir / month_code[:4] / month_code[4:]
        month_dir.mkdir(parents=True, exist_ok=True)

        filepath = month_dir / f"{msg_id}.mbox"
        filepath.write_text(content, encoding='utf-8')
        logger.debug(f"Saved message {msg_id} to {filepath}")

    def message_exists(self, msg_id: str, month_code: str) -> bool:
        """Check if message already exists."""
        month_dir = self.output_dir / month_code[:4] / month_code[4:]
        filepath = month_dir / f"{msg_id}.mbox"
        return filepath.exists()

    def fetch_all(self, start_month: str = None, end_month: str = None, resume: bool = True):
        """Fetch all messages from the archive."""
        months = self.get_month_list()

        # Filter by date range if specified
        if start_month:
            months = [(m, t) for m, t in months if m >= start_month]
        if end_month:
            months = [(m, t) for m, t in months if m <= end_month]

        logger.info(f"Processing {len(months)} months")

        for month_code, month_text in months:
            logger.info(f"Processing {month_code} ({month_text})")

            try:
                message_ids = self.get_messages_for_month(month_code)

                for msg_id in message_ids:
                    if resume and self.message_exists(msg_id, month_code):
                        logger.debug(f"Skipping existing message {msg_id}")
                        self.stats['skipped'] += 1
                        continue

                    content = self.fetch_message_mbox(msg_id)
                    if content:
                        self.save_message(msg_id, month_code, content)
                        self.stats['messages_fetched'] += 1
                    else:
                        self.stats['errors'] += 1

                self.stats['months_processed'] += 1

            except Exception as e:
                logger.error(f"Error processing month {month_code}: {e}")
                self.stats['errors'] += 1

        # Save stats
        stats_file = self.output_dir / "fetch_stats.json"
        stats_data = {
            **self.stats,
            'completed_at': datetime.now().isoformat(),
            'start_month': start_month,
            'end_month': end_month
        }
        stats_file.write_text(json.dumps(stats_data, indent=2))

        logger.info(f"Fetch complete: {self.stats}")
        return self.stats


def main():
    parser = argparse.ArgumentParser(
        description='Batch fetch emails from MARC linux-sparse archive'
    )
    parser.add_argument(
        '-o', '--output',
        default=DEFAULT_OUTPUT_DIR,
        help=f'Output directory (default: {DEFAULT_OUTPUT_DIR})'
    )
    parser.add_argument(
        '-d', '--delay',
        type=float,
        default=REQUEST_DELAY,
        help=f'Delay between requests in seconds (default: {REQUEST_DELAY})'
    )
    parser.add_argument(
        '--start',
        help='Start month in YYYYMM format (e.g., 200201)'
    )
    parser.add_argument(
        '--end',
        help='End month in YYYYMM format (e.g., 202602)'
    )
    parser.add_argument(
        '--no-resume',
        action='store_true',
        help='Do not skip existing messages (re-download all)'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    fetcher = MarcFetcher(args.output, args.delay)
    stats = fetcher.fetch_all(
        start_month=args.start,
        end_month=args.end,
        resume=not args.no_resume
    )

    print(f"\nFetch completed!")
    print(f"  Months processed: {stats['months_processed']}")
    print(f"  Messages fetched: {stats['messages_fetched']}")
    print(f"  Skipped (existing): {stats['skipped']}")
    print(f"  Errors: {stats['errors']}")


if __name__ == '__main__':
    main()
