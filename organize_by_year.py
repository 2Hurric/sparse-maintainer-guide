#!/usr/bin/env python3
"""Organize thread data by year and save to separate JSON files."""

from __future__ import annotations
import json
import os
import re
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Any, Set

def organize_by_year():
    # Load topics data
    with open('topics_data.json', 'r') as f:
        data = json.load(f)

    # Create threads_by_year directory
    output_dir = 'threads_by_year'
    os.makedirs(output_dir, exist_ok=True)

    # Organize threads by year
    yearly_data: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        'threads': [],
        'stats': {
            'total_threads': 0,
            'total_messages': 0,
            'categories': defaultdict(int)
        }
    })

    # Track processed threads to avoid duplicates (by normalized subject)
    processed_threads: Set[str] = set()

    # Process each category's threads
    for category_id, threads_list in data.get('topics', {}).items():
        for thread in threads_list:
            # Skip duplicates
            thread_key = thread.get('normalized_subject', thread.get('subject', ''))
            if thread_key in processed_threads:
                continue
            processed_threads.add(thread_key)

            # Extract year from thread data or first message
            year = None
            if thread.get('year'):
                year = str(thread['year'])
            elif thread.get('first_date'):
                # Parse ISO format date
                match = re.search(r'(20\d{2}|199\d)', thread['first_date'])
                if match:
                    year = match.group(1)

            if not year and thread.get('messages'):
                first_msg = thread['messages'][0]
                timestamp = first_msg.get('date', first_msg.get('timestamp', first_msg.get('date_str', '')))
                if timestamp:
                    match = re.search(r'(20\d{2}|199\d)', str(timestamp))
                    if match:
                        year = match.group(1)

            if not year:
                year = 'unknown'

            # Add thread to year with primary category
            thread_copy = thread.copy()
            thread_copy['primary_category'] = category_id
            yearly_data[year]['threads'].append(thread_copy)
            yearly_data[year]['stats']['total_threads'] += 1
            yearly_data[year]['stats']['total_messages'] += thread.get('message_count', len(thread.get('messages', [])))
            yearly_data[year]['stats']['categories'][category_id] += 1

    # Save each year's data
    summary = {
        'generated_at': datetime.now().isoformat(),
        'years': {}
    }

    for year in sorted(yearly_data.keys()):
        year_info = yearly_data[year]
        # Convert defaultdict to dict for JSON serialization
        year_info['stats']['categories'] = dict(year_info['stats']['categories'])

        # Sort threads by date within year
        year_info['threads'].sort(
            key=lambda t: t.get('first_date', t['messages'][0].get('date', '') if t.get('messages') else ''),
            reverse=True
        )

        # Save year file
        year_file = os.path.join(output_dir, f'{year}.json')
        with open(year_file, 'w') as f:
            json.dump({
                'year': year,
                'stats': year_info['stats'],
                'threads': year_info['threads']
            }, f, indent=2)

        summary['years'][year] = {
            'file': f'{year}.json',
            'threads': year_info['stats']['total_threads'],
            'messages': year_info['stats']['total_messages'],
            'categories': year_info['stats']['categories']
        }

        print(f"Saved {year}: {year_info['stats']['total_threads']} threads, "
              f"{year_info['stats']['total_messages']} messages")

    # Save summary index
    with open(os.path.join(output_dir, 'index.json'), 'w') as f:
        json.dump(summary, f, indent=2)

    print(f"\nTotal: {len(yearly_data)} years saved to {output_dir}/")
    print(f"Summary saved to {output_dir}/index.json")

if __name__ == '__main__':
    organize_by_year()
