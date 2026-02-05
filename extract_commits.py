#!/usr/bin/env python3
"""Extract commit/patch history from email threads for the past 5 years."""

from __future__ import annotations
import json
import re
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Any

def extract_commits():
    # Load topics data
    with open('topics_data.json', 'r') as f:
        data = json.load(f)

    # Focus on past 5 years (2020-2026)
    target_years = [2020, 2021, 2022, 2023, 2024, 2025, 2026]

    commits_by_year: Dict[int, List[Dict[str, Any]]] = defaultdict(list)

    # Process all threads looking for patches/commits
    for category_id, threads_list in data.get('topics', {}).items():
        for thread in threads_list:
            # Check if thread is a patch/commit
            subject = thread.get('subject', '')
            normalized = thread.get('normalized_subject', '')

            # Look for PATCH indicators
            is_patch = bool(re.search(r'\[PATCH', subject, re.I))
            is_rfc = bool(re.search(r'\[RFC', subject, re.I))
            is_pull = bool(re.search(r'\[PULL|[Pp]ull [Rr]equest', subject, re.I))

            # Skip if not a patch/commit type
            if not (is_patch or is_rfc or is_pull):
                continue

            # Extract year
            year = thread.get('year')
            if not year and thread.get('first_date'):
                match = re.search(r'(20\d{2})', thread['first_date'])
                if match:
                    year = int(match.group(1))

            if not year or year not in target_years:
                continue

            # Extract patch series info
            series_match = re.search(r'\[PATCH[^\]]*?(\d+)/(\d+)', subject)
            patch_num = None
            patch_total = None
            if series_match:
                patch_num = int(series_match.group(1))
                patch_total = int(series_match.group(2))

            # Extract version info
            version_match = re.search(r'\[PATCH\s+v(\d+)', subject, re.I)
            version = int(version_match.group(1)) if version_match else 1

            # Clean subject for display
            clean_subject = re.sub(r'\[(?:RFC|PATCH)[^\]]*\]\s*', '', subject).strip()

            # Determine commit type
            commit_type = 'patch'
            if is_rfc:
                commit_type = 'rfc'
            elif is_pull:
                commit_type = 'pull'

            # Extract first message body for summary
            first_body = ''
            if thread.get('messages'):
                first_body = thread['messages'][0].get('body', '')
                # Extract summary from body (first paragraph or until first blank line)
                lines = first_body.split('\n')
                summary_lines = []
                for line in lines:
                    if line.strip() == '':
                        break
                    if not line.startswith('>'):  # Skip quoted text
                        summary_lines.append(line)
                    if len(summary_lines) > 3:  # Limit to 3 lines
                        break
                first_body = ' '.join(summary_lines).strip()
                # Clean up
                first_body = re.sub(r'\s+', ' ', first_body)
                if len(first_body) > 300:
                    first_body = first_body[:297] + '...'

            # Determine status based on thread outcome
            status = 'discussed'
            outcome = thread.get('outcome', '')
            if outcome == 'merged':
                status = 'merged'
            elif outcome == 'rejected':
                status = 'rejected'
            elif outcome == 'accepted':
                status = 'accepted'
            # Check message content for merge indicators
            elif thread.get('messages'):
                for msg in thread['messages']:
                    body_lower = msg.get('body', '').lower()
                    if any(word in body_lower for word in ['applied', 'merged', 'pulled']):
                        status = 'merged'
                        break
                    elif any(word in body_lower for word in ['nack', 'rejected', 'dropped']):
                        status = 'rejected'
                        break

            # Build commit entry
            commit = {
                'subject': clean_subject,
                'original_subject': subject,
                'type': commit_type,
                'status': status,
                'author': thread['participants'][0] if thread.get('participants') else 'Unknown',
                'date': thread.get('first_date', ''),
                'message_count': thread.get('message_count', 1),
                'participants': thread.get('participants', []),
                'summary': first_body,
                'category': category_id,
                'version': version,
                'patch_num': patch_num,
                'patch_total': patch_total
            }

            commits_by_year[year].append(commit)

    # Sort commits within each year by date
    for year in commits_by_year:
        commits_by_year[year].sort(key=lambda x: x['date'], reverse=True)

    # Generate year summaries
    year_summaries = {}
    for year in sorted(target_years, reverse=True):
        commits = commits_by_year.get(year, [])
        if not commits:
            continue

        # Stats
        total = len(commits)
        merged = len([c for c in commits if c['status'] == 'merged'])
        rejected = len([c for c in commits if c['status'] == 'rejected'])
        rfc = len([c for c in commits if c['type'] == 'rfc'])

        # Top contributors
        author_counts = defaultdict(int)
        for c in commits:
            author_counts[c['author']] += 1
        top_authors = sorted(author_counts.items(), key=lambda x: x[1], reverse=True)[:5]

        # Category distribution
        cat_counts = defaultdict(int)
        for c in commits:
            cat_counts[c['category']] += 1

        # Key patches (most discussed)
        key_patches = sorted(commits, key=lambda x: x['message_count'], reverse=True)[:10]

        year_summaries[year] = {
            'year': year,
            'total_patches': total,
            'merged': merged,
            'rejected': rejected,
            'rfc_count': rfc,
            'acceptance_rate': round((merged / total * 100) if total > 0 else 0, 1),
            'top_contributors': top_authors,
            'categories': dict(cat_counts),
            'key_patches': key_patches,
            'all_patches': commits
        }

    # Create output
    output = {
        'generated_at': datetime.now().isoformat(),
        'years': year_summaries,
        'stats': {
            'total_patches': sum(len(commits_by_year[y]) for y in commits_by_year),
            'years_covered': len(year_summaries),
            'total_merged': sum(s['merged'] for s in year_summaries.values()),
            'total_rejected': sum(s['rejected'] for s in year_summaries.values()),
            'total_rfc': sum(s['rfc_count'] for s in year_summaries.values())
        }
    }

    # Save to file
    with open('commits_history.json', 'w') as f:
        json.dump(output, f, indent=2)

    # Print summary
    print("\n=== Commit History Summary (Past 5 Years) ===\n")
    for year in sorted(year_summaries.keys(), reverse=True):
        s = year_summaries[year]
        print(f"{year}:")
        print(f"  - Total patches: {s['total_patches']}")
        print(f"  - Merged: {s['merged']} ({s['acceptance_rate']}%)")
        print(f"  - Rejected: {s['rejected']}")
        print(f"  - RFCs: {s['rfc_count']}")
        print(f"  - Top contributor: {s['top_contributors'][0][0] if s['top_contributors'] else 'N/A'}")
        print()

    print(f"Total patches analyzed: {output['stats']['total_patches']}")
    print(f"Overall merge rate: {round(output['stats']['total_merged'] / output['stats']['total_patches'] * 100, 1)}%")
    print(f"\nOutput saved to commits_history.json")

if __name__ == '__main__':
    extract_commits()