#!/usr/bin/env python3
"""Organize git commits by year with detailed descriptions.

For commits without detailed descriptions, analyzes the changes and generates summaries.
"""

from __future__ import annotations
import json
import os
import subprocess
import re
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Any, Optional


REPO_PATH = 'sparse-repo.git'
OUTPUT_DIR = 'commits_by_year'


def run_git(*args: str) -> str:
    """Run a git command and return output."""
    result = subprocess.run(
        ['git', '-C', REPO_PATH] + list(args),
        capture_output=True,
        text=True,
        errors='replace'
    )
    return result.stdout


def get_all_commits() -> List[str]:
    """Get all commit hashes."""
    output = run_git('rev-list', '--all', '--date-order')
    return [line.strip() for line in output.strip().split('\n') if line.strip()]


def get_commit_details(commit_hash: str) -> Dict[str, Any]:
    """Get detailed information about a commit."""
    # Get commit metadata with full message
    # Use committer date (%cd) instead of author date (%ad) for more reliable timestamps
    format_str = '%H%n%an%n%ae%n%cd%n%s%n%b%n---END---'
    output = run_git('show', '-s', f'--format={format_str}', '--date=iso', commit_hash)

    lines = output.split('\n')

    full_hash = lines[0] if len(lines) > 0 else commit_hash
    author_name = lines[1] if len(lines) > 1 else ''
    author_email = lines[2] if len(lines) > 2 else ''
    date_str = lines[3] if len(lines) > 3 else ''
    subject = lines[4] if len(lines) > 4 else ''

    # Body is everything between subject and ---END---
    body_lines = []
    for i, line in enumerate(lines[5:]):
        if line.strip() == '---END---':
            break
        body_lines.append(line)
    body = '\n'.join(body_lines).strip()

    # Parse year from date
    year = 'unknown'
    if date_str:
        match = re.search(r'(20\d{2}|199\d)', date_str)
        if match:
            year = match.group(1)

    return {
        'hash': full_hash,
        'short_hash': full_hash[:12],
        'author_name': author_name,
        'author_email': author_email,
        'date': date_str,
        'year': year,
        'subject': subject,
        'body': body,
        'has_detailed_description': bool(body.strip())
    }


def get_commit_stats(commit_hash: str) -> Dict[str, Any]:
    """Get diff statistics for a commit."""
    # Get file changes
    stat_output = run_git('show', '--stat', '--format=', commit_hash)

    # Get numstat for precise numbers
    numstat = run_git('show', '--numstat', '--format=', commit_hash)

    files_changed = []
    total_insertions = 0
    total_deletions = 0

    for line in numstat.strip().split('\n'):
        if not line.strip():
            continue
        parts = line.split('\t')
        if len(parts) >= 3:
            added = parts[0]
            deleted = parts[1]
            filename = parts[2]

            try:
                add_count = int(added) if added != '-' else 0
                del_count = int(deleted) if deleted != '-' else 0
                total_insertions += add_count
                total_deletions += del_count
                files_changed.append({
                    'file': filename,
                    'insertions': add_count,
                    'deletions': del_count
                })
            except ValueError:
                files_changed.append({'file': filename, 'insertions': 0, 'deletions': 0})

    return {
        'files_changed': files_changed,
        'total_files': len(files_changed),
        'total_insertions': total_insertions,
        'total_deletions': total_deletions
    }


def analyze_diff_for_summary(commit_hash: str) -> str:
    """Analyze a commit's diff and generate a summary description."""
    stats = get_commit_stats(commit_hash)

    if not stats['files_changed']:
        return "No file changes detected."

    # Categorize changes
    categories = defaultdict(list)
    for f in stats['files_changed']:
        filename = f['file']
        ext = os.path.splitext(filename)[1].lower()
        basename = os.path.basename(filename)
        dirname = os.path.dirname(filename)

        if filename.startswith('validation/'):
            categories['tests'].append(filename)
        elif filename.startswith('Documentation/') or basename in ('README', 'README.md', 'FAQ'):
            categories['docs'].append(filename)
        elif ext == '.c':
            categories['source'].append(filename)
        elif ext == '.h':
            categories['headers'].append(filename)
        elif basename == 'Makefile' or ext == '.mk':
            categories['build'].append(filename)
        elif ext in ('.sh', '.py'):
            categories['scripts'].append(filename)
        else:
            categories['other'].append(filename)

    # Build summary
    parts = []

    if categories['source']:
        if len(categories['source']) == 1:
            parts.append(f"Modifies {categories['source'][0]}")
        else:
            parts.append(f"Modifies {len(categories['source'])} source files: {', '.join(categories['source'][:3])}" +
                        (f" and {len(categories['source'])-3} more" if len(categories['source']) > 3 else ""))

    if categories['headers']:
        if len(categories['headers']) == 1:
            parts.append(f"Updates header {categories['headers'][0]}")
        else:
            parts.append(f"Updates {len(categories['headers'])} headers")

    if categories['tests']:
        parts.append(f"Adds/updates {len(categories['tests'])} test(s)")

    if categories['docs']:
        parts.append(f"Updates documentation")

    if categories['build']:
        parts.append(f"Modifies build system")

    # Add change statistics
    stats_line = f"Changes: +{stats['total_insertions']}/-{stats['total_deletions']} in {stats['total_files']} file(s)"

    if parts:
        return '. '.join(parts) + '. ' + stats_line
    else:
        files = [f['file'] for f in stats['files_changed'][:5]]
        return f"Modifies: {', '.join(files)}" + (f" and {len(stats['files_changed'])-5} more" if len(stats['files_changed']) > 5 else "") + f". {stats_line}"


def process_commit(commit_hash: str) -> Dict[str, Any]:
    """Process a single commit and return full details."""
    details = get_commit_details(commit_hash)
    stats = get_commit_stats(commit_hash)

    # Generate summary if no detailed description
    if not details['has_detailed_description']:
        details['generated_summary'] = analyze_diff_for_summary(commit_hash)

    details['stats'] = stats
    return details


def organize_commits_by_year():
    """Main function to organize all commits by year."""
    print("Fetching all commits...")
    commit_hashes = get_all_commits()
    print(f"Found {len(commit_hashes)} commits")

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Process commits and organize by year
    yearly_data: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        'commits': [],
        'stats': {
            'total_commits': 0,
            'with_description': 0,
            'without_description': 0,
            'total_insertions': 0,
            'total_deletions': 0,
            'contributors': set()
        }
    })

    total = len(commit_hashes)
    for i, commit_hash in enumerate(commit_hashes):
        if (i + 1) % 100 == 0:
            print(f"Processing commit {i + 1}/{total}...")

        try:
            commit = process_commit(commit_hash)
            year = commit['year']

            yearly_data[year]['commits'].append(commit)
            yearly_data[year]['stats']['total_commits'] += 1
            yearly_data[year]['stats']['contributors'].add(commit['author_email'])

            if commit['has_detailed_description']:
                yearly_data[year]['stats']['with_description'] += 1
            else:
                yearly_data[year]['stats']['without_description'] += 1

            yearly_data[year]['stats']['total_insertions'] += commit['stats']['total_insertions']
            yearly_data[year]['stats']['total_deletions'] += commit['stats']['total_deletions']

        except Exception as e:
            print(f"Error processing {commit_hash}: {e}")

    # Save each year's data as txt
    summary_lines = [
        "=" * 80,
        "SPARSE GIT COMMITS BY YEAR",
        "=" * 80,
        f"Generated: {datetime.now().isoformat()}",
        f"Repository: https://git.kernel.org/pub/scm/devel/sparse/sparse.git",
        f"Total Commits: {len(commit_hashes)}",
        "",
        "YEAR SUMMARY:",
        "-" * 40,
    ]

    for year in sorted(yearly_data.keys()):
        year_info = yearly_data[year]

        # Convert set to count
        contributor_count = len(year_info['stats']['contributors'])
        year_info['stats']['contributors'] = contributor_count

        # Sort commits by date (newest first)
        year_info['commits'].sort(key=lambda c: c['date'], reverse=True)

        # Save year file as txt
        year_file = os.path.join(OUTPUT_DIR, f'{year}.txt')
        with open(year_file, 'w') as f:
            # Header
            f.write("=" * 80 + "\n")
            f.write(f"Year: {year} | Commits: {year_info['stats']['total_commits']} | Contributors: {contributor_count}\n")
            f.write(f"Insertions: +{year_info['stats']['total_insertions']} | Deletions: -{year_info['stats']['total_deletions']}\n")
            f.write("=" * 80 + "\n\n")

            # Each commit
            for commit in year_info['commits']:
                f.write(f"[{commit['short_hash']}] {commit['date']}\n")
                f.write(f"Author: {commit['author_name']} <{commit['author_email']}>\n")
                f.write(f"Subject: {commit['subject']}\n")
                f.write("\n")

                # Description
                if commit['body']:
                    f.write(commit['body'] + "\n")
                elif commit.get('generated_summary'):
                    f.write(f"[Auto-summary] {commit['generated_summary']}\n")
                f.write("\n")

                # Files changed
                if commit['stats']['files_changed']:
                    files_str = ", ".join(
                        f"{fc['file']} (+{fc['insertions']}/-{fc['deletions']})"
                        for fc in commit['stats']['files_changed'][:5]
                    )
                    if len(commit['stats']['files_changed']) > 5:
                        files_str += f", ... and {len(commit['stats']['files_changed']) - 5} more"
                    f.write(f"Files: {files_str}\n")

                f.write("-" * 80 + "\n\n")

        summary_lines.append(
            f"  {year}: {year_info['stats']['total_commits']:4d} commits, "
            f"{contributor_count:3d} contributors, "
            f"+{year_info['stats']['total_insertions']}/-{year_info['stats']['total_deletions']}"
        )

        print(f"Saved {year}: {year_info['stats']['total_commits']} commits "
              f"({year_info['stats']['with_description']} with descriptions, "
              f"{year_info['stats']['without_description']} auto-summarized)")

    # Save summary index
    summary_lines.append("")
    with open(os.path.join(OUTPUT_DIR, 'index.txt'), 'w') as f:
        f.write("\n".join(summary_lines))

    print(f"\nTotal: {len(yearly_data)} years saved to {OUTPUT_DIR}/")
    print(f"Summary saved to {OUTPUT_DIR}/index.txt")


if __name__ == '__main__':
    organize_commits_by_year()
