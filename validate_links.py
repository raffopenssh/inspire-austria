#!/usr/bin/env python3
"""Background job to validate service links and update database."""

import sqlite3
import json
import time
import urllib.request
import urllib.error
import ssl
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

DB_PATH = 'inspire_austria.db'
RESULTS_PATH = 'link_validation_results.json'
TIMEOUT = 15  # seconds
MAX_WORKERS = 10  # parallel requests

# Create SSL context that doesn't verify (some gov sites have cert issues)
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

def init_validation_table():
    """Create table to store validation results."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS link_validations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            service_id INTEGER,
            url TEXT,
            status TEXT,
            status_code INTEGER,
            response_time_ms INTEGER,
            content_type TEXT,
            error_message TEXT,
            validated_at TEXT,
            FOREIGN KEY (service_id) REFERENCES dataset_services(id)
        )
    ''')
    
    cur.execute('CREATE INDEX IF NOT EXISTS idx_validations_service ON link_validations(service_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_validations_status ON link_validations(status)')
    
    conn.commit()
    conn.close()

def validate_url(service_id, url, service_type):
    """Validate a single URL and return results."""
    result = {
        'service_id': service_id,
        'url': url,
        'service_type': service_type,
        'status': 'unknown',
        'status_code': None,
        'response_time_ms': None,
        'content_type': None,
        'error_message': None,
        'validated_at': datetime.utcnow().isoformat()
    }
    
    if not url or url.startswith('#'):
        result['status'] = 'invalid_url'
        result['error_message'] = 'Empty or anchor URL'
        return result
    
    try:
        start = time.time()
        
        # Build request
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'INSPIRE-Austria-Validator/1.0'}
        )
        
        # Make request
        response = urllib.request.urlopen(req, timeout=TIMEOUT, context=ssl_context)
        
        elapsed = int((time.time() - start) * 1000)
        result['response_time_ms'] = elapsed
        result['status_code'] = response.getcode()
        result['content_type'] = response.headers.get('Content-Type', '')
        
        # Check if it's actually a service response
        content_type = result['content_type'].lower()
        
        if result['status_code'] == 200:
            # Read a bit of content to verify
            try:
                content_sample = response.read(2000).decode('utf-8', errors='ignore')
            except:
                content_sample = ''
            
            # Check for WFS/WMS capabilities
            if service_type in ('WFS', 'WMS', 'WMTS'):
                if 'capabilities' in content_sample.lower() or 'wfs' in content_sample.lower() or 'wms' in content_sample.lower():
                    result['status'] = 'working'
                elif 'exception' in content_sample.lower() or 'error' in content_sample.lower():
                    result['status'] = 'error_response'
                    result['error_message'] = 'Service returned error'
                else:
                    result['status'] = 'working'  # Assume OK if 200
            elif service_type == 'OGC-API':
                if 'collections' in content_sample.lower() or 'conformsTo' in content_sample:
                    result['status'] = 'working'
                else:
                    result['status'] = 'working'
            elif 'xml' in content_type or 'json' in content_type or 'html' in content_type:
                result['status'] = 'working'
            else:
                result['status'] = 'working'
        else:
            result['status'] = 'http_error'
            
    except urllib.error.HTTPError as e:
        result['status'] = 'http_error'
        result['status_code'] = e.code
        result['error_message'] = str(e.reason)
        
    except urllib.error.URLError as e:
        result['status'] = 'connection_error'
        result['error_message'] = str(e.reason)[:200]
        
    except TimeoutError:
        result['status'] = 'timeout'
        result['error_message'] = f'Timeout after {TIMEOUT}s'
        
    except Exception as e:
        result['status'] = 'error'
        result['error_message'] = str(e)[:200]
    
    return result

def get_services_to_validate(limit=None, service_types=None):
    """Get services that need validation."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    sql = '''
        SELECT s.id, s.url, s.service_type, d.title
        FROM dataset_services s
        JOIN datasets d ON s.dataset_id = d.id
        WHERE s.url IS NOT NULL AND s.url != ''
    '''
    
    if service_types:
        placeholders = ','.join('?' * len(service_types))
        sql += f' AND s.service_type IN ({placeholders})'
        params = list(service_types)
    else:
        params = []
    
    # Prioritize important service types
    sql += ' ORDER BY CASE s.service_type WHEN "WFS" THEN 1 WHEN "OGC-API" THEN 2 WHEN "WMS" THEN 3 ELSE 4 END'
    
    if limit:
        sql += f' LIMIT {limit}'
    
    cur.execute(sql, params)
    services = cur.fetchall()
    conn.close()
    
    return services

def save_results(results):
    """Save validation results to database and JSON file."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    for r in results:
        cur.execute('''
            INSERT INTO link_validations 
            (service_id, url, status, status_code, response_time_ms, content_type, error_message, validated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            r['service_id'], r['url'], r['status'], r['status_code'],
            r['response_time_ms'], r['content_type'], r['error_message'], r['validated_at']
        ))
    
    conn.commit()
    conn.close()
    
    # Also save summary to JSON
    summary = {
        'validated_at': datetime.utcnow().isoformat(),
        'total': len(results),
        'by_status': {},
        'by_service_type': {},
    }
    
    for r in results:
        status = r['status']
        svc_type = r['service_type']
        
        summary['by_status'][status] = summary['by_status'].get(status, 0) + 1
        
        if svc_type not in summary['by_service_type']:
            summary['by_service_type'][svc_type] = {'total': 0, 'working': 0}
        summary['by_service_type'][svc_type]['total'] += 1
        if status == 'working':
            summary['by_service_type'][svc_type]['working'] += 1
    
    with open(RESULTS_PATH, 'w') as f:
        json.dump(summary, f, indent=2)
    
    return summary

def run_validation(limit=None, service_types=None, verbose=True):
    """Run the validation job."""
    init_validation_table()
    
    services = get_services_to_validate(limit, service_types)
    total = len(services)
    
    if verbose:
        print(f"Validating {total} service links...")
    
    results = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(validate_url, svc[0], svc[1], svc[2]): svc
            for svc in services
        }
        
        completed = 0
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1
            
            if verbose and completed % 50 == 0:
                print(f"  Progress: {completed}/{total} ({100*completed//total}%)")
    
    summary = save_results(results)
    
    if verbose:
        print(f"\n=== Validation Summary ===")
        print(f"Total validated: {summary['total']}")
        print(f"\nBy status:")
        for status, count in sorted(summary['by_status'].items(), key=lambda x: -x[1]):
            print(f"  {status}: {count}")
        print(f"\nBy service type:")
        for svc_type, data in summary['by_service_type'].items():
            pct = 100 * data['working'] / data['total'] if data['total'] > 0 else 0
            print(f"  {svc_type}: {data['working']}/{data['total']} working ({pct:.0f}%)")
    
    return summary

def get_broken_links():
    """Get list of broken/problematic links."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute('''
        SELECT v.url, v.status, v.error_message, v.status_code, d.title, s.service_type
        FROM link_validations v
        JOIN dataset_services s ON v.service_id = s.id
        JOIN datasets d ON s.dataset_id = d.id
        WHERE v.status NOT IN ('working')
        AND v.id IN (
            SELECT MAX(id) FROM link_validations GROUP BY service_id
        )
        ORDER BY s.service_type, v.status
    ''')
    
    results = cur.fetchall()
    conn.close()
    
    return results

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate INSPIRE service links')
    parser.add_argument('--limit', type=int, help='Limit number of links to validate')
    parser.add_argument('--types', nargs='+', help='Service types to validate (WFS, WMS, OGC-API, etc.)')
    parser.add_argument('--report', action='store_true', help='Show broken links report')
    
    args = parser.parse_args()
    
    if args.report:
        broken = get_broken_links()
        print(f"\n=== Broken/Problematic Links ({len(broken)}) ===")
        for url, status, error, code, title, svc_type in broken[:50]:
            print(f"\n[{svc_type}] {status}" + (f" ({code})" if code else ""))
            print(f"  Dataset: {title[:60]}")
            print(f"  URL: {url[:80]}")
            if error:
                print(f"  Error: {error[:60]}")
    else:
        run_validation(limit=args.limit, service_types=args.types)
