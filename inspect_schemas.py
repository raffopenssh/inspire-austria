#!/usr/bin/env python3
"""Inspect WFS/OGC-API services to discover actual field schemas.

Fetches small samples from services and stores discovered column names.
Can be run as a background job or triggered via API.
"""

import sqlite3
import json
import requests
import time
import re
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode
import xml.etree.ElementTree as ET

DB_PATH = 'inspire_austria.db'
TIMEOUT = 15  # seconds
MAX_RETRIES = 2

def get_db():
    return sqlite3.connect(DB_PATH)

def discover_ogc_api_fields(base_url, limit=5):
    """Fetch sample from OGC API Features and extract fields."""
    try:
        # Ensure URL ends properly for OGC API
        base = base_url.rstrip('/')
        
        # Try to get collections
        collections_url = f"{base}/collections"
        resp = requests.get(collections_url, timeout=TIMEOUT, headers={'Accept': 'application/json'})
        
        if resp.status_code != 200:
            return None, f"Collections request failed: {resp.status_code}"
        
        data = resp.json()
        collections = data.get('collections', [])
        
        # If no collections array, check if this IS the collections response with links
        if not collections and 'links' in data:
            # Try parsing the root endpoint
            root_resp = requests.get(base, timeout=TIMEOUT, headers={'Accept': 'application/json'})
            if root_resp.status_code == 200:
                root_data = root_resp.json()
                collections = root_data.get('collections', [])
        
        if not collections:
            return None, "No collections found"
        
        # Get first collection's items
        collection_id = collections[0].get('id') or collections[0].get('name')
        if not collection_id:
            return None, "Collection has no id"
            
        items_url = f"{base}/collections/{collection_id}/items?limit={limit}&f=json"
        
        resp = requests.get(items_url, timeout=TIMEOUT, headers={'Accept': 'application/geo+json,application/json'})
        if resp.status_code != 200:
            return None, f"Items request failed: {resp.status_code}"
        
        geojson = resp.json()
        features = geojson.get('features', [])
        if not features:
            return None, "No features returned"
        
        # Extract field names from first feature
        props = features[0].get('properties', {})
        fields = list(props.keys())
        
        # Add geometry field indicator
        if features[0].get('geometry'):
            fields.append('geometry')
        
        return {
            'fields': fields,
            'sample_count': len(features),
            'collection': collection_id,
            'field_types': {k: type(v).__name__ for k, v in props.items()}
        }, None
        
    except requests.Timeout:
        return None, "Timeout"
    except Exception as e:
        return None, str(e)

def discover_wfs_fields(wfs_url, limit=5):
    """Fetch sample from WFS and extract fields."""
    try:
        # Parse the URL to get base and add GetFeature params
        parsed = urlparse(wfs_url)
        params = parse_qs(parsed.query)
        
        # First get capabilities to find feature types
        caps_params = {
            'SERVICE': 'WFS',
            'REQUEST': 'GetCapabilities',
            'VERSION': params.get('VERSION', ['2.0.0'])[0]
        }
        
        base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        caps_url = f"{base_url}?{urlencode(caps_params)}"
        
        resp = requests.get(caps_url, timeout=TIMEOUT)
        if resp.status_code != 200:
            return None, f"GetCapabilities failed: {resp.status_code}"
        
        # Parse XML to find feature type names
        root = ET.fromstring(resp.content)
        ns = {
            'wfs': 'http://www.opengis.net/wfs/2.0',
            'wfs11': 'http://www.opengis.net/wfs',
            'ows': 'http://www.opengis.net/ows/1.1'
        }
        
        # Try different namespace patterns
        feature_types = []
        for ft in root.findall('.//wfs:FeatureType/wfs:Name', ns):
            feature_types.append(ft.text)
        if not feature_types:
            for ft in root.findall('.//{http://www.opengis.net/wfs/2.0}Name'):
                feature_types.append(ft.text)
        if not feature_types:
            for ft in root.findall('.//{http://www.opengis.net/wfs}Name'):
                feature_types.append(ft.text)
        
        if not feature_types:
            return None, "No feature types found in capabilities"
        
        # Get features from first type
        typename = feature_types[0]
        get_feature_params = {
            'SERVICE': 'WFS',
            'REQUEST': 'GetFeature',
            'VERSION': params.get('VERSION', ['2.0.0'])[0],
            'TYPENAMES': typename,
            'COUNT': str(limit),
            'OUTPUTFORMAT': 'application/json'
        }
        
        feature_url = f"{base_url}?{urlencode(get_feature_params)}"
        resp = requests.get(feature_url, timeout=TIMEOUT)
        
        if resp.status_code != 200:
            return None, f"GetFeature failed: {resp.status_code}"
        
        # Try to parse as JSON
        try:
            geojson = resp.json()
            features = geojson.get('features', [])
            if features:
                props = features[0].get('properties', {})
                fields = list(props.keys())
                if features[0].get('geometry'):
                    fields.append('geometry')
                return {
                    'fields': fields,
                    'sample_count': len(features),
                    'feature_type': typename,
                    'field_types': {k: type(v).__name__ for k, v in props.items()}
                }, None
        except:
            pass
        
        # Try parsing as GML
        try:
            root = ET.fromstring(resp.content)
            # Find first feature and extract property names
            for elem in root.iter():
                if 'member' in elem.tag.lower():
                    feature = list(elem)[0] if len(elem) > 0 else None
                    if feature is not None:
                        fields = [child.tag.split('}')[-1] for child in feature]
                        return {
                            'fields': fields,
                            'sample_count': 1,
                            'feature_type': typename,
                            'format': 'GML'
                        }, None
        except:
            pass
        
        return None, "Could not parse response"
        
    except requests.Timeout:
        return None, "Timeout"
    except Exception as e:
        return None, str(e)

def discover_download_fields(download_url):
    """Try to infer fields from download URL (limited)."""
    # For now, just mark as needing manual inspection
    return None, "Download URLs require manual inspection"

def update_service_status(conn, dataset_id, service_url, service_type, result, error):
    """Update service_status table with discovery results."""
    cur = conn.cursor()
    
    now = datetime.utcnow().isoformat()
    status = 'working' if result else ('timeout' if error == 'Timeout' else 'error')
    
    fields_json = json.dumps(result.get('fields')) if result else None
    details_json = json.dumps(result) if result else None
    
    cur.execute('''
        INSERT INTO service_status 
            (dataset_id, service_url, service_type, last_checked, status, 
             sample_fields, error_message, check_count, success_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
        ON CONFLICT(service_url) DO UPDATE SET
            last_checked = excluded.last_checked,
            status = excluded.status,
            sample_fields = COALESCE(excluded.sample_fields, service_status.sample_fields),
            error_message = excluded.error_message,
            check_count = service_status.check_count + 1,
            success_count = service_status.success_count + excluded.success_count
    ''', (
        dataset_id,
        service_url,
        service_type,
        now,
        status,
        fields_json,
        error,
        1 if result else 0
    ))
    
    conn.commit()
    return status

def log_as_feedback(conn, dataset_id, service_url, service_type, result, error):
    """Also log discovery as feedback for tracking."""
    cur = conn.cursor()
    
    cur.execute('''
        INSERT INTO feedback (source, category, dataset_id, service_url, issue_type, details, processed, processed_at, resolution)
        VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
    ''', (
        'schema-inspector',
        'schema',
        dataset_id,
        service_url,
        'success' if result else 'not_accessible',
        json.dumps({
            'service_type': service_type,
            'fields': result.get('fields') if result else None,
            'error': error,
            'discovery_details': result
        }),
        datetime.utcnow().isoformat(),
        'auto-discovered'
    ))
    
    conn.commit()

def inspect_services(limit=50, service_types=None, skip_recent_hours=24):
    """Main inspection loop."""
    conn = get_db()
    cur = conn.cursor()
    
    # Get services to inspect (prioritize OGC-API, then WFS)
    service_types = service_types or ['OGC-API', 'WFS']
    type_placeholders = ','.join('?' * len(service_types))
    
    cur.execute(f'''
        SELECT DISTINCT ds.dataset_id, ds.url, ds.service_type, d.title
        FROM dataset_services ds
        JOIN datasets d ON ds.dataset_id = d.id
        LEFT JOIN service_status ss ON ds.url = ss.service_url
        WHERE ds.service_type IN ({type_placeholders})
          AND (ss.last_checked IS NULL 
               OR ss.last_checked < datetime('now', '-{skip_recent_hours} hours'))
        ORDER BY 
            CASE ds.service_type WHEN 'OGC-API' THEN 1 WHEN 'WFS' THEN 2 ELSE 3 END,
            d.gem_score DESC
        LIMIT ?
    ''', (*service_types, limit))
    
    services = cur.fetchall()
    print(f"Inspecting {len(services)} services...")
    
    results = {'success': 0, 'failed': 0, 'timeout': 0}
    
    for i, (dataset_id, url, svc_type, title) in enumerate(services):
        print(f"  [{i+1}/{len(services)}] {svc_type}: {title[:50]}...")
        
        if svc_type == 'OGC-API':
            result, error = discover_ogc_api_fields(url)
        elif svc_type == 'WFS':
            result, error = discover_wfs_fields(url)
        else:
            result, error = None, f"Unknown service type: {svc_type}"
        
        status = update_service_status(conn, dataset_id, url, svc_type, result, error)
        log_as_feedback(conn, dataset_id, url, svc_type, result, error)
        
        if result:
            results['success'] += 1
            print(f"    ✓ Found {len(result['fields'])} fields")
        elif error == 'Timeout':
            results['timeout'] += 1
            print(f"    ⏱ Timeout")
        else:
            results['failed'] += 1
            print(f"    ✗ {error[:50]}")
        
        # Small delay to be nice to servers
        time.sleep(0.5)
    
    conn.close()
    return results

def get_schema_for_dataset(dataset_id):
    """Get discovered schema for a dataset."""
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute('''
        SELECT ss.service_url, ss.service_type, ss.status, ss.sample_fields,
               ss.last_checked, ss.check_count, ss.success_count
        FROM service_status ss
        WHERE ss.dataset_id = ?
        ORDER BY ss.status = 'working' DESC, ss.last_checked DESC
    ''', (dataset_id,))
    
    rows = cur.fetchall()
    conn.close()
    
    services = []
    for r in rows:
        services.append({
            'url': r[0],
            'type': r[1],
            'status': r[2],
            'fields': json.loads(r[3]) if r[3] else None,
            'last_checked': r[4],
            'checks': r[5],
            'successes': r[6]
        })
    
    return services

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Inspect WFS/OGC-API services for field schemas')
    parser.add_argument('--limit', type=int, default=50, help='Max services to inspect')
    parser.add_argument('--types', nargs='+', default=['OGC-API', 'WFS'], help='Service types to inspect')
    parser.add_argument('--skip-hours', type=int, default=24, help='Skip services checked within N hours')
    parser.add_argument('--dataset', type=str, help='Inspect specific dataset by ID')
    
    args = parser.parse_args()
    
    if args.dataset:
        # Show schema for specific dataset
        services = get_schema_for_dataset(args.dataset)
        print(json.dumps(services, indent=2))
    else:
        # Run inspection
        results = inspect_services(
            limit=args.limit,
            service_types=args.types,
            skip_recent_hours=args.skip_hours
        )
        print(f"\n=== Results ===")
        print(f"Success: {results['success']}")
        print(f"Failed: {results['failed']}")
        print(f"Timeout: {results['timeout']}")
