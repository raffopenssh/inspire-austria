#!/usr/bin/env python3
"""Fetch and analyze WFS schemas to build field mappings."""

import sqlite3
import json
import re
import xml.etree.ElementTree as ET
import urllib.request
import ssl
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

DB_PATH = 'inspire_austria.db'
TIMEOUT = 30
MAX_WORKERS = 5

# SSL context
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Namespace mappings
NAMESPACES = {
    'wfs': 'http://www.opengis.net/wfs/2.0',
    'wfs11': 'http://www.opengis.net/wfs',
    'ows': 'http://www.opengis.net/ows/1.1',
    'xsd': 'http://www.w3.org/2001/XMLSchema',
    'gml': 'http://www.opengis.net/gml/3.2',
    # INSPIRE schemas
    'am': 'http://inspire.ec.europa.eu/schemas/am/4.0',
    'elu': 'http://inspire.ec.europa.eu/schemas/elu/4.0',
    'lu': 'http://inspire.ec.europa.eu/schemas/lu/4.0',
    'ps': 'http://inspire.ec.europa.eu/schemas/ps/4.0',
    'hy': 'http://inspire.ec.europa.eu/schemas/hy/4.0',
    'ad': 'http://inspire.ec.europa.eu/schemas/ad/4.0',
    'cp': 'http://inspire.ec.europa.eu/schemas/cp/4.0',
    'au': 'http://inspire.ec.europa.eu/schemas/au/4.0',
    'gn': 'http://inspire.ec.europa.eu/schemas/gn/4.0',
    'el': 'http://inspire.ec.europa.eu/schemas/el/4.0',
    'base': 'http://inspire.ec.europa.eu/schemas/base/3.3',
}

def init_schema_tables():
    """Create tables for storing schema information."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Feature types table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS wfs_feature_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            service_id INTEGER,
            dataset_id TEXT,
            type_name TEXT,
            type_namespace TEXT,
            title TEXT,
            is_inspire BOOLEAN,
            inspire_theme TEXT,
            fetched_at TEXT,
            FOREIGN KEY (service_id) REFERENCES dataset_services(id),
            FOREIGN KEY (dataset_id) REFERENCES datasets(id)
        )
    ''')
    
    # Field definitions table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS wfs_fields (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            feature_type_id INTEGER,
            field_name TEXT,
            field_type TEXT,
            is_geometry BOOLEAN,
            is_nullable BOOLEAN,
            description TEXT,
            FOREIGN KEY (feature_type_id) REFERENCES wfs_feature_types(id)
        )
    ''')
    
    # Field mappings table (cross-provincial equivalents)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS field_mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            concept_id TEXT,
            canonical_name TEXT,
            canonical_type TEXT,
            description_de TEXT,
            description_en TEXT
        )
    ''')
    
    # Field mapping instances
    cur.execute('''
        CREATE TABLE IF NOT EXISTS field_mapping_instances (
            mapping_id INTEGER,
            field_id INTEGER,
            province TEXT,
            local_name TEXT,
            transformation TEXT,
            FOREIGN KEY (mapping_id) REFERENCES field_mappings(id),
            FOREIGN KEY (field_id) REFERENCES wfs_fields(id)
        )
    ''')
    
    cur.execute('CREATE INDEX IF NOT EXISTS idx_ft_dataset ON wfs_feature_types(dataset_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_fields_ft ON wfs_fields(feature_type_id)')
    
    conn.commit()
    conn.close()

def fetch_capabilities(url):
    """Fetch WFS GetCapabilities."""
    try:
        # Ensure we're requesting capabilities
        if 'GetCapabilities' not in url:
            if '?' in url:
                url += '&REQUEST=GetCapabilities&SERVICE=WFS&VERSION=2.0.0'
            else:
                url += '?REQUEST=GetCapabilities&SERVICE=WFS&VERSION=2.0.0'
        
        req = urllib.request.Request(url, headers={'User-Agent': 'INSPIRE-Schema-Fetcher/1.0'})
        response = urllib.request.urlopen(req, timeout=TIMEOUT, context=ssl_context)
        return response.read().decode('utf-8', errors='ignore')
    except Exception as e:
        return None

def fetch_feature_sample(base_url, type_name):
    """Fetch a sample feature to analyze actual data structure."""
    try:
        # Build GetFeature request
        if '?' in base_url:
            base = base_url.split('?')[0]
        else:
            base = base_url
        
        url = f"{base}?SERVICE=WFS&REQUEST=GetFeature&VERSION=2.0.0&TYPENAMES={type_name}&COUNT=1"
        
        req = urllib.request.Request(url, headers={'User-Agent': 'INSPIRE-Schema-Fetcher/1.0'})
        response = urllib.request.urlopen(req, timeout=TIMEOUT, context=ssl_context)
        return response.read().decode('utf-8', errors='ignore')
    except Exception as e:
        return None

def parse_capabilities(xml_content):
    """Parse WFS capabilities to extract feature types."""
    if not xml_content:
        return []
    
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        return []
    
    feature_types = []
    
    # Try WFS 2.0 structure
    for ft in root.findall('.//{http://www.opengis.net/wfs/2.0}FeatureType'):
        name_el = ft.find('{http://www.opengis.net/wfs/2.0}Name')
        title_el = ft.find('{http://www.opengis.net/wfs/2.0}Title')
        
        if name_el is None:
            # Try without namespace
            name_el = ft.find('Name')
            title_el = ft.find('Title')
        
        if name_el is not None:
            name = name_el.text
            # Extract namespace prefix if present
            if ':' in name:
                ns_prefix, local_name = name.split(':', 1)
            else:
                ns_prefix, local_name = '', name
            
            feature_types.append({
                'name': name,
                'namespace_prefix': ns_prefix,
                'local_name': local_name,
                'title': title_el.text if title_el is not None else local_name
            })
    
    # Try WFS 1.1 structure
    if not feature_types:
        for ft in root.findall('.//{http://www.opengis.net/wfs}FeatureType'):
            name_el = ft.find('{http://www.opengis.net/wfs}Name')
            title_el = ft.find('{http://www.opengis.net/wfs}Title')
            
            if name_el is not None:
                name = name_el.text
                if ':' in name:
                    ns_prefix, local_name = name.split(':', 1)
                else:
                    ns_prefix, local_name = '', name
                
                feature_types.append({
                    'name': name,
                    'namespace_prefix': ns_prefix,
                    'local_name': local_name,
                    'title': title_el.text if title_el is not None else local_name
                })
    
    return feature_types

def extract_fields_from_sample(xml_content, type_name):
    """Extract field names and types from a sample feature."""
    if not xml_content:
        return []
    
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        return []
    
    fields = []
    seen = set()
    
    # Find the first feature element
    # Could be in various namespaces
    for elem in root.iter():
        # Skip container elements
        if 'FeatureCollection' in elem.tag or 'member' in elem.tag:
            continue
        
        # Check if this looks like a feature
        if elem.attrib.get('{http://www.opengis.net/gml/3.2}id'):
            # This is likely our feature - examine its children
            for child in elem:
                # Extract local name from tag
                if '}' in child.tag:
                    ns, local = child.tag.rsplit('}', 1)
                    ns = ns[1:]  # Remove leading {
                else:
                    ns, local = '', child.tag
                
                if local in seen:
                    continue
                seen.add(local)
                
                # Determine if geometry
                is_geom = any(g in local.lower() for g in ['geometry', 'geom', 'shape', 'position'])
                is_geom = is_geom or any(g in str(child.tag) for g in ['Point', 'Polygon', 'Surface', 'Curve', 'MultiSurface'])
                
                # Try to infer type from content
                field_type = 'string'
                if child.text:
                    text = child.text.strip()
                    if re.match(r'^-?\d+$', text):
                        field_type = 'integer'
                    elif re.match(r'^-?\d+\.\d+$', text):
                        field_type = 'decimal'
                    elif re.match(r'^\d{4}-\d{2}-\d{2}', text):
                        field_type = 'dateTime'
                    elif text.lower() in ('true', 'false'):
                        field_type = 'boolean'
                elif is_geom:
                    field_type = 'geometry'
                elif len(list(child)) > 0:
                    field_type = 'complex'
                
                # Check for xlink:href (code list reference)
                href = child.attrib.get('{http://www.w3.org/1999/xlink}href')
                if href:
                    field_type = 'codelist'
                
                fields.append({
                    'name': local,
                    'namespace': ns,
                    'type': field_type,
                    'is_geometry': is_geom,
                    'sample_value': child.text[:100] if child.text else (href[:100] if href else None)
                })
            
            break  # Only process first feature
    
    return fields

def determine_inspire_theme(namespace, type_name):
    """Determine INSPIRE theme from namespace/type."""
    themes = {
        'am': 'Area Management',
        'elu': 'Existing Land Use',
        'lu': 'Land Use',
        'plu': 'Planned Land Use',
        'ps': 'Protected Sites',
        'hy': 'Hydrography',
        'ad': 'Addresses',
        'cp': 'Cadastral Parcels',
        'au': 'Administrative Units',
        'gn': 'Geographical Names',
        'el': 'Elevation',
        'tn': 'Transport Networks',
        'bu': 'Buildings',
        'so': 'Soil',
        'ge': 'Geology',
        'ef': 'Environmental Monitoring',
        'sr': 'Species Distribution',
        'hb': 'Habitats and Biotopes',
    }
    
    for prefix, theme in themes.items():
        if prefix in namespace.lower() or type_name.lower().startswith(prefix + ':'):
            return theme
    
    return None

def process_service(service_info):
    """Process a single WFS service."""
    service_id, dataset_id, url, province, title = service_info
    
    result = {
        'service_id': service_id,
        'dataset_id': dataset_id,
        'province': province,
        'title': title,
        'feature_types': [],
        'error': None
    }
    
    # Fetch capabilities
    caps_xml = fetch_capabilities(url)
    if not caps_xml:
        result['error'] = 'Failed to fetch capabilities'
        return result
    
    # Parse feature types
    feature_types = parse_capabilities(caps_xml)
    if not feature_types:
        result['error'] = 'No feature types found'
        return result
    
    # For each feature type, fetch a sample and extract fields
    for ft in feature_types[:3]:  # Limit to 3 feature types per service
        sample_xml = fetch_feature_sample(url, ft['name'])
        fields = extract_fields_from_sample(sample_xml, ft['name'])
        
        is_inspire = ft['namespace_prefix'] in NAMESPACES or 'inspire' in url.lower()
        inspire_theme = determine_inspire_theme(ft['namespace_prefix'], ft['name'])
        
        ft['fields'] = fields
        ft['is_inspire'] = is_inspire
        ft['inspire_theme'] = inspire_theme
        result['feature_types'].append(ft)
    
    return result

def save_schema_results(results):
    """Save schema analysis results to database."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    now = datetime.now(timezone.utc).isoformat()
    
    for r in results:
        if r['error']:
            continue
        
        for ft in r['feature_types']:
            # Insert feature type
            cur.execute('''
                INSERT INTO wfs_feature_types 
                (service_id, dataset_id, type_name, type_namespace, title, is_inspire, inspire_theme, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                r['service_id'], r['dataset_id'], ft['name'], ft['namespace_prefix'],
                ft['title'], ft['is_inspire'], ft['inspire_theme'], now
            ))
            ft_id = cur.lastrowid
            
            # Insert fields
            for field in ft.get('fields', []):
                cur.execute('''
                    INSERT INTO wfs_fields 
                    (feature_type_id, field_name, field_type, is_geometry, is_nullable, description)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    ft_id, field['name'], field['type'], field['is_geometry'],
                    True, field.get('sample_value', '')[:500] if field.get('sample_value') else None
                ))
    
    conn.commit()
    conn.close()

def get_wfs_services(limit=None):
    """Get WFS services to analyze."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    sql = '''
        SELECT s.id, d.id, s.url, d.province, d.title
        FROM dataset_services s
        JOIN datasets d ON s.dataset_id = d.id
        WHERE s.service_type = 'WFS'
        AND s.url IS NOT NULL
    '''
    
    if limit:
        sql += f' LIMIT {limit}'
    
    cur.execute(sql)
    services = cur.fetchall()
    conn.close()
    
    return services

def generate_field_mapping_report():
    """Generate a report showing field variations across provinces."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Group by INSPIRE theme and show field variations
    cur.execute('''
        SELECT ft.inspire_theme, ft.type_name, d.province, GROUP_CONCAT(DISTINCT f.field_name) as fields
        FROM wfs_feature_types ft
        JOIN datasets d ON ft.dataset_id = d.id
        LEFT JOIN wfs_fields f ON ft.id = f.feature_type_id
        WHERE ft.inspire_theme IS NOT NULL
        GROUP BY ft.inspire_theme, ft.type_name, d.province
        ORDER BY ft.inspire_theme, ft.type_name, d.province
    ''')
    
    results = cur.fetchall()
    conn.close()
    
    # Group by theme for display
    by_theme = {}
    for theme, type_name, province, fields in results:
        if theme not in by_theme:
            by_theme[theme] = {}
        if type_name not in by_theme[theme]:
            by_theme[theme][type_name] = {}
        by_theme[theme][type_name][province or 'National'] = fields.split(',') if fields else []
    
    return by_theme

def run_schema_analysis(limit=None, verbose=True):
    """Run the schema analysis."""
    init_schema_tables()
    
    services = get_wfs_services(limit)
    total = len(services)
    
    if verbose:
        print(f"Analyzing {total} WFS services...")
    
    results = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_service, svc): svc for svc in services}
        
        completed = 0
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1
            
            if verbose and completed % 20 == 0:
                print(f"  Progress: {completed}/{total}")
    
    if verbose:
        print("Saving results...")
    
    save_schema_results(results)
    
    # Generate summary
    success = sum(1 for r in results if not r['error'])
    total_ft = sum(len(r['feature_types']) for r in results)
    total_fields = sum(len(ft.get('fields', [])) for r in results for ft in r['feature_types'])
    
    if verbose:
        print(f"\n=== Schema Analysis Summary ===")
        print(f"Services analyzed: {success}/{total}")
        print(f"Feature types found: {total_ft}")
        print(f"Fields cataloged: {total_fields}")
    
    return results

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze WFS schemas')
    parser.add_argument('--limit', type=int, help='Limit services to analyze')
    parser.add_argument('--report', action='store_true', help='Show field mapping report')
    
    args = parser.parse_args()
    
    if args.report:
        by_theme = generate_field_mapping_report()
        for theme, types in sorted(by_theme.items()):
            print(f"\n=== {theme} ===")
            for type_name, provinces in types.items():
                print(f"  {type_name}:")
                for province, fields in provinces.items():
                    print(f"    {province}: {', '.join(fields[:10])}{'...' if len(fields) > 10 else ''}")
    else:
        run_schema_analysis(limit=args.limit)
