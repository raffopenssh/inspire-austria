#!/usr/bin/env python3
"""Build a comprehensive index of Austrian INSPIRE datasets."""

import json
import os
import re
import sqlite3
from pathlib import Path
from collections import defaultdict

# Austrian provinces (Bundesländer)
PROVINCES = {
    'wien': 'Wien',
    'burgenland': 'Burgenland', 
    'kärnten': 'Kärnten',
    'kaernten': 'Kärnten',
    'niederösterreich': 'Niederösterreich',
    'niederoesterreich': 'Niederösterreich',
    'nö': 'Niederösterreich',
    'oberösterreich': 'Oberösterreich',
    'oberoesterreich': 'Oberösterreich',
    'oö': 'Oberösterreich',
    'salzburg': 'Salzburg',
    'steiermark': 'Steiermark',
    'tirol': 'Tirol',
    'vorarlberg': 'Vorarlberg'
}

# Topic mappings for grouping related datasets
TOPIC_KEYWORDS = {
    'grundwasser': ['grundwasser', 'groundwater', 'aquifer', 'wasserspiegel', 'pegel'],
    'wetter': ['wetter', 'weather', 'niederschlag', 'temperatur', 'klima', 'meteorolog'],
    'hochwasser': ['hochwasser', 'flood', 'überschwemmung', 'überflutung', 'hwz'],
    'gewässer': ['gewässer', 'fluss', 'bach', 'see', 'wasser', 'hydrograph', 'hydro'],
    'boden': ['boden', 'soil', 'erdreich', 'bodenkarte'],
    'wald': ['wald', 'forst', 'forest', 'baum', 'waldkarte'],
    'naturschutz': ['naturschutz', 'natura 2000', 'schutzgebiet', 'biotop', 'habitat'],
    'kataster': ['kataster', 'grundstück', 'parzelle', 'cadastr', 'eigentum'],
    'raumordnung': ['raumordnung', 'raumplan', 'flächenwidmung', 'bebauung', 'landuse', 'land use'],
    'verkehr': ['verkehr', 'strasse', 'straße', 'transport', 'schiene', 'bahn', 'weg'],
    'energie': ['energie', 'strom', 'kraftwerk', 'wind', 'solar', 'photovoltaik'],
    'geologie': ['geologie', 'gestein', 'geology', 'mineral', 'bergbau'],
    'höhenmodell': ['höhen', 'elevation', 'dgm', 'dem', 'dtm', 'gelände', 'relief'],
    'orthofoto': ['orthofoto', 'orthophoto', 'luftbild', 'aerial', 'imagery'],
    'adresse': ['adresse', 'address', 'hausnummer', 'postleitzahl'],
    'gebäude': ['gebäude', 'building', 'bauwerk', 'haus'],
    'bevölkerung': ['bevölkerung', 'population', 'einwohner', 'demograph'],
    'landwirtschaft': ['landwirtschaft', 'agrar', 'farm', 'agricult', 'acker', 'weinbau'],
    'gesundheit': ['gesundheit', 'health', 'krankenhaus', 'arzt', 'medizin'],
    'umwelt': ['umwelt', 'environment', 'emission', 'lärm', 'luft', 'noise'],
}

def extract_province(text):
    """Extract province from text."""
    if not text:
        return None
    text_lower = text.lower()
    for key, province in PROVINCES.items():
        if key in text_lower:
            return province
    return None

def extract_topics(text):
    """Extract topics from text."""
    if not text:
        return []
    text_lower = text.lower()
    topics = []
    for topic, keywords in TOPIC_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            topics.append(topic)
    return topics

def extract_year(text):
    """Extract year from text."""
    if not text:
        return None
    years = re.findall(r'\b(19\d{2}|20[0-2]\d)\b', text)
    return years[0] if years else None

def parse_links(links):
    """Parse service links from dataset."""
    if not links:
        return []
    result = []
    for link in links:
        url = link.get('urlObject', {}).get('default', '')
        protocol = link.get('protocol', '') or ''
        function = link.get('function', '')
        mime = link.get('mimeType', '')
        
        # Determine service type
        service_type = 'unknown'
        url_lower = url.lower()
        protocol_lower = protocol.lower()
        
        if 'wfs' in url_lower or 'wfs' in protocol_lower:
            service_type = 'WFS'
        elif 'wms' in url_lower or 'wms' in protocol_lower:
            service_type = 'WMS'
        elif 'wmts' in url_lower or 'wmts' in protocol_lower:
            service_type = 'WMTS'
        elif 'atom' in url_lower or 'atom' in protocol_lower:
            service_type = 'ATOM'
        elif 'ogcapi' in url_lower or 'api/features' in url_lower:
            service_type = 'OGC-API'
        elif 'download' in protocol_lower or 'download' in function.lower():
            service_type = 'Download'
        elif url:
            service_type = 'Link'
            
        if url:
            result.append({
                'url': url,
                'type': service_type,
                'protocol': protocol,
                'function': function
            })
    return result

def calculate_gem_score(dataset):
    """Calculate a 'gem' score based on data quality indicators."""
    score = 0
    title = (dataset.get('title') or '').lower()
    abstract = (dataset.get('abstract') or '').lower()
    full_text = title + ' ' + abstract
    
    # Has actual data services (not just metadata)
    services = dataset.get('services', [])
    has_wfs = any(s['type'] == 'WFS' for s in services)
    has_ogcapi = any(s['type'] == 'OGC-API' for s in services)
    has_download = any(s['type'] in ('Download', 'ATOM') for s in services)
    
    if has_wfs:
        score += 3
    if has_ogcapi:
        score += 4  # Modern API
    if has_download:
        score += 2
        
    # High resolution indicators
    if any(kw in full_text for kw in ['1m', '1 m', 'meter', 'hochauflösend', 'high resolution', 'detailliert']):
        score += 2
    if any(kw in full_text for kw in ['real-time', 'echtzeit', 'aktuell', 'live', 'stündlich', 'täglich']):
        score += 3
    if any(kw in full_text for kw in ['zeitreihe', 'time series', 'historisch', 'langzeit', 'mehrjährig']):
        score += 3
        
    # Nationwide coverage
    if 'österreich' in full_text or 'austria' in full_text:
        score += 2
    if 'bundesweit' in full_text or 'nationwide' in full_text:
        score += 2
        
    # Quality indicators
    if 'inspire' in full_text:
        score += 1
    if dataset.get('is_open_data'):
        score += 1
        
    return score

def process_dataset(hit):
    """Process a single dataset from the API response."""
    source = hit.get('_source', {})
    
    title = source.get('resourceTitleObject', {}).get('default', '')
    abstract = source.get('resourceAbstractObject', {}).get('default', '')
    
    # Extract keywords from various sources
    all_keywords = []
    keywords_obj = source.get('allKeywords', {})
    for theme_key, theme_data in keywords_obj.items():
        keywords = theme_data.get('keywords', [])
        for kw in keywords:
            if isinstance(kw, dict):
                all_keywords.append(kw.get('default', kw.get('langger', '')))
            else:
                all_keywords.append(str(kw))
    
    # Extract tags
    tags = source.get('tag', [])
    if isinstance(tags, list):
        all_keywords.extend([t.get('default', '') if isinstance(t, dict) else str(t) for t in tags])
    
    # Get links and services
    links = source.get('link', [])
    services = parse_links(links)
    
    # Determine province and topics
    full_text = f"{title} {abstract} {' '.join(all_keywords)}"
    province = extract_province(full_text)
    topics = extract_topics(full_text)
    year = extract_year(title) or extract_year(abstract)
    
    dataset = {
        'id': hit.get('_id', ''),
        'uuid': source.get('metadataIdentifier', ''),
        'title': title,
        'abstract': abstract[:2000] if abstract else '',
        'type': source.get('resourceType', ['unknown'])[0] if source.get('resourceType') else 'unknown',
        'themes': source.get('inspireTheme', []),
        'keywords': list(set(all_keywords)),
        'province': province,
        'topics': topics,
        'year': year,
        'services': services,
        'formats': source.get('format', []),
        'is_open_data': source.get('isOpenData', False),
        'org': source.get('OrgForResourceObject', {}).get('default', '') if source.get('OrgForResourceObject') else '',
        'contact': source.get('contactForResource', [{}])[0].get('email', '') if source.get('contactForResource') else '',
        'create_date': source.get('createDate', ''),
        'update_date': source.get('changeDate', ''),
        'bbox': source.get('geom'),
    }
    
    dataset['gem_score'] = calculate_gem_score(dataset)
    
    return dataset

def load_all_datasets():
    """Load all datasets from raw JSON files."""
    datasets = []
    raw_dir = Path('raw_data')
    
    for i in range(20):
        filepath = raw_dir / f'page_{i}.json'
        if filepath.exists():
            with open(filepath) as f:
                data = json.load(f)
                hits = data.get('hits', {}).get('hits', [])
                for hit in hits:
                    try:
                        ds = process_dataset(hit)
                        datasets.append(ds)
                    except Exception as e:
                        print(f"Error processing dataset: {e}")
    
    return datasets

def build_topic_groups(datasets):
    """Group datasets by topic for unified search."""
    groups = defaultdict(list)
    
    for ds in datasets:
        # Group by topics
        for topic in ds['topics']:
            groups[topic].append(ds['id'])
        
        # Also group by themes
        for theme in ds['themes']:
            groups[f"theme:{theme}"].append(ds['id'])
    
    return dict(groups)

def create_database(datasets, topic_groups):
    """Create SQLite database with all data."""
    db_path = 'inspire_austria.db'
    if os.path.exists(db_path):
        os.remove(db_path)
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Main datasets table
    cur.execute('''
        CREATE TABLE datasets (
            id TEXT PRIMARY KEY,
            uuid TEXT,
            title TEXT,
            abstract TEXT,
            type TEXT,
            province TEXT,
            year TEXT,
            is_open_data BOOLEAN,
            org TEXT,
            contact TEXT,
            create_date TEXT,
            update_date TEXT,
            gem_score INTEGER,
            bbox TEXT
        )
    ''')
    
    # Themes junction table
    cur.execute('''
        CREATE TABLE dataset_themes (
            dataset_id TEXT,
            theme TEXT,
            FOREIGN KEY (dataset_id) REFERENCES datasets(id)
        )
    ''')
    
    # Topics junction table
    cur.execute('''
        CREATE TABLE dataset_topics (
            dataset_id TEXT,
            topic TEXT,
            FOREIGN KEY (dataset_id) REFERENCES datasets(id)
        )
    ''')
    
    # Keywords table
    cur.execute('''
        CREATE TABLE dataset_keywords (
            dataset_id TEXT,
            keyword TEXT,
            FOREIGN KEY (dataset_id) REFERENCES datasets(id)
        )
    ''')
    
    # Services table
    cur.execute('''
        CREATE TABLE dataset_services (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_id TEXT,
            url TEXT,
            service_type TEXT,
            protocol TEXT,
            FOREIGN KEY (dataset_id) REFERENCES datasets(id)
        )
    ''')
    
    # Formats table
    cur.execute('''
        CREATE TABLE dataset_formats (
            dataset_id TEXT,
            format TEXT,
            FOREIGN KEY (dataset_id) REFERENCES datasets(id)
        )
    ''')
    
    # Topic groups for unified search
    cur.execute('''
        CREATE TABLE topic_groups (
            topic TEXT,
            dataset_id TEXT
        )
    ''')
    
    # Full-text search table
    cur.execute('''
        CREATE VIRTUAL TABLE datasets_fts USING fts5(
            id,
            title,
            abstract,
            keywords,
            themes,
            topics,
            province
        )
    ''')
    
    # Insert data
    for ds in datasets:
        cur.execute('''
            INSERT INTO datasets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            ds['id'], ds['uuid'], ds['title'], ds['abstract'], ds['type'],
            ds['province'], ds['year'], ds['is_open_data'], ds['org'], 
            ds['contact'], ds['create_date'], ds['update_date'], ds['gem_score'],
            json.dumps(ds['bbox']) if ds['bbox'] else None
        ))
        
        for theme in ds['themes']:
            cur.execute('INSERT INTO dataset_themes VALUES (?, ?)', (ds['id'], theme))
        
        for topic in ds['topics']:
            cur.execute('INSERT INTO dataset_topics VALUES (?, ?)', (ds['id'], topic))
            
        for kw in ds['keywords']:
            if kw:
                cur.execute('INSERT INTO dataset_keywords VALUES (?, ?)', (ds['id'], kw))
        
        for svc in ds['services']:
            cur.execute('INSERT INTO dataset_services (dataset_id, url, service_type, protocol) VALUES (?, ?, ?, ?)',
                       (ds['id'], svc['url'], svc['type'], svc['protocol']))
            
        for fmt in ds['formats']:
            cur.execute('INSERT INTO dataset_formats VALUES (?, ?)', (ds['id'], fmt))
        
        # FTS entry
        cur.execute('''
            INSERT INTO datasets_fts VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            ds['id'], ds['title'], ds['abstract'],
            ' '.join(ds['keywords']), ' '.join(ds['themes']),
            ' '.join(ds['topics']), ds['province'] or ''
        ))
    
    # Insert topic groups
    for topic, ds_ids in topic_groups.items():
        for ds_id in ds_ids:
            cur.execute('INSERT INTO topic_groups VALUES (?, ?)', (topic, ds_id))
    
    # Create indexes
    cur.execute('CREATE INDEX idx_datasets_type ON datasets(type)')
    cur.execute('CREATE INDEX idx_datasets_province ON datasets(province)')
    cur.execute('CREATE INDEX idx_datasets_gem ON datasets(gem_score DESC)')
    cur.execute('CREATE INDEX idx_themes_theme ON dataset_themes(theme)')
    cur.execute('CREATE INDEX idx_topics_topic ON dataset_topics(topic)')
    cur.execute('CREATE INDEX idx_services_type ON dataset_services(service_type)')
    cur.execute('CREATE INDEX idx_groups_topic ON topic_groups(topic)')
    
    conn.commit()
    conn.close()
    
    print(f"Database created with {len(datasets)} datasets")

def generate_summary(datasets):
    """Generate a summary JSON for quick loading."""
    summary = {
        'total': len(datasets),
        'types': defaultdict(int),
        'provinces': defaultdict(int),
        'themes': defaultdict(int),
        'topics': defaultdict(int),
        'service_types': defaultdict(int),
        'gems': [],
    }
    
    for ds in datasets:
        summary['types'][ds['type']] += 1
        if ds['province']:
            summary['provinces'][ds['province']] += 1
        for theme in ds['themes']:
            summary['themes'][theme] += 1
        for topic in ds['topics']:
            summary['topics'][topic] += 1
        for svc in ds['services']:
            summary['service_types'][svc['type']] += 1
        
        # Collect top gems
        if ds['gem_score'] >= 8:
            summary['gems'].append({
                'id': ds['id'],
                'title': ds['title'],
                'score': ds['gem_score'],
                'topics': ds['topics'],
                'province': ds['province'],
                'services': [s['type'] for s in ds['services']]
            })
    
    # Convert defaultdicts to regular dicts for JSON
    summary['types'] = dict(summary['types'])
    summary['provinces'] = dict(summary['provinces'])
    summary['themes'] = dict(summary['themes'])
    summary['topics'] = dict(summary['topics'])
    summary['service_types'] = dict(summary['service_types'])
    
    # Sort gems by score
    summary['gems'].sort(key=lambda x: -x['score'])
    summary['gems'] = summary['gems'][:100]  # Top 100
    
    with open('summary.json', 'w') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"Summary written with {len(summary['gems'])} gems")
    return summary

if __name__ == '__main__':
    print("Loading datasets...")
    datasets = load_all_datasets()
    print(f"Loaded {len(datasets)} datasets")
    
    print("Building topic groups...")
    topic_groups = build_topic_groups(datasets)
    print(f"Created {len(topic_groups)} topic groups")
    
    print("Creating database...")
    create_database(datasets, topic_groups)
    
    print("Generating summary...")
    summary = generate_summary(datasets)
    
    print("\n=== Summary ===")
    print(f"Total datasets: {summary['total']}")
    print(f"\nBy type: {summary['types']}")
    print(f"\nBy province: {dict(sorted(summary['provinces'].items()))}")
    print(f"\nTop themes: {dict(sorted(summary['themes'].items(), key=lambda x: -x[1])[:10])}")
    print(f"\nTop topics: {dict(sorted(summary['topics'].items(), key=lambda x: -x[1])[:10])}")
    print(f"\nService types: {summary['service_types']}")
    print(f"\nTop gems: {[g['title'][:50] for g in summary['gems'][:10]]}")
