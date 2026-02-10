#!/usr/bin/env python3
"""Update database with concept mappings."""

import sqlite3
import json
import re
from concept_mappings import CONCEPT_MAPPINGS, get_concept_for_dataset

DB_PATH = 'inspire_austria.db'

def init_concept_tables():
    """Create tables for concept mappings."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Concepts table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS concepts (
            id TEXT PRIMARY KEY,
            name_de TEXT,
            name_en TEXT,
            patterns TEXT,
            regional_names TEXT
        )
    ''')
    
    # Dataset-concept mapping
    cur.execute('''
        CREATE TABLE IF NOT EXISTS dataset_concepts (
            dataset_id TEXT,
            concept_id TEXT,
            FOREIGN KEY (dataset_id) REFERENCES datasets(id),
            FOREIGN KEY (concept_id) REFERENCES concepts(id)
        )
    ''')
    
    cur.execute('CREATE INDEX IF NOT EXISTS idx_dc_dataset ON dataset_concepts(dataset_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_dc_concept ON dataset_concepts(concept_id)')
    
    conn.commit()
    conn.close()

def populate_concepts():
    """Populate concepts table from mappings."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Clear existing
    cur.execute('DELETE FROM concepts')
    
    for concept_id, data in CONCEPT_MAPPINGS.items():
        cur.execute('''
            INSERT INTO concepts (id, name_de, name_en, patterns, regional_names)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            concept_id,
            data['de'],
            data['en'],
            json.dumps(data['patterns']),
            json.dumps(data.get('regional_names', {}))
        ))
    
    conn.commit()
    conn.close()
    print(f"Populated {len(CONCEPT_MAPPINGS)} concepts")

def map_datasets_to_concepts():
    """Map all datasets to their concepts."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Clear existing mappings
    cur.execute('DELETE FROM dataset_concepts')
    
    # Get all datasets
    cur.execute('SELECT id, title, abstract FROM datasets')
    datasets = cur.fetchall()
    
    mapped_count = 0
    for ds_id, title, abstract in datasets:
        concepts = get_concept_for_dataset(title, abstract or '')
        for c in concepts:
            cur.execute('INSERT INTO dataset_concepts VALUES (?, ?)', (ds_id, c['concept']))
            mapped_count += 1
    
    conn.commit()
    conn.close()
    print(f"Created {mapped_count} dataset-concept mappings")

def generate_unified_view():
    """Generate a view for unified search across provinces."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute('DROP VIEW IF EXISTS unified_datasets')
    
    cur.execute('''
        CREATE VIEW unified_datasets AS
        SELECT 
            c.id as concept_id,
            c.name_de as concept_name,
            d.province,
            COUNT(DISTINCT d.id) as dataset_count,
            GROUP_CONCAT(DISTINCT d.id) as dataset_ids,
            MAX(d.gem_score) as max_gem_score,
            SUM(CASE WHEN s.service_type = 'WFS' THEN 1 ELSE 0 END) as wfs_count,
            SUM(CASE WHEN s.service_type = 'OGC-API' THEN 1 ELSE 0 END) as ogcapi_count
        FROM concepts c
        JOIN dataset_concepts dc ON c.id = dc.concept_id
        JOIN datasets d ON dc.dataset_id = d.id
        LEFT JOIN dataset_services s ON d.id = s.dataset_id
        GROUP BY c.id, d.province
        ORDER BY c.name_de, d.province
    ''')
    
    conn.commit()
    conn.close()
    print("Created unified_datasets view")

def show_coverage_report():
    """Show coverage by concept and province."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute('''
        SELECT 
            c.name_de,
            COUNT(DISTINCT d.province) as provinces_covered,
            COUNT(DISTINCT d.id) as total_datasets,
            SUM(CASE WHEN s.service_type = 'WFS' THEN 1 ELSE 0 END) as wfs_services
        FROM concepts c
        JOIN dataset_concepts dc ON c.id = dc.concept_id
        JOIN datasets d ON dc.dataset_id = d.id
        LEFT JOIN dataset_services s ON d.id = s.dataset_id
        GROUP BY c.id
        ORDER BY total_datasets DESC
    ''')
    
    print("\n=== Concept Coverage Report ===")
    print(f"{'Concept':<30} {'Provinces':>10} {'Datasets':>10} {'WFS':>8}")
    print("-" * 62)
    
    for name, provinces, datasets, wfs in cur.fetchall():
        print(f"{name:<30} {provinces:>10} {datasets:>10} {wfs:>8}")
    
    # Show gaps
    print("\n=== Provincial Gaps ===")
    cur.execute('''
        SELECT c.name_de, 
               GROUP_CONCAT(DISTINCT COALESCE(d.province, 'National')) as has_data
        FROM concepts c
        JOIN dataset_concepts dc ON c.id = dc.concept_id
        JOIN datasets d ON dc.dataset_id = d.id
        GROUP BY c.id
    ''')
    
    all_provinces = {'Wien', 'Niederösterreich', 'Oberösterreich', 'Salzburg', 'Tirol', 
                     'Vorarlberg', 'Kärnten', 'Steiermark', 'Burgenland'}
    
    for name, has_data in cur.fetchall():
        covered = set(has_data.split(',')) if has_data else set()
        missing = all_provinces - covered
        if missing and 'National' not in covered:
            print(f"{name}: Missing {', '.join(sorted(missing))}")
    
    conn.close()

if __name__ == '__main__':
    print("Initializing concept tables...")
    init_concept_tables()
    
    print("Populating concepts...")
    populate_concepts()
    
    print("Mapping datasets to concepts...")
    map_datasets_to_concepts()
    
    print("Creating unified view...")
    generate_unified_view()
    
    show_coverage_report()
