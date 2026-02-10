#!/usr/bin/env python3
"""INSPIRE Austria Search Server - German Web App with API."""

import json
import sqlite3
import random
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import threading

DB_PATH = 'inspire_austria.db'

def get_db():
    """Get database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

class InspireHandler(BaseHTTPRequestHandler):
    def send_json(self, data, status=200):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))
    
    def send_html(self, content):
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(content.encode('utf-8'))
    
    def send_file(self, filepath, content_type):
        try:
            with open(filepath, 'rb') as f:
                content = f.read()
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.end_headers()
            self.wfile.write(content)
        except FileNotFoundError:
            self.send_error(404)
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        
        # Static files
        if path == '/' or path == '/index.html':
            self.send_file('static/index.html', 'text/html; charset=utf-8')
        elif path == '/style.css':
            self.send_file('static/style.css', 'text/css')
        elif path == '/app.js':
            self.send_file('static/app.js', 'application/javascript')
        
        # API endpoints
        elif path == '/api/search':
            self.handle_search(query)
        elif path == '/api/dataset':
            self.handle_dataset(query)
        elif path == '/api/topics':
            self.handle_topics()
        elif path == '/api/gems':
            self.handle_gems(query)
        elif path == '/api/summary':
            self.handle_summary()
        elif path == '/api/unified':
            self.handle_unified_search(query)
        elif path == '/api/prompt':
            self.handle_prompt(query)
        elif path == '/api/llm':
            self.handle_llm_api(query)
        elif path == '/api/concepts':
            self.handle_concepts(query)
        elif path == '/api/coverage':
            self.handle_coverage(query)
        elif path == '/api/validation':
            self.handle_validation()
        elif path == '/api/schema':
            self.handle_schema(query)
        elif path == '/api/fields':
            self.handle_fields(query)
        elif path == '/api/combine':
            self.handle_combine(query)
        elif path == '/api/smart-search':
            self.handle_smart_search(query)
        else:
            self.send_error(404)
    
    def handle_search(self, query):
        """Full-text search for datasets."""
        q = query.get('q', [''])[0]
        limit = int(query.get('limit', ['50'])[0])
        offset = int(query.get('offset', ['0'])[0])
        type_filter = query.get('type', [None])[0]
        province_filter = query.get('province', [None])[0]
        topic_filter = query.get('topic', [None])[0]
        service_filter = query.get('service', [None])[0]
        
        conn = get_db()
        cur = conn.cursor()
        
        if q:
            # FTS search with ranking
            sql = '''
                SELECT d.*, fts.rank
                FROM datasets d
                JOIN datasets_fts fts ON d.id = fts.id
                WHERE datasets_fts MATCH ?
            '''
            params = [q]
        else:
            sql = 'SELECT d.*, 0 as rank FROM datasets d WHERE 1=1'
            params = []
        
        if type_filter:
            sql += ' AND d.type = ?'
            params.append(type_filter)
        
        if province_filter:
            sql += ' AND d.province = ?'
            params.append(province_filter)
        
        if topic_filter:
            sql += ' AND d.id IN (SELECT dataset_id FROM dataset_topics WHERE topic = ?)'
            params.append(topic_filter)
        
        if service_filter:
            sql += ' AND d.id IN (SELECT dataset_id FROM dataset_services WHERE service_type = ?)'
            params.append(service_filter)
        
        # Count total
        count_sql = f'SELECT COUNT(*) FROM ({sql})'
        cur.execute(count_sql, params)
        total = cur.fetchone()[0]
        
        # Get results
        sql += ' ORDER BY d.gem_score DESC, rank LIMIT ? OFFSET ?'
        params.extend([limit, offset])
        
        cur.execute(sql, params)
        rows = cur.fetchall()
        
        results = []
        for row in rows:
            ds_id = row['id']
            
            # Get themes, topics, services
            cur.execute('SELECT theme FROM dataset_themes WHERE dataset_id = ?', (ds_id,))
            themes = [r[0] for r in cur.fetchall()]
            
            cur.execute('SELECT topic FROM dataset_topics WHERE dataset_id = ?', (ds_id,))
            topics = [r[0] for r in cur.fetchall()]
            
            cur.execute('SELECT service_type, url FROM dataset_services WHERE dataset_id = ?', (ds_id,))
            services = [{'type': r[0], 'url': r[1]} for r in cur.fetchall()]
            
            results.append({
                'id': row['id'],
                'title': row['title'],
                'abstract': row['abstract'][:500] if row['abstract'] else '',
                'type': row['type'],
                'province': row['province'],
                'year': row['year'],
                'themes': themes,
                'topics': topics,
                'services': services,
                'gem_score': row['gem_score'],
                'is_open_data': bool(row['is_open_data']),
                'org': row['org']
            })
        
        conn.close()
        self.send_json({'total': total, 'results': results})
    
    def handle_dataset(self, query):
        """Get full dataset details."""
        ds_id = query.get('id', [None])[0]
        if not ds_id:
            self.send_json({'error': 'id required'}, 400)
            return
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute('SELECT * FROM datasets WHERE id = ?', (ds_id,))
        row = cur.fetchone()
        
        if not row:
            self.send_json({'error': 'not found'}, 404)
            return
        
        # Get all related data
        cur.execute('SELECT theme FROM dataset_themes WHERE dataset_id = ?', (ds_id,))
        themes = [r[0] for r in cur.fetchall()]
        
        cur.execute('SELECT topic FROM dataset_topics WHERE dataset_id = ?', (ds_id,))
        topics = [r[0] for r in cur.fetchall()]
        
        cur.execute('SELECT keyword FROM dataset_keywords WHERE dataset_id = ?', (ds_id,))
        keywords = [r[0] for r in cur.fetchall()]
        
        cur.execute('SELECT service_type, url, protocol FROM dataset_services WHERE dataset_id = ?', (ds_id,))
        services = [{'type': r[0], 'url': r[1], 'protocol': r[2]} for r in cur.fetchall()]
        
        cur.execute('SELECT format FROM dataset_formats WHERE dataset_id = ?', (ds_id,))
        formats = [r[0] for r in cur.fetchall()]
        
        result = {
            'id': row['id'],
            'uuid': row['uuid'],
            'title': row['title'],
            'abstract': row['abstract'],
            'type': row['type'],
            'province': row['province'],
            'year': row['year'],
            'themes': themes,
            'topics': topics,
            'keywords': keywords,
            'services': services,
            'formats': formats,
            'gem_score': row['gem_score'],
            'is_open_data': bool(row['is_open_data']),
            'org': row['org'],
            'contact': row['contact'],
            'create_date': row['create_date'],
            'update_date': row['update_date'],
            'bbox': json.loads(row['bbox']) if row['bbox'] else None,
            'inspire_url': f"https://geometadatensuche.inspire.gv.at/metadatensuche/inspire/ger/catalog.search#/metadata/{row['uuid']}"
        }
        
        conn.close()
        self.send_json(result)
    
    def handle_topics(self):
        """Get all topics with counts."""
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute('''
            SELECT topic, COUNT(*) as count 
            FROM dataset_topics 
            GROUP BY topic 
            ORDER BY count DESC
        ''')
        topics = [{'topic': r[0], 'count': r[1]} for r in cur.fetchall()]
        
        conn.close()
        self.send_json({'topics': topics})
    
    def handle_gems(self, query):
        """Get top gems, optionally random selection."""
        limit = int(query.get('limit', ['10'])[0])
        random_selection = query.get('random', ['false'])[0] == 'true'
        
        conn = get_db()
        cur = conn.cursor()
        
        if random_selection:
            # Get all gems with score >= 8, then random sample
            cur.execute('SELECT id, title, gem_score, province FROM datasets WHERE gem_score >= 8')
            all_gems = cur.fetchall()
            selected = random.sample(all_gems, min(limit, len(all_gems)))
            gems = [{'id': g[0], 'title': g[1], 'score': g[2], 'province': g[3]} for g in selected]
        else:
            cur.execute('''
                SELECT id, title, gem_score, province 
                FROM datasets 
                WHERE gem_score >= 6
                ORDER BY gem_score DESC 
                LIMIT ?
            ''', (limit,))
            gems = [{'id': r[0], 'title': r[1], 'score': r[2], 'province': r[3]} for r in cur.fetchall()]
        
        conn.close()
        self.send_json({'gems': gems})
    
    def handle_summary(self):
        """Get index summary."""
        with open('summary.json') as f:
            summary = json.load(f)
        self.send_json(summary)
    
    def handle_unified_search(self, query):
        """Unified search that groups related datasets across provinces."""
        q = query.get('q', [''])[0]
        
        conn = get_db()
        cur = conn.cursor()
        
        # First find matching topic
        topic_match = None
        from build_index import TOPIC_KEYWORDS
        q_lower = q.lower()
        for topic, keywords in TOPIC_KEYWORDS.items():
            if any(kw in q_lower for kw in keywords):
                topic_match = topic
                break
        
        result = {
            'query': q,
            'matched_topic': topic_match,
            'national': [],
            'by_province': {},
            'services': {'WFS': [], 'WMS': [], 'OGC-API': [], 'Download': []}
        }
        
        # Search datasets
        if q:
            cur.execute('''
                SELECT d.id, d.title, d.province, d.gem_score, d.type
                FROM datasets d
                JOIN datasets_fts fts ON d.id = fts.id
                WHERE datasets_fts MATCH ?
                ORDER BY d.gem_score DESC
            ''', (q,))
        elif topic_match:
            cur.execute('''
                SELECT d.id, d.title, d.province, d.gem_score, d.type
                FROM datasets d
                JOIN dataset_topics t ON d.id = t.dataset_id
                WHERE t.topic = ?
                ORDER BY d.gem_score DESC
            ''', (topic_match,))
        else:
            self.send_json({'error': 'query required'})
            return
        
        rows = cur.fetchall()
        
        for row in rows:
            ds_id, title, province, score, ds_type = row
            
            # Get services
            cur.execute('SELECT service_type, url FROM dataset_services WHERE dataset_id = ?', (ds_id,))
            services = cur.fetchall()
            
            entry = {'id': ds_id, 'title': title, 'gem_score': score, 'type': ds_type}
            
            if not province or 'Österreich' in title.lower() or 'austria' in title.lower():
                result['national'].append(entry)
            else:
                if province not in result['by_province']:
                    result['by_province'][province] = []
                result['by_province'][province].append(entry)
            
            # Collect services by type
            for svc_type, url in services:
                if svc_type in result['services']:
                    result['services'][svc_type].append({'id': ds_id, 'title': title, 'url': url})
        
        conn.close()
        self.send_json(result)
    
    def handle_prompt(self, query):
        """Generate Shelley prompt for selected datasets."""
        ids = query.get('ids', [''])[0].split(',')
        ids = [i.strip() for i in ids if i.strip()]
        
        if not ids:
            self.send_json({'error': 'ids required'}, 400)
            return
        
        conn = get_db()
        cur = conn.cursor()
        
        datasets_info = []
        for ds_id in ids:
            cur.execute('SELECT title, type, province FROM datasets WHERE id = ?', (ds_id,))
            row = cur.fetchone()
            if row:
                cur.execute('SELECT service_type, url FROM dataset_services WHERE dataset_id = ?', (ds_id,))
                services = [{'type': r[0], 'url': r[1]} for r in cur.fetchall()]
                datasets_info.append({
                    'title': row[0],
                    'type': row[1],
                    'province': row[2],
                    'services': services
                })
        
        conn.close()
        
        # Generate prompt
        prompt_lines = ["Arbeite mit folgenden österreichischen INSPIRE-Geodatensätzen:\n"]
        
        for ds in datasets_info:
            prompt_lines.append(f"**{ds['title']}** ({ds['type']}, {ds['province'] or 'Österreich'})")
            for svc in ds['services']:
                if svc['type'] in ('WFS', 'OGC-API'):
                    prompt_lines.append(f"  - {svc['type']}: {svc['url']}")
            prompt_lines.append("")
        
        prompt_lines.append("\nLade die Daten, analysiere die Struktur und erstelle eine Zusammenfassung.")
        
        prompt = '\n'.join(prompt_lines)
        self.send_json({'prompt': prompt, 'datasets': datasets_info})
    
    def handle_llm_api(self, query):
        """LLM-optimized API for other Claude instances.
        
        Provides compact, token-efficient responses for programmatic access.
        """
        action = query.get('action', ['help'])[0]
        
        if action == 'help':
            self.send_json({
                'api': 'INSPIRE Austria LLM API v1',
                'description': 'Österreichische Geodaten-Suche für LLM-Agenten',
                'endpoints': {
                    'search': '/api/llm?action=search&q=QUERY - Suche Datensätze',
                    'topic': '/api/llm?action=topic&name=NAME - Alle Daten zu einem Thema',
                    'services': '/api/llm?action=services&type=WFS|WMS|OGC-API - Verfügbare Services',
                    'gems': '/api/llm?action=gems - Top-Datensätze',
                    'access': '/api/llm?action=access&id=ID - Zugriffsinformationen'
                },
                'topics': ['grundwasser', 'wetter', 'hochwasser', 'gewässer', 'boden', 'wald', 
                          'naturschutz', 'kataster', 'raumordnung', 'verkehr', 'energie', 
                          'geologie', 'höhenmodell', 'orthofoto', 'adresse', 'gebäude',
                          'bevölkerung', 'landwirtschaft', 'gesundheit', 'umwelt']
            })
        
        elif action == 'search':
            q = query.get('q', [''])[0]
            conn = get_db()
            cur = conn.cursor()
            cur.execute('''
                SELECT d.id, d.title, d.type, d.province, d.gem_score
                FROM datasets d
                JOIN datasets_fts fts ON d.id = fts.id
                WHERE datasets_fts MATCH ?
                ORDER BY d.gem_score DESC
                LIMIT 20
            ''', (q,))
            results = [{'id': r[0], 't': r[1], 'type': r[2], 'prov': r[3], 'gem': r[4]} for r in cur.fetchall()]
            conn.close()
            self.send_json({'q': q, 'n': len(results), 'r': results})
        
        elif action == 'topic':
            name = query.get('name', [''])[0]
            conn = get_db()
            cur = conn.cursor()
            cur.execute('''
                SELECT d.id, d.title, d.type, d.province
                FROM datasets d
                JOIN dataset_topics t ON d.id = t.dataset_id
                WHERE t.topic = ?
                ORDER BY d.gem_score DESC
                LIMIT 30
            ''', (name,))
            results = [{'id': r[0], 't': r[1], 'type': r[2], 'prov': r[3]} for r in cur.fetchall()]
            conn.close()
            self.send_json({'topic': name, 'n': len(results), 'r': results})
        
        elif action == 'services':
            svc_type = query.get('type', ['WFS'])[0]
            conn = get_db()
            cur = conn.cursor()
            cur.execute('''
                SELECT DISTINCT d.id, d.title, s.url
                FROM datasets d
                JOIN dataset_services s ON d.id = s.dataset_id
                WHERE s.service_type = ?
                ORDER BY d.gem_score DESC
                LIMIT 50
            ''', (svc_type,))
            results = [{'id': r[0], 't': r[1], 'url': r[2]} for r in cur.fetchall()]
            conn.close()
            self.send_json({'type': svc_type, 'n': len(results), 'r': results})
        
        elif action == 'gems':
            conn = get_db()
            cur = conn.cursor()
            cur.execute('''
                SELECT d.id, d.title, d.gem_score, d.province
                FROM datasets d
                WHERE d.gem_score >= 8
                ORDER BY d.gem_score DESC
                LIMIT 20
            ''')
            results = [{'id': r[0], 't': r[1], 'gem': r[2], 'prov': r[3]} for r in cur.fetchall()]
            conn.close()
            self.send_json({'n': len(results), 'r': results})
        
        elif action == 'access':
            ds_id = query.get('id', [''])[0]
            conn = get_db()
            cur = conn.cursor()
            cur.execute('SELECT title, uuid, abstract FROM datasets WHERE id = ?', (ds_id,))
            row = cur.fetchone()
            if not row:
                self.send_json({'error': 'not found'}, 404)
                return
            
            cur.execute('SELECT service_type, url FROM dataset_services WHERE dataset_id = ?', (ds_id,))
            services = [{'type': r[0], 'url': r[1]} for r in cur.fetchall()]
            
            conn.close()
            self.send_json({
                'id': ds_id,
                't': row[0],
                'uuid': row[1],
                'abs': row[2][:300] if row[2] else '',
                'svc': services,
                'inspire': f"https://geometadatensuche.inspire.gv.at/metadatensuche/inspire/ger/catalog.search#/metadata/{row[1]}"
            })
        
        else:
            self.send_json({'error': 'unknown action'}, 400)
    
    def handle_concepts(self, query):
        """Get all concepts with their coverage."""
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute('''
            SELECT c.id, c.name_de, c.name_en, c.regional_names,
                   COUNT(DISTINCT dc.dataset_id) as dataset_count,
                   COUNT(DISTINCT d.province) as province_count,
                   COUNT(DISTINCT CASE WHEN s.service_type = 'WFS' THEN d.id END) as wfs_count
            FROM concepts c
            LEFT JOIN dataset_concepts dc ON c.id = dc.concept_id
            LEFT JOIN datasets d ON dc.dataset_id = d.id
            LEFT JOIN dataset_services s ON d.id = s.dataset_id
            GROUP BY c.id
            ORDER BY dataset_count DESC
        ''')
        
        concepts = []
        for row in cur.fetchall():
            concepts.append({
                'id': row[0],
                'name_de': row[1],
                'name_en': row[2],
                'regional_names': json.loads(row[3]) if row[3] else {},
                'datasets': row[4],
                'provinces': row[5],
                'wfs_count': row[6]
            })
        
        conn.close()
        self.send_json({'concepts': concepts})
    
    def handle_coverage(self, query):
        """Get coverage matrix: concept x province."""
        concept_id = query.get('concept', [None])[0]
        
        conn = get_db()
        cur = conn.cursor()
        
        if concept_id:
            # Get all datasets for a specific concept, grouped by province
            cur.execute('''
                SELECT d.province, d.id, d.title, d.gem_score,
                       GROUP_CONCAT(DISTINCT s.service_type) as services
                FROM dataset_concepts dc
                JOIN datasets d ON dc.dataset_id = d.id
                LEFT JOIN dataset_services s ON d.id = s.dataset_id
                WHERE dc.concept_id = ?
                GROUP BY d.id
                ORDER BY d.province, d.gem_score DESC
            ''', (concept_id,))
            
            by_province = {}
            for row in cur.fetchall():
                province = row[0] or 'National'
                if province not in by_province:
                    by_province[province] = []
                by_province[province].append({
                    'id': row[1],
                    'title': row[2],
                    'gem_score': row[3],
                    'services': row[4].split(',') if row[4] else []
                })
            
            # Get concept info
            cur.execute('SELECT name_de, name_en, regional_names FROM concepts WHERE id = ?', (concept_id,))
            concept_row = cur.fetchone()
            
            conn.close()
            self.send_json({
                'concept': concept_id,
                'name_de': concept_row[0] if concept_row else '',
                'name_en': concept_row[1] if concept_row else '',
                'regional_names': json.loads(concept_row[2]) if concept_row and concept_row[2] else {},
                'by_province': by_province
            })
        else:
            # Return coverage matrix
            cur.execute('''
                SELECT c.id, c.name_de, d.province, 
                       COUNT(DISTINCT d.id) as count,
                       SUM(CASE WHEN s.service_type = 'WFS' THEN 1 ELSE 0 END) > 0 as has_wfs
                FROM concepts c
                JOIN dataset_concepts dc ON c.id = dc.concept_id
                JOIN datasets d ON dc.dataset_id = d.id
                LEFT JOIN dataset_services s ON d.id = s.dataset_id
                GROUP BY c.id, d.province
            ''')
            
            matrix = {}
            for row in cur.fetchall():
                concept = row[0]
                if concept not in matrix:
                    matrix[concept] = {'name': row[1], 'provinces': {}}
                province = row[2] or 'National'
                matrix[concept]['provinces'][province] = {
                    'count': row[3],
                    'has_wfs': bool(row[4])
                }
            
            conn.close()
            self.send_json({'matrix': matrix})
    
    def handle_validation(self):
        """Get link validation results."""
        try:
            with open('link_validation_results.json') as f:
                results = json.load(f)
            self.send_json(results)
        except FileNotFoundError:
            self.send_json({'error': 'No validation results yet'})
    
    def handle_schema(self, query):
        """Get schema info for a dataset."""
        dataset_id = query.get('id', [None])[0]
        
        conn = get_db()
        cur = conn.cursor()
        
        if dataset_id:
            cur.execute('''
                SELECT ft.type_name, ft.inspire_theme, f.field_name, f.field_type, f.is_geometry
                FROM wfs_feature_types ft
                LEFT JOIN wfs_fields f ON ft.id = f.feature_type_id
                WHERE ft.dataset_id = ?
            ''', (dataset_id,))
            
            rows = cur.fetchall()
            if not rows:
                self.send_json({'error': 'No schema found'})
                return
            
            feature_types = {}
            for type_name, theme, field_name, field_type, is_geom in rows:
                if type_name not in feature_types:
                    feature_types[type_name] = {
                        'name': type_name,
                        'theme': theme,
                        'fields': []
                    }
                if field_name:
                    feature_types[type_name]['fields'].append({
                        'name': field_name,
                        'type': field_type,
                        'is_geometry': bool(is_geom)
                    })
            
            conn.close()
            self.send_json({'feature_types': list(feature_types.values())})
        else:
            # Return schema statistics
            cur.execute('SELECT COUNT(*) FROM wfs_feature_types')
            ft_count = cur.fetchone()[0]
            cur.execute('SELECT COUNT(*) FROM wfs_fields')
            field_count = cur.fetchone()[0]
            cur.execute('SELECT inspire_theme, COUNT(*) FROM wfs_feature_types WHERE inspire_theme IS NOT NULL GROUP BY inspire_theme')
            themes = dict(cur.fetchall())
            
            conn.close()
            self.send_json({
                'feature_types': ft_count,
                'fields': field_count,
                'themes': themes
            })
    
    def handle_fields(self, query):
        """Get canonical field mappings."""
        field_name = query.get('name', [None])[0]
        theme = query.get('theme', [None])[0]
        
        conn = get_db()
        cur = conn.cursor()
        
        if field_name:
            # Look up canonical field
            cur.execute('''
                SELECT cf.id, cf.type, cf.description_de, cf.description_en, fs.source
                FROM field_synonyms fs
                JOIN canonical_fields cf ON fs.canonical_id = cf.id
                WHERE LOWER(fs.field_name) = LOWER(?)
            ''', (field_name,))
            
            row = cur.fetchone()
            if row:
                # Get all synonyms
                cur.execute('SELECT source, field_name FROM field_synonyms WHERE canonical_id = ?', (row[0],))
                synonyms = cur.fetchall()
                
                conn.close()
                self.send_json({
                    'canonical_id': row[0],
                    'type': row[1],
                    'description_de': row[2],
                    'description_en': row[3],
                    'synonyms': [{'source': s[0], 'name': s[1]} for s in synonyms]
                })
            else:
                conn.close()
                self.send_json({'error': 'Field not found'})
        else:
            # Return all canonical fields
            cur.execute('SELECT id, type, description_de, description_en FROM canonical_fields')
            fields = []
            for row in cur.fetchall():
                cur.execute('SELECT source, field_name FROM field_synonyms WHERE canonical_id = ?', (row[0],))
                synonyms = cur.fetchall()
                fields.append({
                    'id': row[0],
                    'type': row[1],
                    'description_de': row[2],
                    'description_en': row[3],
                    'synonyms': [{'source': s[0], 'name': s[1]} for s in synonyms]
                })
            
            conn.close()
            self.send_json({'fields': fields})
    
    def handle_combine(self, query):
        """Analyze how to combine datasets across provinces."""
        concept_id = query.get('concept', [None])[0]
        ids = query.get('ids', [''])[0].split(',')
        ids = [i.strip() for i in ids if i.strip()]
        
        conn = get_db()
        cur = conn.cursor()
        
        if concept_id:
            # Get all datasets for this concept with their schemas
            cur.execute('''
                SELECT d.id, d.title, d.province, d.gem_score,
                       ft.type_name, ft.inspire_theme,
                       GROUP_CONCAT(DISTINCT s.service_type) as services,
                       GROUP_CONCAT(DISTINCT f.field_name) as fields
                FROM dataset_concepts dc
                JOIN datasets d ON dc.dataset_id = d.id
                LEFT JOIN wfs_feature_types ft ON d.id = ft.dataset_id
                LEFT JOIN wfs_fields f ON ft.id = f.feature_type_id
                LEFT JOIN dataset_services s ON d.id = s.dataset_id
                WHERE dc.concept_id = ?
                GROUP BY d.id
                ORDER BY d.gem_score DESC
            ''', (concept_id,))
        elif ids:
            placeholders = ','.join('?' * len(ids))
            cur.execute(f'''
                SELECT d.id, d.title, d.province, d.gem_score,
                       ft.type_name, ft.inspire_theme,
                       GROUP_CONCAT(DISTINCT s.service_type) as services,
                       GROUP_CONCAT(DISTINCT f.field_name) as fields
                FROM datasets d
                LEFT JOIN wfs_feature_types ft ON d.id = ft.dataset_id
                LEFT JOIN wfs_fields f ON ft.id = f.feature_type_id
                LEFT JOIN dataset_services s ON d.id = s.dataset_id
                WHERE d.id IN ({placeholders})
                GROUP BY d.id
            ''', ids)
        else:
            conn.close()
            self.send_json({'error': 'concept or ids required'})
            return
        
        rows = cur.fetchall()
        
        # Analyze compatibility
        datasets = []
        all_fields = set()
        field_counts = {}  # Count how often each field appears
        provinces_covered = set()
        has_wfs = []
        datasets_with_fields = 0
        
        for row in rows:
            ds_id, title, province, gem, type_name, theme, services, fields = row
            field_set = set(fields.split(',')) if fields else set()
            svc_set = set(services.split(',')) if services else set()
            
            all_fields.update(field_set)
            if field_set:
                datasets_with_fields += 1
                for f in field_set:
                    field_counts[f] = field_counts.get(f, 0) + 1
            
            if province:
                provinces_covered.add(province)
            
            # Get WFS URL if available
            wfs_url = None
            if 'WFS' in svc_set:
                cur.execute('''
                    SELECT url FROM dataset_services 
                    WHERE dataset_id = ? AND service_type = 'WFS' LIMIT 1
                ''', (ds_id,))
                wfs_row = cur.fetchone()
                if wfs_row:
                    wfs_url = wfs_row[0]
                    has_wfs.append({'id': ds_id, 'title': title, 'province': province, 'url': wfs_url, 'fields': list(field_set)})
            
            datasets.append({
                'id': ds_id,
                'title': title,
                'province': province or 'National',
                'gem_score': gem,
                'type_name': type_name,
                'theme': theme,
                'services': list(svc_set),
                'fields': list(field_set),
                'wfs_url': wfs_url
            })
        
        # Common fields = fields appearing in at least 50% of datasets with schemas
        threshold = max(2, datasets_with_fields * 0.5)
        common_fields = set(f for f, count in field_counts.items() if count >= threshold)
        
        # Calculate coverage
        all_provinces = {'Wien', 'Niederösterreich', 'Oberösterreich', 'Salzburg', 
                        'Tirol', 'Vorarlberg', 'Kärnten', 'Steiermark', 'Burgenland'}
        missing_provinces = all_provinces - provinces_covered
        
        # Get canonical field mappings for common fields
        field_mappings = []
        for field in (common_fields or []):
            cur.execute('''
                SELECT cf.id, cf.description_de
                FROM field_synonyms fs
                JOIN canonical_fields cf ON fs.canonical_id = cf.id
                WHERE LOWER(fs.field_name) = LOWER(?)
            ''', (field,))
            mapping = cur.fetchone()
            if mapping:
                field_mappings.append({'field': field, 'canonical': mapping[0], 'description': mapping[1]})
        
        conn.close()
        
        # Generate combination advice
        combinable = len(has_wfs) >= 2
        
        result = {
            'concept': concept_id,
            'datasets': datasets,
            'analysis': {
                'total_datasets': len(datasets),
                'provinces_covered': list(provinces_covered),
                'missing_provinces': list(missing_provinces),
                'coverage_pct': len(provinces_covered) / len(all_provinces) * 100,
                'datasets_with_wfs': len(has_wfs),
                'common_fields': list(common_fields) if common_fields else [],
                'all_fields': list(all_fields),
                'field_mappings': field_mappings,
                'combinable': combinable
            },
            'wfs_services': has_wfs
        }
        
        # Generate combination prompt
        if combinable:
            result['combination_prompt'] = self.generate_combination_prompt(concept_id, has_wfs, common_fields, field_mappings)
        
        self.send_json(result)
    
    def generate_combination_prompt(self, concept, wfs_services, common_fields, field_mappings):
        """Generate a Shelley prompt for combining datasets."""
        lines = [f"Kombiniere folgende {concept}-Datensätze zu einem österreichweiten Datensatz:\n"]
        
        for svc in wfs_services[:6]:  # Limit to 6
            lines.append(f"**{svc['title']}** ({svc['province'] or 'National'})")
            lines.append(f"  WFS: {svc['url']}")
            if svc.get('fields'):
                lines.append(f"  Felder: {', '.join(svc['fields'][:8])}")
        
        if common_fields:
            lines.append(f"\n**Gemeinsame/ähnliche Felder:** {', '.join(list(common_fields)[:10])}")
        
        if field_mappings:
            lines.append("\n**Kanonische Feldnamen (für Vereinheitlichung):**")
            for fm in field_mappings[:8]:
                lines.append(f"  - {fm['field']} → {fm['canonical']} ({fm['description']})")
        
        lines.append("\n**Aufgabe:**")
        lines.append("1. Lade alle WFS-Daten mit OWSLib oder requests als GeoDataFrames")
        lines.append("2. Prüfe die Spaltennamen jedes DataFrames")
        lines.append("3. Mappe auf gemeinsame Spaltennamen (siehe Feldmappings)")
        lines.append("4. Füge 'bundesland' Spalte hinzu")
        lines.append("5. Kombiniere mit gpd.pd.concat()")
        lines.append("6. Exportiere als GeoPackage: combined.to_file('output.gpkg', driver='GPKG')")
        
        return '\n'.join(lines)
    
    def handle_smart_search(self, query):
        """Enhanced search with concept grouping and combination suggestions."""
        q = query.get('q', [''])[0]
        
        if not q:
            self.send_json({'error': 'query required'})
            return
        
        conn = get_db()
        cur = conn.cursor()
        
        # Find matching concepts
        q_lower = q.lower()
        matched_concepts = []
        
        cur.execute('SELECT id, name_de, name_en FROM concepts')
        for cid, name_de, name_en in cur.fetchall():
            if q_lower in name_de.lower() or q_lower in name_en.lower() or q_lower in cid:
                # Get stats for this concept
                cur.execute('''
                    SELECT COUNT(DISTINCT d.id), COUNT(DISTINCT d.province),
                           SUM(CASE WHEN s.service_type = 'WFS' THEN 1 ELSE 0 END)
                    FROM dataset_concepts dc
                    JOIN datasets d ON dc.dataset_id = d.id
                    LEFT JOIN dataset_services s ON d.id = s.dataset_id
                    WHERE dc.concept_id = ?
                ''', (cid,))
                stats = cur.fetchone()
                
                matched_concepts.append({
                    'id': cid,
                    'name_de': name_de,
                    'name_en': name_en,
                    'datasets': stats[0],
                    'provinces': stats[1],
                    'wfs_count': stats[2] or 0
                })
        
        # FTS search for datasets
        cur.execute('''
            SELECT d.id, d.title, d.province, d.gem_score, d.type,
                   GROUP_CONCAT(DISTINCT dc.concept_id) as concepts,
                   GROUP_CONCAT(DISTINCT s.service_type) as services
            FROM datasets d
            JOIN datasets_fts fts ON d.id = fts.id
            LEFT JOIN dataset_concepts dc ON d.id = dc.dataset_id
            LEFT JOIN dataset_services s ON d.id = s.dataset_id
            WHERE datasets_fts MATCH ?
            GROUP BY d.id
            ORDER BY d.gem_score DESC
            LIMIT 50
        ''', (q,))
        
        datasets = []
        by_concept = {}
        by_province = {}
        
        for row in cur.fetchall():
            ds_id, title, province, gem, dtype, concepts, services = row
            concept_list = concepts.split(',') if concepts else []
            service_list = services.split(',') if services else []
            
            ds = {
                'id': ds_id,
                'title': title,
                'province': province or 'National',
                'gem_score': gem,
                'type': dtype,
                'concepts': concept_list,
                'services': service_list,
                'has_wfs': 'WFS' in service_list
            }
            datasets.append(ds)
            
            # Group by concept
            for c in concept_list:
                if c not in by_concept:
                    by_concept[c] = []
                by_concept[c].append(ds)
            
            # Group by province
            prov = province or 'National'
            if prov not in by_province:
                by_province[prov] = []
            by_province[prov].append(ds)
        
        # Find combinable groups (same concept, multiple provinces, have WFS)
        combinable_groups = []
        for concept_id, ds_list in by_concept.items():
            provinces = set(d['province'] for d in ds_list)
            wfs_count = sum(1 for d in ds_list if d['has_wfs'])
            
            if len(provinces) >= 2 and wfs_count >= 2:
                cur.execute('SELECT name_de FROM concepts WHERE id = ?', (concept_id,))
                name_row = cur.fetchone()
                
                combinable_groups.append({
                    'concept': concept_id,
                    'name': name_row[0] if name_row else concept_id,
                    'provinces': list(provinces),
                    'dataset_count': len(ds_list),
                    'wfs_count': wfs_count,
                    'datasets': [d['id'] for d in ds_list if d['has_wfs']][:5]
                })
        
        conn.close()
        
        self.send_json({
            'query': q,
            'matched_concepts': matched_concepts,
            'datasets': datasets,
            'by_province': {k: len(v) for k, v in by_province.items()},
            'combinable_groups': combinable_groups,
            'total': len(datasets)
        })
    
    def log_message(self, format, *args):
        print(f"[{self.client_address[0]}] {args[0]}")

def run_server(port=8000):
    server = HTTPServer(('0.0.0.0', port), InspireHandler)
    print(f"Server running on http://localhost:{port}")
    print(f"Public URL: https://inspire-austria.exe.xyz:{port}")
    server.serve_forever()

if __name__ == '__main__':
    run_server()
