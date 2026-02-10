#!/usr/bin/env python3
"""Field mappings across regional datasets.

Maps equivalent fields across different provincial naming conventions.
"""

import sqlite3
import json

DB_PATH = 'inspire_austria.db'

# Canonical field definitions with regional variations
# Format: canonical_name -> {type, description, regional_mappings}
FIELD_MAPPINGS = {
    # === COMMON INSPIRE FIELDS ===
    'inspire_id': {
        'type': 'string',
        'description_de': 'INSPIRE Identifikator',
        'description_en': 'INSPIRE Identifier',
        'mappings': {
            '_inspire': ['inspireId', 'inspireID', 'INSPIREID'],
            'Oberösterreich': ['InspireID', 'InspireId'],
        }
    },
    'geometry': {
        'type': 'geometry',
        'description_de': 'Geometrie',
        'description_en': 'Geometry',
        'mappings': {
            '_inspire': ['geometry', 'geometry2D', 'location'],
            'Oberösterreich': ['Shape', 'SHAPE'],
            '_arcgis': ['Shape', 'SHAPE', 'geom'],
        }
    },
    'name': {
        'type': 'string',
        'description_de': 'Name/Bezeichnung',
        'description_en': 'Name',
        'mappings': {
            '_inspire': ['name', 'siteName', 'geographicalName'],
            'Oberösterreich': ['Name', 'NAME', 'Bezeichnung'],
        }
    },
    'begin_lifespan': {
        'type': 'dateTime',
        'description_de': 'Gültig ab',
        'description_en': 'Valid from',
        'mappings': {
            '_inspire': ['beginLifespanVersion', 'beginLifeSpanVersion', 'validFrom'],
            'Oberösterreich': ['ValidFrom', 'GueltigAb'],
        }
    },
    
    # === AREA MANAGEMENT / PROTECTED SITES ===
    'zone_type': {
        'type': 'codelist',
        'description_de': 'Zonentyp',
        'description_en': 'Zone Type',
        'mappings': {
            '_inspire': ['zoneType', 'siteDesignation', 'siteProtectionClassification'],
        }
    },
    'legal_basis': {
        'type': 'string',
        'description_de': 'Rechtsgrundlage',
        'description_en': 'Legal Basis',
        'mappings': {
            '_inspire': ['legalBasis', 'legalFoundationDocument'],
            'Oberösterreich': ['OfficialDocument', 'Richtlinie'],
        }
    },
    'legal_date': {
        'type': 'date',
        'description_de': 'Rechtskraft-Datum',
        'description_en': 'Legal Foundation Date',
        'mappings': {
            '_inspire': ['legalFoundationDate'],
            'Oberösterreich': ['ValidFrom'],
        }
    },
    'authority': {
        'type': 'string',
        'description_de': 'Zuständige Behörde',
        'description_en': 'Competent Authority',
        'mappings': {
            '_inspire': ['competentAuthority'],
        }
    },
    
    # === LAND USE / ZONING ===
    'land_use_type': {
        'type': 'codelist',
        'description_de': 'Nutzungsart',
        'description_en': 'Land Use Type',
        'mappings': {
            '_inspire': ['hilucsLandUse', 'specificLandUse'],
            'Oberösterreich': ['RegulationNature', 'SpecificRegulationNature', 'KENNZAHL'],
            'Tirol': ['WIDMUNG', 'Widmungsart'],
        }
    },
    'supplementary_regulation': {
        'type': 'string',
        'description_de': 'Ergänzende Bestimmung',
        'description_en': 'Supplementary Regulation',
        'mappings': {
            '_inspire': ['supplementaryRegulation'],
            'Oberösterreich': ['SupplementaryRegulation', 'ZUSATZTEXT'],
        }
    },
    'background_map': {
        'type': 'string',
        'description_de': 'Kartengrundlage',
        'description_en': 'Background Map',
        'mappings': {
            'Oberösterreich': ['BackgroundMap', 'BackgroundMapDate'],
        }
    },
    
    # === TRANSPORT ===
    'road_class': {
        'type': 'codelist',
        'description_de': 'Straßenklasse',
        'description_en': 'Functional Road Class',
        'mappings': {
            '_inspire': ['functionalClass'],
        }
    },
    'num_lanes': {
        'type': 'integer',
        'description_de': 'Anzahl Fahrspuren',
        'description_en': 'Number of Lanes',
        'mappings': {
            '_inspire': ['numberOfLanes'],
        }
    },
    'traffic_direction': {
        'type': 'codelist',
        'description_de': 'Verkehrsrichtung',
        'description_en': 'Traffic Direction',
        'mappings': {
            '_inspire': ['direction'],
        }
    },
    
    # === HYDROGRAPHY ===
    'water_level': {
        'type': 'decimal',
        'description_de': 'Wasserstand',
        'description_en': 'Water Level',
        'mappings': {
            '_ehyd': ['W', 'Wasserstand', 'Pegel'],
        }
    },
    'discharge': {
        'type': 'decimal',
        'description_de': 'Abfluss',
        'description_en': 'Discharge',
        'mappings': {
            '_ehyd': ['Q', 'Abfluss', 'Durchfluss'],
        }
    },
    
    # === HAZARD / RISK ZONES ===
    'hazard_type': {
        'type': 'codelist',
        'description_de': 'Gefahrentyp',
        'description_en': 'Hazard Type',
        'mappings': {
            '_inspire': ['typeOfHazard'],
            'Tirol': ['Gefahrenart', 'GEFAHR'],
        }
    },
    'risk_level': {
        'type': 'codelist',
        'description_de': 'Risikostufe',
        'description_en': 'Risk Level',
        'mappings': {
            '_inspire': ['levelOfRisk'],
            'Tirol': ['Risikostufe', 'ZONE'],
        }
    },
    'likelihood': {
        'type': 'string',
        'description_de': 'Eintrittswahrscheinlichkeit',
        'description_en': 'Likelihood of Occurrence',
        'mappings': {
            '_inspire': ['likelihoodOfOccurrence'],
        }
    },
    
    # === BUILDINGS ===
    'building_height': {
        'type': 'decimal',
        'description_de': 'Gebäudehöhe',
        'description_en': 'Building Height',
        'mappings': {
            '_inspire': ['heightAboveGround'],
            'Oberösterreich': ['Hoehe', 'HEIGHT'],
        }
    },
    'building_condition': {
        'type': 'codelist',
        'description_de': 'Gebäudezustand',
        'description_en': 'Condition of Construction',
        'mappings': {
            '_inspire': ['conditionOfConstruction'],
        }
    },
    
    # === HABITAT / BIOTOPES ===
    'habitat_type': {
        'type': 'codelist',
        'description_de': 'Lebensraumtyp',
        'description_en': 'Habitat Type',
        'mappings': {
            '_inspire': ['habitat'],
            'Oberösterreich': ['Biotoptyp', 'BIOTOP_TYP'],
        }
    },
    
    # === COMMON TECHNICAL FIELDS ===
    'object_id': {
        'type': 'integer',
        'description_de': 'Objekt-ID',
        'description_en': 'Object ID',
        'mappings': {
            '_arcgis': ['OBJECTID', 'OID', 'FID'],
        }
    },
    'global_id': {
        'type': 'string',
        'description_de': 'Globale ID',
        'description_en': 'Global ID',
        'mappings': {
            '_arcgis': ['GlobalID', 'GLOBALID'],
        }
    },
    'municipality_code': {
        'type': 'string',
        'description_de': 'Gemeindekennzahl',
        'description_en': 'Municipality Code',
        'mappings': {
            '_at': ['GEM_NR', 'GKZ', 'Gemeindekennzahl', 'MunicipalityCode'],
        }
    },
    
    # === NETWORK / REFERENCE FIELDS ===
    'identifier': {
        'type': 'string',
        'description_de': 'Kennung',
        'description_en': 'Identifier',
        'mappings': {
            '_inspire': ['identifier', 'Identifier'],
        }
    },
    'network_ref': {
        'type': 'reference',
        'description_de': 'Netzwerk-Referenz',
        'description_en': 'Network Reference',
        'mappings': {
            '_inspire': ['networkRef', 'inNetwork', 'link'],
        }
    },
    'description': {
        'type': 'string',
        'description_de': 'Beschreibung',
        'description_en': 'Description',
        'mappings': {
            '_inspire': ['description'],
        }
    },
    
    # === AREA MANAGEMENT ADDITIONAL ===
    'designation_period': {
        'type': 'period',
        'description_de': 'Ausweisungszeitraum',
        'description_en': 'Designation Period',
        'mappings': {
            '_inspire': ['designationPeriod', 'validityPeriod'],
        }
    },
    'environmental_domain': {
        'type': 'codelist',
        'description_de': 'Umweltbereich',
        'description_en': 'Environmental Domain',
        'mappings': {
            '_inspire': ['environmentalDomain'],
        }
    },
    'end_lifespan': {
        'type': 'dateTime',
        'description_de': 'Gültig bis',
        'description_en': 'Valid until',
        'mappings': {
            '_inspire': ['endLifespanVersion', 'endLifeSpanVersion', 'validTo'],
        }
    },
    'plan_reference': {
        'type': 'reference',
        'description_de': 'Plan-Referenz',
        'description_en': 'Plan Reference',
        'mappings': {
            '_inspire': ['plan', 'relatedZone'],
        }
    },
    'specialized_zone_type': {
        'type': 'codelist',
        'description_de': 'Spezieller Zonentyp',
        'description_en': 'Specialized Zone Type',
        'mappings': {
            '_inspire': ['specialisedZoneType'],
        }
    },
    
    # === HABITAT ADDITIONAL ===
    'species': {
        'type': 'complex',
        'description_de': 'Arten',
        'description_en': 'Species',
        'mappings': {
            '_inspire': ['habitatSpecies', 'species'],
        }
    },
    'vegetation': {
        'type': 'complex',
        'description_de': 'Vegetation',
        'description_en': 'Vegetation',
        'mappings': {
            '_inspire': ['habitatVegetation', 'vegetation'],
        }
    },
    
    # === LAND USE ADDITIONAL ===
    'observation_date': {
        'type': 'date',
        'description_de': 'Beobachtungsdatum',
        'description_en': 'Observation Date',
        'mappings': {
            '_inspire': ['observationDate'],
        }
    },
    'land_use_presence': {
        'type': 'complex',
        'description_de': 'Nutzungspräsenz',
        'description_en': 'Land Use Presence',
        'mappings': {
            '_inspire': ['hilucsPresence', 'specificPresence'],
        }
    },
    'dataset_ref': {
        'type': 'reference',
        'description_de': 'Datensatz-Referenz',
        'description_en': 'Dataset Reference',
        'mappings': {
            '_inspire': ['dataset', 'member'],
        }
    },
}

# INSPIRE Theme to canonical field mapping
THEME_FIELD_PROFILES = {
    'Area Management': [
        'inspire_id', 'name', 'geometry', 'zone_type', 'legal_basis', 
        'legal_date', 'authority', 'begin_lifespan'
    ],
    'Protected Sites': [
        'inspire_id', 'name', 'geometry', 'zone_type', 'legal_basis',
        'legal_date', 'begin_lifespan'
    ],
    'Existing Land Use': [
        'inspire_id', 'name', 'geometry', 'land_use_type', 'begin_lifespan'
    ],
    'Transport Networks': [
        'inspire_id', 'name', 'geometry', 'road_class', 'num_lanes',
        'traffic_direction', 'begin_lifespan'
    ],
    'Habitats and Biotopes': [
        'inspire_id', 'geometry', 'habitat_type', 'begin_lifespan'
    ],
    'Buildings': [
        'inspire_id', 'name', 'geometry', 'building_height', 
        'building_condition', 'begin_lifespan'
    ],
}

def init_mapping_tables():
    """Initialize field mapping tables."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute('DROP TABLE IF EXISTS canonical_fields')
    cur.execute('DROP TABLE IF EXISTS field_synonyms')
    
    cur.execute('''
        CREATE TABLE canonical_fields (
            id TEXT PRIMARY KEY,
            type TEXT,
            description_de TEXT,
            description_en TEXT
        )
    ''')
    
    cur.execute('''
        CREATE TABLE field_synonyms (
            canonical_id TEXT,
            source TEXT,
            field_name TEXT,
            FOREIGN KEY (canonical_id) REFERENCES canonical_fields(id)
        )
    ''')
    
    cur.execute('CREATE INDEX IF NOT EXISTS idx_synonyms_name ON field_synonyms(field_name)')
    
    conn.commit()
    conn.close()

def populate_mappings():
    """Populate field mappings from definitions."""
    init_mapping_tables()
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    for field_id, field_def in FIELD_MAPPINGS.items():
        cur.execute('''
            INSERT INTO canonical_fields (id, type, description_de, description_en)
            VALUES (?, ?, ?, ?)
        ''', (field_id, field_def['type'], field_def['description_de'], field_def['description_en']))
        
        for source, names in field_def['mappings'].items():
            for name in names:
                cur.execute('''
                    INSERT INTO field_synonyms (canonical_id, source, field_name)
                    VALUES (?, ?, ?)
                ''', (field_id, source, name))
    
    conn.commit()
    conn.close()
    print(f"Populated {len(FIELD_MAPPINGS)} canonical fields")

def lookup_canonical_field(field_name):
    """Look up canonical field for a given field name."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute('''
        SELECT cf.id, cf.type, cf.description_de, fs.source
        FROM field_synonyms fs
        JOIN canonical_fields cf ON fs.canonical_id = cf.id
        WHERE LOWER(fs.field_name) = LOWER(?)
    ''', (field_name,))
    
    result = cur.fetchone()
    conn.close()
    
    if result:
        return {
            'canonical_id': result[0],
            'type': result[1],
            'description': result[2],
            'source': result[3]
        }
    return None

def get_field_mappings_for_theme(theme):
    """Get expected fields for an INSPIRE theme."""
    fields = THEME_FIELD_PROFILES.get(theme, [])
    result = []
    
    for field_id in fields:
        if field_id in FIELD_MAPPINGS:
            result.append({
                'id': field_id,
                'type': FIELD_MAPPINGS[field_id]['type'],
                'description_de': FIELD_MAPPINGS[field_id]['description_de'],
                'description_en': FIELD_MAPPINGS[field_id]['description_en'],
                'known_names': FIELD_MAPPINGS[field_id]['mappings']
            })
    
    return result

def analyze_schema_coverage():
    """Analyze how well actual schemas match canonical fields."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Get all fields from schema analysis
    cur.execute('''
        SELECT DISTINCT f.field_name, ft.inspire_theme, d.province
        FROM wfs_fields f
        JOIN wfs_feature_types ft ON f.feature_type_id = ft.id
        JOIN datasets d ON ft.dataset_id = d.id
        WHERE ft.inspire_theme IS NOT NULL
    ''')
    
    results = cur.fetchall()
    conn.close()
    
    # Match against canonical fields
    matched = 0
    unmatched = []
    
    for field_name, theme, province in results:
        canonical = lookup_canonical_field(field_name)
        if canonical:
            matched += 1
        else:
            unmatched.append((field_name, theme, province))
    
    return {
        'total_fields': len(results),
        'matched': matched,
        'unmatched': len(unmatched),
        'match_rate': matched / len(results) if results else 0,
        'unmatched_fields': unmatched[:30]  # Sample of unmatched
    }

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--analyze':
        populate_mappings()
        coverage = analyze_schema_coverage()
        print(f"\n=== Field Mapping Coverage ===")
        print(f"Total fields: {coverage['total_fields']}")
        print(f"Matched to canonical: {coverage['matched']} ({coverage['match_rate']*100:.1f}%)")
        print(f"Unmatched: {coverage['unmatched']}")
        print(f"\nSample unmatched fields:")
        for name, theme, province in coverage['unmatched_fields']:
            print(f"  {name} ({theme}, {province})")
    else:
        populate_mappings()
        
        # Show summary
        print("\n=== INSPIRE Theme Field Profiles ===")
        for theme, fields in THEME_FIELD_PROFILES.items():
            print(f"\n{theme}:")
            for field_id in fields:
                fd = FIELD_MAPPINGS.get(field_id, {})
                print(f"  - {field_id} ({fd.get('type', '?')}): {fd.get('description_de', '')}")
