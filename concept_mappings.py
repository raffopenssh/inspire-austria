#!/usr/bin/env python3
"""Cross-regional concept mappings for Austrian INSPIRE data.

Maps different naming conventions across provinces to unified concepts.
"""

# Unified concepts with regional naming variations
CONCEPT_MAPPINGS = {
    # === SPATIAL PLANNING ===
    'flächenwidmung': {
        'de': 'Flächenwidmungsplan',
        'en': 'Zoning Plan',
        'patterns': [
            'flächenwidmung',
            'widmungsplan', 
            'bebauungsplan',
            'nutzungsplan',
            'raumordnung',
            'örtliche.*raumordnung',
            'überörtliche.*raumordnung',
            'landnutzung',
        ],
        'regional_names': {
            'Burgenland': 'Flächenwidmung Burgenland',
            'Kärnten': 'Flächenwidmungsplan Kärnten',
            'Niederösterreich': 'Flächenwidmungsplan NÖ',
            'Oberösterreich': 'Flächenwidmung Oberösterreich',
            'Salzburg': 'Flächenwidmung Land Salzburg',
            'Steiermark': 'Flächenwidmungsplan Steiermark',
            'Tirol': 'Örtliche Raumordnungskonzepte Tirol',
            'Vorarlberg': 'Flächenwidmungsplan Vorarlberg',
            'Wien': 'Flächenwidmungsplan Wien',
        }
    },
    
    # === PROTECTED AREAS ===
    'naturschutzgebiet': {
        'de': 'Naturschutzgebiete',
        'en': 'Nature Reserves',
        'patterns': [
            'naturschutzgebiet',
            'naturschutz',
            'schutzgebiet(?!.*wasser)',  # not water protection
            'natura.?2000',
            'habitatrichtlinie',
            'vogelschutzrichtlinie',
            'ffh',
            'spa',  # Special Protection Area
            'sci',  # Site of Community Importance
            'naturpark',
            'nationalpark',
            'biosphärenpark',
            'landschaftsschutz',
            'naturdenkmal',
        ],
        'regional_names': {
            'Burgenland': ['Naturschutzgebiete Burgenland', 'Natura 2000 Burgenland'],
            'Kärnten': 'Schutzgebiete Naturschutz Kärnten',
            'Niederösterreich': 'Naturschutzgebiete NÖ',
            'Oberösterreich': 'Naturschutzgebiete OÖ',
            'Salzburg': 'Naturschutzgebiete Salzburg',
            'Steiermark': 'Naturschutzgebiete Steiermark',
            'Tirol': ['Naturschutzgebiete Tirol', 'Natura 2000 Gebiete Tirol'],
            'Vorarlberg': 'Naturschutzgebiete Vorarlberg',
            'Wien': 'Naturschutzgebiete Wien',
            'national': 'Schutzgebiete Österreich',
        }
    },
    
    # === WATER PROTECTION ===
    'wasserschutzgebiet': {
        'de': 'Wasserschutzgebiete',
        'en': 'Water Protection Zones',
        'patterns': [
            'wasserschutz',
            'wasserschon',
            'trinkwasserschutz',
            'grundwasserschutz',
            'quellschutz',
        ],
        'regional_names': {
            'Burgenland': 'Wasserschongebiete Burgenland',
            'Kärnten': 'Wasserschutz- und -schongebiete Kärnten',
            'Niederösterreich': 'Wasserschutzgebiete NÖ',
            'Oberösterreich': 'Wasserschutzgebiete Oberösterreich',
            'Salzburg': 'Wasserschutzgebiete Land Salzburg',
            'Steiermark': 'Wasserschutzgebiete Steiermark',
            'Tirol': 'Wasserschutzgebiete Tirol',
            'Vorarlberg': 'Wasserschutzgebiete Vorarlberg',
            'Wien': 'Wasserschutzgebiete Wien',
        }
    },
    
    # === FLOOD ZONES ===
    'hochwasser': {
        'de': 'Hochwasserrisiko / Überflutungsflächen',
        'en': 'Flood Risk Zones',
        'patterns': [
            'hochwasser',
            'überflutung',
            'überschwemmung',
            'hwz',
            'hq\d+',  # HQ100, HQ30, etc.
            'flood',
            'gefahrenzone.*wasser',
        ],
        'regional_names': {
            'Burgenland': 'Hochwasserrisikogebiete Burgenland',
            'Kärnten': 'Hochwasserzonierung Kärnten',
            'Niederösterreich': 'Hochwasserrisikogebiete NÖ',
            'Oberösterreich': 'Hochwasserrisikogebiete OÖ',
            'Salzburg': 'Hochwasserrisikogebiete Salzburg',
            'Steiermark': 'Hochwasserrisikogebiete Steiermark',
            'Tirol': 'Gefahrenzonenpläne Tirol',
            'Vorarlberg': 'Hochwasserrisikogebiete Vorarlberg',
            'Wien': 'Hochwasserrisikokarten Wien',
        }
    },
    
    # === CADASTRE ===
    'kataster': {
        'de': 'Kataster / Grundstücke',
        'en': 'Cadastral Parcels',
        'patterns': [
            'kataster',
            'grundstück',
            'parzelle',
            'flurstück',
            'gst',
            'dkm',  # Digitale Katastralmappe
        ],
        'regional_names': {
            'national': 'Kataster Grafik INSPIRE tagesaktuell',
        }
    },
    
    # === ADDRESSES ===
    'adressen': {
        'de': 'Adressen',
        'en': 'Addresses',
        'patterns': [
            'adress',
            'hausnummer',
            'straßenname',
            'postleitzahl',
            'plz',
        ],
        'regional_names': {
            'national': 'Adressregister tagesaktuell',
        }
    },
    
    # === ELEVATION / TERRAIN ===
    'höhenmodell': {
        'de': 'Digitales Höhenmodell',
        'en': 'Digital Elevation Model',
        'patterns': [
            'höhenmodell',
            'höhenraster',
            'dgm',
            'dhm',
            'dem',
            'dtm',
            'dsm',
            'als.*crs',  # ALS LiDAR tiles
            'geländemodell',
            'oberflächenmodell',
            'lidar',
        ],
        'regional_names': {
            'national': ['ALS DTM/DSM Höhenraster 1m', 'Digitales Höhenmodell BEV'],
        }
    },
    
    # === ORTHOPHOTOS ===
    'orthofoto': {
        'de': 'Orthofoto / Luftbild',
        'en': 'Orthophoto / Aerial Image',
        'patterns': [
            'orthofoto',
            'orthophoto',
            'luftbild',
            'dop',
            'aerial',
            'rgbi',
        ],
        'regional_names': {
            'national': 'Digitales Orthophoto BEV',
        }
    },
    
    # === FOREST ===
    'wald': {
        'de': 'Wald / Forst',
        'en': 'Forest',
        'patterns': [
            'wald(?!brand)',
            'forst',
            'waldkarte',
            'waldfläche',
            'waldbestand',
            'baumkataster',
        ],
        'regional_names': {
            'national': 'Waldkarte BFW Österreich',
            'Tirol': 'Waldtypisierung Tirol',
            'Wien': 'Baumkataster Wien',
        }
    },
    
    # === GROUNDWATER ===
    'grundwasser': {
        'de': 'Grundwasser',
        'en': 'Groundwater',
        'patterns': [
            'grundwasser',
            'groundwater',
            'aquifer',
            'grundwasserkörper',
            'gwk',
        ],
        'regional_names': {
            'national': 'Grundwasser Aktuell Österreich',
            'Steiermark': 'Grundwasserkörper Steiermark',
            'Vorarlberg': 'Grundwasserfelder Vorarlberg',
        }
    },
    
    # === WATER LEVELS / HYDROLOGY ===
    'pegel': {
        'de': 'Pegelstände / Hydrologie',
        'en': 'Water Levels / Hydrology',
        'patterns': [
            'pegel',
            'wasserstand',
            'abfluss',
            'durchfluss',
            'hydrograph',
            'ehyd',
            'messstelle.*wasser',
        ],
        'regional_names': {
            'national': 'Aktuelle Pegelstände Österreich',
        }
    },
    
    # === PRECIPITATION ===
    'niederschlag': {
        'de': 'Niederschlag',
        'en': 'Precipitation',
        'patterns': [
            'niederschlag',
            'regen',
            'precipitation',
            'rainfall',
        ],
        'regional_names': {
            'national': 'Aktuelle Niederschläge Österreich',
        }
    },
    
    # === ADMINISTRATIVE BOUNDARIES ===
    'verwaltungsgrenzen': {
        'de': 'Verwaltungsgrenzen',
        'en': 'Administrative Boundaries',
        'patterns': [
            'verwaltungsgrenze',
            'verwaltungseinheit',
            'gemeindegrenze',
            'bezirksgrenze',
            'landesgrenze',
            'vgd',
        ],
        'regional_names': {
            'national': 'Verwaltungsgrenzen (VGD) INSPIRE tagesaktuell',
        }
    },
    
    # === BIOTOPES ===
    'biotop': {
        'de': 'Biotope',
        'en': 'Biotopes',
        'patterns': [
            'biotop',
            'lebensraum',
            'habitat',
        ],
        'regional_names': {
            'Burgenland': 'Biotopkartierung Burgenland',
            'Kärnten': 'Biotopkartierung Kärnten',
            'Salzburg': 'Biotopkartierung Salzburg',
            'Steiermark': 'Biotopkartierung Steiermark',
            'Tirol': 'Biotopkartierung Tirol',
            'Vorarlberg': 'Biotopinventar Vorarlberg',
        }
    },
    
    # === ENERGY ===
    'energie': {
        'de': 'Energieinfrastruktur',
        'en': 'Energy Infrastructure',
        'patterns': [
            'kraftwerk',
            'windkraft',
            'photovoltaik',
            'solar(?!strahlung)',
            'wasserkraft',
            'stromleitung',
            'energieversorgung',
        ],
        'regional_names': {
            'Kärnten': 'Wasserkraftwerke Kärnten',
            'Tirol': 'Wasserkraftanlagen Tirol',
        }
    },
    
    # === SOIL ===
    'boden': {
        'de': 'Bodenkarte',
        'en': 'Soil Map',
        'patterns': [
            'bodenkarte',
            'bodentyp',
            'bodenschätzung',
            'eBOD',
        ],
        'regional_names': {
            'national': 'eBOD Digitale Bodenkarte',
        }
    },
}

# Service type equivalents
SERVICE_EQUIVALENTS = {
    'WFS': ['wfs', 'feature service', 'vector download'],
    'WMS': ['wms', 'view service', 'darstellungsdienst'],
    'WMTS': ['wmts', 'tile service'],
    'OGC-API': ['ogc api', 'ogcapi', 'api features'],
    'ATOM': ['atom', 'atom-feed', 'atom feed'],
    'Download': ['download', 'downloaddienst'],
}

def get_concept_for_dataset(title, abstract=''):
    """Find matching concept(s) for a dataset."""
    import re
    text = f"{title} {abstract}".lower()
    matches = []
    
    for concept_id, concept in CONCEPT_MAPPINGS.items():
        for pattern in concept['patterns']:
            if re.search(pattern, text):
                matches.append({
                    'concept': concept_id,
                    'name_de': concept['de'],
                    'name_en': concept['en'],
                    'pattern_matched': pattern
                })
                break
    
    return matches

def find_equivalent_datasets(concept_id, province=None):
    """Find all datasets that match a concept, optionally filtered by province."""
    if concept_id not in CONCEPT_MAPPINGS:
        return []
    
    concept = CONCEPT_MAPPINGS[concept_id]
    regional_names = concept.get('regional_names', {})
    
    if province and province in regional_names:
        return regional_names[province] if isinstance(regional_names[province], list) else [regional_names[province]]
    elif province is None:
        # Return all regional variants
        all_names = []
        for names in regional_names.values():
            if isinstance(names, list):
                all_names.extend(names)
            else:
                all_names.append(names)
        return all_names
    
    return []

if __name__ == '__main__':
    # Test
    test_titles = [
        'Flächenwidmung Burgenland',
        'Örtliche Raumordnungskonzepte Tirol',
        'Wasserschongebiete Burgenland',
        'Wasserschutz- und -schongebiete Kärnten',
        'Natura 2000 Gebiete FFH-Richtlinie Tirol',
        'ALS DTM CRS3035RES50000mN2700000E4350000 Höhenraster 1m',
    ]
    
    for title in test_titles:
        concepts = get_concept_for_dataset(title)
        print(f"{title[:50]:50} -> {[c['concept'] for c in concepts]}")
