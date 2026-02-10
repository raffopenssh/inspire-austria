// INSPIRE Österreich Search App

const state = {
    query: '',
    offset: 0,
    limit: 20,
    total: 0,
    filters: {
        type: '',
        province: '',
        service: '',
        topic: ''
    },
    selected: new Set(),
    lastClickedIndex: null,
    results: [],
    favorites: new Set(JSON.parse(localStorage.getItem('inspire_favorites') || '[]'))
};

// Topic translations
const topicLabels = {
    'grundwasser': 'Grundwasser',
    'wetter': 'Wetter',
    'hochwasser': 'Hochwasser',
    'gewässer': 'Gewässer',
    'boden': 'Boden',
    'wald': 'Wald',
    'naturschutz': 'Naturschutz',
    'kataster': 'Kataster',
    'raumordnung': 'Raumordnung',
    'verkehr': 'Verkehr',
    'energie': 'Energie',
    'geologie': 'Geologie',
    'höhenmodell': 'Höhenmodell',
    'orthofoto': 'Orthofoto',
    'adresse': 'Adressen',
    'gebäude': 'Gebäude',
    'bevölkerung': 'Bevölkerung',
    'landwirtschaft': 'Landwirtschaft',
    'gesundheit': 'Gesundheit',
    'umwelt': 'Umwelt'
};

async function init() {
    // Load topics
    const topicsRes = await fetch('/api/topics');
    const topicsData = await topicsRes.json();
    renderTopics(topicsData.topics);
    
    // Load random gems
    await loadGems();
    
    // Event listeners
    document.getElementById('search-input').addEventListener('keypress', e => {
        if (e.key === 'Enter') search();
    });
    document.getElementById('search-btn').addEventListener('click', search);
    
    document.getElementById('filter-type').addEventListener('change', e => {
        state.filters.type = e.target.value;
        state.offset = 0;
        search();
    });
    document.getElementById('filter-province').addEventListener('change', e => {
        state.filters.province = e.target.value;
        state.offset = 0;
        search();
    });
    document.getElementById('filter-service').addEventListener('change', e => {
        state.filters.service = e.target.value;
        state.offset = 0;
        search();
    });
    
    document.getElementById('copy-prompt-btn').addEventListener('click', copyPrompt);
    document.getElementById('clear-selection-btn').addEventListener('click', clearSelection);
    document.getElementById('analyze-btn').addEventListener('click', analyzeSelection);
    
    document.querySelector('.close-btn').addEventListener('click', closeModal);
    document.getElementById('detail-modal').addEventListener('click', e => {
        if (e.target.id === 'detail-modal') closeModal();
    });
    
    document.getElementById('favorites-btn').addEventListener('click', toggleFavoritesFilter);
    updateFavoritesCount();
    
    // Check URL params
    const params = new URLSearchParams(window.location.search);
    if (params.get('q')) {
        document.getElementById('search-input').value = params.get('q');
        search();
    }
}

function renderTopics(topics) {
    const container = document.getElementById('topics-bar');
    const sortedTopics = topics
        .filter(t => topicLabels[t.topic])
        .sort((a, b) => b.count - a.count)
        .slice(0, 15);
    
    container.innerHTML = sortedTopics.map(t => `
        <span class="topic-tag" data-topic="${t.topic}">
            ${topicLabels[t.topic] || t.topic}
            <span class="count">${t.count}</span>
        </span>
    `).join('');
    
    container.querySelectorAll('.topic-tag').forEach(tag => {
        tag.addEventListener('click', () => {
            const topic = tag.dataset.topic;
            
            // Toggle active state
            document.querySelectorAll('.topic-tag').forEach(t => t.classList.remove('active'));
            
            if (state.filters.topic === topic) {
                state.filters.topic = '';
            } else {
                state.filters.topic = topic;
                tag.classList.add('active');
            }
            
            state.offset = 0;
            search();
        });
    });
}

let currentGemIndex = 0;
let gemsData = [];

async function loadGems() {
    const res = await fetch('/api/gems?random=true&limit=8');
    const data = await res.json();
    gemsData = data.gems;
    
    const container = document.getElementById('gems-list');
    container.innerHTML = gemsData.map((gem, i) => `
        <div class="gem-card ${i === 0 ? 'active' : ''}" data-id="${gem.id}" data-index="${i}">
            <span class="gem-badge">*</span>
            <div class="title">${escapeHtml(gem.title)}</div>
            <div class="meta">${gem.province || 'Österreich'}</div>
        </div>
    `).join('');
    
    container.querySelectorAll('.gem-card').forEach(card => {
        card.addEventListener('click', () => showDetail(card.dataset.id));
    });
    
    // Start rotation
    if (gemsData.length > 1) {
        setInterval(rotateGems, 4000);
    }
}

function rotateGems() {
    const cards = document.querySelectorAll('.gem-card');
    if (cards.length === 0) return;
    
    cards[currentGemIndex].classList.remove('active');
    currentGemIndex = (currentGemIndex + 1) % cards.length;
    cards[currentGemIndex].classList.add('active');
}

async function search() {
    state.query = document.getElementById('search-input').value;
    
    // Also run smart search for concept matching
    if (state.query) {
        smartSearch();
    } else {
        document.getElementById('smart-results').style.display = 'none';
    }
    
    const params = new URLSearchParams();
    if (state.query) params.set('q', state.query);
    if (state.filters.type) params.set('type', state.filters.type);
    if (state.filters.province) params.set('province', state.filters.province);
    if (state.filters.service) params.set('service', state.filters.service);
    if (state.filters.topic) params.set('topic', state.filters.topic);
    params.set('limit', state.limit);
    params.set('offset', state.offset);
    
    // Update URL
    if (state.query) {
        history.replaceState(null, '', `?q=${encodeURIComponent(state.query)}`);
    }
    
    const res = await fetch(`/api/search?${params}`);
    const data = await res.json();
    
    state.total = data.total;
    state.results = data.results;
    
    renderResults(data.results);
    renderPagination();
    
    document.getElementById('results-info').textContent = 
        `${data.total} Ergebnisse gefunden` + 
        (state.query ? ` für "${state.query}"` : '');
}

function renderResults(results) {
    const container = document.getElementById('results');
    
    if (results.length === 0) {
        container.innerHTML = '<p style="text-align:center;color:var(--text-muted)">Keine Ergebnisse gefunden</p>';
        return;
    }
    
    container.innerHTML = results.map((r, idx) => {
        const isSelected = state.selected.has(r.id);
        const serviceTypes = [...new Set(r.services.map(s => s.type))];
        
        const isFavorite = state.favorites.has(r.id);
        return `
            <div class="result-card ${isSelected ? 'selected' : ''}" 
                 data-id="${r.id}" data-index="${idx}">
                <div class="checkbox"></div>
                <span class="favorite-btn ${isFavorite ? 'active' : ''}" onclick="toggleFavorite('${r.id}', event)" title="Favorit">
                    ${isFavorite ? '*' : '-'}
                </span>
                <div class="title">
                    <a href="#" onclick="showDetail('${r.id}'); return false;">
                        ${escapeHtml(r.title)}
                    </a>
                    ${r.gem_score >= 8 ? '<span class="gem-badge">*</span>' : ''}
                </div>
                <div class="abstract">${escapeHtml(r.abstract)}</div>
                <div class="meta">
                    <span class="badge type-${r.type}">${typeLabel(r.type)}</span>
                    ${r.province ? `<span class="badge">${r.province}</span>` : ''}
                    ${r.org ? `<span class="badge">${escapeHtml(r.org)}</span>` : ''}
                    <div class="service-badges">
                        ${serviceTypes.map(s => `<span class="service-badge ${s.toLowerCase().replace('-', '')}">${s}</span>`).join('')}
                    </div>
                    <div class="topics">
                        ${r.topics.slice(0, 3).map(t => 
                            `<span class="topic-mini">${topicLabels[t] || t}</span>`
                        ).join('')}
                    </div>
                </div>
            </div>
        `;
    }).join('');
    
    // Add click handlers
    container.querySelectorAll('.result-card').forEach(card => {
        card.addEventListener('click', e => {
            if (e.target.tagName === 'A') return;
            
            const id = card.dataset.id;
            const idx = parseInt(card.dataset.index);
            
            if (e.shiftKey && state.lastClickedIndex !== null) {
                // Shift-click: select range
                const start = Math.min(state.lastClickedIndex, idx);
                const end = Math.max(state.lastClickedIndex, idx);
                
                for (let i = start; i <= end; i++) {
                    const resultId = state.results[i].id;
                    state.selected.add(resultId);
                }
            } else {
                // Normal click: toggle selection
                if (state.selected.has(id)) {
                    state.selected.delete(id);
                } else {
                    state.selected.add(id);
                }
            }
            
            state.lastClickedIndex = idx;
            updateSelectionUI();
        });
    });
}

function updateSelectionUI() {
    const bar = document.getElementById('selection-bar');
    const count = state.selected.size;
    
    if (count > 0) {
        bar.style.display = 'flex';
        document.getElementById('selection-count').textContent = `${count} ausgewählt`;
    } else {
        bar.style.display = 'none';
    }
    
    // Update card styles
    document.querySelectorAll('.result-card').forEach(card => {
        const id = card.dataset.id;
        card.classList.toggle('selected', state.selected.has(id));
    });
}

function clearSelection() {
    state.selected.clear();
    state.lastClickedIndex = null;
    updateSelectionUI();
}

async function copyPrompt() {
    if (state.selected.size === 0) return;
    
    const ids = Array.from(state.selected).join(',');
    const res = await fetch(`/api/prompt?ids=${encodeURIComponent(ids)}`);
    const data = await res.json();
    
    await navigator.clipboard.writeText(data.prompt);
    
    const btn = document.getElementById('copy-prompt-btn');
    const orig = btn.textContent;
    btn.textContent = '✓ Kopiert!';
    setTimeout(() => btn.textContent = orig, 2000);
}

function renderPagination() {
    const container = document.getElementById('pagination');
    const totalPages = Math.ceil(state.total / state.limit);
    const currentPage = Math.floor(state.offset / state.limit) + 1;
    
    if (totalPages <= 1) {
        container.innerHTML = '';
        return;
    }
    
    let html = '';
    
    html += `<button ${currentPage === 1 ? 'disabled' : ''} onclick="goToPage(${currentPage - 1})">&laquo;</button>`;
    
    for (let i = 1; i <= Math.min(totalPages, 10); i++) {
        html += `<button class="${i === currentPage ? 'active' : ''}" onclick="goToPage(${i})">${i}</button>`;
    }
    
    if (totalPages > 10) {
        html += `<span>...</span>`;
        html += `<button onclick="goToPage(${totalPages})">${totalPages}</button>`;
    }
    
    html += `<button ${currentPage === totalPages ? 'disabled' : ''} onclick="goToPage(${currentPage + 1})">&raquo;</button>`;
    
    container.innerHTML = html;
}

function goToPage(page) {
    state.offset = (page - 1) * state.limit;
    search();
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

async function showDetail(id) {
    const res = await fetch(`/api/dataset?id=${encodeURIComponent(id)}`);
    const data = await res.json();
    
    const content = document.getElementById('detail-content');
    const serviceTypes = [...new Set(data.services.map(s => s.type))];
    
    content.innerHTML = `
        <h2>${escapeHtml(data.title)}</h2>
        ${data.gem_score >= 8 ? '<span class="gem-badge">* Hochwertig</span>' : ''}
        
        <div class="section">
            <h4>Beschreibung</h4>
            <p>${escapeHtml(data.abstract || 'Keine Beschreibung verfügbar')}</p>
        </div>
        
        <div class="section">
            <h4>Details</h4>
            <p>
                <strong>Typ:</strong> ${typeLabel(data.type)}<br>
                <strong>Bundesland:</strong> ${data.province || 'Österreich'}<br>
                ${data.org ? `<strong>Organisation:</strong> ${escapeHtml(data.org)}<br>` : ''}
                ${data.year ? `<strong>Jahr:</strong> ${data.year}<br>` : ''}
                <strong>Open Data:</strong> ${data.is_open_data ? 'Ja' : 'Nein'}<br>
                ${data.update_date ? `<strong>Aktualisiert:</strong> ${data.update_date}<br>` : ''}
            </p>
        </div>
        
        ${data.services.length > 0 ? `
        <div class="section">
            <h4>Services (${data.services.length})</h4>
            <div class="services-list">
                ${data.services.map(s => `
                    <div class="service-item">
                        <span class="service-badge ${s.type.toLowerCase()}">${s.type}</span>
                        <a href="${escapeHtml(s.url)}" target="_blank">${truncateUrl(s.url)}</a>
                    </div>
                `).join('')}
            </div>
        </div>
        ` : ''}
        
        ${data.themes.length > 0 ? `
        <div class="section">
            <h4>INSPIRE Themen</h4>
            <div class="keywords">
                ${data.themes.map(t => `<span class="keyword">${escapeHtml(t)}</span>`).join('')}
            </div>
        </div>
        ` : ''}
        
        ${data.keywords.length > 0 ? `
        <div class="section">
            <h4>Schlüsselwörter</h4>
            <div class="keywords">
                ${data.keywords.slice(0, 20).map(k => `<span class="keyword">${escapeHtml(k)}</span>`).join('')}
            </div>
        </div>
        ` : ''}
        
        <div class="section">
            <a href="${data.inspire_url}" target="_blank" class="action-btn">-> INSPIRE Portal</a>
        </div>
    `;
    
    document.getElementById('detail-modal').classList.add('open');
}

function closeModal() {
    document.getElementById('detail-modal').classList.remove('open');
}

function typeLabel(type) {
    const labels = {
        'dataset': 'Datensatz',
        'service': 'Service',
        'series': 'Serie',
        'featureCatalog': 'Merkmalskatalog'
    };
    return labels[type] || type;
}

function toggleFavorite(id, event) {
    event.stopPropagation();
    if (state.favorites.has(id)) {
        state.favorites.delete(id);
    } else {
        state.favorites.add(id);
    }
    saveFavorites();
    renderResults(state.results);
}

function saveFavorites() {
    localStorage.setItem('inspire_favorites', JSON.stringify([...state.favorites]));
    updateFavoritesCount();
}

function updateFavoritesCount() {
    document.getElementById('favorites-count').textContent = state.favorites.size;
}

let showingFavorites = false;

function toggleFavoritesFilter() {
    showingFavorites = !showingFavorites;
    const btn = document.getElementById('favorites-btn');
    btn.classList.toggle('active', showingFavorites);
    
    if (showingFavorites) {
        showFavorites();
    } else {
        search();
    }
}

async function showFavorites() {
    if (state.favorites.size === 0) {
        document.getElementById('results').innerHTML = '<p style="text-align:center;color:var(--text-muted)">Keine Favoriten gespeichert. Klicke auf ☆ um Datensätze zu speichern.</p>';
        document.getElementById('results-info').textContent = '0 Favoriten';
        document.getElementById('pagination').innerHTML = '';
        return;
    }
    
    // Fetch each favorite
    const results = [];
    for (const id of state.favorites) {
        const res = await fetch(`/api/dataset?id=${encodeURIComponent(id)}`);
        if (res.ok) {
            const data = await res.json();
            results.push(data);
        }
    }
    
    state.results = results;
    state.total = results.length;
    renderResults(results);
    document.getElementById('results-info').textContent = `${results.length} Favoriten`;
    document.getElementById('pagination').innerHTML = '';
}

async function analyzeSelection() {
    if (state.selected.size < 2) {
        alert('Wähle mindestens 2 Datensätze aus');
        return;
    }
    
    const ids = Array.from(state.selected).join(',');
    const res = await fetch(`/api/combine?ids=${encodeURIComponent(ids)}`);
    const data = await res.json();
    
    showCombinationPanel(data);
}

function showCombinationPanel(data) {
    const panel = document.getElementById('combination-panel');
    const content = document.getElementById('combination-content');
    
    const analysis = data.analysis;
    const coveragePct = analysis.coverage_pct.toFixed(0);
    
    const allProvinces = ['Wien', 'Niederösterreich', 'Oberösterreich', 'Salzburg', 'Tirol', 'Vorarlberg', 'Kärnten', 'Steiermark', 'Burgenland'];
    
    content.innerHTML = `
        <div class="coverage-bar">
            <strong>Österreich-Abdeckung: ${coveragePct}%</strong>
            <div class="coverage-meter">
                <div class="fill" style="width: ${coveragePct}%"></div>
            </div>
            <div class="province-chips">
                ${allProvinces.map(p => `
                    <span class="province-chip ${analysis.provinces_covered.includes(p) ? 'covered' : 'missing'}">
                        ${analysis.provinces_covered.includes(p) ? '+' : '-'} ${p}
                    </span>
                `).join('')}
            </div>
        </div>
        
        <div class="analysis-section">
            <strong>${analysis.datasets_with_wfs} von ${analysis.total_datasets} haben WFS</strong>
            ${analysis.combinable ? 
                '<span style="color: var(--success); margin-left: 1rem;">[ok] Kombinierbar</span>' : 
                '<span style="color: var(--warning); margin-left: 1rem;">[!] Eingeschr\u00e4nkt</span>'
            }
        </div>
        
        ${analysis.common_fields.length > 0 ? `
        <div class="analysis-section" style="margin-top: 1rem;">
            <strong>Gemeinsame Felder (${analysis.common_fields.length}):</strong>
            <div class="field-list">
                ${analysis.common_fields.slice(0, 15).map(f => `<span class="field-chip">${f}</span>`).join('')}
                ${analysis.common_fields.length > 15 ? `<span class="field-chip">+${analysis.common_fields.length - 15} mehr</span>` : ''}
            </div>
        </div>
        ` : ''}
        
        ${analysis.field_mappings.length > 0 ? `
        <div class="analysis-section" style="margin-top: 1rem;">
            <strong>Feld-Mappings:</strong>
            <table style="width: 100%; margin-top: 0.5rem; font-size: 0.85rem;">
                ${analysis.field_mappings.slice(0, 8).map(fm => `
                    <tr>
                        <td style="font-family: monospace; color: var(--accent);">${fm.field}</td>
                        <td>→</td>
                        <td>${fm.canonical}</td>
                        <td style="color: var(--text-muted);">${fm.description}</td>
                    </tr>
                `).join('')}
            </table>
        </div>
        ` : ''}
        
        ${data.combination_prompt ? `
        <div class="analysis-section" style="margin-top: 1rem;">
            <strong>Shelley Prompt für Kombination:</strong>
            <div class="combination-prompt">${escapeHtml(data.combination_prompt)}</div>
            <button class="copy-btn" onclick="copyText(document.querySelector('.combination-prompt').innerText)">
                [+] Prompt kopieren
            </button>
        </div>
        ` : ''}
        
        <div class="analysis-section" style="margin-top: 1rem;">
            <strong>WFS-Endpunkte:</strong>
            ${data.wfs_services.map(svc => `
                <div style="margin: 0.5rem 0; padding: 0.5rem; background: var(--bg); border-radius: 4px;">
                    <div><strong>${svc.province || 'National'}</strong>: ${svc.title}</div>
                    <div style="font-size: 0.8rem; color: var(--text-muted); word-break: break-all;">${svc.url}</div>
                </div>
            `).join('')}
        </div>
    `;
    
    panel.style.display = 'block';
}

function closeCombinationPanel() {
    document.getElementById('combination-panel').style.display = 'none';
}

function copyText(text) {
    navigator.clipboard.writeText(text).then(() => {
        const btn = event.target;
        const orig = btn.textContent;
        btn.textContent = '✓ Kopiert!';
        setTimeout(() => btn.textContent = orig, 2000);
    });
}

async function smartSearch() {
    const q = document.getElementById('search-input').value;
    if (!q) return;
    
    const res = await fetch(`/api/smart-search?q=${encodeURIComponent(q)}`);
    const data = await res.json();
    
    // Show concept matches
    const conceptsEl = document.getElementById('concept-matches');
    const groupsEl = document.getElementById('combinable-groups');
    const smartEl = document.getElementById('smart-results');
    
    if (data.matched_concepts.length > 0 || data.combinable_groups.length > 0) {
        smartEl.style.display = 'block';
        
        // Concepts
        if (data.matched_concepts.length > 0) {
            conceptsEl.innerHTML = data.matched_concepts.map(c => `
                <div class="concept-card" onclick="searchByConcept('${c.id}')">
                    <h4>${c.name_de}</h4>
                    <div class="stats">
                        ${c.datasets} Datensätze • ${c.provinces} Bundesländer • ${c.wfs_count} WFS
                    </div>
                </div>
            `).join('');
        } else {
            conceptsEl.innerHTML = '';
        }
        
        // Combinable groups
        if (data.combinable_groups.length > 0) {
            groupsEl.innerHTML = `
                <h4># Kombinierbare Datensätze</h4>
                <div>
                    ${data.combinable_groups.map(g => `
                        <div class="combinable-group" onclick="showCombineForConcept('${g.concept}')">
                            <span class="name">${g.name}</span>
                            <span class="info">${g.provinces.length} Bundesl. / ${g.wfs_count} WFS</span>
                        </div>
                    `).join('')}
                </div>
            `;
        } else {
            groupsEl.innerHTML = '';
        }
    } else {
        smartEl.style.display = 'none';
    }
}

function searchByConcept(conceptId) {
    state.filters.topic = conceptId;
    document.querySelectorAll('.topic-tag').forEach(t => {
        t.classList.toggle('active', t.dataset.topic === conceptId);
    });
    state.offset = 0;
    search();
}

async function showCombineForConcept(conceptId) {
    const res = await fetch(`/api/combine?concept=${encodeURIComponent(conceptId)}`);
    const data = await res.json();
    showCombinationPanel(data);
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function truncateUrl(url) {
    if (url.length > 80) {
        return url.substring(0, 77) + '...';
    }
    return url;
}

// Keyboard shortcuts
document.addEventListener('keydown', e => {
    if (e.key === 'Escape') closeModal();
    if (e.key === '/' && document.activeElement.tagName !== 'INPUT') {
        e.preventDefault();
        document.getElementById('search-input').focus();
    }
});

// Initialize
document.addEventListener('DOMContentLoaded', init);
