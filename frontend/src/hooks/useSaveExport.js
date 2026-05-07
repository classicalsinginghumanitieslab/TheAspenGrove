import { useState, useRef, useEffect } from 'react';
import * as d3 from 'd3';
import { DEFAULT_BIRTH_RANGE, DEFAULT_DEATH_RANGE } from '../constants/defaults';
import { SOURCE_FIELD_KEYS } from '../constants/fieldNames';
import { computeCenteredTransform } from '../utils/graphLayout';

const useSaveExport = ({
  networkData,
  itemDetails,
  currentView,
  searchType,
  selectedVoiceTypes,
  selectedBirthplaces,
  birthYearRange,
  deathYearRange,
  visualizationHeight,
  selectedNode,
  currentCenterNode,
  hasExecutedSearch,
  selectedItem,
  searchQuery,
  token,
  uiZoomRef,
  historyRef,
  API_BASE,
  handleRateLimitResponse,
  fetchWithRetry,
  applySnapshot,
  setCurrentView,
  setSearchQuery,
  setProfileCard,
  setHistoryCounts,
  setHasExecutedSearch,
  setError,
  showHelperMessage,
  isSaveExportEligible,
  viewportWidth,
  viewportHeight,
  isHeaderMobile,
} = {}) => {
  const [savedViews, setSavedViews] = useState([]);
  const [isSavingView, setIsSavingView] = useState(false);
  const [saveLabel, setSaveLabel] = useState('');
  const [loadToken, setLoadToken] = useState('');
  const [isLoadingView, setIsLoadingView] = useState(false);
  const [showSaveExportMenu, setShowSaveExportMenu] = useState(false);
  const isLoadingViewRef = useRef(false);
  const [showSavedViewDialog, setShowSavedViewDialog] = useState(false);
  const [savedViewToken, setSavedViewToken] = useState('');
  const [savedViewLabel, setSavedViewLabel] = useState('');
  const [isExporting, setIsExporting] = useState(false);

  useEffect(() => {
    if (!isSaveExportEligible && showSaveExportMenu) {
      setShowSaveExportMenu(false);
    }
  }, [isSaveExportEligible, showSaveExportMenu]);

  // Private utility: run async tasks with a concurrency limit
  const runWithLimit = async (tasks, limit = 3) => {
    const results = [];
    let idx = 0;
    const workers = new Array(Math.min(limit, tasks.length)).fill(0).map(async () => {
      while (true) {
        const i = idx++;
        if (i >= tasks.length) break;
        results[i] = await tasks[i]();
      }
    });
    await Promise.all(workers);
    return results;
  };

  const saveCurrentView = async () => {
    try {
      setIsSavingView(true);
      // Build a lean snapshot that strips simulation fields and large transient data
      const round = (v) => (Number.isFinite(v) ? Math.round(v) : undefined);
      const slimNode = (n) => {
        const base = {
          id: n.id,
          name: n.name,
          type: n.type,
          voiceType: n.voiceType,
          birthYear: n.birthYear,
          deathYear: n.deathYear,
          birthplace: n.birthplace || n.citizen || undefined,
          spelling_source: n.spelling_source,
          voice_type_source: n.voice_type_source || n.voiceType_source || n.voiceTypeSource,
          dates_source: n.dates_source || n.datesSource,
          birthplace_source: n.birthplace_source || n.birthplaceSource
        };
        // Preserve coarse positions so layout looks familiar when reloaded
        const withPos = {
          ...base,
          x: round(n.x),
          y: round(n.y)
        };
        return Object.fromEntries(Object.entries(withPos).filter(([, v]) => v !== undefined && v !== null));
      };
      // Save all relevant relationship source fields for each link
      const REL_SOURCE_FIELDS = [
        'relationshipSourceDisplay',
        'relationship_source_display',
        'teacher_rel_source',
        'teacher_rel_source_text',
        'relationship_rel_source',
        'relationship_source',
        'relationshipSource',
        'sourceInfo',
        'source',
        'relSource',
        'reference_source',
        'referenceSource',
        'citation',
        'notes',
        'text',
        'label',
        'opera_source_text',
        'opera_source_url',
        'teacher_rel_source_url',
        'sourceUrl',
        'meta'
      ];
      const slimLink = (l) => {
        const base = {
          source: (typeof l.source === 'string' ? l.source : (l.source && l.source.id) || l.source),
          target: (typeof l.target === 'string' ? l.target : (l.target && l.target.id) || l.target),
          label: l.label,
          role: l.role,
          type: l.type
        };
        // Copy all relevant relationship source fields if present
        REL_SOURCE_FIELDS.forEach(field => {
          if (l[field] !== undefined) base[field] = l[field];
        });
        return base;
      };
      const slimGraph = {
        nodes: (networkData.nodes || []).map(slimNode),
        links: (networkData.links || []).map(slimLink)
      };
      const zoom = (window.__cmg_zoomTransform || uiZoomRef.current || d3.zoomIdentity);
      const snapshot = {
        version: 2,
        graph: slimGraph,
        ui: { zoom: { k: zoom.k, x: round(zoom.x), y: round(zoom.y) }, visualizationHeight },
        view: {
          currentView,
          searchType,
          selectedNodeId: selectedNode ? selectedNode.id : null,
          currentCenterNode,
          hasExecutedSearch,
          filters: {
            selectedVoiceTypes: Array.from(selectedVoiceTypes || []),
            selectedBirthplaces: Array.from(selectedBirthplaces || []),
            birthYearRange,
            deathYearRange
          }
        },
        // Keep minimal details to avoid re-fetch on reload; omit giant lists if present
        details: { itemDetails: itemDetails ? { ...itemDetails, family: itemDetails.family } : null, selectedItem },
        meta: { savedAt: new Date().toISOString(), label: saveLabel || '' }
      };
      const resp = await fetch(`${API_BASE}/views`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body: JSON.stringify({ snapshot, label: saveLabel || '' })
      });
      const rateInfo = handleRateLimitResponse(resp);
      if (rateInfo) throw new Error(rateInfo.message);
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.error || 'Failed');
      setSaveLabel('');
      // refresh list
      try { await refreshSavedViews(); } catch (_) {}
      setSavedViewToken(data.token || '');
      setSavedViewLabel(saveLabel || '');
      setShowSavedViewDialog(true);
    } catch (e) {
      setError(e.message || 'Failed to save view');
    } finally {
      setIsSavingView(false);
    }
  };

  const refreshSavedViews = async () => {
    try {
      const resp = await fetch(`${API_BASE}/views`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      const rateInfo = handleRateLimitResponse(resp);
      if (rateInfo) throw new Error(rateInfo.message);
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.error || 'Failed');
      setSavedViews(data.views || []);
    } catch (e) {
      // non-fatal
    }
  };

  const loadViewByToken = async (tokenToLoad, options = {}) => {
    try {
      const resp = await fetchWithRetry(`${API_BASE}/views/${encodeURIComponent(tokenToLoad)}`, {
        headers: { 'Authorization': `Bearer ${token}` }
      }, { retries: 2, baseDelay: 600 });
      const textResp = await resp.text();
      let data;
      try { data = textResp ? JSON.parse(textResp) : {}; } catch (_) { data = { error: textResp || 'Invalid response' }; }
      if (!resp.ok) throw new Error(data.error || `Failed (${resp.status})`);
      if (!data.snapshot) throw new Error('Invalid snapshot');

      const snapshot = data.snapshot || {};
      const graph = snapshot.graph || {};
      const view = snapshot.view || {};
      const filters = view.filters || {};
      const details = snapshot.details || {};
      const ui = snapshot.ui || {};
      const center = details.itemDetails?.center || null;
      const centerName = center?.full_name || view.currentCenterNode || snapshot.currentCenterNode || '';
      const searchQueryFromSnapshot = snapshot.searchQuery || view.searchQuery || centerName || '';
      const snapshotSearchResults = Array.isArray(snapshot.searchResults) ? snapshot.searchResults : [];
      const viewSearchResults = Array.isArray(view.searchResults) ? view.searchResults : [];
      const derivedSearchResults = snapshotSearchResults.length ? snapshotSearchResults
        : (viewSearchResults.length ? viewSearchResults
        : (centerName ? [{ name: centerName, properties: center ? { ...center } : {} }] : []));

      const snapshotToApply = {
        nodes: (graph.nodes || snapshot.nodes || []).map(n => ({ ...n })),
        // Restore all relationship source fields for each link
        links: (graph.links || snapshot.links || []).map(l => ({ ...l })),
        currentView: view.currentView || snapshot.currentView || 'network',
        searchType: view.searchType || snapshot.searchType || 'singers',
        searchQuery: searchQueryFromSnapshot,
        searchResults: derivedSearchResults,
        originalSearchResults: snapshot.originalSearchResults || derivedSearchResults,
        selectedNodeId: view.selectedNodeId ?? snapshot.selectedNodeId ?? null,
        currentCenterNode: (view.currentCenterNode ?? snapshot.currentCenterNode ?? centerName) || null,
        selectedVoiceTypes: snapshot.selectedVoiceTypes || filters.selectedVoiceTypes || [],
        selectedBirthplaces: snapshot.selectedBirthplaces || filters.selectedBirthplaces || [],
        birthYearRange: snapshot.birthYearRange || filters.birthYearRange || [...DEFAULT_BIRTH_RANGE],
        deathYearRange: snapshot.deathYearRange || filters.deathYearRange || [...DEFAULT_DEATH_RANGE],
        showFilterPanel: snapshot.showFilterPanel ?? false,
        showPathPanel: snapshot.showPathPanel ?? false,
        pathInfo: snapshot.pathInfo || null,
        itemDetails: details.itemDetails || snapshot.itemDetails || null,
        selectedItem: details.selectedItem || snapshot.selectedItem || null,
        zoom: (ui.zoom || snapshot.zoom || null),
        visualizationHeight: ui.visualizationHeight || snapshot.visualizationHeight || visualizationHeight,
        ui,
        hasExecutedSearch: view.hasExecutedSearch ?? true
      };

      if (options.centerOnLoad) {
        const containerWidth =
          Number(viewportWidth) ||
          (typeof window !== 'undefined' ? window.innerWidth : 0) ||
          1;
        const containerHeight =
          Number(visualizationHeight) ||
          Number(viewportHeight) ||
          (typeof window !== 'undefined' ? window.innerHeight : 0) ||
          1;
        const fitTransform = computeCenteredTransform(
          snapshotToApply.nodes,
          containerWidth,
          containerHeight,
          isHeaderMobile ? 48 : 80
        );
        if (fitTransform) {
          const plainTransform = { k: fitTransform.k, x: fitTransform.x, y: fitTransform.y };
          snapshotToApply.zoom = plainTransform;
          snapshotToApply.ui = { ...(snapshotToApply.ui || {}), zoom: plainTransform };
        }
      }

      applySnapshot(snapshotToApply);
      setHasExecutedSearch(snapshotToApply.hasExecutedSearch ?? true);

      if (options.treatAsSearch) {
        setCurrentView('network');
        setSearchQuery(snapshotToApply.searchQuery || (snapshotToApply.itemDetails?.center?.full_name || ''));
        setProfileCard({ show: false, data: null });
      } else {
        setCurrentView(snapshotToApply.currentView || 'network');
      }

      setHistoryCounts({
        past: historyRef.current.past.length,
        future: historyRef.current.future.length
      });
    } catch (e) {
      setError(e.message || 'Failed to load view');
    }
  };

  const attemptLoadSavedView = async () => {
    if (!token || !loadToken || isLoadingViewRef.current) return;
    try {
      setIsLoadingView(true);
      isLoadingViewRef.current = true;
      await loadViewByToken(loadToken, { treatAsSearch: true });
    } finally {
      setIsLoadingView(false);
      isLoadingViewRef.current = false;
    }
  };

  // Auto-load from ?view=TOKEN if present
  useEffect(() => {
    try {
      const params = new URLSearchParams(window.location.search);
      const view = params.get('view');
      if (view && token) {
        setLoadToken(view);
        loadViewByToken(view, { treatAsSearch: true });
      }
    } catch (_) {}
  }, [token]); // eslint-disable-line react-hooks/exhaustive-deps

  // Export helpers (JSON and CSV)
  const downloadBlob = (blob, filename) => {
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = filename; a.style.display = 'none';
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  };

  // Ensure family data is present for a person response; merge if missing
  const enrichWithFamily = async (data, personName) => {
    try {
      const hasFamily = !!(data?.family && data.family.length) || !!(data?.center?.family && data.center.family.length);
      if (hasFamily || !personName) return data;
      const resp = await fetchWithRetry(`${API_BASE}/singer/network`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body: JSON.stringify({ singerName: personName, depth: 1 })
      }, { retries: 2, baseDelay: 600 });
      const text = await resp.text();
      let more; try { more = text ? JSON.parse(text) : {}; } catch (_) { more = {}; }
      const familyList = (more?.family || more?.center?.family || []);
      if (familyList && familyList.length) {
        return { ...data, family: Array.isArray(data.family) && data.family.length ? data.family : familyList };
      }
    } catch (_) {}
    return data;
  };

  const toCSV = (rows, headers) => {
    const escape = (v) => {
      const s = v == null ? '' : String(v);
      if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
      return s;
    };
    const lines = [headers.join(',')];
    rows.forEach(r => {
      lines.push(headers.map(h => escape(r[h])).join(','));
    });
    return lines.join('\n');
  };

  const exportAsCSV = async () => {
    setIsExporting(true);
    const valToText = (v) => {
      if (v == null) return '';
      if (Array.isArray(v)) return v.filter(Boolean).join('; ');
      if (typeof v === 'object') {
        // Favor common string-like props; otherwise JSON
        return v.full_name || v.opera_name || v.title || v.name || JSON.stringify(v);
      }
      return String(v);
    };
    const pickSourceValue = (entity, key) => {
      if (!entity) return '';
      const candidates = SOURCE_FIELD_KEYS[key] || [];
      for (const candidate of candidates) {
        if (Object.prototype.hasOwnProperty.call(entity, candidate)) {
          const value = entity[candidate];
          if (value !== undefined && value !== null && value !== '') return value;
        }
      }
      return '';
    };

    // Collect names/ids to fetch for export-only enrichment (no UI changes)
    const personNamesToFetch = new Set();
    const operaNamesToFetch = new Set();
    const bookTitlesToFetch = new Set();
    const operaTypedIdsToFetch = new Set(); // e.g. 'opera:654' or 'opera:great scott'

    (networkData.nodes || []).forEach(n => {
      if (n.type === 'person' && n.name) {
        const hasAnySource =
          pickSourceValue(n, 'spelling') ||
          pickSourceValue(n, 'voiceType') ||
          pickSourceValue(n, 'dates') ||
          pickSourceValue(n, 'birthplace');
        const hasDetails = n.voiceType || n.birthYear || n.deathYear || n.birthplace || n.citizen || n.birthplace;
        if (!hasAnySource || !hasDetails) personNamesToFetch.add(n.name);
      } else if (n.type === 'opera' && n.name) {
        operaNamesToFetch.add(n.name);
      } else if (n.type === 'book' && n.name) {
        bookTitlesToFetch.add(n.name);
      }
    });

    (networkData.links || []).forEach(l => {
      const lbl = (l.label || '').toLowerCase();
      const sId = typeof l.source === 'string' ? l.source : (l.source?.id || '');
      const tId = typeof l.target === 'string' ? l.target : (l.target?.id || '');
      const sNode = (networkData.nodes || []).find(x => x.id === sId);
      const tNode = (networkData.nodes || []).find(x => x.id === tId);
      if ((lbl.startsWith('premiered') || lbl === 'composed')) {
        if (sNode?.type === 'opera' && sNode.name) operaNamesToFetch.add(sNode.name);
        if (tNode?.type === 'opera' && tNode.name) operaNamesToFetch.add(tNode.name);
      }
      if ((lbl === 'authored' || lbl === 'edited')) {
        if (sNode?.type === 'book' && sNode.name) bookTitlesToFetch.add(sNode.name);
        if (tNode?.type === 'book' && tNode.name) bookTitlesToFetch.add(tNode.name);
      }
      if (sNode?.type === 'person' && sNode.name) personNamesToFetch.add(sNode.name);
      if (tNode?.type === 'person' && tNode.name) personNamesToFetch.add(tNode.name);

      // Capture any typed opera ids present directly on links (when opera node isn't in graph)
      if (typeof sId === 'string' && sId.startsWith('opera:')) operaTypedIdsToFetch.add(sId);
      if (typeof tId === 'string' && tId.startsWith('opera:')) operaTypedIdsToFetch.add(tId);
    });

    // Fetch details in parallel (scoped to export)
    const personDetails = new Map();
    const operaDetails = new Map(); // keyed by opera name (original case)
    const bookDetails = new Map();
    const operaNameByTypedId = new Map(); // keyed by typed id e.g. 'opera:654' => 'Rigoletto'
    try {
      const tasks = [];
      for (const full_name of Array.from(personNamesToFetch)) {
        tasks.push(async () => {
          try {
            const resp = await fetch(`${API_BASE}/singer/network`, {
              method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
              body: JSON.stringify({ singerName: full_name, depth: 1 })
            });
            if (handleRateLimitResponse(resp, undefined, { suppressMessage: true })) return;
            const data = await resp.json();
            if (resp.ok && data && data.center) personDetails.set(full_name, data);
          } catch (_) {}
        });
      }
      for (const operaName of Array.from(operaNamesToFetch)) {
        tasks.push(async () => {
          try {
            const resp = await fetch(`${API_BASE}/opera/details`, {
              method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
              body: JSON.stringify({ operaName })
            });
            if (handleRateLimitResponse(resp, undefined, { suppressMessage: true })) return;
            const data = await resp.json();
            if (resp.ok && data) operaDetails.set(operaName, data);
          } catch (_) {}
        });
      }
      for (const title of Array.from(bookTitlesToFetch)) {
        tasks.push(async () => {
          try {
            const resp = await fetch(`${API_BASE}/book/details`, {
              method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
              body: JSON.stringify({ bookTitle: title })
            });
            if (handleRateLimitResponse(resp, undefined, { suppressMessage: true })) return;
            const data = await resp.json();
            if (resp.ok && data) bookDetails.set(title, data);
          } catch (_) {}
        });
      }
      for (const typedId of Array.from(operaTypedIdsToFetch)) {
        tasks.push(async () => {
          try {
            const raw = String(typedId.slice(6) || '').trim();
            if (!raw) return;
            const resp = await fetch(`${API_BASE}/opera/details`, {
              method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
              body: JSON.stringify({ operaId: raw, opera_id: raw, operaName: raw })
            });
            if (handleRateLimitResponse(resp, undefined, { suppressMessage: true })) return;
            const data = await resp.json();
            const name = data?.opera?.opera_name;
            if (resp.ok && name) {
              operaNameByTypedId.set(typedId, name);
              if (!operaDetails.has(name)) operaDetails.set(name, data);
            }
          } catch (_) {}
        });
      }
      await runWithLimit(tasks, 4);
    } catch (_) {}
    const getNodeSources = (n) => {
      const empty = { spellingSource: '', voiceTypeSource: '', datesSource: '', birthplaceSource: '' };
      // Prefer direct properties on node
      const fromNode = {
        spellingSource: pickSourceValue(n, 'spelling'),
        voiceTypeSource: pickSourceValue(n, 'voiceType'),
        datesSource: pickSourceValue(n, 'dates'),
        birthplaceSource: pickSourceValue(n, 'birthplace')
      };
      if (fromNode.spellingSource || fromNode.voiceTypeSource || fromNode.datesSource || fromNode.birthplaceSource) {
        return {
          spellingSource: valToText(fromNode.spellingSource),
          voiceTypeSource: valToText(fromNode.voiceTypeSource),
          datesSource: valToText(fromNode.datesSource),
          birthplaceSource: valToText(fromNode.birthplaceSource)
        };
      }
      // Fall back to fetched person details, then itemDetails center/lists by name match
      const name = n.name || n.id;
      const pd = personDetails.get(name);
      if (pd && pd.center) {
        const c = pd.center;
        return {
          spellingSource: valToText(pickSourceValue(c, 'spelling')),
          voiceTypeSource: valToText(pickSourceValue(c, 'voiceType')),
          datesSource: valToText(pickSourceValue(c, 'dates')),
          birthplaceSource: valToText(pickSourceValue(c, 'birthplace'))
        };
      }
      const center = (itemDetails && itemDetails.center) ? itemDetails.center : null;
      const matchByName = (arr) => (arr || []).find(x => (x && (x.full_name === name || x.name === name)));
      if (center && (center.full_name === name)) {
        return {
          spellingSource: valToText(pickSourceValue(center, 'spelling')),
          voiceTypeSource: valToText(pickSourceValue(center, 'voiceType')),
          datesSource: valToText(pickSourceValue(center, 'dates')),
          birthplaceSource: valToText(pickSourceValue(center, 'birthplace'))
        };
      }
      const t = matchByName(itemDetails?.teachers);
      if (t) {
        return {
          spellingSource: valToText(pickSourceValue(t, 'spelling')),
          voiceTypeSource: valToText(pickSourceValue(t, 'voiceType')),
          datesSource: valToText(pickSourceValue(t, 'dates')),
          birthplaceSource: valToText(pickSourceValue(t, 'birthplace'))
        };
      }
      const s = matchByName(itemDetails?.students);
      if (s) {
        return {
          spellingSource: valToText(pickSourceValue(s, 'spelling')),
          voiceTypeSource: valToText(pickSourceValue(s, 'voiceType')),
          datesSource: valToText(pickSourceValue(s, 'dates')),
          birthplaceSource: valToText(pickSourceValue(s, 'birthplace'))
        };
      }
      const f = matchByName(itemDetails?.family);
      if (f) {
        return {
          spellingSource: valToText(pickSourceValue(f, 'spelling')),
          voiceTypeSource: valToText(pickSourceValue(f, 'voiceType')),
          datesSource: valToText(pickSourceValue(f, 'dates')),
          birthplaceSource: valToText(pickSourceValue(f, 'birthplace'))
        };
      }
      // For non-person nodes or if nothing found, return empty
      return empty;
    };

    const nameById = (id) => {
      const n = (networkData.nodes || []).find(x => x.id === id);
      return (n && (n.name || n.opera_name || n.title || n.id)) || id;
    };
    const getCenterName = () => {
      if (itemDetails?.center?.full_name) return itemDetails.center.full_name;
      if (itemDetails?.opera?.opera_name) return itemDetails.opera.opera_name;
      if (itemDetails?.book?.title) return itemDetails.book.title;
      return '';
    };
    // Relationship source resolver will be defined after export-only fetches
    // Nodes CSV (persons/operas/books): user-friendly headers; omit id
    const nodeHeaders = ['Name','Type','Voice Type','Birth year','Death year','Birthplace','Spelling source','Voice type source','Dates source','Birthplace source'];
    const getNodeValues = (n) => {
      const center = (itemDetails && itemDetails.center) ? itemDetails.center : null;
      const name = n.name || n.id;
      const matchByName = (arr) => (arr || []).find(x => (x && (x.full_name === name || x.name === name)));
      // Start with existing node values
      let voiceType = n.voiceType || '';
      let birthYear = n.birthYear || '';
      let deathYear = n.deathYear || '';
      let birthplace = n.birthplace || n.citizen || '';
      // If missing, try center person
      if ((!voiceType || !birthYear || !deathYear || !birthplace) && center && center.full_name === name) {
        voiceType = voiceType || center.voice_type || '';
        birthYear = birthYear || center.birth_year || '';
        deathYear = deathYear || center.death_year || '';
        birthplace = birthplace || center.birthplace || center.citizen || '';
      }
      // Then try teachers/students/family entries
      if (!voiceType || !birthYear || !deathYear || !birthplace) {
        const t = matchByName(itemDetails?.teachers);
        const s = matchByName(itemDetails?.students);
        const f = matchByName(itemDetails?.family);
        const src = t || s || f || null;
        if (src) {
          voiceType = voiceType || src.voice_type || '';
          birthYear = birthYear || src.birth_year || '';
          deathYear = deathYear || src.death_year || '';
          birthplace = birthplace || src.birthplace || src.citizen || '';
        }
      }
      return { voiceType, birthYear, deathYear, birthplace };
    };
    const nodeRows = (networkData.nodes || []).map(n => {
      const { voiceType, birthYear, deathYear, birthplace } = getNodeValues(n);
      const src = getNodeSources(n);
      return {
        'Name': n.name,
        'Type': n.type,
        'Voice Type': voiceType,
        'Birth year': birthYear,
        'Death year': deathYear,
        'Birthplace': birthplace,
        'Spelling source': src.spellingSource,
        'Voice type source': src.voiceTypeSource,
        'Dates source': src.datesSource,
        'Birthplace source': src.birthplaceSource
      };
    });
    // Relationship source resolver (local to export)
    const getRelationshipSource = (l) => {
      if (l.sourceInfo) return valToText(l.sourceInfo);
      const centerName = getCenterName();
      const sName = nameById(typeof l.source === 'string' ? l.source : (l.source?.id || ''));
      const tName = nameById(typeof l.target === 'string' ? l.target : (l.target?.id || ''));
      const label = (l.label || '').toLowerCase();

      if (label === 'taught') {
        if (centerName && sName === centerName) {
          const m = (itemDetails?.students || []).find(x => x?.full_name === tName);
          if (m && m.teacher_rel_source) return valToText(m.teacher_rel_source);
        } else if (centerName && tName === centerName) {
          const m = (itemDetails?.teachers || []).find(x => x?.full_name === sName);
          if (m && m.teacher_rel_source) return valToText(m.teacher_rel_source);
        }
      }
      if (label === 'family' || label === 'parent' || label === 'spouse' || label === 'sibling' || label === 'grandparent') {
        if (centerName && sName === centerName) {
          const m = (itemDetails?.family || []).find(x => x?.full_name === tName);
          if (m && m.teacher_rel_source) return valToText(m.teacher_rel_source);
        } else if (centerName && tName === centerName) {
          const m = (itemDetails?.family || []).find(x => x?.full_name === sName);
          if (m && m.teacher_rel_source) return valToText(m.teacher_rel_source);
        }
      }
      if (label.startsWith('premiered')) {
        if (itemDetails?.premieredRoles && centerName && sName === centerName) {
          const m = (itemDetails.premieredRoles || []).find(x => x?.opera_name === tName);
          if (m && m.source) return valToText(m.source);
        }
      }
      if (label === 'composed') {
        if (itemDetails?.works?.composedOperas && centerName && sName === centerName) {
          const m = (itemDetails.works.composedOperas || []).find(x => x?.title === tName);
          if (m && m.source) return valToText(m.source);
        }
      }
      if (label === 'authored') {
        if (itemDetails?.book?.title && tName === (itemDetails.book.title)) {
          const m = (itemDetails?.authors || []).find(x => x?.author === sName);
          if (m && m.source) return valToText(m.source);
        }
      }
      if (label === 'edited') {
        if (itemDetails?.book?.title && tName === (itemDetails.book.title)) {
          const m = (itemDetails?.editors || []).find(x => x?.editor === sName);
          if (m && m.source) return valToText(m.source);
        }
      }
      return valToText(l.relationship_source || l.teacher_rel_source || '');
    };
    // Relationship source resolver (local; uses fetched maps)
    const getRelationshipSource2 = (l) => {
      if (l.sourceInfo) return valToText(l.sourceInfo);
      const centerName = getCenterName();
      const sName = nameById(typeof l.source === 'string' ? l.source : (l.source?.id || ''));
      const tName = nameById(typeof l.target === 'string' ? l.target : (l.target?.id || ''));
      const label = (l.label || '').toLowerCase();

      if (label === 'taught') {
        if (centerName && sName === centerName) {
          const fromItems = (itemDetails?.students || []);
          const fromFetched = (personDetails.get(sName)?.students || []);
          const m = [...fromItems, ...fromFetched].find(x => x?.full_name === tName);
          if (m && m.teacher_rel_source) return valToText(m.teacher_rel_source);
        } else if (centerName && tName === centerName) {
          const fromItems = (itemDetails?.teachers || []);
          const fromFetched = (personDetails.get(tName)?.teachers || []);
          const m = [...fromItems, ...fromFetched].find(x => x?.full_name === sName);
          if (m && m.teacher_rel_source) return valToText(m.teacher_rel_source);
        }
      }
      if (label === 'family' || label === 'parent' || label === 'spouse' || label === 'sibling' || label === 'grandparent') {
        if (centerName && sName === centerName) {
          const fromItems = (itemDetails?.family || []);
          const fromFetched = (personDetails.get(sName)?.family || []);
          const m = [...fromItems, ...fromFetched].find(x => x?.full_name === tName);
          if (m && m.teacher_rel_source) return valToText(m.teacher_rel_source);
        } else if (centerName && tName === centerName) {
          const fromItems = (itemDetails?.family || []);
          const fromFetched = (personDetails.get(tName)?.family || []);
          const m = [...fromItems, ...fromFetched].find(x => x?.full_name === sName);
          if (m && m.teacher_rel_source) return valToText(m.teacher_rel_source);
        }
      }
      if (label.startsWith('premiered')) {
        if (centerName && sName === centerName) {
          const roles = (itemDetails?.premieredRoles || personDetails.get(sName)?.premieredRoles || []);
          const m = roles.find(x => x?.opera_name === tName);
          if (m && m.source) return valToText(m.source);
          // also try opera details by singer
          const od = operaDetails.get(tName);
          if (od && Array.isArray(od.premieredRoles)) {
            const r = od.premieredRoles.find(x => x?.singer === sName);
            if (r && r.source) return valToText(r.source);
          }
        }
      }
      if (label === 'composed') {
        if (centerName && sName === centerName) {
          const comps = (itemDetails?.works?.composedOperas || personDetails.get(sName)?.works?.composedOperas || []);
          const m = comps.find(x => x?.title === tName);
          if (m && m.source) return valToText(m.source);
        }
      }
      if (label === 'authored') {
        const det = bookDetails.get(tName);
        if (det && Array.isArray(det.authors)) {
          const m = det.authors.find(x => x?.author === sName);
          if (m && m.source) return valToText(m.source);
        }
      }
      if (label === 'edited') {
        const det = bookDetails.get(tName);
        if (det && Array.isArray(det.editors)) {
          const m = det.editors.find(x => x?.editor === sName);
          if (m && m.source) return valToText(m.source);
        }
      }
      return valToText(l.relationship_source || l.teacher_rel_source || '');
    };

    // Helper to derive a human-friendly display for a node endpoint (object or typed id string)
    const toTitleCase = (s) => String(s || '')
      .split(' ')
      .map(w => w ? (w.charAt(0).toUpperCase() + w.slice(1)) : '')
      .join(' ');
    const displayForEndpoint = (endpoint) => {
      if (endpoint && typeof endpoint === 'object') {
        return endpoint.name || endpoint.opera_name || endpoint.title || endpoint.full_name || valToText(endpoint);
      }
      const id = String(endpoint || '').trim();
      if (!id) return '';
      const colon = id.indexOf(':');
      if (colon > 0) {
        const type = id.slice(0, colon).toLowerCase();
        const raw = id.slice(colon + 1);
        if (type === 'opera') {
          return operaNameByTypedId.get(id) || toTitleCase(raw);
        }
        if (type === 'book') {
          // Best-effort: try to recover title from existing nodes or title-case the raw part
          const byNode = nameById(id);
          if (byNode && byNode !== id) return byNode;
          return toTitleCase(raw);
        }
        if (type === 'person') {
          const byNode = nameById(id);
          if (byNode && byNode !== id) return byNode;
          return toTitleCase(raw);
        }
      }
      // Non-typed id or already a name
      return nameById(id);
    };

    // Relationships CSV: remove 'type', include only relationship source (resolved from itemDetails/fetched or link)
    const linkHeaders = ['source','label','target','role','relationshipSource'];
    const linkRows = (networkData.links || []).map(l => {
      const relSrc = l.teacher_rel_source_url ? valToText(l.teacher_rel_source_url) : getRelationshipSource2(l);
      const sourceDisplay = displayForEndpoint(l.source);
      const targetDisplay = displayForEndpoint(l.target);
      return {
        source: sourceDisplay,
        label: l.label || '',
        target: targetDisplay,
        role: l.role || '',
        relationshipSource: relSrc
      };
    });

    const nodesCSV = toCSV(nodeRows, nodeHeaders);
    const linksCSV = toCSV(linkRows, linkHeaders);

    const ts = new Date().toISOString().replace(/[-:T.Z]/g, '').slice(0, 14);
    downloadBlob(new Blob([nodesCSV], { type: 'text/csv;charset=utf-8' }), `nodes-${ts}.csv`);
    downloadBlob(new Blob([linksCSV], { type: 'text/csv;charset=utf-8' }), `links-${ts}.csv`);
    setIsExporting(false);
    showHelperMessage('Export complete. Check your downloads.', 2400);
  };

  return {
    savedViews,
    isSavingView,
    saveLabel, setSaveLabel,
    loadToken, setLoadToken,
    isLoadingView,
    showSaveExportMenu, setShowSaveExportMenu,
    showSavedViewDialog, setShowSavedViewDialog,
    savedViewToken,
    savedViewLabel,
    isExporting,
    saveCurrentView,
    refreshSavedViews,
    loadViewByToken,
    attemptLoadSavedView,
    downloadBlob,
    enrichWithFamily,
    toCSV,
    exportAsCSV,
  };
};

export default useSaveExport;
