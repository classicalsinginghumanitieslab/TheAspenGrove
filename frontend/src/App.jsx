import React, { useState, useEffect, useRef, useLayoutEffect } from 'react';
import * as d3 from 'd3';
import useViewport from './useViewport';
import useDebounce from './useDebounce';

const SESSION_SNAPSHOT_KEY = 'cmgActiveSession_v1';
const SESSION_SNAPSHOT_FILTERLESS_KEY = 'cmgActiveSession_filtersReset';
const TOKEN_LOGIN_TS_KEY = 'cmgTokenLoginTs';
const LOGIN_MAX_AGE_MS = 14 * 24 * 60 * 60 * 1000;

const ClassicalMusicGenealogy = () => {
  const viewport = useViewport();
  const { width: viewportWidth, height: viewportHeight, isTablet, isPhone } = viewport;
  const isMobileViewport = !!isPhone;
  const isHeaderMobile = !!isPhone || (viewportWidth > 0 && viewportWidth <= 600);
  const debouncedViewportHeight = useDebounce(viewportHeight, 150);
  const backgroundMinHeight = isMobileViewport ? '100dvh' : '100vh';
  const backgroundAttachmentMode = isMobileViewport ? 'scroll' : 'fixed';

  const [token, setToken] = useState('');


  const [currentView, setCurrentView] = useState('search');
  const [searchType, setSearchType] = useState('singers');
  const [originalSearchType, setOriginalSearchType] = useState('singers');
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [originalSearchResults, setOriginalSearchResults] = useState([]);
  const [selectedItem, setSelectedItem] = useState(null);
  const [itemDetails, setItemDetails] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [networkData, setNetworkData] = useState({ nodes: [], links: [] });
  const [shouldRunSimulation, setShouldRunSimulation] = useState(false);
  const [contextMenu, setContextMenu] = useState({ show: false, x: 0, y: 0, node: null });
  const [linkContextMenu, setLinkContextMenu] = useState({ show: false, x: 0, y: 0, role: '', source: '' });
  const [visualizationHeight, setVisualizationHeight] = useState(550);
  // Visualization height adapts for smaller viewports; defaults to 550px on desktop
  const [selectedNode, setSelectedNode] = useState(null);
  // Align Saved view token + Open with Save/Export + Logout
  const saveExportBtnRef = useRef(null);
  const logoutBtnRef = useRef(null);
  const openBtnBelowRef = useRef(null);
  const [savedInputBelowWidth, setSavedInputBelowWidth] = useState(160);
  const [rightGroupWidthPx, setRightGroupWidthPx] = useState(0);
  const [expandSubmenu, setExpandSubmenu] = useState(null);
  const [profileCard, setProfileCard] = useState({ show: false, data: null });
  const [actualCounts, setActualCounts] = useState({});
  const [fetchingCounts, setFetchingCounts] = useState({});
  const [failedFetches, setFailedFetches] = useState({}); // Track nodes that failed to fetch
  const [currentCenterNode, setCurrentCenterNode] = useState(null); // Track current center to prevent re-triggering
  const [isExpansionSimulation, setIsExpansionSimulation] = useState(false); // Track if simulation is for expansion
  const [filtersVersion, setFiltersVersion] = useState(0); // Bump to force viz refresh on Apply
  const [showFilterPanel, setShowFilterPanel] = useState(false); // Control filter panel visibility
  const [selectedVoiceTypes, setSelectedVoiceTypes] = useState(new Set()); // Selected voice type filters
  const [selectedBirthplaces, setSelectedBirthplaces] = useState(new Set()); // Selected birthplace filters
  const [birthYearRange, setBirthYearRange] = useState([1534, 2005]); // Birth year range filter
  const [deathYearRange, setDeathYearRange] = useState([1575, 2025]); // Death year range filter
  const [birthRangeIsUserSet, setBirthRangeIsUserSet] = useState(false);
  const [deathRangeIsUserSet, setDeathRangeIsUserSet] = useState(false);
  // Reverted: only force-directed layout
  const [filterSectionsOpen, setFilterSectionsOpen] = useState({ voice: false, birth: false, death: false, birthplaces: false });
  // Disable global click outside handlers while any path input is focused
  const [pathInputFocused, setPathInputFocused] = useState(false);
  // Path panel toggle (default off)
  const [showPathPanel, setShowPathPanel] = useState(false);
  const [showMobileToolbarMenu, setShowMobileToolbarMenu] = useState(false);
  const [showHeaderMenu, setShowHeaderMenu] = useState(false);
  const [showSupportPanel, setShowSupportPanel] = useState(false);
  const [justLoggedIn, setJustLoggedIn] = useState(false);
  const toolbarPointerStartRef = useRef(null);
  const toolbarSkipClickRef = useRef(false);
  const pathFromRef = useRef(null);
  const pathToRef = useRef(null);
  const pathFromValRef = useRef('');
  const pathToValRef = useRef('');
  const pathOverlayRef = useRef({ addedNodeIds: new Set(), addedLinkKeys: new Set() });
  const prePathNetworkRef = useRef(null);
  const pathPanelRef = useRef(null);
  const pathListRef = useRef(null);
  const [pathInfo, setPathInfo] = useState(null);

  const svgRef = useRef(null);
  const simulationRef = useRef(null);
  const submenuTimeoutRef = useRef(null);
  // Group-drag tracking for path overlay clusters
  const dragGroupIdsRef = useRef(new Set());
  const dragGroupInitialPosRef = useRef(new Map());
  const dragLeaderInitialPosRef = useRef({ x: 0, y: 0 });
  const dragActiveRef = useRef(false);
  // UI-visible zoom transform accessible outside D3 effect
  const uiZoomRef = useRef(d3.zoomIdentity);
  // History navigation (Back/Forward)
  const historyRef = useRef({ past: [], future: [] });
  const [historyCounts, setHistoryCounts] = useState({ past: 0, future: 0 });
  const [savedViews, setSavedViews] = useState([]);
  const [isSavingView, setIsSavingView] = useState(false);
  const [saveLabel, setSaveLabel] = useState('');
  const [loadToken, setLoadToken] = useState('');
  const [isLoadingView, setIsLoadingView] = useState(false);
  const [showSaveExportMenu, setShowSaveExportMenu] = useState(false);
  const isLoadingViewRef = useRef(false);
  // Temporary halo effect for search result cards after a search
  const [showResultsHalo, setShowResultsHalo] = useState(false);
  const resultsHaloTimeoutRef = useRef(null);
  const [rateLimitedUntil, setRateLimitedUntil] = useState(0);
  const rateLimitedUntilRef = useRef(0);
  const [showSavedViewDialog, setShowSavedViewDialog] = useState(false);
  const [savedViewToken, setSavedViewToken] = useState('');
  const [savedViewLabel, setSavedViewLabel] = useState('');
  // Cache person details fetched during expansions/path overlays so nodes can be enriched immediately
  const personCacheRef = useRef(new Map());
  const isSearchingRef = useRef(false);
  const supportPanelLoginFlagRef = useRef(false);
  const sessionRestoredRef = useRef(false);
  const sessionPersistReadyRef = useRef(false);
  const filtersResetRef = useRef(false);
  const headerContainerRef = useRef(null);
  const [headerWidth, setHeaderWidth] = useState(null);

  const clearStoredToken = () => {
    try { localStorage.removeItem('token'); } catch (_) {}
    try { localStorage.removeItem(TOKEN_LOGIN_TS_KEY); } catch (_) {}
  };

  const hasSearchResults = Array.isArray(searchResults) && searchResults.length > 0;

  const isSaveExportEligible = Array.isArray(networkData?.nodes) && networkData.nodes.length > 0 && hasSearchResults;

  const renderSaveExportFields = ({ containerStyle = {}, isMobileLayout = false } = {}) => {
    const disabledSave = !isSaveExportEligible || !token || isSavingView;
    const disabledExport = !isSaveExportEligible;
    const actionButtonBase = {
      padding: '8px 12px',
      backgroundColor: '#ffffff',
      color: '#374151',
      border: '2px solid #3e96e2',
      borderRadius: 8,
      fontSize: '16px',
      height: '48px',
      display: 'inline-flex',
      alignItems: 'center',
      boxSizing: 'border-box',
      justifyContent: 'center',
      width: isMobileLayout ? '100%' : 'auto'
    };
  return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 8, width: '100%', ...containerStyle }}>
        <input
          placeholder="Optional label"
          value={saveLabel}
          onChange={e => setSaveLabel(e.target.value)}
          style={{ padding: '6px 8px', border: '2px solid #3e96e2', borderRadius: 8, height: '48px', boxSizing: 'border-box', fontSize: '16px' }}
        />
        <button
          onClick={saveCurrentView}
          disabled={disabledSave}
          style={{
            ...actionButtonBase,
            cursor: disabledSave ? 'not-allowed' : 'pointer',
            opacity: disabledSave ? 0.6 : 1
          }}
        >
          {isSavingView ? 'Saving…' : 'Save View'}
        </button>
        <button
          onClick={() => { exportAsCSV(); setShowSaveExportMenu(false); }}
          disabled={disabledExport}
          style={{
            ...actionButtonBase,
            cursor: disabledExport ? 'not-allowed' : 'pointer',
            opacity: disabledExport ? 0.6 : 1
          }}
        >
          Export CSV
        </button>
      </div>
    );
  };

  useLayoutEffect(() => {
    const node = headerContainerRef.current;
    if (!node) return;

    const updateWidth = () => {
      try {
        const rect = node.getBoundingClientRect();
        const width = rect?.width ?? node.offsetWidth;
        if (Number.isFinite(width)) {
          const rounded = Math.round(width);
          setHeaderWidth(prev => (prev === rounded ? prev : rounded));
        }
      } catch (_) {}
    };

    updateWidth();

    if (typeof ResizeObserver !== 'undefined') {
      const observer = new ResizeObserver(() => updateWidth());
      observer.observe(node);
      return () => observer.disconnect();
    }

    if (typeof window !== 'undefined') {
      const resizeHandler = () => updateWidth();
      window.addEventListener('resize', resizeHandler);
      return () => {
        window.removeEventListener('resize', resizeHandler);
      };
    }

    return undefined;
  }, []);

  useEffect(() => {
    const baseHeight = 550;
    const viewportH = debouncedViewportHeight || (typeof window !== 'undefined' ? window.innerHeight : 0);

    let nextHeight = baseHeight;
    if (isPhone) {
      nextHeight = Math.max(360, Math.round((viewportH || baseHeight) * 0.65));
    } else if (isTablet) {
      nextHeight = Math.max(420, Math.round((viewportH || baseHeight) * 0.72));
    }

    if (Number.isFinite(nextHeight) && nextHeight !== visualizationHeight) {
      setVisualizationHeight(nextHeight);
    }
  }, [isPhone, isTablet, debouncedViewportHeight, visualizationHeight]);

  useEffect(() => {
    if (!isMobileViewport && showMobileToolbarMenu) {
      setShowMobileToolbarMenu(false);
    }
  }, [isMobileViewport, showMobileToolbarMenu]);

  useEffect(() => {
    if (!isHeaderMobile && showHeaderMenu) {
      setShowHeaderMenu(false);
    }
  }, [isHeaderMobile, showHeaderMenu]);

  useEffect(() => {
    if (typeof document === 'undefined') return undefined;
    const originalBg = document.body.style.backgroundColor;
    document.body.style.backgroundColor = '#0f172a';
    return () => {
      document.body.style.backgroundColor = originalBg;
    };
  }, []);

  useEffect(() => {
    if (!isSaveExportEligible && showSaveExportMenu) {
      setShowSaveExportMenu(false);
    }
  }, [isSaveExportEligible, showSaveExportMenu]);

  useEffect(() => {
    if (currentView !== 'network' && showMobileToolbarMenu) {
      setShowMobileToolbarMenu(false);
    }
  }, [currentView, showMobileToolbarMenu]);

  const handleToolbarPointerDown = (e) => {
    if (e.pointerType === 'touch') {
      toolbarPointerStartRef.current = { x: e.clientX, y: e.clientY };
    } else {
      toolbarPointerStartRef.current = null;
    }
  };

  const handleToolbarPointerUp = (e, action) => {
    if (e.pointerType === 'touch') {
      const start = toolbarPointerStartRef.current;
      toolbarPointerStartRef.current = null;
      if (!start) {
        action();
        toolbarSkipClickRef.current = true;
        return;
      }
      const deltaX = Math.abs(e.clientX - start.x);
      const deltaY = Math.abs(e.clientY - start.y);
      if (deltaX > 12 || deltaY > 12) {
        toolbarSkipClickRef.current = true;
        return;
      }
      toolbarSkipClickRef.current = true;
      action();
    } else {
      action();
    }
  };

  const handleToolbarClick = (action) => (e) => {
    if (toolbarSkipClickRef.current) {
      toolbarSkipClickRef.current = false;
      return;
    }
    action();
  };

  // Centralized unauthorized handler: clears token and prompts re-login
  const handleUnauthorized = (resp) => {
    try {
      if (resp && (resp.status === 401 || resp.status === 403)) {
        setError('Session expired. Please log in again.');
        setToken('');
        clearStoredToken();
        return true;
      }
    } catch (_) {}
    return false;
  };

  const createSnapshot = () => {
    // Normalize links to id strings for stability
    const nodesSnap = networkData.nodes.map(n => ({ ...n }));
    const linksSnap = networkData.links.map(l => ({
      ...l,
      source: (typeof l.source === 'string' ? l.source : l.source?.id),
      target: (typeof l.target === 'string' ? l.target : l.target?.id)
    }));
    // Deep copy itemDetails as cards depend on it
    let detailsSnap = null;
    try { detailsSnap = itemDetails ? JSON.parse(JSON.stringify(itemDetails)) : null; } catch (_) { detailsSnap = null; }
    let zoom = null;
    try {
      const z = (window.__cmg_zoomTransform || uiZoomRef?.current || null);
      zoom = z && typeof z.k === 'number' && typeof z.x === 'number' && typeof z.y === 'number'
        ? { k: z.k, x: z.x, y: z.y }
        : null;
    } catch (_) { zoom = null; }
    let searchResultsSnap = [];
    let originalSearchResultsSnap = [];
    let pathInfoSnap = null;
    try { searchResultsSnap = JSON.parse(JSON.stringify(searchResults || [])); } catch (_) { searchResultsSnap = []; }
    try {
      originalSearchResultsSnap = JSON.parse(JSON.stringify(originalSearchResults || []));
    } catch (_) {
      originalSearchResultsSnap = searchResultsSnap;
    }
    try { pathInfoSnap = pathInfo ? JSON.parse(JSON.stringify(pathInfo)) : null; } catch (_) { pathInfoSnap = null; }
    const selectedVoiceTypesSnap = Array.from(selectedVoiceTypes || []);
    const selectedBirthplacesSnap = Array.from(selectedBirthplaces || []);
    const birthRangeSnap = Array.isArray(birthYearRange) ? [...birthYearRange] : [1534, 2005];
    const deathRangeSnap = Array.isArray(deathYearRange) ? [...deathYearRange] : [1575, 2025];
    return {
      snapshotVersion: 2,
      nodes: nodesSnap,
      links: linksSnap,
      currentView,
      searchType,
      searchQuery,
      searchResults: searchResultsSnap,
      originalSearchResults: originalSearchResultsSnap,
      selectedVoiceTypes: selectedVoiceTypesSnap,
      selectedBirthplaces: selectedBirthplacesSnap,
      birthYearRange: birthRangeSnap,
      deathYearRange: deathRangeSnap,
      showFilterPanel,
      showPathPanel,
      pathInfo: pathInfoSnap,
      selectedNodeId: selectedNode ? selectedNode.id : null,
      currentCenterNode,
      visualizationHeight,
      itemDetails: detailsSnap,
      selectedItem: selectedItem ? { ...selectedItem } : null,
      zoom };
  };

  const applySnapshot = (snap, options = {}) => {
    const { restoreFilters = true } = options;
    if (!snap) return;
    const clonedNodes = snap.nodes.map(n => ({ ...n }));
    const clonedLinks = snap.links.map(l => ({ ...l }));
    // Normalize link endpoints to string ids to avoid stale object refs
    const normalizedLinks = clonedLinks.map(l => ({
      ...l,
      source: (typeof l.source === 'string' ? l.source : (l.source && l.source.id) || l.source),
      target: (typeof l.target === 'string' ? l.target : (l.target && l.target.id) || l.target)
    }));
    setNetworkData({ nodes: clonedNodes, links: normalizedLinks });
    // If the snapshot contains explicit positions, preserve layout and build a dormant sim
    const hasPositions = Array.isArray(clonedNodes) && clonedNodes.length > 0 && clonedNodes.every(n => Number.isFinite(n.x) && Number.isFinite(n.y));
    try { setShouldRunSimulation(!hasPositions); } catch (_) {}
    // Force D3 to rebuild with the new data and reattach forces/drag regardless of counts
    try { setFiltersVersion(v => v + 1); } catch (_) {}
    try { setCurrentView(snap.currentView); } catch (_) {}
    try { setSearchType(snap.searchType); } catch (_) {}
    try { setSelectedNode(null); } catch (_) {}
    try { setCurrentCenterNode(snap.currentCenterNode || null); } catch (_) {}
    // Fixed height; ignore saved visualizationHeight from snapshots
    // Reapply zoom transform immediately if available
    try {
      const z = snap.zoom;
      const dz = (z && typeof z.k === 'number' && typeof z.x === 'number' && typeof z.y === 'number')
        ? d3.zoomIdentity.translate(z.x, z.y).scale(z.k)
        : d3.zoomIdentity;
      uiZoomRef.current = dz;
      try { window.__cmg_zoomTransform = dz; } catch (_) {}
      const svgSel = d3.select(svgRef.current);
      svgSel.property('__zoom', dz);
      svgSel.select('g').attr('transform', dz);
    } catch (_) {}
    // Restore detail cards state
    try { setItemDetails(snap.itemDetails || null); } catch (_) {}
    try { setSelectedItem(snap.selectedItem || null); } catch (_) {}
    try {
      if (typeof snap.searchQuery === 'string') {
        setSearchQuery(snap.searchQuery);
      }
    } catch (_) {}
    try {
      if (Array.isArray(snap.searchResults)) {
        setSearchResults(snap.searchResults);
      }
    } catch (_) {}
    try {
      if (Array.isArray(snap.originalSearchResults)) {
        setOriginalSearchResults(snap.originalSearchResults);
      } else if (Array.isArray(snap.searchResults)) {
        setOriginalSearchResults(snap.searchResults);
      }
    } catch (_) {}
    if (restoreFilters) {
      try {
        const voices = Array.isArray(snap.selectedVoiceTypes) ? snap.selectedVoiceTypes : [];
        setSelectedVoiceTypes(new Set(voices));
      } catch (_) {}
      try {
        const places = Array.isArray(snap.selectedBirthplaces) ? snap.selectedBirthplaces : [];
        setSelectedBirthplaces(new Set(places));
      } catch (_) {}
      try {
        if (Array.isArray(snap.birthYearRange) && snap.birthYearRange.length === 2) {
          setBirthYearRange(snap.birthYearRange);
        }
      } catch (_) {}
      try {
        if (Array.isArray(snap.deathYearRange) && snap.deathYearRange.length === 2) {
          setDeathYearRange(snap.deathYearRange);
        }
      } catch (_) {}
      try { setShowFilterPanel(!!snap.showFilterPanel); } catch (_) {}
    } else {
      try { setSelectedVoiceTypes(new Set()); } catch (_) {}
      try { setSelectedBirthplaces(new Set()); } catch (_) {}
      try { setBirthYearRange([1534, 2005]); } catch (_) {}
      try { setDeathYearRange([1575, 2025]); } catch (_) {}
      try { setShowFilterPanel(false); } catch (_) {}
      setTimeout(() => {
        try { resetDateRanges(); } catch (_) {}
      }, 0);
    }
    try { setShowPathPanel(!!snap.showPathPanel); } catch (_) {}
    try { setPathInfo(snap.pathInfo || null); } catch (_) {}
  };

  // Utility: sleep
  const sleep = (ms) => new Promise(res => setTimeout(res, ms));

  // Fetch with retry and exponential backoff (handles 429 with Retry-After)
  const fetchWithRetry = async (url, options = {}, { retries = 2, baseDelay = 500 } = {}) => {
    let attempt = 0;
    let lastErr;
    let lastStatus = null;
    while (attempt <= retries) {
      try {
        // Global cooldown if we've recently been rate-limited
        const now = Date.now();
        const until = rateLimitedUntilRef.current || 0;
        if (until && now < until) {
          await sleep(Math.min(until - now, 10000));
        }
        const resp = await fetch(url, options);
        lastStatus = resp.status;
        if (resp.status === 429) {
          // Too many requests: respect Retry-After if provided
          const ra = resp.headers && (resp.headers.get('Retry-After') || resp.headers.get('retry-after'));
          const waitMs = ra ? (parseFloat(ra) * 1000) : (baseDelay * Math.pow(2, attempt));
          const capped = Math.min(waitMs || baseDelay, 10000);
          const untilTs = Date.now() + capped;
          rateLimitedUntilRef.current = untilTs;
          try { setRateLimitedUntil(untilTs); } catch (_) {}
          await sleep(capped);
          attempt += 1;
          continue;
        }
        return resp;
      } catch (e) {
        lastErr = e;
        await sleep(baseDelay * Math.pow(2, attempt));
        attempt += 1;
      }
    }
    if (lastErr) throw lastErr;
    if (lastStatus === 429) throw new Error('Too many requests, please try again later');
    throw new Error('Request failed');
  };

  // Simple concurrency limiter for an array of async tasks
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

  // Fetch and cache details for a single person name
  const fetchAndCachePersonDetails = async (fullName) => {
    if (!fullName) return null;
    const cache = personCacheRef.current;
    if (cache.has(fullName)) return cache.get(fullName);
    try {
      const resp = await fetch(`${API_BASE}/singer/network`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body: JSON.stringify({ singerName: fullName, depth: 1 })
      });
      const data = await resp.json();
      if (resp.ok && data && data.center) {
        cache.set(fullName, data);
        return data;
      }
    } catch (_) {}
    return null;
  };

  // Enrich specified person nodes in the current network with details and sources
  const enrichPersonNodes = async (personNames) => {
    try {
      const unique = Array.from(new Set((personNames || []).filter(Boolean)));
      if (!unique.length) return;
      // fetch missing details in parallel
      await Promise.all(unique.map(nm => fetchAndCachePersonDetails(nm)));
      const cache = personCacheRef.current;
      setNetworkData(prev => {
        const namesSet = new Set(unique);
        // Mutate in place to preserve node object identity for D3 simulation
        (prev.nodes || []).forEach(n => {
          if (n.type === 'person' && namesSet.has(n.name)) {
            const d = cache.get(n.name);
            const c = d && d.center ? d.center : null;
            if (c) {
              const birthVal = (c.birth_year ?? (c.birth && (c.birth.low ?? c.birth))) || null;
              const deathVal = (c.death_year ?? (c.death && (c.death.low ?? c.death))) || null;
              const birthplaceVal = c.birthplace || c.citizen || c.birthplace || null;
              n.voiceType = n.voiceType || c.voice_type || n.voiceType;
              n.birthYear = n.birthYear || birthVal || n.birthYear;
              n.deathYear = n.deathYear || deathVal || n.deathYear;
              n.birthplace = n.birthplace || birthplaceVal || n.birthplace;
              n.spelling_source = n.spelling_source || c.spelling_source || n.spelling_source || null;
              n.voice_type_source = n.voice_type_source || c.voice_type_source || n.voice_type_source || null;
              n.dates_source = n.dates_source || c.dates_source || n.dates_source || null;
              n.birthplace_source = n.birthplace_source || c.birthplace_source || null;
            }
          }
        });
        // Return same array reference to keep D3 simulation pointers valid
        return { ...prev, nodes: prev.nodes };
      });
    } catch (_) {}
  };

  const pushHistory = (label) => {
    try {
      const snap = createSnapshot();
      historyRef.current.past.push(snap);
      historyRef.current.future = [];
      setHistoryCounts({ past: historyRef.current.past.length, future: 0 });
    } catch (_) {}
  };
  // Save current snapshot to backend
  const saveCurrentView = async () => {
    try {
      setIsSavingView(true);
      const snapshot = {
        version: 1,
        graph: { nodes: networkData.nodes, links: networkData.links },
        ui: { zoom: (window.__cmg_zoomTransform || uiZoomRef.current || d3.zoomIdentity), visualizationHeight },
        view: {
          currentView,
          searchType,
          selectedNodeId: selectedNode ? selectedNode.id : null,
          currentCenterNode,
          filters: {
            selectedVoiceTypes: Array.from(selectedVoiceTypes || []),
            selectedBirthplaces: Array.from(selectedBirthplaces || []),
            birthYearRange,
            deathYearRange
          }
        },
        details: { itemDetails, selectedItem },
        meta: { savedAt: new Date().toISOString(), label: saveLabel || '' }
      };
      const resp = await fetch(`${API_BASE}/views`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body: JSON.stringify({ snapshot, label: saveLabel || '' })
      });
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
        birthYearRange: snapshot.birthYearRange || filters.birthYearRange || [1534, 2005],
        deathYearRange: snapshot.deathYearRange || filters.deathYearRange || [1575, 2025],
        showFilterPanel: snapshot.showFilterPanel ?? false,
        showPathPanel: snapshot.showPathPanel ?? false,
        pathInfo: snapshot.pathInfo || null,
        itemDetails: details.itemDetails || snapshot.itemDetails || null,
        selectedItem: details.selectedItem || snapshot.selectedItem || null,
        zoom: (ui.zoom || snapshot.zoom || null),
        visualizationHeight: ui.visualizationHeight || snapshot.visualizationHeight || visualizationHeight,
        ui
      };

      applySnapshot(snapshotToApply);

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
  }, [token]);

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

  /* const exportAsJSON = () => {
    // Disabled per request: JSON export commented out
    const snapshot = {};
  }; */

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
    const valToText = (v) => {
      if (v == null) return '';
      if (Array.isArray(v)) return v.filter(Boolean).join('; ');
      if (typeof v === 'object') {
        // Favor common string-like props; otherwise JSON
        return v.full_name || v.opera_name || v.title || v.name || JSON.stringify(v);
      }
      return String(v);
    };

    // Collect names to fetch for export-only enrichment (no UI changes)
    const personNamesToFetch = new Set();
    const operaNamesToFetch = new Set();
    const bookTitlesToFetch = new Set();

    (networkData.nodes || []).forEach(n => {
      if (n.type === 'person' && n.name) {
        const hasAnySource = n.spelling_source || n.voice_type_source || n.dates_source || n.birthplace_source;
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
    });

    // Fetch details in parallel (scoped to export)
    const personDetails = new Map();
    const operaDetails = new Map();
    const bookDetails = new Map();
    try {
      await Promise.all([
        ...Array.from(personNamesToFetch).map(async (full_name) => {
          try {
            const resp = await fetch(`${API_BASE}/singer/network`, {
              method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
              body: JSON.stringify({ singerName: full_name, depth: 1 })
            });
            const data = await resp.json();
            if (resp.ok && data && data.center) personDetails.set(full_name, data);
          } catch (_) {}
        }),
        ...Array.from(operaNamesToFetch).map(async (operaName) => {
          try {
            const resp = await fetch(`${API_BASE}/opera/details`, {
              method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
              body: JSON.stringify({ operaName })
            });
            const data = await resp.json();
            if (resp.ok && data) operaDetails.set(operaName, data);
          } catch (_) {}
        }),
        ...Array.from(bookTitlesToFetch).map(async (title) => {
          try {
            const resp = await fetch(`${API_BASE}/book/details`, {
              method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
              body: JSON.stringify({ bookTitle: title })
            });
            const data = await resp.json();
            if (resp.ok && data) bookDetails.set(title, data);
          } catch (_) {}
        })
      ]);
    } catch (_) {}
    const getNodeSources = (n) => {
      const empty = { spellingSource: '', voiceTypeSource: '', datesSource: '', birthplaceSource: '' };
      // Prefer direct properties on node
      const fromNode = {
        spellingSource: n.spelling_source || n.spellingSource,
        voiceTypeSource: n.voice_type_source || n.voiceType_source || n.voiceTypeSource,
        datesSource: n.dates_source || n.datesSource,
        birthplaceSource: n.birthplace_source || n.birthplaceSource || null
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
          spellingSource: valToText(c.spelling_source),
          voiceTypeSource: valToText(c.voice_type_source),
          datesSource: valToText(c.dates_source),
          birthplaceSource: valToText(c.birthplace_source)
        };
      }
      const center = (itemDetails && itemDetails.center) ? itemDetails.center : null;
      const matchByName = (arr) => (arr || []).find(x => (x && (x.full_name === name || x.name === name)));
      if (center && (center.full_name === name)) {
        return {
          spellingSource: valToText(center.spelling_source),
          voiceTypeSource: valToText(center.voice_type_source),
          datesSource: valToText(center.dates_source),
          birthplaceSource: valToText(center.birthplace_source)
        };
      }
      const t = matchByName(itemDetails?.teachers);
      if (t) {
        return {
          spellingSource: valToText(t.spelling_source),
          voiceTypeSource: valToText(t.voice_type_source),
          datesSource: valToText(t.dates_source),
          birthplaceSource: valToText(t.birthplace_source)
        };
      }
      const s = matchByName(itemDetails?.students);
      if (s) {
        return {
          spellingSource: valToText(s.spelling_source),
          voiceTypeSource: valToText(s.voice_type_source),
          datesSource: valToText(s.dates_source),
          birthplaceSource: valToText(s.birthplace_source)
        };
      }
      const f = matchByName(itemDetails?.family);
      if (f) {
        return {
          spellingSource: valToText(f.spelling_source),
          voiceTypeSource: valToText(f.voice_type_source),
          datesSource: valToText(f.dates_source),
          birthplaceSource: valToText(f.birthplace_source)
        };
      }
      // For non-person nodes or if nothing found, return empty
      return empty;
    };

    const nameById = (id) => {
      const n = (networkData.nodes || []).find(x => x.id === id);
      return (n && (n.name || n.id)) || id;
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

    // Relationships CSV: remove 'type', include only relationship source (resolved from itemDetails/fetched or link)
    const linkHeaders = ['source','label','target','role','relationshipSource'];
    const linkRows = (networkData.links || []).map(l => {
      const relSrc = getRelationshipSource2(l);
      return {
        source: typeof l.source === 'string' ? l.source : (l.source?.id || ''),
        label: l.label || '',
        target: typeof l.target === 'string' ? l.target : (l.target?.id || ''),
        role: l.role || '',
        relationshipSource: relSrc
      };
    });

    const nodesCSV = toCSV(nodeRows, nodeHeaders);
    const linksCSV = toCSV(linkRows, linkHeaders);

    const ts = new Date().toISOString().replace(/[-:T.Z]/g, '').slice(0, 14);
    downloadBlob(new Blob([nodesCSV], { type: 'text/csv;charset=utf-8' }), `nodes-${ts}.csv`);
    downloadBlob(new Blob([linksCSV], { type: 'text/csv;charset=utf-8' }), `links-${ts}.csv`);
  };

  const goBack = () => {
    const past = historyRef.current.past;
    if (!past.length) return;
    // Move current to future
    const current = createSnapshot();
    historyRef.current.future.push(current);
    const snap = past.pop();
    applySnapshot(snap);
    setHistoryCounts({ past: past.length, future: historyRef.current.future.length });
  };
  const goForward = () => {
    const future = historyRef.current.future;
    if (!future.length) return;
    // Move current to past
    const current = createSnapshot();
    historyRef.current.past.push(current);
    const snap = future.pop();
    applySnapshot(snap);
    setHistoryCounts({ past: historyRef.current.past.length, future: future.length });
  };

  const resolveApiBase = () => {
    try {
      if (typeof window !== 'undefined') {
        const override = window.__CMG_API_BASE;
        if (typeof override === 'string' && override.trim()) {
          return override.trim().replace(/\/$/, '');
        }
      }
    } catch (_) {}

    let envBase = '';
    if (typeof import.meta !== 'undefined' && import.meta.env && typeof import.meta.env.VITE_API_BASE === 'string') {
      envBase = import.meta.env.VITE_API_BASE;
    } else if (typeof process !== 'undefined' && process?.env?.REACT_APP_API_BASE) {
      envBase = process.env.REACT_APP_API_BASE;
    }

    envBase = (envBase || '').trim();
    if (envBase) {
      return envBase.replace(/\/$/, '');
    }

    if (typeof window !== 'undefined') {
      const { protocol, hostname } = window.location;
      if (hostname === 'localhost' || hostname === '127.0.0.1') {
        return 'http://localhost:3001';
      }
      return `${protocol}//${hostname}`;
    }

    return 'http://localhost:3001';
  };

  const API_BASE = resolveApiBase();
  // Voice types configuration with colors
  const VOICE_TYPES = [
    // Traditional Female Voices
    { name: 'Soprano', color: '#ae996b' }, // tree trunk
    { name: 'Mezzo-soprano', color: '#695531' }, // brown trunk
    { name: 'Contralto', color: '#443f39' }, // knot
    
    // Traditional Male Voices
    { name: 'Countertenor', color: '#4e2d06' }, // Brown
    { name: 'Tenor', color: '#e4a201' }, // yellow leaf
    { name: 'Baritone', color: '#6a7304' }, // darker green leaf
    { name: 'Bass-baritone', color: '#a09602' }, // lighter green leaf
    { name: 'Bass', color: '#a09602' }, // dark green-grey
    
    // Historical/Specialized Voices
    { name: 'Castrato', color: '#99c0e3' }, // Pale blue
   	{ name: 'Soprano castrato', color: '#99c0e3' }, // Pale blue
    { name: 'Alto castrato', color: '#99c0e3' }, // Pale blue
    { name: 'Haute-contre', color: '#99c0e3' }, // Pale blue
    { name: 'Treble, unchanged voice', color: '#99c0e3' }, // Pale blue
    
    // Professional Roles - Music
    { name: 'Composer', color: '#7c8b23' }, // Pale blue
    { name: 'Conductor', color: '#7c8b23' }, // Pale blue
    { name: 'Instrumentalist', color: '#7c8b23' }, // Pale blue
    { name: 'Opera director', color: '#7c8b23' }, // Pale blue
    
    // Professional Roles - Education
    { name: 'Teacher, other', color: '#7c8b23' }, // Pale blue
    { name: 'Vocal coach', color: '#7c8b23' }, // Pale blue
    { name: 'Speech Language Pathologist', color: '#7c8b23' }, // Pale blue
    
    // Professional Roles - Literary/Creative
    { name: 'Librettist', color: '#7c8b23' }, // Pale blue
    { name: 'Critic', color: '#7c8b23' }, // Pale blue
    { name: 'Actor', color: '#7c8b23' }, // Pale blue
    { name: 'Inventor', color: '#7c8b23' }, // Pale blue
    
    // Other/Special Categories
    { name: 'Non-singing', color: '#7c8b23' }, // Pale blue
    { name: 'Unknown', color: '##7c8b23' } // Pale blue
  ];

  // Enhanced filter setter functions
  const updateSelectedVoiceTypes = (newSelection) => {
    setSelectedVoiceTypes(newSelection);
  };

  const updateBirthYearRange = (newRange) => {
    setBirthYearRange(newRange);
  };

  const updateDeathYearRange = (newRange) => {
    setDeathYearRange(newRange);
  };

  const parseYearValue = (value) => {
    if (value === null || value === undefined) return NaN;
    if (typeof value === 'number') {
      return Number.isFinite(value) ? value : NaN;
    }
    if (typeof value === 'string') {
      const match = value.match(/-?\d{3,4}/);
      return match ? parseInt(match[0], 10) : NaN;
    }
    if (typeof value === 'object') {
      if (Number.isFinite(value.year)) return value.year;
      if (typeof value.year === 'string') {
        const match = value.year.match(/-?\d{3,4}/);
        if (match) return parseInt(match[0], 10);
      }
      if (Number.isFinite(value.low)) return value.low;
      if (Number.isFinite(value.high)) return value.high;
    }
    return NaN;
  };

  const normalizePersonNode = (node) => {
    if (!node || node.type !== 'person') return node;
    let normalized = node;
    const ensureClone = () => {
      if (normalized === node) {
        normalized = { ...node };
      }
    };
    if (normalized.voiceType === undefined && normalized.voice_type) {
      ensureClone();
      normalized.voiceType = normalized.voice_type;
    }
    if (normalized.birthplace === undefined && normalized.birth_place) {
      ensureClone();
      normalized.birthplace = normalized.birth_place;
    }
    const birthCandidate =
      normalized.birthYear ??
      normalized.birth_year ??
      (normalized.birth && (normalized.birth.year ?? normalized.birth.low ?? normalized.birth.high)) ??
      null;
    const deathCandidate =
      normalized.deathYear ??
      normalized.death_year ??
      (normalized.death && (normalized.death.year ?? normalized.death.low ?? normalized.death.high)) ??
      null;
    const birthYear = parseYearValue(birthCandidate);
    const deathYear = parseYearValue(deathCandidate);
    if (Number.isFinite(birthYear) && normalized.birthYear !== birthYear) {
      ensureClone();
      normalized.birthYear = birthYear;
    }
    if (Number.isFinite(deathYear) && normalized.deathYear !== deathYear) {
      ensureClone();
      normalized.deathYear = deathYear;
    }
    return normalized;
  };

  const extendDateRangesForNodes = (nodesList = [], options = {}) => {
    const { resetUserRangeFlags = false } = options;
    if (!Array.isArray(nodesList) || nodesList.length === 0) return;
    let [birthMin, birthMax] = birthYearRange;
    let [deathMin, deathMax] = deathYearRange;
    let birthChanged = false;
    let deathChanged = false;

    nodesList.forEach((node) => {
      if (node && node.type === 'person') {
        const birthValue =
          node.birthYear ??
          node.birth_year ??
          (node.birth ? (node.birth.year ?? node.birth.low ?? node.birth.high ?? node.birth) : null);
        const deathValue =
          node.deathYear ??
          node.death_year ??
          (node.death ? (node.death.year ?? node.death.low ?? node.death.high ?? node.death) : null);

        const birthYear = parseYearValue(birthValue);
        if (!Number.isNaN(birthYear)) {
          if (birthYear < birthMin) { birthMin = birthYear; birthChanged = true; }
          if (birthYear > birthMax) { birthMax = birthYear; birthChanged = true; }
        }

        const deathYear = parseYearValue(deathValue);
        if (!Number.isNaN(deathYear)) {
          if (deathYear < deathMin) { deathMin = deathYear; deathChanged = true; }
          if (deathYear > deathMax) { deathMax = deathYear; deathChanged = true; }
        }
      }
    });

    if (birthChanged) {
      updateBirthYearRange([birthMin, birthMax]);
      if (resetUserRangeFlags && birthRangeIsUserSet) {
        setBirthRangeIsUserSet(false);
      }
    }
    if (deathChanged) {
      updateDeathYearRange([deathMin, deathMax]);
      if (resetUserRangeFlags && deathRangeIsUserSet) {
        setDeathRangeIsUserSet(false);
      }
    }
  };

  // Filter helper functions
  const toggleVoiceTypeFilter = (voiceType) => {
    const newSelection = new Set(selectedVoiceTypes);
    if (newSelection.has(voiceType)) {
      newSelection.delete(voiceType);
    } else {
      newSelection.add(voiceType);
    }
    updateSelectedVoiceTypes(newSelection);
  };

  const normalizePlaceName = (s) => (s && typeof s === 'string') ? s.trim().toLowerCase() : '';

  const toggleBirthplaceFilter = (birthplace) => {
    const key = normalizePlaceName(birthplace);
    if (!key) return;
    const newSelection = new Set(selectedBirthplaces);
    if (newSelection.has(key)) {
      newSelection.delete(key);
    } else {
      newSelection.add(key);
    }
    setSelectedBirthplaces(newSelection);
  };

  const computeRangesFromNodes = (nodesList = []) => {
    const defaults = { birthRange: [1534, 2005], deathRange: [1575, 2025] };
    const personNodes = Array.isArray(nodesList)
      ? nodesList.filter(node => node && node.type === 'person')
      : [];

    if (personNodes.length === 0) {
      return defaults;
    }

    const birthYears = personNodes
      .map(node => node.birthYear)
      .filter(year => year && !isNaN(year))
      .map(year => parseInt(year, 10));

    const deathYears = personNodes
      .map(node => node.deathYear)
      .filter(year => year && !isNaN(year))
      .map(year => parseInt(year, 10));

    const birthRange = birthYears.length > 0
      ? [Math.min(...birthYears), Math.max(...birthYears)]
      : defaults.birthRange;

    const deathRange = deathYears.length > 0
      ? [Math.min(...deathYears), Math.max(...deathYears)]
      : defaults.deathRange;

    return { birthRange, deathRange };
  };

  const resetFiltersForNodeSet = (nodesList) => {
    updateSelectedVoiceTypes(new Set());
    setSelectedBirthplaces(new Set());
    const { birthRange, deathRange } = computeRangesFromNodes(nodesList ?? networkData.nodes);
    updateBirthYearRange(birthRange);
    updateDeathYearRange(deathRange);
    setBirthRangeIsUserSet(false);
    setDeathRangeIsUserSet(false);
  };

  const clearFiltersForNewSearch = (nodesList = []) => {
    const nextNodes = (Array.isArray(nodesList) && nodesList.length > 0) ? nodesList : networkData.nodes;
    resetFiltersForNodeSet(nextNodes);
    setShowFilterPanel(false);
  };

  const clearAllFilters = () => {
    resetFiltersForNodeSet();
  };

  // Helper function to get date ranges from current network data
  const getDateRanges = () => {
    const personNodes = networkData.nodes.filter(node => node.type === 'person');
    
    const birthYears = personNodes
      .map(node => node.birthYear)
      .filter(year => year && !isNaN(year))
      .map(year => parseInt(year));
    
    const deathYears = personNodes
      .map(node => node.deathYear)
      .filter(year => year && !isNaN(year))
      .map(year => parseInt(year));
    
    const minBirth = birthYears.length > 0 ? Math.min(...birthYears) : 1534;
    const maxBirth = birthYears.length > 0 ? Math.max(...birthYears) : 2005;
    const minDeath = deathYears.length > 0 ? Math.min(...deathYears) : 1575;
    const maxDeath = deathYears.length > 0 ? Math.max(...deathYears) : 2025;
    
    return {
      birthRange: [minBirth, maxBirth],
      deathRange: [minDeath, maxDeath]
    };
  };
  const getVisibleBirthplaces = () => {
    const personNodes = networkData.nodes.filter(node => node.type === 'person');
    const counts = new Map(); // normalized -> { name, count }
    personNodes.forEach(node => {
      if (!isNodeVisibleWithoutBirthplaceFilter(node)) return;
      const raw = (node.birthplace || node.citizen || '').trim();
      if (!raw) return;
      const key = normalizePlaceName(raw);
      if (!key) return;
      if (!counts.has(key)) counts.set(key, { name: raw, count: 0 });
      counts.get(key).count += 1;
    });
    return Array.from(counts.values()).sort((a, b) => a.name.localeCompare(b.name));
  };

  // Helper: visibility excluding voice-type filter (used for voice-type counts)
  const isNodeVisibleWithoutVoiceFilter = (node) => {
    if (node.type === 'person') {
      // Birthplace filter
      if (selectedBirthplaces.size > 0) {
        const place = node.birthplace || node.citizen || null;
        const match = place && selectedBirthplaces.has(normalizePlaceName(place));
        if (!match) return false;
      }

      if (birthRangeIsUserSet && node.birthYear) {
        const birthYear = parseInt(node.birthYear);
        if (!isNaN(birthYear)) {
          if (birthYear < birthYearRange[0] || birthYear > birthYearRange[1]) {
            return false;
          }
        }
      }

      if (deathRangeIsUserSet && node.deathYear) {
        const deathYear = parseInt(node.deathYear);
        if (!isNaN(deathYear)) {
          if (deathYear < deathYearRange[0] || deathYear > deathYearRange[1]) {
            return false;
          }
        }
      }
    }
    return true;
  };

  // Helper: visibility excluding birthplace filter (used for birthplace counts)
  const isNodeVisibleWithoutBirthplaceFilter = (node) => {
    if (node.type === 'person') {
      // Voice type filter
      if (selectedVoiceTypes.size > 0) {
        const voiceTypeMatch = !node.voiceType ? 
          selectedVoiceTypes.has('Unknown') : 
          selectedVoiceTypes.has(node.voiceType);
        if (!voiceTypeMatch) return false;
      }

      if (birthRangeIsUserSet && node.birthYear) {
        const birthYear = parseInt(node.birthYear);
        if (!isNaN(birthYear)) {
          if (birthYear < birthYearRange[0] || birthYear > birthYearRange[1]) {
            return false;
          }
        }
      }

      if (deathRangeIsUserSet && node.deathYear) {
        const deathYear = parseInt(node.deathYear);
        if (!isNaN(deathYear)) {
          if (deathYear < deathYearRange[0] || deathYear > deathYearRange[1]) {
            return false;
          }
        }
      }
    }
    return true;
  };

  // Derive dynamic voice types from currently visualized person nodes
  const getVisibleVoiceTypes = () => {
    const personNodes = networkData.nodes.filter(node => node.type === 'person');
    const counts = new Map();
    personNodes.forEach(node => {
      if (!isNodeVisibleWithoutVoiceFilter(node)) return;
      const vt = (node.voiceType && String(node.voiceType).trim()) || 'Unknown';
      counts.set(vt, (counts.get(vt) || 0) + 1);
    });
    // Build color map from the static VOICE_TYPES palette
    const colorMap = {};
    VOICE_TYPES.forEach(v => { colorMap[v.name] = v.color; });
    return Array.from(counts.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([name, count]) => ({ name, color: colorMap[name] || '#6B7280', count }));
  };

  const resetDateRanges = () => {
    const { birthRange, deathRange } = getDateRanges();
    updateBirthYearRange(birthRange);
    updateDeathYearRange(deathRange);
    setBirthRangeIsUserSet(false);
    setDeathRangeIsUserSet(false);
  };

  const isNodeVisible = (node) => {
    // For person nodes, check all applicable filters
    if (node.type === 'person') {
      // Voice type filter
      if (selectedVoiceTypes.size > 0) {
        const voiceTypeMatch = !node.voiceType ? 
          selectedVoiceTypes.has('Unknown') : 
          selectedVoiceTypes.has(node.voiceType);
        if (!voiceTypeMatch) return false;
      }

      // Birthplace filter
      if (selectedBirthplaces.size > 0) {
        const place = node.birthplace || node.citizen || null;
        const match = place && selectedBirthplaces.has(normalizePlaceName(place));
        if (!match) return false;
      }
      
      if (birthRangeIsUserSet && node.birthYear) {
        const birthYear = parseInt(node.birthYear);
        if (!isNaN(birthYear)) {
          if (birthYear < birthYearRange[0] || birthYear > birthYearRange[1]) {
            return false;
          }
        }
      }
      
      if (deathRangeIsUserSet && node.deathYear) {
        const deathYear = parseInt(node.deathYear);
        if (!isNaN(deathYear)) {
          if (deathYear < deathYearRange[0] || deathYear > deathYearRange[1]) {
            return false;
          }
        }
      }
    }
    
    // Show opera and book nodes by default (could add filters for these later)
    return true;
  };

  const isLinkVisible = (link) => {
    // A link is only visible if both its source and target nodes are visible
    const sourceNode = typeof link.source === 'string' ? 
      networkData.nodes.find(n => n.id === link.source) : link.source;
    const targetNode = typeof link.target === 'string' ? 
      networkData.nodes.find(n => n.id === link.target) : link.target;
    
    return sourceNode && targetNode && isNodeVisible(sourceNode) && isNodeVisible(targetNode);
  };
  const getFilterCounts = () => {
    const totalNodes = networkData.nodes.length;
    const visibleNodes = networkData.nodes.filter(isNodeVisible).length;
    return { totalNodes, visibleNodes };
  };

  // Initialize token on component mount
  useEffect(() => {
    let savedToken = null;
    let loginTs = null;
    try { savedToken = localStorage.getItem('token'); } catch (_) {}
    try { const rawTs = localStorage.getItem(TOKEN_LOGIN_TS_KEY); loginTs = rawTs ? parseInt(rawTs, 10) : null; } catch (_) {}
    if (!savedToken) return;
    const now = Date.now();
    const isExpired = !Number.isFinite(loginTs) || now - loginTs > LOGIN_MAX_AGE_MS;
    if (isExpired) {
      clearStoredToken();
      setError('Session expired. Please log in again.');
      setToken('');
      return;
    }
    setToken(savedToken);
  }, []);

  useEffect(() => {
    if (!token) {
      supportPanelLoginFlagRef.current = false;
      setJustLoggedIn(false);
      try { setShowSupportPanel(false); } catch (_) {}
      return;
    }
    if (!justLoggedIn) return;
    if (supportPanelLoginFlagRef.current) return;
    supportPanelLoginFlagRef.current = true;
    try { setShowSupportPanel(false); } catch (_) {}
    const timer = setTimeout(() => {
      try { setShowSupportPanel(true); } catch (_) {}
      setJustLoggedIn(false);
    }, 180);
    return () => { clearTimeout(timer); };
  }, [token, justLoggedIn]);

  useEffect(() => {
    if (!token) {
      sessionRestoredRef.current = false;
      sessionPersistReadyRef.current = false;
      try { localStorage.removeItem(SESSION_SNAPSHOT_KEY); } catch (_) {}
      try { localStorage.removeItem(SESSION_SNAPSHOT_FILTERLESS_KEY); } catch (_) {}
      filtersResetRef.current = false;
      return;
    }
    if (sessionRestoredRef.current) {
      sessionPersistReadyRef.current = true;
      return;
    }
    let snapshotLoaded = false;
    try {
      const raw = localStorage.getItem(SESSION_SNAPSHOT_KEY);
      if (raw) {
        const snap = JSON.parse(raw);
        if (snap && typeof snap === 'object') {
          applySnapshot(snap, { restoreFilters: false });
          snapshotLoaded = true;
          filtersResetRef.current = true;
          try { localStorage.setItem(SESSION_SNAPSHOT_FILTERLESS_KEY, '1'); } catch (_) {}
        }
      }
    } catch (_) {}
    if (!snapshotLoaded && !filtersResetRef.current) {
      try {
        const marker = localStorage.getItem(SESSION_SNAPSHOT_FILTERLESS_KEY);
        if (marker !== '1') {
          clearAllFilters();
          localStorage.setItem(SESSION_SNAPSHOT_FILTERLESS_KEY, '1');
          filtersResetRef.current = true;
        }
      } catch (_) {}
    }
    sessionRestoredRef.current = true;
    sessionPersistReadyRef.current = true;
  }, [token]);

  useEffect(() => {
    if (!token || !sessionPersistReadyRef.current) return;
    try {
      const snap = createSnapshot();
      snap.snapshotVersion = 2;
      snap.searchQuery = searchQuery;
      try { snap.searchResults = JSON.parse(JSON.stringify(searchResults || [])); } catch (_) { snap.searchResults = []; }
      try { snap.originalSearchResults = JSON.parse(JSON.stringify(originalSearchResults || [])); } catch (_) { snap.originalSearchResults = snap.searchResults || []; }
      snap.selectedVoiceTypes = Array.from(selectedVoiceTypes || []);
      snap.selectedBirthplaces = Array.from(selectedBirthplaces || []);
      snap.birthYearRange = Array.isArray(birthYearRange) ? [...birthYearRange] : snap.birthYearRange;
      snap.deathYearRange = Array.isArray(deathYearRange) ? [...deathYearRange] : snap.deathYearRange;
      snap.showFilterPanel = !!showFilterPanel;
      snap.showPathPanel = !!showPathPanel;
      try { snap.pathInfo = pathInfo ? JSON.parse(JSON.stringify(pathInfo)) : null; } catch (_) { snap.pathInfo = null; }
      localStorage.setItem(SESSION_SNAPSHOT_KEY, JSON.stringify(snap));
      localStorage.setItem(SESSION_SNAPSHOT_FILTERLESS_KEY, '1');
    } catch (_) {}
  }, [
    token,
    networkData.nodes.length,
    networkData.links.length,
    currentView,
    searchType,
    searchQuery,
    searchResults,
    originalSearchResults,
    showFilterPanel,
    showPathPanel,
    selectedVoiceTypes,
    selectedBirthplaces,
    birthYearRange,
    deathYearRange,
    pathInfo,
    filtersVersion
  ]);

  // Filter application will be handled manually through button clicks or direct calls
  // No automatic reactive filter application to avoid infinite loops



  // Close context menu when clicking elsewhere (never recenter or modify zoom)
  useEffect(() => {
    const handleClick = (event) => {
      // Don't interfere with form inputs or if user is in the auth form
      if (event.target.tagName === 'INPUT' || 
          event.target.tagName === 'TEXTAREA' || 
          event.target.closest('form') ||
          !token) {
        return;
      }
      
      // Explicitly re-apply current zoom to avoid any external listeners causing recenter
      try { window.__cmg_reapplyZoom && window.__cmg_reapplyZoom(); } catch (_) {}
      setContextMenu({ show: false, x: 0, y: 0, node: null });
      setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' });
      setExpandSubmenu(null);
      // Clear any pending submenu timeout
      if (submenuTimeoutRef.current) {
        clearTimeout(submenuTimeoutRef.current);
      }
    };
    
    // Only add listener if user is logged in
    if (token && !pathInputFocused) {
      document.addEventListener('click', handleClick);
    }
    
    return () => document.removeEventListener('click', handleClick);
  }, [token, pathInputFocused]);

  // Fetch actual counts when context menu opens for any node that doesn't have them cached
  useEffect(() => {
    if (contextMenu.show && contextMenu.node) {
      const nodeId = contextMenu.node.id;
      // Fetch immediately so the Expand state is accurate at open
      if (!actualCounts[nodeId] && !fetchingCounts[nodeId] && !failedFetches[nodeId]) {
          setFetchingCounts(prev => ({ ...prev, [nodeId]: true }));
          fetchActualCounts(contextMenu.node)
            .catch(() => {
              setFailedFetches(prev => ({ ...prev, [nodeId]: true }));
            })
            .finally(() => {
              setFetchingCounts(prev => ({ ...prev, [nodeId]: false }));
            });
      }
    }
  }, [contextMenu.show, contextMenu.node?.id]);

  // Prefetch actual expandable relationship counts for newly added nodes to avoid delay on right-click
  const prevNodeIdsRef = useRef(new Set());
  useEffect(() => {
    if (!token || loading || isSearchingRef.current || currentView !== 'network' || (rateLimitedUntilRef.current && Date.now() < rateLimitedUntilRef.current)) return;
    const currentIds = new Set(networkData.nodes.map(n => n.id));
    const newlyAddedIds = [];
    currentIds.forEach((id) => {
      if (!prevNodeIdsRef.current.has(id)) newlyAddedIds.push(id);
    });
    prevNodeIdsRef.current = currentIds;

    if (newlyAddedIds.length === 0) return;

    const fetchingIds = new Set(Object.keys(fetchingCounts).filter(k => fetchingCounts[k]));
    const PREFETCH_LIMIT = 2;
    const STAGGER_MS = 600;
    newlyAddedIds.slice(0, PREFETCH_LIMIT).forEach((id, index) => {
      if (actualCounts[id] || fetchingIds.has(id) || failedFetches[id]) return;
      const node = networkData.nodes.find(n => n.id === id);
      if (!node) return;
      setFetchingCounts(prev => ({ ...prev, [id]: true }));
      // Stagger requests slightly to avoid bursts
      setTimeout(() => {
        fetchActualCounts(node)
          .catch((err) => {
            // Don't permanently mark failed on rate limit; allow retry later
            if (err && err.status && err.status !== 429) {
              setFailedFetches(prev => ({ ...prev, [id]: true }));
            }
          })
          .finally(() => {
            setFetchingCounts(prev => ({ ...prev, [id]: false }));
          });
      }, index * STAGGER_MS);
    });
  }, [networkData.nodes, token, loading, currentView]);

  // Resize functionality removed; fixed height

  // Close context menu when clicking/right-clicking outside (never recenter or modify zoom)
  useEffect(() => {
    const handleClickOutside = (event) => {
      // For left-clicks, avoid interfering with active form inputs; for right-clicks, always allow closing
      const isFormTarget = event.target.tagName === 'INPUT' || event.target.tagName === 'TEXTAREA' || event.target.closest('form');
      if (event.type === 'click' && (isFormTarget || !token)) return;
      // Explicitly re-apply current zoom to avoid any external listeners causing recenter
      try { window.__cmg_reapplyZoom && window.__cmg_reapplyZoom(); } catch (_) {}
      if (contextMenu.show && !event.target.closest('.context-menu')) {
        setContextMenu({ show: false, x: 0, y: 0, node: null });
      }
      if (linkContextMenu.show && !event.target.closest('.context-menu')) {
        setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' });
      }
    };

    // Add listeners if any context menu is showing and user is logged in
    const shouldBind = (contextMenu.show || linkContextMenu.show) && token && !pathInputFocused;
    if (shouldBind) {
      document.addEventListener('click', handleClickOutside);
      document.addEventListener('contextmenu', handleClickOutside);
    }

    return () => {
      document.removeEventListener('click', handleClickOutside);
      document.removeEventListener('contextmenu', handleClickOutside);
    };
  }, [contextMenu.show, linkContextMenu.show, token, pathInputFocused]);

  // Disable global click outside handlers while any path input is focused
  useEffect(() => {
    const onFocus = () => setPathInputFocused(true);
    const onBlur = () => setTimeout(() => setPathInputFocused(false), 0);
    const inputs = [pathFromRef.current, pathToRef.current].filter(Boolean);
    inputs.forEach(inp => {
      inp && inp.addEventListener('focus', onFocus);
      inp && inp.addEventListener('blur', onBlur);
    });
    return () => {
      inputs.forEach(inp => {
        inp && inp.removeEventListener('focus', onFocus);
        inp && inp.removeEventListener('blur', onBlur);
      });
    };
  }, [showPathPanel]);

  // When path panel opens, focus the first input so Tab continues within the panel
  useEffect(() => {
    if (showPathPanel) {
      setTimeout(() => {
        try { pathFromRef.current && pathFromRef.current.focus(); } catch (_) {}
      }, 0);
    }
  }, [showPathPanel]);

  // Freeze simulation and node positions while any context menu is open
  useEffect(() => {
    const sim = simulationRef.current;
    const anyMenuOpen = contextMenu.show || linkContextMenu.show;
    if (anyMenuOpen) {
      // Lock current positions
      if (networkData && Array.isArray(networkData.nodes)) {
        networkData.nodes.forEach(n => {
          n.fx = n.x;
          n.fy = n.y;
        });
      }
      if (sim) {
        try { sim.stop(); } catch (_) {}
      }
    } else {
      // Release locks; do not auto-resume to avoid snap-backs after clicks
      if (networkData && Array.isArray(networkData.nodes)) {
        networkData.nodes.forEach(n => { n.fx = null; n.fy = null; });
      }
    }
    // No re-render needed; positions are frozen/unfrozen without relayout
  }, [contextMenu.show, linkContextMenu.show, shouldRunSimulation]);

  // Compute alignment for Saved view token below row
  useLayoutEffect(() => {
    try {
      // Measure placeholder text to size the input exactly like a button: text width + horizontal padding + borders
      let desiredWidth = 240;
      // Removed text measurement for paste input (no longer displayed)
      const finalWidth = Math.max(200, desiredWidth);
      if (Number.isFinite(finalWidth) && finalWidth > 0) setSavedInputBelowWidth(finalWidth);
    } catch (_) {}
  }, [showSaveExportMenu, token, loadToken, currentView]);

  // Resize handler removed

  const login = async (email, password) => {
    try {
      setLoading(true);
      const response = await fetch(`${API_BASE}/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password })
      });
      
      const text = await response.text();
      let data;
      try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { error: text || 'Invalid response' }; }
      if (response.ok) {
        setJustLoggedIn(true);
        setToken(data.token);
        localStorage.setItem('token', data.token);
        try { localStorage.setItem(TOKEN_LOGIN_TS_KEY, String(Date.now())); } catch (_) {}
        setError('');
      } else {
        setError(data.error || `Login failed (${response.status})`);
      }
    } catch (err) {
      setError(err?.message || 'Login failed - please try again');
    } finally {
      setLoading(false);
    }
  };

  const register = async (email, password) => {
    try {
      setLoading(true);
      const response = await fetch(`${API_BASE}/auth/register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password })
      });
      
      const text = await response.text();
      let data;
      try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { error: text || 'Invalid response' }; }
      if (response.ok) {
        setJustLoggedIn(true);
        setToken(data.token);
        localStorage.setItem('token', data.token);
        try { localStorage.setItem(TOKEN_LOGIN_TS_KEY, String(Date.now())); } catch (_) {}
        setError('');
      } else {
        setError(data.error || `Registration failed (${response.status})`);
      }
    } catch (err) {
      setError(err?.message || 'Registration failed - please try again');
    } finally {
      setLoading(false);
    }
  };

  const performSearch = async () => {
    if (!searchQuery.trim() || searchQuery.length < 2) {
      setError('Please enter at least 2 characters');
      return;
    }

    try {
      clearFiltersForNewSearch([]);
      setLoading(true);
      setError('');
      // Clear any open Full information card on new search
      setProfileCard({ show: false, data: null });
      // Clear any path-finding info card and inputs
      try { setPathInfo(null); } catch (_) {}
      try { if (pathFromRef.current) pathFromRef.current.value = ''; } catch (_) {}
      try { if (pathToRef.current) pathToRef.current.value = ''; } catch (_) {}
      try { pathFromValRef.current = ''; } catch (_) {}
      try { pathToValRef.current = ''; } catch (_) {}
      // If we're currently under a global rate-limit cooldown, fail fast with a clear message
      const until = rateLimitedUntilRef.current || 0;
      const now = Date.now();
      if (until && now < until) {
        const secs = Math.max(1, Math.ceil((until - now) / 1000));
        setError(`Too many requests – please try again in ${secs}s`);
        setLoading(false);
        return;
      }
      
      // Make API call to Neo4j backend
      const endpoint = searchType === 'singers' ? '/search/singers' : 
                     searchType === 'operas' ? '/search/operas' : '/search/books';
      
      if (isSearchingRef.current) return; // drop rapid repeats
      isSearchingRef.current = true;
      const response = await fetchWithRetry(`${API_BASE}${endpoint}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({ query: searchQuery })
      }, { retries: 3, baseDelay: 900 });

      const text = await response.text();
      let data;
      try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { error: text || 'Invalid response' }; }
      
      if (response.ok) {
        setSearchResults(data[searchType] || []);
        setOriginalSearchResults(data[searchType] || []);
        setOriginalSearchType(searchType);
        setCurrentView('results');
        // Trigger halo for search result cards (persistent for now)
        setShowResultsHalo(true);
        
        // Generate network data from search results
        generateNetworkFromSearchResults(data[searchType] || [], searchType);
      } else {
        if (handleUnauthorized(response)) return;
        setError(data.error || `Search failed (${response.status})`);
      }
    } catch (err) {
      console.error('Search error:', err);
      setError(err?.message || 'Search failed - please try again');
    } finally {
      isSearchingRef.current = false;
      setLoading(false);
    }
  };
  // Universal anti-overlap positioning system
  const positionNodesWithoutOverlap = (nodes, containerWidth = 800, containerHeight = 600) => {
    const minDistance = 120; // Minimum distance between any two nodes
    const maxAttempts = 50; // Maximum attempts to find a good position
    
    nodes.forEach((node, index) => {
      let validPosition = false;
      let attempts = 0;
      
      while (!validPosition && attempts < maxAttempts) {
        // Generate candidate position
        const margin = 100;
        const x = margin + Math.random() * (containerWidth - 2 * margin);
        const y = margin + Math.random() * (containerHeight - 2 * margin);
        
        // Check distance from all previously positioned nodes
        let hasCollision = false;
        for (let i = 0; i < index; i++) {
          const otherNode = nodes[i];
          const distance = Math.sqrt(
            Math.pow(x - otherNode.x, 2) + 
            Math.pow(y - otherNode.y, 2)
          );
          
          if (distance < minDistance) {
            hasCollision = true;
            break;
          }
        }
        
        if (!hasCollision) {
          node.x = x;
          node.y = y;
          validPosition = true;
        }
        
        attempts++;
      }
      
      // Fallback: use grid positioning if can't find collision-free spot
      if (!validPosition) {
        const cols = Math.ceil(Math.sqrt(nodes.length));
        const spacing = Math.min(
          (containerWidth - 200) / cols, 
          (containerHeight - 200) / Math.ceil(nodes.length / cols)
        );
        const col = index % cols;
        const row = Math.floor(index / cols);
        
        node.x = 100 + col * Math.max(spacing, 150);
        node.y = 100 + row * Math.max(spacing, 150);
      }
    });
    
    return nodes;
  };
  const generateNetworkFromSearchResults = (results, type) => {
    const nodes = [];
    const links = [];
    
    results.forEach((item, index) => {
      if (type === 'singers') {
        const name = item.name || item.properties.full_name || `Unknown Singer ${index}`;
        nodes.push({
          id: name,
          name: name,
          type: 'person',
          voiceType: item.properties.voice_type,
          birthYear: (item.properties.birth_year ?? (item.properties.birth && (item.properties.birth.low ?? item.properties.birth))) || null,
          deathYear: (item.properties.death_year ?? (item.properties.death && (item.properties.death.low ?? item.properties.death))) || null,
          birthplace: item.properties.birthplace || item.properties.citizen || null,
          x: 0, // Will be positioned by anti-overlap system
          y: 0
        });
      } else if (type === 'operas') {
        const operaName = item.properties.opera_name || `Unknown Opera ${index}`;
        nodes.push({
          id: operaName,
          name: operaName,
          type: 'opera',
          composer: item.properties.composer,
          x: 0, // Will be positioned by anti-overlap system
          y: 0
        });
      } else if (type === 'books') {
        const bookTitle = item.properties.title || `Unknown Book ${index}`;
        nodes.push({
          id: bookTitle,
          name: bookTitle,
          type: 'book',
          author: item.properties.author,
          x: 0, // Will be positioned by anti-overlap system
          y: 0
        });
      }
    });

    // Apply anti-overlap positioning to all nodes
    positionNodesWithoutOverlap(nodes);

    setNetworkData({ nodes, links });
    resetFiltersForNodeSet(nodes);
    setShowFilterPanel(false);
    setCurrentCenterNode(null); // Reset center tracking for search results
    setShouldRunSimulation(true); // Trigger simulation for search results
  };
  const generateNetworkFromDetails = (details, centerName, type) => {
    const nodes = [];
    const links = [];
    const addedNodes = new Set(); // Track which people have been added
    
    // Helper function to add a person node only if not already added
    const addPersonNode = (person, defaultX, defaultY) => {
      if (!addedNodes.has(person.full_name)) {
        nodes.push({
          id: person.full_name,
          name: person.full_name,
          type: 'person',
          voiceType: person.voice_type,
          birthplace: person.birthplace || person.citizen || null,
          birthYear: (person.birth_year ?? (person.birth && (person.birth.low ?? person.birth))) || null,
          deathYear: (person.death_year ?? (person.death && (person.death.low ?? person.death))) || null,
          spelling_source: person.spelling_source || null,
          voice_type_source: person.voice_type_source || null,
          dates_source: person.dates_source || null,
          birthplace_source: person.birthplace_source || null,
          x: defaultX,
          y: defaultY
        });
        addedNodes.add(person.full_name);
      }
    };
    
    // Add center node with correct type
    const centerNode = {
      id: centerName,
      name: centerName,
      type: type === 'singers' ? 'person' : (type === 'operas' ? 'opera' : 'book'),
      isCenter: true,
      x: 400,
      y: 300
    };
    
    if (type === 'singers' && details.center) {
      centerNode.voiceType = details.center.voice_type;
      centerNode.birthYear = (details.center.birth_year ?? (details.center.birth && (details.center.birth.low ?? details.center.birth))) || null;
      centerNode.deathYear = (details.center.death_year ?? (details.center.death && (details.center.death.low ?? details.center.death))) || null;
      centerNode.birthplace = details.center.birthplace || details.center.citizen || null;
    } else if (type === 'operas' && details.opera) {
      centerNode.composer = details.opera.composer;
    } else if (type === 'books' && details.book) {
      centerNode.author = details.book.author;
    }
    
    nodes.push(centerNode);
    addedNodes.add(centerName);

    // Add composer(s) for opera center via wrote list; fallback to single property if present
    if (type === 'operas') {
      const wroteList = Array.isArray(details.wrote) ? details.wrote : [];
      if (wroteList.length > 0) {
        wroteList.forEach((row, idx) => {
          const composerId = row && (row.composer || row.name || row.full_name);
          if (!composerId) return;
          if (!addedNodes.has(composerId)) {
            nodes.push({ id: composerId, name: composerId, type: 'person', x: 250 + (idx * 40), y: 180 });
            addedNodes.add(composerId);
          }
          links.push({ source: composerId, target: centerName, type: 'wrote', label: 'wrote', sourceInfo: row.source || '' });
        });
      } else if (details.opera && details.opera.composer) {
        const composerId = details.opera.composer;
        if (!addedNodes.has(composerId)) {
          nodes.push({ id: composerId, name: composerId, type: 'person', x: 250, y: 180 });
          addedNodes.add(composerId);
        }
        links.push({ source: composerId, target: centerName, type: 'wrote', label: 'wrote', sourceInfo: '' });
      }
    }

    // Add teachers
    if (details.teachers) {
      details.teachers.forEach((teacher, index) => {
        addPersonNode(teacher, 200 + (index * 50), 150);
        
        links.push({
          source: teacher.full_name,
          target: centerName,
          type: 'taught',
          label: 'taught',
          sourceInfo: teacher.teacher_rel_source || ''
        });
      });
    }

    // Add students
    if (details.students) {
      details.students.forEach((student, index) => {
        addPersonNode(student, 200 + (index * 50), 450);
        
        links.push({
          source: centerName,
          target: student.full_name,
          type: 'taught',
          label: 'taught',
          sourceInfo: student.teacher_rel_source || ''
        });
      });
    }

    // Add family (fallback: some responses may nest under center)
    const familyList = (details.family || details.center?.family || []);
    if (familyList && familyList.length > 0) {
      familyList.forEach((relative, index) => {
        const relName = relative.full_name || relative.name;
        const relativeNorm = { ...relative, full_name: relName };
        addPersonNode(relativeNorm, 600 + (index * 50), 200 + (index * 50));
        
        // Determine correct direction based on relationship_type from backend
        const relType = (relative.relationship_type || '').toLowerCase();
        let src = centerName;
        let tgt = relName;
        if ((relType.includes('parent') && !relType.includes('of')) ||
            (relType.includes('grandparent') && !relType.includes('of'))) {
          // relative is ancestor of center
          src = relName;
          tgt = centerName;
        } else if (relType.includes('parentof') || relType.includes('grandparentof')) {
          // center is ancestor of relative (already src=center, tgt=relative)
        } // spouse/sibling/default keep center -> relative for determinism
        
        links.push({
          source: src,
          target: tgt,
          type: 'family',
          label: relative.relationship_type || 'family',
          sourceInfo: relative.teacher_rel_source || relative.source || ''
        });
      });
    }

    // Add works
    if (details.works) {
      // Add operas
      if (details.works.operas) {
        details.works.operas.forEach((opera, index) => {
          const operaId = opera.opera_name || opera.title || `Unknown Opera ${index}`;
          const operaName = opera.opera_name || opera.title || `Unknown Opera ${index}`;
          nodes.push({
            id: operaId,
            name: operaName,
            type: 'opera',
            role: opera.role,
            composer: opera.composer,
            source: opera.source,
            x: 100 + (index * 80),
            y: 500
          });
          
          links.push({
            source: centerName,
            target: operaId,
            type: 'premiered',
            label: 'premiered role in',
            role: opera.role,
            sourceInfo: opera.source || ''
          });
        });
      }

      // Add books
      if (details.works.books) {
        details.works.books.forEach((book, index) => {
          const bookId = book.title; // Use just the title for consistency
          nodes.push({
            id: bookId,
            name: book.title,
            type: 'book',
            x: 500 + (index * 80),
            y: 500
          });
          
          links.push({
            source: centerName,
            target: bookId,
            type: 'authored',
            label: 'authored',
            sourceInfo: ''
          });
        });
      }

      // Add composed operas (person center) - keep 'composed' label for legacy, but also treat as 'wrote'
      if (details.works.composedOperas) {
        details.works.composedOperas.forEach((opera, index) => {
          const operaTitle = opera.title || `Unknown Opera ${index}`;
          const operaId = `composed_opera_${operaTitle}`;
          nodes.push({
            id: operaId,
            name: operaTitle,
            type: 'opera',
            x: 100 + (index * 80),
            y: 400
          });
          
          links.push({
            source: centerName,
            target: operaId,
            type: 'wrote',
            label: 'wrote'
          });
        });
      }
    }

    // Add premiered roles - behavior depends on center type
    if (details.premieredRoles) {
      if (type === 'operas') {
        // For opera networks: show people who premiered roles in this opera
        details.premieredRoles.forEach((role, index) => {
          const singerId = role.singer || `Unknown Singer ${index}`;
          // Use the same deduplication logic
          if (!addedNodes.has(singerId)) {
            nodes.push({
              id: singerId,
              name: singerId,
              type: 'person',
              voiceType: role.voice_type, // Include voice type for proper styling
              x: 300 + (index * 60),
              y: 400
            });
            addedNodes.add(singerId);
          }
          
          links.push({
            source: singerId,
            target: centerName,
            type: 'premiered',
            label: 'premiered role in',
            role: role.role,
            sourceInfo: role.source
          });
        });
      } else if (type === 'singers') {
        // For person networks: show operas the person premiered roles in
        // This data should be in works.operas, not premieredRoles
        // Skip processing premieredRoles for person networks to avoid confusion
        console.log('Skipping premieredRoles for person network - should use works.operas instead');
      }
    }

    // Add authors for books
    if (details.authors) {
      details.authors.forEach((author, index) => {
        const authorId = author.author || `Unknown Author ${index}`;
        // Use the same deduplication logic
        if (!addedNodes.has(authorId)) {
          nodes.push({
            id: authorId,
            name: authorId,
            type: 'person',
            voiceType: author.voice_type, // Include voice type for proper styling
            x: 200 + (index * 60),
            y: 200
          });
          addedNodes.add(authorId);
        }
        
        links.push({
          source: authorId,
          target: centerName,
          type: 'authored',
          label: 'authored',
          sourceInfo: ''
        });
      });
    }

    // Add editors for books
    if (details.editors) {
      details.editors.forEach((editor, index) => {
        const editorId = editor.editor || `Unknown Editor ${index}`;
        // Use the same deduplication logic
        if (!addedNodes.has(editorId)) {
          nodes.push({
            id: editorId,
            name: editorId,
            type: 'person',
            voiceType: editor.voice_type, // Include voice type for proper styling
            x: 600 + (index * 60),
            y: 200
          });
          addedNodes.add(editorId);
        }
        
        links.push({
          source: editorId,
          target: centerName,
          type: 'edited',
          label: 'edited'
        });
      });
    }

    // Apply anti-overlap positioning to all nodes
    positionNodesWithoutOverlap(nodes);

    clearFiltersForNewSearch(nodes);

    setNetworkData({ nodes, links });
    setCurrentCenterNode(centerName); // Set the center node for this network
    setShouldRunSimulation(true); // Trigger force simulation for new network
  };

  // Function to show full information profile card
  const showFullInformation = async (node) => {
    try {
      setLoading(true);
      let response, data;
      
      if (node.type === 'person') {
        response = await fetchWithRetry(`${API_BASE}/singer/network`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ singerName: node.name, depth: 1 })
        }, { retries: 2, baseDelay: 600 });
        {
          const text = await response.text();
          let parsed; try { parsed = text ? JSON.parse(text) : {}; } catch (_) { parsed = { error: text || 'Invalid response' }; }
        if (response.ok) {
            setProfileCard({ show: true, data: parsed.center });
          } else {
            setError(parsed.error || `Failed to fetch information (${response.status})`);
          }
        }
      } else if (node.type === 'opera') {
        response = await fetchWithRetry(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ operaName: node.name })
        }, { retries: 2, baseDelay: 600 });
        {
          const text = await response.text();
          let parsed; try { parsed = text ? JSON.parse(text) : {}; } catch (_) { parsed = { error: text || 'Invalid response' }; }
        if (response.ok) {
            setProfileCard({ show: true, data: parsed.opera });
          } else {
            setError(parsed.error || `Failed to fetch information (${response.status})`);
          }
        }
      } else if (node.type === 'book') {
        response = await fetchWithRetry(`${API_BASE}/book/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ bookTitle: node.name })
        }, { retries: 2, baseDelay: 600 });
        {
          const text = await response.text();
          let parsed; try { parsed = text ? JSON.parse(text) : {}; } catch (_) { parsed = { error: text || 'Invalid response' }; }
        if (response.ok) {
            setProfileCard({ show: true, data: parsed.book });
          } else {
            setError(parsed.error || `Failed to fetch information (${response.status})`);
        }
      }
      }
    } catch (err) {
      setError('Failed to fetch profile information');
    } finally {
      setLoading(false);
    }
  };
  const normalizeNodeId = (value) => {
    if (value === null || value === undefined) return '';
    return String(value).replace(/\s+/g, ' ').trim();
  };

  const mergeNodeAttributes = (base, incoming) => {
    if (!incoming) return base;
    const result = { ...base };
    Object.entries(incoming).forEach(([key, value]) => {
      if (value === undefined || value === null) return;
      if (key === 'id') {
        result.id = normalizeNodeId(value);
        return;
      }
      if (key === 'name') {
        const name = String(value).trim();
        if (!String(result.name || '').trim()) {
          result.name = name || result.id || '';
        }
        return;
      }
      if (key === 'x' || key === 'y' || key === 'vx' || key === 'vy' || key === 'fx' || key === 'fy' || key === 'homeX' || key === 'homeY') {
        if (!Number.isFinite(result[key])) {
          result[key] = value;
        }
        return;
      }
      if (typeof value === 'boolean') {
        if (result[key] === undefined) {
          result[key] = value;
        }
        return;
      }
      if (typeof value === 'number') {
        if (!Number.isFinite(result[key])) {
          result[key] = value;
        }
        return;
      }
      if (typeof value === 'string') {
        if (!String(result[key] ?? '').trim()) {
          result[key] = value;
        }
        return;
      }
      if (Array.isArray(value)) {
        if (!Array.isArray(result[key]) || result[key].length === 0) {
          result[key] = value;
        }
        return;
      }
      if (typeof value === 'object') {
        if (!result[key]) {
          result[key] = value;
        }
        return;
      }
      if (result[key] === undefined) {
        result[key] = value;
      }
    });
    return result;
  };

  const finalizeNodeCandidate = (candidate) => {
    if (!candidate) return null;
    const normalizedId = normalizeNodeId(candidate.id ?? candidate.name);
    if (!normalizedId) return null;
    const normalizedName = candidate.name ? String(candidate.name).trim() : normalizedId;
    const nodeObj = { ...candidate, id: normalizedId, name: normalizedName };
    if (!Number.isFinite(nodeObj.x)) nodeObj.x = undefined;
    if (!Number.isFinite(nodeObj.y)) nodeObj.y = undefined;
    return nodeObj;
  };

  const normalizeLinkForMerge = (link) => {
    if (!link) return { key: '', link: null };
    const resolveEndpoint = (endpoint) => {
      if (endpoint === null || endpoint === undefined) return '';
      if (typeof endpoint === 'string') return normalizeNodeId(endpoint);
      if (typeof endpoint === 'object') {
        return normalizeNodeId(endpoint.id ?? endpoint.name);
      }
      return normalizeNodeId(endpoint);
    };
    const sourceId = resolveEndpoint(link.source);
    const targetId = resolveEndpoint(link.target);
    if (!sourceId || !targetId) return { key: '', link: null };
    const typeKey = String(link.type || '').toLowerCase();
    const normalizedLink = { ...link, source: sourceId, target: targetId };
    return {
      key: `${sourceId}|${targetId}|${typeKey}`,
      link: normalizedLink
    };
  };

  const mergeNetworkUpdates = (prev, nodesToAdd = [], linksToAdd = [], nodeUpdates) => {
    const updatesMap = new Map();
    const registerUpdate = (key, payload) => {
      const normalizedKey = normalizeNodeId(key);
      if (!normalizedKey) return;
      const candidate = finalizeNodeCandidate({ id: normalizedKey, ...payload });
      if (!candidate) return;
      const current = updatesMap.get(normalizedKey) || { id: normalizedKey };
      updatesMap.set(normalizedKey, mergeNodeAttributes(current, candidate));
    };

    if (nodeUpdates) {
      if (nodeUpdates instanceof Map) {
        nodeUpdates.forEach((value, key) => registerUpdate(key, value));
      } else {
        Object.entries(nodeUpdates).forEach(([key, value]) => registerUpdate(key, value));
      }
    }

    const updatedNodes = (prev.nodes || []).map(node => {
      const key = normalizeNodeId(node.id ?? node.name);
      if (!key) return node;
      const patch = updatesMap.get(key);
      if (!patch) return node;
      return mergeNodeAttributes(node, patch);
    });

    const existingIds = new Set(updatedNodes.map(n => normalizeNodeId(n.id ?? n.name)).filter(Boolean));
    const pendingNewMap = new Map();

    (nodesToAdd || []).forEach(node => {
      const candidate = finalizeNodeCandidate(node);
      if (!candidate) return;
      if (existingIds.has(candidate.id)) {
        const idx = updatedNodes.findIndex(n => normalizeNodeId(n.id ?? n.name) === candidate.id);
        if (idx !== -1) {
          updatedNodes[idx] = mergeNodeAttributes(updatedNodes[idx], candidate);
        }
        return;
      }
      if (pendingNewMap.has(candidate.id)) {
        pendingNewMap.set(candidate.id, mergeNodeAttributes(pendingNewMap.get(candidate.id), candidate));
      } else {
        pendingNewMap.set(candidate.id, candidate);
      }
    });

    const existingLinkKeys = new Set();
    (prev.links || []).forEach(link => {
      const { key } = normalizeLinkForMerge(link);
      if (key) existingLinkKeys.add(key);
    });

    const mergedLinks = [...(prev.links || [])];
    const pendingLinkKeys = new Set();
    (linksToAdd || []).forEach(link => {
      const normalized = normalizeLinkForMerge(link);
      if (!normalized.key || !normalized.link) return;
      if (existingLinkKeys.has(normalized.key) || pendingLinkKeys.has(normalized.key)) return;
      pendingLinkKeys.add(normalized.key);
      mergedLinks.push(normalized.link);
    });

    return {
      nodes: [...updatedNodes, ...pendingNewMap.values()],
      links: mergedLinks
    };
  };

  // Function to expand all relationships for a node
  const expandAllRelationships = async (node) => {
    try {
      pushHistory('expand-all');
      setLoading(true);
      let response, data;
      
      if (node.type === 'person') {
        response = await fetch(`${API_BASE}/singer/network`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ singerName: node.name, depth: 5 })
        });
      } else if (node.type === 'opera') {
        response = await fetch(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ operaName: node.name })
        });
      } else if (node.type === 'book') {
        response = await fetch(`${API_BASE}/book/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ bookTitle: node.name })
        });
      }

      if (response) {
        if (!response.ok) {
          if (handleUnauthorized(response)) return;
        }
        data = await response.json();
        if (response.ok) {
          // Merge new data with existing network
          const existingNodes = new Set(
            (networkData.nodes || []).map(n => normalizeNodeId(n.id ?? n.name)).filter(Boolean)
          );
          const existingLinks = new Set(
            (networkData.links || []).map(l => {
              const sourceId = normalizeNodeId(typeof l.source === 'string' ? l.source : l.source?.id);
              const targetId = normalizeNodeId(typeof l.target === 'string' ? l.target : l.target?.id);
              if (!sourceId || !targetId) return null;
              return `${sourceId}-${targetId}-${String(l.type || '').toLowerCase()}`;
            }).filter(Boolean)
          );
          
          const newNodes = [];
          const newLinks = [];
          const anchorId = normalizeNodeId(node.id ?? node.name);
          const anchorX = Number.isFinite(node?.x) ? node.x : 400;
          const anchorY = Number.isFinite(node?.y) ? node.y : 300;

          const registerNode = (payload) => {
            const candidate = finalizeNodeCandidate({
              ...payload,
              x: Number.isFinite(payload?.x) ? payload.x : anchorX,
              y: Number.isFinite(payload?.y) ? payload.y : anchorY
            });
            if (!candidate) return null;
            const key = candidate.id;
            if (existingNodes.has(key)) {
              return key;
            }
            existingNodes.add(key);
            newNodes.push(candidate);
            return key;
          };

          const addLink = (sourceIdRaw, targetIdRaw, type, extra = {}) => {
            const sourceId = normalizeNodeId(sourceIdRaw);
            const targetId = normalizeNodeId(targetIdRaw);
            if (!sourceId || !targetId) return;
            const linkTypeKey = String(type || '').toLowerCase();
            const linkKey = `${sourceId}-${targetId}-${linkTypeKey}`;
            if (existingLinks.has(linkKey)) return;
            existingLinks.add(linkKey);
            newLinks.push({
              source: sourceId,
              target: targetId,
              type,
              ...extra
            });
          };
          
          // Handle different node types and their data structures
          if (node.type === 'person') {
            // Add new nodes from the expanded data for people
            if (data.teachers) {
              data.teachers.forEach(teacher => {
                const teacherId = registerNode({
                  id: teacher.full_name || teacher.name,
                  name: teacher.full_name || teacher.name,
                  type: 'person',
                  voiceType: teacher.voice_type,
                  spelling_source: teacher.spelling_source || null,
                  voice_type_source: teacher.voice_type_source || null,
                  dates_source: teacher.dates_source || null,
                  birthplace_source: teacher.birthplace_source || null,
                  birthYear: teacher.birth_year,
                  deathYear: teacher.death_year
                });
                if (!teacherId) return;
                addLink(teacherId, anchorId, 'taught', {
                  label: 'taught',
                  sourceInfo: teacher.teacher_rel_source || ''
                });
              });
              // Enrich teacher nodes with full details for CSV immediately
              enrichPersonNodes((data.teachers || []).map(t => t.full_name));
            }
            
            if (data.students) {
              data.students.forEach(student => {
                const studentId = registerNode({
                  id: student.full_name || student.name,
                  name: student.full_name || student.name,
                  type: 'person',
                  voiceType: student.voice_type,
                  spelling_source: student.spelling_source || null,
                  voice_type_source: student.voice_type_source || null,
                  dates_source: student.dates_source || null,
                  birthplace_source: student.birthplace_source || null,
                  birthYear: student.birth_year,
                  deathYear: student.death_year
                });
                if (!studentId) return;
                addLink(anchorId, studentId, 'taught', {
                  label: 'taught',
                  sourceInfo: student.teacher_rel_source || ''
                });
              });
              // Enrich student nodes with full details for CSV immediately
              enrichPersonNodes((data.students || []).map(s => s.full_name));
            }
            
            {
              const familyList = (data.family || data.center?.family || []);
              if (familyList && familyList.length > 0) familyList.forEach(relative => {
                const relId = registerNode({
                  id: relative.full_name || relative.name,
                  name: relative.full_name || relative.name,
                  type: 'person',
                  voiceType: relative.voice_type,
                  spelling_source: relative.spelling_source || null,
                  voice_type_source: relative.voice_type_source || null,
                  dates_source: relative.dates_source || null,
                  birthplace_source: relative.birthplace_source || null,
                  birthYear: relative.birth_year,
                  deathYear: relative.death_year
                });
                if (!relId) return;
                
                const relType = (relative.relationship_type || '').toLowerCase();
                let src = anchorId;
                let tgt = relId;
                if ((relType.includes('parent') && !relType.includes('of')) ||
                    (relType.includes('grandparent') && !relType.includes('of'))) {
                  src = relId;
                  tgt = anchorId;
                }
                addLink(src, tgt, 'family', {
                  label: relative.relationship_type || 'family',
                  sourceInfo: relative.teacher_rel_source || relative.source || ''
                });
              });
              // Enrich family person nodes for CSV immediately
              if (familyList && familyList.length > 0) enrichPersonNodes(familyList.map(r => r.full_name));
            }
            
            if (data.works) {
              if (data.works.operas) {
                data.works.operas.forEach(opera => {
                  const operaId = registerNode({
                    id: opera.opera_name || opera.title || 'Unknown Opera',
                    name: opera.opera_name || opera.title || 'Unknown Opera',
                    type: 'opera',
                    role: opera.role,
                    composer: opera.composer,
                    source: opera.source
                  });
                  if (!operaId) return;
                  addLink(anchorId, operaId, 'premiered', {
                    label: 'premiered role in',
                    role: opera.role,
                    sourceInfo: opera.source
                  });
                });
              }
              
              if (data.works.books) {
                data.works.books.forEach(book => {
                  const bookId = registerNode({
                    id: book.title,
                    name: book.title,
                    type: 'book'
                  });
                  if (!bookId) return;
                  addLink(anchorId, bookId, 'authored', {
                    label: 'authored',
                    sourceInfo: ''
                  });
                });
              }
            }
          } else if (node.type === 'opera') {
            if (data.premieredRoles) {
              data.premieredRoles.forEach(role => {
                const singerId = registerNode({
                  id: role.singer,
                  name: role.singer,
                  type: 'person',
                  voiceType: role.voice_type
                });
                if (!singerId) return;
                addLink(singerId, anchorId, 'premiered', {
                  label: 'premiered role in',
                  role: role.role,
                  sourceInfo: role.source
                });
              });
            }
            
            if (data.opera && data.opera.composer) {
              const composerId = registerNode({
                id: data.opera.composer,
                name: data.opera.composer,
                type: 'person',
                voiceType: 'Composer'
              });
              if (composerId) {
                addLink(composerId, anchorId, 'wrote', {
                  label: 'wrote'
                });
              }
            }
          } else if (node.type === 'book') {
            if (data.book && data.book.author) {
              const authorId = registerNode({
                id: data.book.author,
                name: data.book.author,
                type: 'person'
              });
              if (authorId) {
                addLink(authorId, anchorId, 'authored', {
                  label: 'authored',
                  sourceInfo: ''
                });
              }
            }
          }
          
          if (newNodes.length > 0) {
            const radius = Math.max(180, Math.min(400, 100 + newNodes.length * 30));
            newNodes.forEach((n, idx) => {
              if (!n) return;
              const angle = (idx / newNodes.length) * Math.PI * 2;
              n.x = anchorX + radius * Math.cos(angle);
              n.y = anchorY + radius * Math.sin(angle);
            });
            extendDateRangesForNodes(newNodes);
          }

          setNetworkData(prev => mergeNetworkUpdates(prev, newNodes, newLinks));
          // Refresh counts for the expanded node to keep context menu accurate
          try {
            const updatedCounts = await fetchActualCounts(node);
            setActualCounts(prev => ({ ...prev, [node.id]: updatedCounts }));
          } catch (e) {}

          setIsExpansionSimulation(true);
          setShouldRunSimulation(true);

          // Keep hierarchy root unchanged to ensure additive expansion
        } else {
          setError(data.error);
        }
      }
    } catch (err) {
      setError('Failed to expand relationships');
    } finally {
      setLoading(false);
    }
  };
  // Function to expand specific relationship type
  const expandSpecificRelationship = async (node, relationshipType) => {
    try {
      pushHistory(`expand-${relationshipType}`);
      setLoading(true);
      let response, data;
      
      if (node.type === 'person') {
        response = await fetchWithRetry(`${API_BASE}/singer/network`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ singerName: node.name, depth: 2 })
        }, { retries: 2, baseDelay: 600 });
      } else if (node.type === 'opera') {
        response = await fetchWithRetry(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ operaName: node.name })
        }, { retries: 2, baseDelay: 600 });
      } else if (node.type === 'book') {
        response = await fetchWithRetry(`${API_BASE}/book/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ bookTitle: node.name })
        }, { retries: 2, baseDelay: 600 });
      }

      if (response) {
        if (!response.ok) {
          if (handleUnauthorized(response)) return;
        }
        data = await response.json();
        if (response.ok) {
          // Merge new data with existing network
          const existingNodes = new Set(
            (networkData.nodes || []).map(n => normalizeNodeId(n.id ?? n.name)).filter(Boolean)
          );
          const existingLinks = new Set(
            (networkData.links || []).map(l => {
              const sourceId = normalizeNodeId(typeof l.source === 'string' ? l.source : l.source?.id);
              const targetId = normalizeNodeId(typeof l.target === 'string' ? l.target : l.target?.id);
              if (!sourceId || !targetId) return null;
              return `${sourceId}-${targetId}-${String(l.type || '').toLowerCase()}`;
            }).filter(Boolean)
          );
          
          console.log(`🔍 Expanding "${relationshipType}" for "${node.name}"`);
          console.log(`📊 Current network: ${networkData.nodes.length} nodes, ${networkData.links.length} links`);
          console.log(`🗂️ Existing node IDs:`, Array.from(existingNodes));
          
          const newNodes = [];
          const newLinks = [];
          const anchorId = normalizeNodeId(node.id ?? node.name);
          const anchorX = Number.isFinite(node?.x) ? node.x : 400;
          const anchorY = Number.isFinite(node?.y) ? node.y : 300;

          const registerNode = (payload) => {
            const candidate = finalizeNodeCandidate({
              ...payload,
              x: Number.isFinite(payload?.x) ? payload.x : anchorX,
              y: Number.isFinite(payload?.y) ? payload.y : anchorY
            });
            if (!candidate) return null;
            const key = candidate.id;
            if (existingNodes.has(key)) {
              return key;
            }
            existingNodes.add(key);
            newNodes.push(candidate);
            return key;
          };

          const addLink = (sourceIdRaw, targetIdRaw, type, extra = {}) => {
            const sourceId = normalizeNodeId(sourceIdRaw);
            const targetId = normalizeNodeId(targetIdRaw);
            if (!sourceId || !targetId) return;
            const linkTypeKey = String(type || '').toLowerCase();
            const linkKey = `${sourceId}-${targetId}-${linkTypeKey}`;
            if (existingLinks.has(linkKey)) return;
            existingLinks.add(linkKey);
            newLinks.push({
              source: sourceId,
              target: targetId,
              type,
              ...extra
            });
          };
          
          // Handle specific relationship types for people
          if (node.type === 'person') {
            if (relationshipType === 'taughtBy' && data.teachers) {
              data.teachers.forEach(teacher => {
                const teacherId = registerNode({
                  id: teacher.full_name || teacher.name,
                  name: teacher.full_name || teacher.name,
                  type: 'person',
                  voiceType: teacher.voice_type,
                  spelling_source: teacher.spelling_source || null,
                  voice_type_source: teacher.voice_type_source || null,
                  dates_source: teacher.dates_source || null,
                  birthplace_source: teacher.birthplace_source || null,
                  birthYear: teacher.birth_year,
                  deathYear: teacher.death_year
                });
                if (!teacherId) return;
                addLink(teacherId, anchorId, 'taught', {
                  label: 'taught',
                  sourceInfo: teacher.teacher_rel_source || ''
                });
              });
              enrichPersonNodes((data.teachers || []).map(t => t.full_name));
            }
            
            if (relationshipType === 'taught' && data.students) {
              data.students.forEach(student => {
                const studentId = registerNode({
                  id: student.full_name || student.name,
                  name: student.full_name || student.name,
                  type: 'person',
                  voiceType: student.voice_type,
                  spelling_source: student.spelling_source || null,
                  voice_type_source: student.voice_type_source || null,
                  dates_source: student.dates_source || null,
                  birthplace_source: student.birthplace_source || null,
                  birthYear: student.birth_year,
                  deathYear: student.death_year
                });
                if (!studentId) return;
                addLink(anchorId, studentId, 'taught', {
                  label: 'taught',
                  sourceInfo: student.teacher_rel_source || ''
                });
              });
              enrichPersonNodes((data.students || []).map(s => s.full_name));
            }
            
            if ((relationshipType === 'parent' || relationshipType === 'parentOf' || 
                 relationshipType === 'spouse' || relationshipType === 'spouseOf' ||
                 relationshipType === 'grandparent' || relationshipType === 'grandparentOf' ||
                 relationshipType === 'sibling') && data.family) {
              data.family.forEach(relative => {
                const relType = relative.relationship_type?.toLowerCase() || '';
                let shouldInclude = false;
                
                if (relationshipType === 'parent' && relType.includes('parent') && !relType.includes('of')) {
                  shouldInclude = true;
                } else if (relationshipType === 'parentOf' && relType.includes('parent') && relType.includes('of')) {
                  shouldInclude = true;
                } else if (relationshipType === 'spouse' && relType.includes('spouse')) {
                  shouldInclude = true;
                } else if (relationshipType === 'spouseOf' && relType.includes('spouse')) {
                  shouldInclude = true;
                } else if (relationshipType === 'grandparent' && relType.includes('grandparent') && !relType.includes('of')) {
                  shouldInclude = true;
                } else if (relationshipType === 'grandparentOf' && relType.includes('grandparent') && relType.includes('of')) {
                  shouldInclude = true;
                } else if (relationshipType === 'sibling' && relType.includes('sibling')) {
                  shouldInclude = true;
                }
                
                if (shouldInclude) {
                  const relId = registerNode({
                    id: relative.full_name || relative.name,
                    name: relative.full_name || relative.name,
                    type: 'person',
                    voiceType: relative.voice_type,
                    spelling_source: relative.spelling_source || null,
                    voice_type_source: relative.voice_type_source || null,
                    dates_source: relative.dates_source || null,
                    birthplace_source: relative.birthplace_source || null,
                    birthYear: relative.birth_year,
                    deathYear: relative.death_year
                  });
                  if (!relId) return;
                  
                  const dirType = (relative.relationship_type || '').toLowerCase();
                  let src = anchorId;
                  let tgt = relId;
                  if ((dirType.includes('parent') && !dirType.includes('of')) ||
                      (dirType.includes('grandparent') && !dirType.includes('of'))) {
                    src = relId;
                    tgt = anchorId;
                  }
                  addLink(src, tgt, 'family', {
                    label: relative.relationship_type || 'family',
                    sourceInfo: relative.teacher_rel_source || relative.source || ''
                  });
                }
              });
              enrichPersonNodes((data.family || []).map(r => r.full_name));
            }
            
            if (relationshipType === 'authored' && data.works && data.works.books) {
              data.works.books.forEach(book => {
                const bookId = registerNode({
                  id: book.title,
                  name: book.title,
                  type: 'book'
                });
                if (!bookId) return;
                addLink(anchorId, bookId, 'authored', {
                  label: 'authored',
                  sourceInfo: ''
                });
              });
            }
            
            if (relationshipType === 'premieredRoleIn' && data.works && data.works.operas) {
              data.works.operas.forEach(opera => {
                const operaId = registerNode({
                  id: opera.opera_name || opera.title || 'Unknown Opera',
                  name: opera.opera_name || opera.title || 'Unknown Opera',
                  type: 'opera',
                  role: opera.role,
                  composer: opera.composer,
                  source: opera.source
                });
                if (!operaId) return;
                addLink(anchorId, operaId, 'premiered', {
                  label: 'premiered role in',
                  role: opera.role,
                  sourceInfo: opera.source
                });
              });
            }
          } else if (node.type === 'opera') {
            // Handle singers who premiered roles in this opera
            if (relationshipType === 'premieredRoleIn' && data.premieredRoles) {
              data.premieredRoles.forEach(role => {
                const singerId = registerNode({
                  id: role.singer,
                  name: role.singer,
                  type: 'person',
                  voiceType: role.voice_type
                });
                if (!singerId) return;
                addLink(singerId, anchorId, 'premiered', {
                  label: 'premiered role in',
                  role: role.role,
                  sourceInfo: role.source
                });
              });
            }
            
            // Handle composer who wrote this opera
            if (relationshipType === 'wrote' && data.opera && data.opera.composer) {
              const composerId = registerNode({
                id: data.opera.composer,
                name: data.opera.composer,
                type: 'person',
                voiceType: 'Composer'
              });
              if (composerId) {
                addLink(composerId, anchorId, 'composed', {
                  label: 'composed'
                });
              }
            }
          } else if (node.type === 'book') {
            if ((relationshipType === 'authored' || relationshipType === 'authoredBy') && data.book && data.book.author) {
              const authorId = registerNode({
                id: data.book.author,
                name: data.book.author,
                type: 'person'
              });
              if (authorId) {
                addLink(authorId, anchorId, 'authored', {
                  label: 'authored',
                  sourceInfo: ''
                });
              }
            }
          }
          
          if (newNodes.length > 0 && anchorId) {
            const attachedToAnchor = new Set();
            (networkData.links || []).forEach(link => {
              const sourceId = normalizeNodeId(typeof link.source === 'string' ? link.source : link.source?.id);
              const targetId = normalizeNodeId(typeof link.target === 'string' ? link.target : link.target?.id);
              if (sourceId === anchorId && targetId) attachedToAnchor.add(targetId);
              if (targetId === anchorId && sourceId) attachedToAnchor.add(sourceId);
            });
            newLinks.forEach(link => {
              const sourceId = normalizeNodeId(link.source);
              const targetId = normalizeNodeId(link.target);
              if (sourceId === anchorId && targetId) attachedToAnchor.add(targetId);
              if (targetId === anchorId && sourceId) attachedToAnchor.add(sourceId);
            });

            const relationshipLabel = typeof relationshipType === 'string' ? relationshipType : 'related';
            const normalizedLabel = relationshipLabel.toLowerCase();
            const fallbackType = normalizedLabel.includes('parent') || normalizedLabel.includes('sibling') || normalizedLabel.includes('grandparent')
              ? 'family'
              : 'related';

            newNodes.forEach(n => {
              const nodeId = normalizeNodeId(n.id);
              if (!nodeId || attachedToAnchor.has(nodeId)) return;
              addLink(anchorId, nodeId, fallbackType, {
                label: relationshipLabel,
                sourceInfo: ''
              });
              attachedToAnchor.add(nodeId);
            });

            const simNodeMap = new Map();
            const simNodes = [];
            const register = (id, x, y, pin = false) => {
              const simNode = { id, x, y };
              if (pin) {
                simNode.fx = x;
                simNode.fy = y;
              }
              simNodes.push(simNode);
              simNodeMap.set(id, simNode);
              return simNode;
            };

            register(anchorId, anchorX, anchorY, true);
            const initialRadius = Math.max(60, Math.min(120, 40 + newNodes.length * 10));
            newNodes.forEach((n, idx) => {
              const angle = (idx / newNodes.length) * Math.PI * 2;
              const px = anchorX + Math.cos(angle) * initialRadius;
              const py = anchorY + Math.sin(angle) * initialRadius;
              register(n.id, px, py, false);
            });

            const simLinks = newLinks
              .map(link => ({
                source: typeof link.source === 'string' ? link.source : link.source?.id,
                target: typeof link.target === 'string' ? link.target : link.target?.id
              }))
              .filter(l => simNodeMap.has(l.source) && simNodeMap.has(l.target))
              .map(l => ({
                source: simNodeMap.get(l.source),
                target: simNodeMap.get(l.target)
              }));

            if (simLinks.length > 0) {
              const sim = d3.forceSimulation(simNodes)
                .force('link', d3.forceLink(simLinks).distance(220).strength(1))
                .force('charge', d3.forceManyBody().strength(-260))
                .force('collision', d3.forceCollide().radius(75))
                .force('center', d3.forceCenter(anchorX, anchorY))
                .stop();

              for (let i = 0; i < 200; i += 1) sim.tick();
              sim.stop();
            }

            newNodes.forEach(n => {
              const simNode = simNodeMap.get(n.id);
              if (simNode) {
                n.x = simNode.x;
                n.y = simNode.y;
              } else {
                n.x = anchorX;
                n.y = anchorY;
              }
            });

            extendDateRangesForNodes(newNodes);
          }

          setNetworkData(prev => mergeNetworkUpdates(prev, newNodes, newLinks));

          setIsExpansionSimulation(true);
          setShouldRunSimulation(true);
        } else {
          setError(data.error);
        }
      }
    } catch (err) {
      setError('Failed to expand specific relationship');
    } finally {
      setLoading(false);
    }
  };

  // Function to dismiss other nodes (keep only the selected node, no relationships)
  const dismissOtherNodes = (selectedNode) => {
    const filteredNodes = networkData.nodes.filter(node => node.id === selectedNode.id);
    
    // Remove all relationships - this creates a new visualization starting from this node
    setNetworkData({
      nodes: filteredNodes,
      links: [] // No relationships - clean slate
    });
  };

  // Function to dismiss the selected node
  const dismissNode = (nodeToRemove) => {
    const filteredNodes = networkData.nodes.filter(node => node.id !== nodeToRemove.id);
    const filteredLinks = networkData.links.filter(link => {
      const sourceId = typeof link.source === 'string' ? link.source : link.source?.id;
      const targetId = typeof link.target === 'string' ? link.target : link.target?.id;
      return sourceId !== nodeToRemove.id && targetId !== nodeToRemove.id;
    });
    
    setNetworkData({
      nodes: filteredNodes,
      links: filteredLinks
    });
  };

  const getItemDetails = async (item, itemType = null) => {
    try {
      setLoading(true);
      setSelectedItem(item);
      
      // Use passed itemType or fall back to current searchType
      const typeToUse = itemType || searchType;
      
      if (typeToUse === 'singers') {
        const response = await fetchWithRetry(`${API_BASE}/singer/network`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ singerName: item.name, depth: 2 })
        }, { retries: 2, baseDelay: 600 });

        const text = await response.text();
        let data; try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { error: text || 'Invalid response' }; }
        if (response.ok) {
          data = await enrichWithFamily(data, item.name);
          setItemDetails(data);
          setCurrentView('network');
          generateNetworkFromDetails(data, item.name, 'singers');
        } else {
          setError(data.error || `Failed (${response.status})`);
        }
      } else if (typeToUse === 'operas') {
        const response = await fetchWithRetry(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ operaName: item.properties.opera_name || item.properties.title })
        }, { retries: 2, baseDelay: 600 });

        const text = await response.text();
        let data; try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { error: text || 'Invalid response' }; }
        if (response.ok) {
          data = await enrichWithFamily(data, item.properties.opera_name || item.properties.title);
          setItemDetails(data);
          setCurrentView('network');
          generateNetworkFromDetails(data, item.properties.opera_name || item.properties.title, 'operas');
        } else {
          setError(data.error || `Failed (${response.status})`);
        }
      } else if (typeToUse === 'books') {
        const response = await fetchWithRetry(`${API_BASE}/book/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ bookTitle: item.properties.title })
        }, { retries: 2, baseDelay: 600 });

        const text = await response.text();
        let data; try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { error: text || 'Invalid response' }; }
        if (response.ok) {
          data = await enrichWithFamily(data, item.properties.title);
          setItemDetails(data);
          setCurrentView('network');
          generateNetworkFromDetails(data, item.properties.title, 'books');
        } else {
          setError(data.error || `Failed (${response.status})`);
        }
      }
    } catch (err) {
      setError('Failed to fetch details');
    } finally {
      setLoading(false);
    }
  };

  const searchForPerson = async (personName) => {
    clearFiltersForNewSearch([]);
    try {
      setLoading(true);
      setError('');
      
      const response = await fetchWithRetry(`${API_BASE}/singer/network`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({ singerName: personName, depth: 2 })
      }, { retries: 2, baseDelay: 600 });

      const data = await response.json();
      if (response.ok) {
        const withFam = await enrichWithFamily(data, personName);
        setItemDetails(withFam);
        setSelectedItem({ name: personName });
        setSearchType('singers');
        setCurrentView('network');
        generateNetworkFromDetails(withFam, personName, 'singers');
        setShouldRunSimulation(true); // Trigger simulation for person search
      } else {
        setError(data.error || 'Person not found');
      }
    } catch (err) {
      setError('Failed to fetch person details');
    } finally {
      setLoading(false);
    }
  };
  const searchForPersonFromOpera = async (personName, currentOpera) => {
    clearFiltersForNewSearch([]);
    try {
      setLoading(true);
      setError('');
      
      const response = await fetchWithRetry(`${API_BASE}/singer/network`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({ singerName: personName, depth: 2 })
      }, { retries: 2, baseDelay: 600 });

      const data = await response.json();
      if (response.ok) {
        // Add the current opera to the works if not already present
        if (currentOpera && !data.works.operas.find(opera => (opera.opera_name || opera.title) === currentOpera.name)) {
          data.works.operas.push({
            opera_name: currentOpera.name,
            composer: currentOpera.composer
          });
        }
        
        const withFam = await enrichWithFamily(data, personName);
        setItemDetails(withFam);
        setSelectedItem({ name: personName });
        setSearchType('singers');
        setCurrentView('network');
        generateNetworkFromDetails(withFam, personName, 'singers');
        setShouldRunSimulation(true); // Trigger simulation for person search
      } else {
        setError(data.error || 'Person not found');
      }
    } catch (err) {
      setError('Failed to fetch person details');
    } finally {
      setLoading(false);
    }
  };

  // Generate colors based on voice type
  const getNodeColor = (node) => {
    if (node.type === 'opera') return '#9CA3AF';
    if (node.type === 'book') return '#9CA3AF';
    
    // Create color map from VOICE_TYPES array for consistency
    const colorMap = {};
    VOICE_TYPES.forEach(voiceType => {
      colorMap[voiceType.name] = voiceType.color;
    });
    
    // Handle null/undefined voice types
    if (!node.voiceType) {
      return '#8cc400'; // Unknown voice type color
    }
    
    return colorMap[node.voiceType] || '#6B7280'; // Fallback gray for unmapped types
  };

  // Comprehensive node styling based on type and selection state
  const getNodeStyle = (node, selectedNode) => {
    const baseColor = getNodeColor(node);
    const isSelected = selectedNode && selectedNode.id === node.id;
    
    let stroke, strokeWidth;
    
    if (isSelected) {
      stroke = d3.color(baseColor).darker(0.5);
      strokeWidth = 3;
    } else if (node.type === 'opera') {
      stroke = "#FFFFFF"; // White border for operas
      strokeWidth = 3;
    } else if (node.type === 'book') {
      stroke = "#6a7304"; // Book nodes get olive border
      strokeWidth = 3;
    } else {
      stroke = "none"; // No border for persons
      strokeWidth = 0;
    }
    
    return {
      fill: baseColor,
      stroke: stroke,
      strokeWidth: strokeWidth
    };
  };

  // Accessible text color selection per WCAG contrast
  const srgbToLinear = (c) => {
    const cs = c / 255;
    return cs <= 0.03928 ? cs / 12.92 : Math.pow((cs + 0.055) / 1.055, 2.4);
  };
  const relativeLuminance = (hex) => {
    const m = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(String(hex) || '');
    if (!m) return 1; // default to white luminance
    const r = srgbToLinear(parseInt(m[1], 16));
    const g = srgbToLinear(parseInt(m[2], 16));
    const b = srgbToLinear(parseInt(m[3], 16));
    return 0.2126 * r + 0.7152 * g + 0.0722 * b;
  };
  const contrastRatio = (L1, L2) => {
    const maxL = Math.max(L1, L2);
    const minL = Math.min(L1, L2);
    return (maxL + 0.05) / (minL + 0.05);
  };
  const BLACK = '#111827';
  const WHITE = '#FFFFFF';
  const textColorCache = new Map();
  const getAccessibleTextColor = (bgHex, isLarge = false) => {
    const key = `${bgHex}|${isLarge ? 1 : 0}`;
    if (textColorCache.has(key)) return textColorCache.get(key);
    const Lbg = relativeLuminance(bgHex);
    const Lwhite = 1;
    const Lblack = relativeLuminance(BLACK);
    const threshold = isLarge ? 3 : 4.5;
    const cWhite = contrastRatio(Lwhite, Lbg);
    const cBlack = contrastRatio(Lblack, Lbg);
    let fill = cWhite >= cBlack ? WHITE : BLACK;
    let ratio = Math.max(cWhite, cBlack);
    let needsHalo = ratio < threshold;
    const result = { fill, needsHalo };
    textColorCache.set(key, result);
    return result;
  };
  // Function to handle expansion spacing - called when new nodes are added to network
  ;
  // Network visualization using D3
  const NetworkVisualization = ({ viewport: viewportInfo = {} }) => {
    const viewportIsPhone = !!viewportInfo.isPhone;
    const containerRef = useRef(null);
    const isSimulationActiveRef = useRef(true);
    const zoomRef = useRef(null);
    const zoomTransformRef = useRef(d3.zoomIdentity);
    const zoomLockedRef = useRef(false);
    const baseChargeStrengthRef = useRef(-1000);
    const hasAppliedInitialFitRef = useRef(false);
    const longPressTimeoutRef = useRef(null);
    const touchDragStateRef = useRef(null);

    const LONG_PRESS_DELAY_MS = 550;
    const TOUCH_DRAG_DISTANCE_THRESHOLD = 8;

    const clearLongPress = () => {
      if (longPressTimeoutRef.current) {
        clearTimeout(longPressTimeoutRef.current);
        longPressTimeoutRef.current = null;
      }
    };

    const resetTouchTracking = (pointerId = null) => {
      const state = touchDragStateRef.current;
      if (state && (pointerId === null || state.pointerId === pointerId)) {
        touchDragStateRef.current = null;
      }
    };

    const attachLongPress = (selection) => {
      if (!viewportIsPhone) {
        return selection;
      }
      return selection
        .on('pointerdown.longpress', function(event, datum) {
          if (event.pointerType !== 'touch' || !event.isPrimary) {
            return;
          }
          clearLongPress();
          const targetElement = this;
          const pointerId = event.pointerId;
          const clientX = Number.isFinite(event.clientX) ? event.clientX : (Number.isFinite(event.pageX) ? event.pageX : 0);
          const clientY = Number.isFinite(event.clientY) ? event.clientY : (Number.isFinite(event.pageY) ? event.pageY : 0);
          const pageX = Number.isFinite(event.pageX) ? event.pageX : clientX;
          const pageY = Number.isFinite(event.pageY) ? event.pageY : clientY;
          touchDragStateRef.current = {
            pointerId,
            startX: clientX,
            startY: clientY,
            startPageX: pageX,
            startPageY: pageY,
            hasMoved: false,
            longPressFired: false,
            target: targetElement,
            datum
          };
          try {
            targetElement.setPointerCapture(pointerId);
          } catch (_) {}
          longPressTimeoutRef.current = window.setTimeout(() => {
            const state = touchDragStateRef.current;
            if (!state || state.pointerId !== pointerId || state.hasMoved) {
              return;
            }
            longPressTimeoutRef.current = null;
            state.longPressFired = true;
            const handler = d3.select(targetElement).on('contextmenu');
            if (typeof handler === 'function') {
              const syntheticEvent = {
                preventDefault: () => {},
                stopPropagation: () => {},
                target: targetElement,
                currentTarget: targetElement,
                pointerType: 'touch',
                clientX: state.startX,
                clientY: state.startY,
                pageX: state.startPageX,
                pageY: state.startPageY
              };
              handler.call(targetElement, syntheticEvent, state.datum);
            }
            try { targetElement.releasePointerCapture(pointerId); } catch (_) {}
          }, LONG_PRESS_DELAY_MS);
        })
        .on('pointermove.longpress', (event) => {
          if (event.pointerType === 'touch') {
            const state = touchDragStateRef.current;
            if (state && state.pointerId === event.pointerId) {
              const dx = (event.clientX ?? 0) - state.startX;
              const dy = (event.clientY ?? 0) - state.startY;
              if (!state.hasMoved && Math.sqrt((dx * dx) + (dy * dy)) > TOUCH_DRAG_DISTANCE_THRESHOLD) {
                state.hasMoved = true;
                clearLongPress();
              }
            }
          }
        })
        .on('pointerup.longpress pointercancel.longpress pointerleave.longpress', function(event) {
          if (event.pointerType === 'touch') {
            try { this.releasePointerCapture(event.pointerId); } catch (_) {}
            resetTouchTracking(event.pointerId);
          }
          clearLongPress();
        });
    };

    // Date ranges will be reset manually when needed to avoid setState in useEffect

    useEffect(() => {
      if (!networkData.nodes.length || !containerRef.current) return;

      const container = containerRef.current;
      const width = container.clientWidth;
      const height = visualizationHeight;

      // Restore zoom transform from a global cache across remounts
      try {
        if (window.__cmg_zoomTransform) {
          zoomTransformRef.current = window.__cmg_zoomTransform;
        }
      } catch (_) {}

      // Clear previous visualization
      d3.select(svgRef.current).selectAll("*").remove();

      const svg = d3.select(svgRef.current)
        .attr("width", width)
        .attr("height", height)
        .style("background", "transparent")
        .style("user-select", "none")
        .style("-webkit-user-select", "none")
        .style("touch-action", "none");
      // Prevent default browser context menu on background to avoid accidental pan/zoom
      svg.on('contextmenu', (event) => {
        event.preventDefault();
        if (event.stopImmediatePropagation) event.stopImmediatePropagation();
        event.stopPropagation();
        // Close any open menus on background right-click
        try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch (_) {}
        try { setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' }); } catch (_) {}
        // Reassert current transform to ensure no movement occurs on right-click
        try { applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity); } catch (_) {}
      });
      // Block non-primary button presses from initiating any zoom/drag gesture
      svg.on('mousedown', (event) => {
        if (event.button !== 0) {
          event.preventDefault();
          if (event.stopImmediatePropagation) event.stopImmediatePropagation();
          event.stopPropagation();
          try { applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity); } catch (_) {}
        }
      });
      svg.on('pointerdown.cmg', (event) => {
        if (event.pointerType !== 'touch' && event.buttons && event.buttons !== 1) {
          event.preventDefault();
          if (event.stopImmediatePropagation) event.stopImmediatePropagation();
          event.stopPropagation();
          try { applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity); } catch (_) {}
        }
      });
      svg.on('touchstart.cmg', (event) => {
        event.preventDefault();
      });
      svg.on('mouseup', (event) => {
        if (event.button !== 0) {
          event.preventDefault();
          if (event.stopImmediatePropagation) event.stopImmediatePropagation();
          event.stopPropagation();
          try { applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity); } catch (_) {}
        }
      });
      svg.on('pointerup', (event) => {
        if (event.pointerType !== 'touch' && event.button && event.button !== 0) {
          event.preventDefault();
          if (event.stopImmediatePropagation) event.stopImmediatePropagation();
          event.stopPropagation();
          try { applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity); } catch (_) {}
        }
      });
      svg.on('auxclick', (event) => {
        event.preventDefault();
        if (event.stopImmediatePropagation) event.stopImmediatePropagation();
        event.stopPropagation();
      });

      // Background left-click: anchor all nodes to prevent any drift
      const anchorAllNodes = (event) => {
        if (event.button !== 0) return;
        // Ignore clicks on nodes/links/labels
        const target = event.target;
        if (target.closest && (target.closest('circle') || target.closest('path') || target.closest('text') || target.closest('rect'))) return;
        try {
          networkData.nodes.forEach(n => { n.fx = n.x; n.fy = n.y; n.vx = 0; n.vy = 0; });
          if (simulationRef.current) { try { simulationRef.current.stop(); } catch (_) {} }
          try { setShouldRunSimulation(false); } catch (_) {}
        } catch (_) {}
        try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch (_) {}
        try { setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' }); } catch (_) {}
        setExpandSubmenu(null);
      };
      svg.on('click', anchorAllNodes);
      svg.on('pointerdown.dismissMenu', (event) => {
        if (event.pointerType !== 'touch') return;
        const target = event.target;
        if (target.closest && (target.closest('circle') || target.closest('path') || target.closest('text') || target.closest('rect'))) return;
        try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch (_) {}
        try { setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' }); } catch (_) {}
        setExpandSubmenu(null);
      });

      // Create main group for zooming/panning
      const g = svg.append("g");
      // Also guard the group for safety (in case events bind to inner elements)
      g.on('contextmenu', (event) => {
        event.preventDefault();
        if (event.stopImmediatePropagation) event.stopImmediatePropagation();
        event.stopPropagation();
        // Close any open menus on right-click within main group
        try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch (_) {}
        try { setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' }); } catch (_) {}
        try { applyZoomTransformSilently(zoomTransformRef.current || d3.zoomIdentity); } catch (_) {}
      });

      const longPressTargets = ['circle', 'path', 'text', 'rect.link-label-hit'];
      const applyLongPressHandlers = () => {
        if (viewportIsPhone) {
          longPressTargets.forEach(selector => {
            try { g.selectAll(selector).call(attachLongPress); } catch (_) {}
          });
        } else {
          longPressTargets.forEach(selector => {
            try { g.selectAll(selector).on('.longpress', null); } catch (_) {}
          });
        }
      };
      // Always restore previous zoom/pan silently (no zoom event)
      try {
        const prev = uiZoomRef.current || d3.zoomIdentity;
        d3.select(svgRef.current).property('__zoom', prev);
        g.attr('transform', prev);
      } catch (_) {}

      // Helper to apply a zoom transform silently (no zoom event)
      const applyZoomTransformSilently = (t) => {
        try {
          d3.select(svgRef.current).property('__zoom', t);
          g.attr('transform', t);
          zoomTransformRef.current = t;
          try { window.__cmg_zoomTransform = t; } catch (_) {}
        } catch (_) {}
      };
      // Expose reapply helper globally so outer click handlers can prevent accidental recenter
      try { window.__cmg_reapplyZoom = () => applyZoomTransformSilently(zoomTransformRef.current || d3.zoomIdentity); } catch (_) {}

      // Create zoom behavior
      const minZoom = viewportIsPhone ? 0.2 : 0.1;
      const maxZoom = viewportIsPhone ? 3 : 4;
      const zoom = d3.zoom()
        .filter((event) => {
          // Allow wheel zoom always; block double-click zoom entirely
          if (event.type === 'wheel') return true;
          if (event.type === 'dblclick') return false;
          if (event.pointerType === 'touch' || (typeof event.type === 'string' && event.type.startsWith('touch'))) {
            return true;
          }
          // Explicitly block context menu/right-click and middle-click from initiating zoom/pan
          if (event.button === 2 || event.buttons === 2) return false;
          if (event.button === 1 || event.buttons === 4) return false;
          // Only allow primary button drag without Ctrl/Cmd/Meta
          const isPrimary = (event.buttons === 1) || (event.button === 0);
          return isPrimary && !event.ctrlKey && !event.metaKey;
        })
        .scaleExtent([minZoom, maxZoom])
        .touchable(() => true)
        .on("zoom", (event) => {
          // Hard block any zoom while menus are open or during menu open/close
          if (zoomLockedRef.current || contextMenu.show || linkContextMenu.show) {
            applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity);
            return;
          }
          // Only honor primary-button drag or wheel changes; ignore any other source
          const e = event.sourceEvent;
          const isWheel = e && e.type === 'wheel';
          const isPointerMove = e && (e.type === 'pointermove' || e.type === 'mousemove');
          const isPrimaryDrag = isPointerMove && ((e.buttons === 1) || (e.pointerType === 'touch'));
          // Ignore if the originating pointer is right or middle button
          if (e && (e.buttons === 2 || e.button === 2 || e.buttons === 4 || e.button === 1)) {
            applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity);
            return;
          }
          if (!isWheel && !isPrimaryDrag) {
            // Reassert previous transform to avoid unintended resets (e.g., right-click)
            applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity);
            return;
          }
          g.attr("transform", event.transform);
          zoomTransformRef.current = event.transform;
          uiZoomRef.current = event.transform;
          hasAppliedInitialFitRef.current = true;
        });

      svg.call(zoom);
      // Reassert current zoom once more after zoom is attached
      try {
        const prev = uiZoomRef.current || d3.zoomIdentity;
        d3.select(svgRef.current).property('__zoom', prev);
        g.attr('transform', prev);
      } catch (_) {}
      zoomRef.current = zoom;


      // Hoisted helper to wrap label text inside circles
      function wrapText(textElement, text, maxWidth, fontSize) {
        if (!text || typeof text !== 'string') {
          return ['Unknown'];
        }
        const words = text.split(/(\s+|-)/);
        const lines = [];
        let currentLine = '';
        const charWidth = fontSize * 0.55;
        const maxCharsPerLine = Math.floor(maxWidth / charWidth);
        for (let word of words) {
          if (word === '') continue;
          const testLine = currentLine + word;
          if (testLine.length <= maxCharsPerLine || currentLine === '') {
            currentLine = testLine;
          } else {
            if (currentLine) {
              lines.push(currentLine);
              currentLine = word;
            } else {
              lines.push(word.substring(0, maxCharsPerLine - 1) + '-');
              currentLine = word.substring(maxCharsPerLine - 1);
            }
          }
        }
        if (currentLine) {
          lines.push(currentLine);
        }
        const finalLines = lines.slice(0, 3);
        if (lines.length > 3) {
          finalLines[2] = finalLines[2].substring(0, finalLines[2].length - 3) + '...';
        }
        return finalLines;
      }
      if (false) {
        // Helpers to resolve link labels from current network
        const normalizeId = v => (typeof v === 'string' ? v : (v?.id || v?.name || v));
        const getLinkLabel = (srcId, trgId) => {
          const match = networkData.links.find(l => normalizeId(l.source) === srcId && normalizeId(l.target) === trgId);
          if (!match) return '';
          if (match.label) return match.label;
          switch (match.type) {
            case 'taught': return 'taught';
            case 'premiered': return 'premiered role in';
            case 'composed': return 'wrote';
            case 'wrote': return 'wrote';
            case 'authored': return 'authored';
            case 'family': return match.label || 'family';
            default: return '';
          }
        };
        // Define arrowhead marker
        const defs = svg.append('defs');
        defs.append('marker')
          .attr('id', 'arrowGrey')
          .attr('markerWidth', 10)
          .attr('markerHeight', 10)
          .attr('refX', 8)
          .attr('refY', 3)
          .attr('orient', 'auto')
          .append('path')
          .attr('d', 'M0,0 L0,6 L9,3 z')
          .attr('fill', '#FFFFFF');
        // Directional hierarchy: teachers above (incoming TAUGHT), students and works below (outgoing)
        const nodeById = Object.fromEntries(networkData.nodes.map(n => [n.id, n]));
        const personNodes = networkData.nodes.filter(n => n.type === 'person');
        const rootId = currentCenterNode && networkData.nodes.find(n => n.id === currentCenterNode)
          ? currentCenterNode
          : (personNodes[0]?.id || null);
        if (!rootId) return;

        const rootNode = nodeById[rootId];
        // Opera-centered hierarchical view: people above, opera below
        if (rootNode && rootNode.type === 'opera') {
          const normalizeId = v => (typeof v === 'string' ? v : (v?.id || v?.name || v));
          const relatedPersons = Array.from(new Set(
            networkData.links
              .filter(l => (l.type === 'premiered' || l.type === 'composed' || l.type === 'wrote') && normalizeId(l.target) === rootId)
              .map(l => normalizeId(l.source))
              .filter(id => !!id && nodeById[id] && nodeById[id].type === 'person')
          ));

          const upData = { id: rootId, children: relatedPersons.map(pid => ({ id: pid, children: [] })) };
          const upRoot = d3.hierarchy(upData, d => d.children && d.children.length ? d.children : null);
          const upLayout = d3.tree().nodeSize([160, 100]);
          upLayout(upRoot);

          // Links: arrowheads toward opera
          g.selectAll('path.h-up')
            .data(upRoot.links())
            .enter()
            .append('path')
            .attr('class', 'h-up')
            .attr('fill', 'none')
            .attr('stroke', '#FFFFFF')
            .attr('stroke-width', 1.5)
            .attr('marker-end', 'url(#arrowGrey)')
            .attr('d', d => {
              const sx = d.target.x, sy = -d.target.y; // person
              const tx = d.source.x, ty = -d.source.y; // opera (root)
              const dx = tx - sx, dy = ty - sy; const dist = Math.hypot(dx, dy) || 1; const nodeRadius = 40;
              const ex = tx - (dx / dist) * (nodeRadius + 8); const ey = ty - (dy / dist) * (nodeRadius + 8);
              const my = (sy + ey) / 2; return `M${sx},${sy}C${sx},${my} ${ex},${my} ${ex},${ey}`;
            });

          const upNode = g.selectAll('g.h-up-node')
            .data(upRoot.descendants())
            .enter()
            .append('g')
            .attr('class', 'h-up-node')
            .attr('transform', d => `translate(${d.x},${-d.y})`);

          upNode.append('circle')
            .attr('r', 40)
            .attr('fill', d => {
              const node = nodeById[d.data.id] || { type: 'opera' };
              return getNodeStyle(node, selectedNode).fill;
            })
            .attr('stroke', d => {
              const node = nodeById[d.data.id] || { type: 'opera' };
              return getNodeStyle(node, selectedNode).stroke;
            })
            .attr('stroke-width', 2)
            .style('cursor', 'pointer')
            .attr('opacity', d => {
              const node = nodeById[d.data.id];
              return node ? (isNodeVisible(node) ? 1 : 0.2) : 1;
            })
            .on('contextmenu', (event, d) => {
              event.preventDefault(); event.stopPropagation();
              try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
              try { setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' }); } catch(_) {}
              const nodeData = nodeById[d.data.id]; if (!nodeData) return;
              const menuOffset = 20; const nodeRadius = 40; const containerRect = container.getBoundingClientRect();
              const ctm = g.node().getScreenCTM(); const pt = svgRef.current.createSVGPoint(); pt.x = d.x; pt.y = -d.y;
              const sp = pt.matrixTransform(ctm); const nodeCX = sp.x - containerRect.left; const nodeCY = sp.y - containerRect.top;
              let finalX = nodeCX + nodeRadius + menuOffset; let finalY = nodeCY - nodeRadius;
              const menuWidth = 250, menuHeight = 300, containerWidth = containerRect.width, containerHeight = containerRect.height;
              if (finalX + menuWidth > containerWidth) finalX = nodeCX - nodeRadius - menuOffset - menuWidth;
              if (finalY + menuHeight > containerHeight) finalY = containerHeight - menuHeight;
              if (finalY < 0) finalY = 0; if (finalX < 0) finalX = 0;
              setTimeout(() => { setContextMenu({ show: true, x: finalX, y: finalY, node: nodeData }); setExpandSubmenu(null); }, 0);
            })
            .on('click', (event, d) => {
              event.stopPropagation();
              // Close any open menus when clicking a node
              try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
              try { setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' }); } catch(_) {}
              // Also clear any open Full information card
              try { setProfileCard({ show: false, data: null }); } catch(_) {}
              const node = nodeById[d.data.id]; if (!node) return;
              if (node.type === 'person') { setSearchType('singers'); getItemDetails({ name: node.id }, 'singers'); }
              if (node.type === 'opera') { setSearchType('operas'); getItemDetails({ properties: { opera_name: node.name, title: node.name } }, 'operas'); }
            });

          upNode.each(function(d){
            const group = d3.select(this); const fontSize = 11; const radius = 40; const maxWidth = radius * 1.6;
            const label = nodeById[d.data.id]?.name || d.data.id;
            const lines = wrapText(this, label, maxWidth, fontSize); const lineHeight = fontSize * 1.2;
            if (lines.length === 1) {
              const node = nodeById[d.data.id] || { type: 'opera' };
              const bg = getNodeStyle(node, selectedNode).fill;
              const tc = getAccessibleTextColor(bg, (fontSize >= 18));
              group.append('text').attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif").attr('font-size', `${fontSize}px`).attr('font-weight', '600').attr('fill', tc.fill).attr('stroke', 'none').attr('stroke-width', 0).attr('text-anchor','middle').attr('dy','0.35em').style('pointer-events','none').text(lines[0]);
            } else {
              const totalHeight = (lines.length - 1) * lineHeight; const startOffset = -(totalHeight / 2);
              lines.forEach((line,i)=>{
                const node = nodeById[d.data.id] || { type: 'opera' };
                const bg = getNodeStyle(node, selectedNode).fill;
                const tc = getAccessibleTextColor(bg, (fontSize >= 18));
                group.append('text').attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif").attr('font-size', `${fontSize}px`).attr('font-weight','600').attr('fill',tc.fill).attr('stroke', 'none').attr('stroke-width', 0).attr('text-anchor','middle').attr('y', startOffset + (i * lineHeight)).attr('dy','0.35em').style('pointer-events','none').text(line);
              });
            }
          });

          // Render per-person downward subtrees (students/works) so expansions are visible without re-rooting
          const workTypes = new Set(['premiered', 'wrote', 'composed', 'authored']);
          const outgoingStudents = new Map();
          const worksByPerson = new Map();
          networkData.nodes.filter(n => n.type === 'person').forEach(n => { outgoingStudents.set(n.id, []); worksByPerson.set(n.id, []); });
          networkData.links.forEach(l => {
            const src = normalizeId(l.source); const trg = normalizeId(l.target);
            if (l.type === 'taught' && outgoingStudents.has(src)) outgoingStudents.get(src).push(trg);
            if (workTypes.has(l.type) && worksByPerson.has(src)) worksByPerson.get(src).push(trg);
          });

          const buildDownLocal = (id, visited = new Set(), depth = 3) => {
            if (visited.has(id) || depth <= 0) return [];
            visited.add(id);
            const studentIds = Array.from(new Set((outgoingStudents.get(id) || []).filter(s => s !== id)));
            const workIds = Array.from(new Set(worksByPerson.get(id) || []));
            const children = [];
            studentIds.forEach(s => children.push({ id: s, children: buildDownLocal(s, visited, depth - 1) }));
            workIds.forEach(w => children.push({ id: w, children: [] }));
            return children;
          };

          upRoot.descendants().forEach(personNode => {
            if (personNode.data.id === rootId) return; // skip opera root
            const pid = personNode.data.id;
            const downData = { id: pid, children: buildDownLocal(pid) };
            const downLocalRoot = d3.hierarchy(downData, d => d.children && d.children.length ? d.children : null);
            const downLocalLayout = d3.tree().nodeSize([120, 80]);
            downLocalLayout(downLocalRoot);

            // Draw links for this person's subtree (positive y from person)
            g.selectAll(`path.h-down-local-${pid.replace(/[^a-zA-Z0-9_-]/g,'_')}`)
              .data(downLocalRoot.links().filter(l => l.source.data.id !== l.target.data.id))
              .enter()
              .append('path')
              .attr('class', `h-down-local-${pid.replace(/[^a-zA-Z0-9_-]/g,'_')}`)
              .attr('fill', 'none')
              .attr('stroke', '#FFFFFF')
              .attr('stroke-width', 1.5)
              .attr('marker-end', 'url(#arrowGrey)')
              .attr('d', d => {
                // anchor at person's coordinates (px, py)
                const px = personNode.x, py = -personNode.y; // person is above (negative y)
                const sx = px + (d.source.x - downLocalRoot.x);
                const sy = py + (d.source.y - downLocalRoot.y) * 0.8; // compress depth
                const tx = px + (d.target.x - downLocalRoot.x);
                const ty = py + (d.target.y - downLocalRoot.y) * 0.8;
                const dx = tx - sx, dy = ty - sy; const dist = Math.hypot(dx, dy) || 1; const nodeRadius = 40;
                const ex = tx - (dx / dist) * (nodeRadius + 8); const ey = ty - (dy / dist) * (nodeRadius + 8);
                const my = (sy + ey) / 2; return `M${sx},${sy}C${sx},${my} ${ex},${my} ${ex},${ey}`;
              });

            // Link labels for subtree
            g.selectAll(`text.h-down-local-label-${pid.replace(/[^a-zA-Z0-9_-]/g,'_')}`)
              .data(downLocalRoot.links())
              .enter()
              .append('text')
              .attr('class', `h-down-local-label-${pid.replace(/[^a-zA-Z0-9_-]/g,'_')}`)
              .attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif")
              .attr('font-size', 10)
              .attr('fill', '#FFFFFF')
              .attr('text-anchor', 'middle')
              .style('pointer-events','none')
              .attr('x', d => {
                const px = personNode.x; return px + (d.target.x + d.source.x - 2 * downLocalRoot.x) / 2;
              })
              .attr('y', d => {
                const py = -personNode.y; return py + ((d.target.y + d.source.y - 2 * downLocalRoot.y) / 2) * 0.8 - 6;
              })
              .text(d => getLinkLabel(d.source.data.id, d.target.data.id));

            // Draw nodes for this subtree
            const localNodes = g.selectAll(`g.h-down-local-node-${pid.replace(/[^a-zA-Z0-9_-]/g,'_')}`)
              .data(downLocalRoot.descendants().filter(n => n.data.id !== pid))
              .enter()
              .append('g')
              .attr('class', `h-down-local-node-${pid.replace(/[^a-zA-Z0-9_-]/g,'_')}`)
              .attr('transform', d => {
                const px = personNode.x, py = -personNode.y;
                const nx = px + (d.x - downLocalRoot.x);
                const ny = py + (d.y - downLocalRoot.y) * 0.8;
                return `translate(${nx},${ny})`;
              });

            localNodes.append('circle')
              .attr('r', 40)
              .attr('fill', d => {
                const node = nodeById[d.data.id] || { type: 'opera' };
                return getNodeStyle(node, selectedNode).fill;
              })
              .attr('stroke', d => {
                const node = nodeById[d.data.id] || { type: 'opera' };
                return getNodeStyle(node, selectedNode).stroke;
              })
              .attr('stroke-width', 2)
              .attr('opacity', d => {
                const node = nodeById[d.data.id]; return node ? (isNodeVisible(node) ? 1 : 0.2) : 1;
              })
              .style('cursor', 'pointer')
              .on('contextmenu', (event, d) => {
                event.preventDefault(); event.stopPropagation();
                try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
                try { setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' }); } catch(_) {}
                const nd = nodeById[d.data.id]; if (!nd) return;
                const menuOffset = 20; const nodeRadius = 40; const containerRect = container.getBoundingClientRect();
                const ctm = g.node().getScreenCTM(); const pt = svgRef.current.createSVGPoint(); pt.x = d.x; pt.y = d.y;
                const sp = pt.matrixTransform(ctm); const nodeCX = sp.x - containerRect.left; const nodeCY = sp.y - containerRect.top;
                let finalX = nodeCX + nodeRadius + menuOffset; let finalY = nodeCY - nodeRadius;
                const menuWidth = 250, menuHeight = 300, containerWidth = containerRect.width, containerHeight = containerRect.height;
                if (finalX + menuWidth > containerWidth) finalX = nodeCX - nodeRadius - menuOffset - menuWidth;
                if (finalY + menuHeight > containerHeight) finalY = containerHeight - menuHeight;
                if (finalY < 0) finalY = 0; if (finalX < 0) finalX = 0;
                setTimeout(() => { setContextMenu({ show: true, x: finalX, y: finalY, node: nd }); setExpandSubmenu(null); }, 0);
              })
              .on('click', (event, d) => {
                event.stopPropagation();
                // Close any open menus when clicking a node
                try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
                try { setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' }); } catch(_) {}
                // Also clear any open Full information card
                try { setProfileCard({ show: false, data: null }); } catch(_) {}
                const nd = nodeById[d.data.id]; if (!nd) return;
                if (nd.type === 'person') { setSearchType('singers'); getItemDetails({ name: nd.id }, 'singers'); }
                else if (nd.type === 'opera') { setSearchType('operas'); getItemDetails({ properties: { opera_name: nd.name, title: nd.name } }, 'operas'); }
                else if (nd.type === 'book') { setSearchType('books'); getItemDetails({ properties: { title: nd.name } }, 'books'); }
              });

            localNodes.each(function(d){
              const group = d3.select(this); const fontSize = 11; const radius = 40; const maxWidth = radius * 1.6;
              const label = nodeById[d.data.id]?.name || d.data.id;
              const lines = wrapText(this, label, maxWidth, fontSize); const lineHeight = fontSize * 1.2;
              if (lines.length === 1) {
                const node = nodeById[d.data.id] || { type: 'opera' };
                const bg = getNodeStyle(node, selectedNode).fill;
                const tc = getAccessibleTextColor(bg, (fontSize >= 18));
              group.append('text').attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif").attr('font-size', `${fontSize}px`).attr('font-weight', '600').attr('fill', tc.fill).attr('stroke', 'none').attr('stroke-width', 0).attr('text-anchor','middle').attr('dy','0.35em').style('pointer-events','none').text(lines[0]);
              } else {
                const totalHeight = (lines.length - 1) * lineHeight; const startOffset = -(totalHeight / 2);
                lines.forEach((line,i)=>{
                  const node = nodeById[d.data.id] || { type: 'opera' };
                  const bg = getNodeStyle(node, selectedNode).fill;
                  const tc = getAccessibleTextColor(bg, (fontSize >= 18));
                group.append('text').attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif").attr('font-size', `${fontSize}px`).attr('font-weight','600').attr('fill',tc.fill).attr('stroke', 'none').attr('stroke-width', 0).attr('text-anchor','middle').attr('y', startOffset + (i * lineHeight)).attr('dy','0.35em').style('pointer-events','none').text(line);
                });
              }
            });
          });

          // Intentionally skip any automatic fit to avoid unintended recentering

          return;
        }
        // reuse normalizeId above
        const taughtLinks = networkData.links.filter(l => l.type === 'taught').map(l => ({
          source: normalizeId(l.source),
          target: normalizeId(l.target)
        }));
        // incoming teachers map (teacher -> student is a taught link, so teacher is source, student is target)
        const incomingTeachers = new Map();
        const outgoingStudents = new Map();
        personNodes.forEach(n => { incomingTeachers.set(n.id, []); outgoingStudents.set(n.id, []); });
        taughtLinks.forEach(l => {
          if (incomingTeachers.has(l.target)) incomingTeachers.get(l.target).push(l.source);
          if (outgoingStudents.has(l.source)) outgoingStudents.get(l.source).push(l.target);
        });

        // Works down (premiered/composed/authored)
        const workTypes = new Set(['premiered', 'wrote', 'composed', 'authored']);
        const worksByPerson = new Map();
        personNodes.forEach(n => worksByPerson.set(n.id, []));
        networkData.links.forEach(l => {
          const t = l.type;
          if (!workTypes.has(t)) return;
          const src = normalizeId(l.source);
          const trg = normalizeId(l.target);
          if (worksByPerson.has(src) && nodeById[trg]) worksByPerson.get(src).push(trg);
        });
        const buildUp = (id, visited = new Set()) => {
          if (visited.has(id)) return [];
          visited.add(id);
          const parents = Array.from(new Set((incomingTeachers.get(id) || []).filter(p => p !== id)));
          return parents.map(p => ({ id: p, children: buildUp(p, visited) }));
        };

        const buildDown = (id, visitedPeople = new Set()) => {
          if (visitedPeople.has(id)) return [];
          visitedPeople.add(id);
          const studentIds = Array.from(new Set((outgoingStudents.get(id) || []).filter(s => s !== id)));
          const workIds = Array.from(new Set(worksByPerson.get(id) || []));
          const children = [];
          studentIds.forEach(s => children.push({ id: s, children: buildDown(s, visitedPeople) }));
          workIds.forEach(w => children.push({ id: w, children: [] }));
          return children;
        };

        const upData = { id: rootId, children: buildUp(rootId) };
        const downData = { id: rootId, children: buildDown(rootId) };

        const upRoot = d3.hierarchy(upData, d => d.children && d.children.length ? d.children : null);
        const downRoot = d3.hierarchy(downData, d => d.children && d.children.length ? d.children : null);
        const upLayout = d3.tree().nodeSize([160, 100]); // wider x, shallow y up
        const downLayout = d3.tree().nodeSize([160, 100]); // match spacing
        upLayout(upRoot);
        downLayout(downRoot);

        // Draw upward teachers (negative y)
        const upTeachLinks = g.selectAll('path.h-up')
          .data(upRoot.links())
          .enter()
          .append('path')
          .attr('class', 'h-up')
          .attr('fill', 'none')
          .attr('stroke', '#FFFFFF')
          .attr('stroke-width', 1.5)
          .attr('marker-end', 'url(#arrowGrey)')
          .attr('d', d => {
            // Arrow from teacher (parent = d.target) to student (child = d.source)
            const sx = d.target.x, sy = -d.target.y; // teacher
            const tx = d.source.x, ty = -d.source.y; // student
            const dx = tx - sx, dy = ty - sy; const dist = Math.hypot(dx, dy) || 1;
            const nodeRadius = 40; const ex = tx - (dx / dist) * (nodeRadius + 8); const ey = ty - (dy / dist) * (nodeRadius + 8);
            const my = (sy + ey) / 2;
            return `M${sx},${sy}C${sx},${my} ${ex},${my} ${ex},${ey}`;
          });

        // Labels for teacher->student
        g.selectAll('text.h-up-label')
          .data(upRoot.links())
          .enter()
          .append('text')
          .attr('class','h-up-label')
          .attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif")
          .attr('font-size', 10)
          .attr('fill', '#FFFFFF')
          .attr('text-anchor', 'middle')
          .style('pointer-events','none')
          .attr('x', d => (d.target.x + d.source.x) / 2)
          .attr('y', d => (-(d.target.y) + (-(d.source.y))) / 2 - 6)
          .text('taught');

        const upNode = g.selectAll('g.h-up-node')
          .data(upRoot.descendants())
          .enter()
          .append('g')
          .attr('class', 'h-up-node')
          .attr('transform', d => `translate(${d.x},${-d.y})`);

        upNode.append('circle')
          .attr('r', 40)
          .attr('fill', d => {
            const node = nodeById[d.data.id];
            return getNodeStyle(node || { type: 'person' }, selectedNode).fill;
          })
          .attr('stroke', d => {
            const node = nodeById[d.data.id];
            return getNodeStyle(node || { type: 'person' }, selectedNode).stroke;
          })
          .attr('stroke-width', 2)
          .attr('opacity', d => {
            const node = nodeById[d.data.id];
            return node ? (isNodeVisible(node) ? 1 : 0.2) : 1;
          })
          .style('cursor', 'pointer')
          .on('contextmenu', (event, d) => {
            event.preventDefault(); event.stopPropagation();
            try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
            try { setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' }); } catch(_) {}
            const nodeData = nodeById[d.data.id]; if (!nodeData) return;
            const menuOffset = 20; const nodeRadius = 40;
            const containerRect = container.getBoundingClientRect();
            // Convert local (d.x, -d.y) to screen coordinates accounting for g transform
            const ctm = g.node().getScreenCTM();
            const pt = svgRef.current.createSVGPoint();
            pt.x = d.x; pt.y = -d.y;
            const sp = pt.matrixTransform(ctm);
            const nodeCX = sp.x - containerRect.left;
            const nodeCY = sp.y - containerRect.top;
            let finalX = nodeCX + nodeRadius + menuOffset;
            let finalY = nodeCY - nodeRadius;
            const menuWidth = 250, menuHeight = 300, containerWidth = containerRect.width, containerHeight = containerRect.height;
            if (finalX + menuWidth > containerWidth) finalX = nodeCX - nodeRadius - menuOffset - menuWidth;
            if (finalY + menuHeight > containerHeight) finalY = containerHeight - menuHeight;
            if (finalY < 0) finalY = 0; if (finalX < 0) finalX = 0;
            setTimeout(() => { setContextMenu({ show: true, x: finalX, y: finalY, node: nodeData }); setExpandSubmenu(null); }, 0);
          })
          .on('click', (event, d) => {
            event.stopPropagation();
            const node = nodeById[d.data.id];
            if (node && node.type === 'person') {
              setSearchType('singers');
              getItemDetails({ name: node.id }, 'singers');
            }
          });

        // Wrapped label inside circle
        upNode.each(function(d){
          const group = d3.select(this); const fontSize = 11; const radius = 40; const maxWidth = radius * 1.6;
          const lines = wrapText(this, d.data.id, maxWidth, fontSize); const lineHeight = fontSize * 1.2;
          if (lines.length === 1) {
            const node = nodeById[d.data.id] || { type: 'opera' };
            const bg = getNodeStyle(node, selectedNode).fill;
            const tc = getAccessibleTextColor(bg, (fontSize >= 18));
            group.append('text').attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif").attr('font-size', `${fontSize}px`).attr('font-weight', '600').attr('fill', tc.fill).attr('stroke', 'none').attr('stroke-width', 0).attr('text-anchor','middle').attr('dy','0.35em').style('pointer-events','none').text(lines[0]);
          } else {
            const totalHeight = (lines.length - 1) * lineHeight; const startOffset = -(totalHeight / 2);
            lines.forEach((line,i)=>{
              const node = nodeById[d.data.id] || { type: 'opera' };
              const bg = getNodeStyle(node, selectedNode).fill;
              const tc = getAccessibleTextColor(bg, (fontSize >= 18));
              group.append('text').attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif").attr('font-size', `${fontSize}px`).attr('font-weight','600').attr('fill',tc.fill).attr('stroke', 'none').attr('stroke-width', 0).attr('text-anchor','middle').attr('y', startOffset + (i * lineHeight)).attr('dy','0.35em').style('pointer-events','none').text(line);
            });
          }
        });

        // Draw downward students and works (positive y)
        const downLinks = g.selectAll('path.h-down')
          .data(downRoot.links())
          .enter()
          .append('path')
          .attr('class', 'h-down')
          .attr('fill', 'none')
          .attr('stroke', '#9CA3AF')
          .attr('stroke-width', 1.5)
          .attr('marker-end', 'url(#arrowGrey)')
          .attr('d', d => {
            const sx = d.source.x, sy = d.source.y; const tx = d.target.x, ty = d.target.y;
            const dx = tx - sx, dy = ty - sy; const dist = Math.hypot(dx, dy) || 1; const nodeRadius = 40;
            const ex = tx - (dx / dist) * (nodeRadius + 8); const ey = ty - (dy / dist) * (nodeRadius + 8);
            const my = (sy + ey) / 2; return `M${sx},${sy}C${sx},${my} ${ex},${my} ${ex},${ey}`;
          });

        // Labels for down links
        g.selectAll('text.h-down-label')
          .data(downRoot.links())
          .enter()
          .append('text')
          .attr('class','h-down-label')
          .attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif")
          .attr('font-size', 10)
          .attr('fill', '#666')
          .attr('text-anchor', 'middle')
          .style('pointer-events','none')
          .attr('x', d => (d.target.x + d.source.x) / 2)
          .attr('y', d => ((d.target.y) + (d.source.y)) / 2 - 6)
          .text(d => getLinkLabel(d.source.data.id, d.target.data.id));

        const downNode = g.selectAll('g.h-down-node')
          .data(downRoot.descendants())
          .enter()
          .append('g')
          .attr('class', 'h-down-node')
          .attr('transform', d => `translate(${d.x},${d.y})`);

        downNode.append('circle')
          .attr('r', 40)
          .attr('fill', d => {
            const node = nodeById[d.data.id];
            const base = node || { type: 'opera' };
            return getNodeStyle(base, selectedNode).fill;
          })
          .attr('stroke', d => {
            const node = nodeById[d.data.id];
            const base = node || { type: 'opera' };
            return getNodeStyle(base, selectedNode).stroke;
          })
          .attr('stroke-width', 2)
          .attr('opacity', d => {
            const node = nodeById[d.data.id];
            return node ? (isNodeVisible(node) ? 1 : 0.2) : 1;
          })
          .style('cursor', 'pointer')
          .on('contextmenu', (event, d) => {
            event.preventDefault(); event.stopPropagation();
            try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
            try { setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' }); } catch(_) {}
            const node = nodeById[d.data.id]; if (!node) return;
            const menuOffset = 20; const nodeRadius = 40; const containerRect = container.getBoundingClientRect();
            // Convert local (d.x, d.y) to screen coordinates
            const ctm = g.node().getScreenCTM();
            const pt = svgRef.current.createSVGPoint();
            pt.x = d.x; pt.y = d.y;
            const sp = pt.matrixTransform(ctm);
            const nodeCX = sp.x - containerRect.left; const nodeCY = sp.y - containerRect.top;
            let finalX = nodeCX + nodeRadius + menuOffset; let finalY = nodeCY - nodeRadius;
            const menuWidth = 250, menuHeight = 300, containerWidth = containerRect.width, containerHeight = containerRect.height;
            if (finalX + menuWidth > containerWidth) finalX = nodeCX - nodeRadius - menuOffset - menuWidth;
            if (finalY + menuHeight > containerHeight) finalY = containerHeight - menuHeight;
            if (finalY < 0) finalY = 0; if (finalX < 0) finalX = 0;
            setTimeout(() => { setContextMenu({ show: true, x: finalX, y: finalY, node }); setExpandSubmenu(null); }, 0);
          })
          .on('click', (event, d) => {
            event.stopPropagation();
            const node = nodeById[d.data.id];
            if (!node) return;
            if (node.type === 'person') {
              setSearchType('singers');
              getItemDetails({ name: node.id }, 'singers');
            } else if (node.type === 'opera') {
              setSearchType('operas');
              getItemDetails({ properties: { opera_name: node.name, title: node.name } }, 'operas');
            } else if (node.type === 'book') {
              setSearchType('books');
              getItemDetails({ properties: { title: node.name } }, 'books');
            }
          });

        // Wrapped labels inside circle
        downNode.each(function(d){
          const group = d3.select(this); const fontSize = 11; const radius = 40; const maxWidth = radius * 1.6;
          const lines = wrapText(this, d.data.id, maxWidth, fontSize); const lineHeight = fontSize * 1.2;
          if (lines.length === 1) {
            const node = nodeById[d.data.id] || { type: 'opera' };
            const bg = getNodeStyle(node, selectedNode).fill;
            const tc = getAccessibleTextColor(bg, (fontSize >= 18));
            group.append('text').attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif").attr('font-size', `${fontSize}px`).attr('font-weight', '600').attr('fill', tc.fill).attr('stroke', tc.needsHalo ? (tc.fill === '#FFFFFF' ? '#111827' : '#FFFFFF') : 'none').attr('stroke-width', tc.needsHalo ? 0.8 : 0).attr('text-anchor','middle').attr('dy','0.35em').style('pointer-events','none').text(lines[0]);
          } else {
            const totalHeight = (lines.length - 1) * lineHeight; const startOffset = -(totalHeight / 2);
            lines.forEach((line,i)=>{
              const node = nodeById[d.data.id] || { type: 'opera' };
              const bg = getNodeStyle(node, selectedNode).fill;
              const tc = getAccessibleTextColor(bg, (fontSize >= 18));
              group.append('text').attr('font-family', "'Inter', 'Helvetica Neue', Arial, sans-serif").attr('font-size', `${fontSize}px`).attr('font-weight','600').attr('fill',tc.fill).attr('stroke', tc.needsHalo ? (tc.fill === '#FFFFFF' ? '#111827' : '#FFFFFF') : 'none').attr('stroke-width', tc.needsHalo ? 0.8 : 0).attr('text-anchor','middle').attr('y', startOffset + (i * lineHeight)).attr('dy','0.35em').style('pointer-events','none').text(line);
            });
          }
        });

        // Intentionally skip any automatic fit to avoid unintended recentering

        return;
      }

      // Prepare link data to always reference node objects (not just id strings)
      const nodeById = new Map(networkData.nodes.map(n => [n.id, n]));
      const linkData = (networkData.links || []).map(l => ({
        ...l,
        source: (typeof l.source === 'string' ? nodeById.get(l.source) : l.source),
        target: (typeof l.target === 'string' ? nodeById.get(l.target) : l.target)
      }));

      // Create links (using paths)
      const link = g.append("g")
        .selectAll("path")
        .data(linkData)
        .enter()
        .append("path")
        .attr("stroke", "#6B7280")
        .attr("stroke-opacity", .6)
        .attr("stroke-width", 1.5)
        .attr("fill", "none")
        .attr("opacity", d => isLinkVisible(d) ? 0.6 : 0.12) // Apply filter-based opacity
        .on("contextmenu", (event, d) => {
          event.preventDefault();
          event.stopPropagation();
          // Close any open menus before handling link right-click
          try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
          try { setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' }); } catch(_) {}
          const linkType = (d && d.type ? String(d.type).toLowerCase() : '');
          if (linkType === 'authored' || linkType === 'edited' || linkType === 'wrote') {
            return;
          }
          const containerRect = container.getBoundingClientRect();
          const mouseX = isMobileViewport ? 0 : Math.max(0, event.clientX - containerRect.left);
          const mouseY = isMobileViewport ? 0 : Math.max(0, event.clientY - containerRect.top);
          setTimeout(() => {
            const srcNode = typeof d.source === 'string' ? networkData.nodes.find(n => n.id === d.source) : d.source;
            const tgtNode = typeof d.target === 'string' ? networkData.nodes.find(n => n.id === d.target) : d.target;
            const isPersonToOpera = srcNode?.type === 'person' && tgtNode?.type === 'opera';
            setLinkContextMenu({
              show: true,
              x: Math.max(0, mouseX),
              y: Math.max(0, mouseY),
              role: isPersonToOpera && d.type === 'premiered' ? (d.role || d.target?.role || '') : '',
              source: d.sourceInfo || d.target?.source || ''
            });
            try { applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity); } catch (_) {}
          }, 0);
        });

      // Invisible, wider hit area for easier right-click on links
      const linkHit = g.append("g")
        .selectAll("path.link-hit")
        .data(linkData)
        .enter()
        .append("path")
        .attr("class", "link-hit")
        .attr("stroke", "transparent")
        .attr("stroke-width", 12)
        .attr("fill", "none")
        .style("pointer-events", "stroke")
        .on("contextmenu", (event, d) => {
          event.preventDefault();
          event.stopPropagation();
          // Close any open menus before handling link right-click
          try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
          try { setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' }); } catch(_) {}
          const linkType = (d && d.type ? String(d.type).toLowerCase() : '');
          if (linkType === 'authored' || linkType === 'edited' || linkType === 'wrote') {
            return;
          }
          const containerRect = container.getBoundingClientRect();
          const mouseX = isMobileViewport ? 0 : Math.max(0, event.clientX - containerRect.left);
          const mouseY = isMobileViewport ? 0 : Math.max(0, event.clientY - containerRect.top);
          setTimeout(() => {
            const srcNode = typeof d.source === 'string' ? networkData.nodes.find(n => n.id === d.source) : d.source;
            const tgtNode = typeof d.target === 'string' ? networkData.nodes.find(n => n.id === d.target) : d.target;
            const isPersonToOpera = srcNode?.type === 'person' && tgtNode?.type === 'opera';
            setLinkContextMenu({
              show: true,
              x: Math.max(0, mouseX),
              y: Math.max(0, mouseY),
              role: isPersonToOpera && d.type === 'premiered' ? (d.role || d.target?.role || '') : '',
              source: d.sourceInfo || d.target?.source || ''
            });
            try { applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity); } catch (_) {}
          }, 0);
        });

      // Build directed-pair groups so multiple relationships share one label (joined with ", ")
      const getNodeId = n => (typeof n === 'string' ? n : (n?.id || n?.name || ''));
      const normalizeRelLabel = (lbl) => {
        if (!lbl && lbl !== 0) return '';
        const v = String(lbl).toLowerCase();
        if (v === 'parentof' || v === 'parent') return 'parent';
        if (v === 'grandparentof' || v === 'grandparent') return 'grandparent';
        return v;
      };
      const toNodeObj = n => (typeof n === 'string' ? networkData.nodes.find(nn => nn.id === n) : n);
      const directedPairKey = (s, t) => `${getNodeId(s)}->${getNodeId(t)}`;
      const directedGroups = new Map();
      linkData.forEach(ld => {
        const sObj = toNodeObj(ld.source);
        const tObj = toNodeObj(ld.target);
        if (!sObj || !tObj) return;
        const key = directedPairKey(sObj, tObj);
        if (!directedGroups.has(key)) {
          directedGroups.set(key, { source: sObj, target: tObj, labels: [], isPath: false });
        }
        const g = directedGroups.get(key);
        if (ld.label) g.labels.push(normalizeRelLabel(ld.label));
        if (ld.role && !ld.label) g.labels.push(normalizeRelLabel(ld.role));
        g.isPath = g.isPath || !!ld.isPath;
      });
      const linkLabelData = Array.from(directedGroups.values()).map(g => ({
        source: g.source,
        target: g.target,
        label: (g.labels.length ? Array.from(new Set(g.labels)).join(', ') : ''),
        count: g.labels.length,
        isPath: g.isPath
      }));

      // Create link labels
      const linkLabels = g.append("g")
        .selectAll("text")
        .data(linkLabelData)
        .enter()
        .append("text")
        .attr("font-family", "'Inter', 'Helvetica Neue', Arial, sans-serif")
        .attr("font-size", "10px")
        .attr("font-weight", "500")
        .attr("fill", "#FFFFFF")
        .attr("text-anchor", "middle")
        .attr("dy", "-5px")
        .style("pointer-events", "auto")
        .on("contextmenu", (event, d) => {
          event.preventDefault();
          event.stopPropagation();
          // Close any open menus before handling link-label right-click
          try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
          try { setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' }); } catch(_) {}
          const containerRect = container.getBoundingClientRect();
          const mouseX = isMobileViewport ? 0 : Math.max(0, event.clientX - containerRect.left);
          const mouseY = isMobileViewport ? 0 : Math.max(0, event.clientY - containerRect.top);
          // Find a representative link for this directed pair
          const srcId = typeof d.source === 'string' ? d.source : d.source?.id;
          const tgtId = typeof d.target === 'string' ? d.target : d.target?.id;
          const matching = networkData.links.find(l => (typeof l.source === 'string' ? l.source : l.source?.id) === srcId && (typeof l.target === 'string' ? l.target : l.target?.id) === tgtId);
          const linkType = (matching && matching.type ? String(matching.type).toLowerCase() : '');
          if (linkType === 'authored' || linkType === 'edited' || linkType === 'wrote') {
            return;
          }
          const srcNode = typeof d.source === 'string' ? networkData.nodes.find(n => n.id === d.source) : d.source;
          const tgtNode = typeof d.target === 'string' ? networkData.nodes.find(n => n.id === d.target) : d.target;
          const isPersonToOpera = srcNode?.type === 'person' && tgtNode?.type === 'opera';
          setTimeout(() => {
            setLinkContextMenu({
              show: true,
              x: Math.max(0, mouseX),
              y: Math.max(0, mouseY),
              role: isPersonToOpera && matching?.type === 'premiered' ? (matching.role || '') : '',
              source: matching?.sourceInfo || ''
            });
            try { applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity); } catch (_) {}
          }, 0);
        })
        .attr("opacity", d => isLinkVisible(d) ? 1 : 0.2) // Apply filter-based opacity
        .text(d => d.label);

      // Invisible hit rectangles for labels to enlarge click area
      const approximateTextWidth = (text) => Math.max(30, (text || '').length * 6);
      const linkLabelHits = g.append("g")
        .selectAll("rect.link-label-hit")
        .data(linkLabelData)
        .enter()
        .append("rect")
        .attr("class", "link-label-hit")
        .attr("fill", "transparent")
        .attr("stroke", "none")
        .style("opacity", 0)
        .style("pointer-events", "all")
        .on("contextmenu", (event, d) => {
          event.preventDefault();
          event.stopPropagation();
          // Close any open menus before handling link-label-hit right-click
          try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
          try { setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' }); } catch(_) {}
          const containerRect = container.getBoundingClientRect();
          const mouseX = isMobileViewport ? 0 : Math.max(0, event.clientX - containerRect.left);
          const mouseY = isMobileViewport ? 0 : Math.max(0, event.clientY - containerRect.top);
          const srcId = typeof d.source === 'string' ? d.source : d.source?.id;
          const tgtId = typeof d.target === 'string' ? d.target : d.target?.id;
          const matching = networkData.links.find(l => (typeof l.source === 'string' ? l.source : l.source?.id) === srcId && (typeof l.target === 'string' ? l.target : l.target?.id) === tgtId);
          const linkType = (matching && matching.type ? String(matching.type).toLowerCase() : '');
          if (linkType === 'authored' || linkType === 'edited' || linkType === 'wrote') {
            return;
          }
          const srcNode = typeof d.source === 'string' ? networkData.nodes.find(n => n.id === d.source) : d.source;
          const tgtNode = typeof d.target === 'string' ? networkData.nodes.find(n => n.id === d.target) : d.target;
          const isPersonToOpera = srcNode?.type === 'person' && tgtNode?.type === 'opera';
          setTimeout(() => {
            setLinkContextMenu({
              show: true,
              x: Math.max(0, mouseX),
              y: Math.max(0, mouseY),
              role: isPersonToOpera && matching?.type === 'premiered' ? (matching.role || '') : '',
              source: matching?.sourceInfo || ''
            });
            try { applyZoomTransformSilently(uiZoomRef.current || d3.zoomIdentity); } catch (_) {}
          }, 0);
        });
      // Create nodes
      const node = g.append("g")
        .selectAll("circle")
        .data(networkData.nodes)
        .enter()
        .append("circle")
        .attr("r", 40)
        .attr("fill", d => getNodeStyle(d, selectedNode).fill)
        .attr("stroke", d => getNodeStyle(d, selectedNode).stroke)
        .attr("stroke-width", d => getNodeStyle(d, selectedNode).strokeWidth)
        .attr("opacity", d => isNodeVisible(d) ? 1 : 0.2) // Apply filter-based opacity
        .style("cursor", "pointer")
        .on("click", (event, d) => {
          // Snapshot before potentially changing the network by click
          pushHistory('node-click');
          // Prevent event from bubbling to background
          event.stopPropagation();
          // Close any open menus when clicking a node
          try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch(_) {}
          try { setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' }); } catch(_) {}
          // Also clear any open Full information card
          try { setProfileCard({ show: false, data: null }); } catch(_) {}
          
          // Only trigger new simulation if this is a different node than current center
          if (currentCenterNode === d.id) {
            // Same node clicked - only update visual selection, no simulation
            // Use setTimeout to avoid setState in useEffect
            setTimeout(() => {
              setSelectedNode(selectedNode && selectedNode.id === d.id ? null : d);
            }, 0);
            return;
          }
          
          // New node clicked - trigger full details and simulation
          setCurrentCenterNode(d.id);
          
          // Create appropriate mock item based on node type
          if (d.type === 'person') {
            const mockSearchItem = {
              name: d.name,
              properties: {
                full_name: d.name,
                voice_type: d.voiceType,
                birth_year: d.birthYear,
                death_year: d.deathYear
              }
            };
            setSearchType('singers'); // Update UI state
            getItemDetails(mockSearchItem, 'singers'); // Pass type directly
          } else if (d.type === 'opera') {
            const mockSearchItem = {
              properties: {
                title: d.name,
                composer: d.composer
              }
            };
            setSearchType('operas'); // Update UI state
            getItemDetails(mockSearchItem, 'operas'); // Pass type directly
          } else if (d.type === 'book') {
            const mockSearchItem = {
              properties: {
                title: d.name,
                author: d.author
              }
            };
            setSearchType('books'); // Update UI state
            getItemDetails(mockSearchItem, 'books'); // Pass type directly
          }
          
          // Set selected node for visual feedback
          // Use setTimeout to avoid setState in useEffect
          setTimeout(() => {
            setSelectedNode(d);
          }, 0);
        })
        .on("contextmenu", (event, d) => {
          event.preventDefault();
          event.stopPropagation();
          // Ensure any menus are closed when right-clicking a node
          try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch (_) {}
          try { setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' }); } catch (_) {}
          
          // Calculate screen position using current zoom/pan transform
          const nodeRadius = 40;
          const menuOffset = 20;
          const containerRect = container.getBoundingClientRect();
          const ctm = g.node().getScreenCTM();
          const pt = svgRef.current.createSVGPoint();
          pt.x = d.x; pt.y = d.y;
          const sp = pt.matrixTransform(ctm);
          const nodeCX = sp.x - containerRect.left;
          const nodeCY = sp.y - containerRect.top;
          
          let finalX;
          let finalY;
          if (isMobileViewport) {
            finalX = 0;
            finalY = 0;
          } else {
            // Position relative to the container (not absolute)
            finalX = nodeCX + nodeRadius + menuOffset;
            finalY = nodeCY - nodeRadius;

            // Keep menu within container bounds
            const menuWidth = 250; // estimated menu width
            const menuHeight = 300; // estimated menu height
            const containerWidth = containerRect.width;
            const containerHeight = containerRect.height;

            if (finalX + menuWidth > containerWidth) {
              finalX = nodeCX - nodeRadius - menuOffset - menuWidth;
            }
            if (finalY + menuHeight > containerHeight) {
              finalY = containerHeight - menuHeight;
            }
            if (finalY < 0) finalY = 0;
            if (finalX < 0) finalX = 0;
          }
          
          // Temporarily lock zoom updates while opening/closing the menu
          setTimeout(() => {
            // Snapshot prior to opening context menu / expansions
            pushHistory('context-open');
            zoomLockedRef.current = true;
            setContextMenu({ show: true, x: finalX, y: finalY, node: d });
            setExpandSubmenu(null);
            try {
              const t = zoomTransformRef.current || d3.zoomIdentity;
              const svgSel = d3.select(svgRef.current);
              svgSel.property('__zoom', t);
              g.attr('transform', t);
            } catch (_) {}
            // Unlock after a short delay to allow React to render menu without D3 zoom interference
            setTimeout(() => { zoomLockedRef.current = false; }, 50);
          }, 0);
          // Clear any pending submenu timeout
          if (submenuTimeoutRef.current) {
            clearTimeout(submenuTimeoutRef.current);
          }
        })
        .call(
          d3.drag()
            .touchable(() => true)
            .clickDistance(viewportIsPhone ? TOUCH_DRAG_DISTANCE_THRESHOLD : 0)
            .filter(event => {
              const src = event?.sourceEvent || event;
              if (!src) return false;
              const pointerType = src.pointerType || (src.touches ? 'touch' : undefined);
              if (pointerType === 'touch') {
                if ((src.touches && src.touches.length > 1) || src.ctrlKey || src.metaKey) {
                  return false;
                }
                return true;
              }
              const buttons = typeof src.buttons === 'number' ? src.buttons : null;
              const button = typeof src.button === 'number' ? src.button : null;
              const isPrimary = (buttons === 1) || (button === 0) || (buttons === null && button === null);
              if (!isPrimary) return false;
              return !src.ctrlKey && !src.metaKey;
            })
            .on("start", dragstarted)
            .on("drag", dragged)
            .on("end", dragended)
        );

      // Directed-pair labels already grouped; no additional precompute needed

      // Create node labels with multi-line text support
      const nodeLabels = g.append("g")
        .selectAll("g")
        .data(networkData.nodes)
        .enter()
        .append("g")
        .attr("class", "node-label");
      // Add text elements for each node
      nodeLabels.each(function(d) {
        const group = d3.select(this);
        const fontSize = d.isCenter ? 11 : 10;
        const radius = 40;
        const maxWidth = radius * 1.6;
        
        const lines = wrapText(this, d.name, maxWidth, fontSize);
        const lineHeight = fontSize * 1.2;
        
        if (lines.length === 1) {
          const bg = getNodeStyle(d, selectedNode).fill;
          const tc = getAccessibleTextColor(bg, (fontSize >= 18));
          group.append("text")
            .attr("font-family", "'Inter', 'Helvetica Neue', Arial, sans-serif")
            .attr("font-size", `${fontSize}px`)
            .attr("font-weight", d.isCenter ? "600" : "500")
            .attr("fill", tc.fill)
            .attr("stroke", 'none')
            .attr("stroke-width", 0)
            .attr("text-anchor", "middle")
            .attr("x", 0)
            .attr("y", 0)
            .attr("dy", "0.35em")
            .style("pointer-events", "none")
            .text(lines[0]);
        } else {
          const totalHeight = (lines.length - 1) * lineHeight;
          const startOffset = -(totalHeight / 2);
          
          lines.forEach((line, i) => {
            const bg = getNodeStyle(d, selectedNode).fill;
            const tc = getAccessibleTextColor(bg, (fontSize >= 18));
            group.append("text")
              .attr("font-family", "'Inter', 'Helvetica Neue', Arial, sans-serif")
              .attr("font-size", `${fontSize}px`)
              .attr("font-weight", d.isCenter ? "600" : "500")
              .attr("fill", tc.fill)
              .attr("stroke", 'none')
              .attr("stroke-width", 0)
              .attr("text-anchor", "middle")
              .attr("x", 0)
              .attr("y", startOffset + (i * lineHeight))
              .attr("dy", "0.35em")
              .style("pointer-events", "none")
              .text(line);
          });
        }
      });

      
      const renderNetwork = () => {
        // Compute parallel link metadata so multiple relationships between the same pair don't overlap
        const groupMap = new Map();
        const pairKey = (l) => {
          const s = typeof l.source === 'string' ? l.source : l.source?.id;
          const t = typeof l.target === 'string' ? l.target : l.target?.id;
          return `${[s, t].sort().join('~')}~${l.type}`;
        };
        networkData.links.forEach(l => {
          const key = pairKey(l);
          if (!groupMap.has(key)) groupMap.set(key, []);
          groupMap.get(key).push(l);
        });
        groupMap.forEach(arr => {
          arr.forEach((l, i) => { l._parallelIndex = i; l._parallelCount = arr.length; });
        });
        // Convert link source/target to objects if they're strings
        const processedLinks = linkData;

        // Position links
        link
          .attr("stroke", _d => "#FFFFFF")
          .attr("stroke-width", d => d.isPath ? 2.5 : 1.5)
          .attr("stroke-opacity", _d => 1)
          .attr("d", d => {
          const source = d.source;
          const target = d.target;
            
          if (!source || !target) return '';
          
          const dx = target.x - source.x;
          const dy = target.y - source.y;
          const distance = Math.sqrt(dx * dx + dy * dy);
          const nodeRadius = 40;
          const baseMargin = d.isPath ? 14 : 16;
          const margin = d.type === 'premiered' ? 8 : baseMargin;
          const adjusted = Math.max(0, margin - 5);
          const adjustedStart = Math.max(0, adjusted - 5);
          const endX = target.x - (dx / distance) * (nodeRadius + adjusted);
          const endY = target.y - (dy / distance) * (nodeRadius + adjusted);
          const startX = source.x + (dx / distance) * (nodeRadius + adjustedStart);
          const startY = source.y + (dy / distance) * (nodeRadius + adjustedStart);

          // Curve offset perpendicular to the line for parallel links
          const count = d._parallelCount || 1;
          const index = d._parallelIndex || 0;
          const spread = 14; // revert to previous spacing
          const offset = count > 1 ? (index - (count - 1) / 2) * spread : 0;
          const nx = distance ? -dy / distance : 0;
          const ny = distance ? dx / distance : 0;
          const mx = (startX + endX) / 2 + nx * offset;
          const my = (startY + endY) / 2 + ny * offset;

          return `M${startX},${startY} Q ${mx},${my} ${endX},${endY}`;
        })
        .attr("opacity", d => isLinkVisible(d) ? 1 : 0.12); // Apply filter-based opacity

        // Keep hit area in sync with link positions
        linkHit.attr("d", d => {
          const source = typeof d.source === 'string' ? 
            networkData.nodes.find(n => n.id === d.source) : d.source;
          const target = typeof d.target === 'string' ? 
            networkData.nodes.find(n => n.id === d.target) : d.target;
          if (!source || !target) return '';
          const dx = target.x - source.x;
          const dy = target.y - source.y;
          const distance = Math.sqrt(dx * dx + dy * dy);
          const nodeRadius = 40;
          const baseMargin = d.isPath ? 14 : 16;
          const margin = d.type === 'premiered' ? 8 : baseMargin;
          const adjusted = Math.max(0, margin - 5);
          const adjustedStart = Math.max(0, adjusted - 5);
          const endX = target.x - (dx / distance) * (nodeRadius + adjusted);
          const endY = target.y - (dy / distance) * (nodeRadius + adjusted);
          const startX = source.x + (dx / distance) * (nodeRadius + adjustedStart);
          const startY = source.y + (dy / distance) * (nodeRadius + adjustedStart);
          const count = d._parallelCount || 1;
          const index = d._parallelIndex || 0;
          const spread = 14;
          const offset = count > 1 ? (index - (count - 1) / 2) * spread : 0;
          const nx = distance ? -dy / distance : 0;
          const ny = distance ? dx / distance : 0;
          const mx = (startX + endX) / 2 + nx * offset;
          const my = (startY + endY) / 2 + ny * offset;
          return `M${startX},${startY} Q ${mx},${my} ${endX},${endY}`;
        });

        // Remove arrows and link paths, then redraw from merged link data to avoid stale directions
        g.selectAll(".arrow-group").remove();
        link.attr("d", d => {
          const source = typeof d.source === 'string' ? networkData.nodes.find(n => n.id === d.source) : d.source;
          const target = typeof d.target === 'string' ? networkData.nodes.find(n => n.id === d.target) : d.target;
          if (!source || !target) return '';
          const dx = target.x - source.x;
          const dy = target.y - source.y;
          const distance = Math.sqrt(dx * dx + dy * dy);
          const nodeRadius = 40;
          const margin = d.isPath ? 15 : 17;
          const adjusted = Math.max(0, margin - 5);
          const adjustedStart = Math.max(0, adjusted - 5);
          if (distance < 1e-6) {
            // Self-loop: U-shaped loop below the node, from bottom-right to bottom-left
            const x = source.x;
            const y = source.y;
            const sideOffset = 6; // narrower
            const loopHeight = 80; // longer
            const sx = x + sideOffset;
            const sy = y + (nodeRadius + adjustedStart);
            const ex = x - sideOffset;
            const ey = y + (nodeRadius + adjustedStart);
            const cp1x = x + 25;
            const cp1y = y + nodeRadius + loopHeight;
            const cp2x = x - 25;
            const cp2y = y + nodeRadius + loopHeight;
            return `M${sx},${sy} C ${cp1x},${cp1y} ${cp2x},${cp2y} ${ex},${ey}`;
          }
          const endX = target.x - (dx / distance) * (nodeRadius + adjusted);
          const endY = target.y - (dy / distance) * (nodeRadius + adjusted);
          const startX = source.x + (dx / distance) * (nodeRadius + adjustedStart);
          const startY = source.y + (dy / distance) * (nodeRadius + adjustedStart);
          return `M${startX},${startY}L${endX},${endY}`;
        });
        
        // Create arrows directly for each link
        processedLinks.forEach(linkData => {
          if (!linkData.source || !linkData.target) return;
          
          const dx = linkData.target.x - linkData.source.x;
          const dy = linkData.target.y - linkData.source.y;
          const distance = Math.sqrt(dx * dx + dy * dy);
          
          if (distance > 0) {
            const nodeRadius = 40;
            const extraBase = linkData.isPath ? 12 : 10;
            const extra = Math.max(0, extraBase - 5);
            const arrowX = linkData.target.x - (dx / distance) * (nodeRadius + extra);
            const arrowY = linkData.target.y - (dy / distance) * (nodeRadius + extra);
            
            const angle = Math.atan2(dy, dx);
            const arrowLength = linkData.isPath ? 10 : 8;
            
            const x1 = arrowX - Math.cos(angle - Math.PI / 6) * arrowLength;
            const y1 = arrowY - Math.sin(angle - Math.PI / 6) * arrowLength;
            
            const x2 = arrowX - Math.cos(angle + Math.PI / 6) * arrowLength;
            const y2 = arrowY - Math.sin(angle + Math.PI / 6) * arrowLength;
            
            g.append("polygon")
              .attr("class", "arrow-group")
              .attr("points", `${arrowX},${arrowY} ${x1},${y1} ${x2},${y2}`)
              .attr("fill", "#FFFFFF")
              .attr("opacity", isLinkVisible(linkData) ? 1 : 0.12)
              .attr("stroke", "none");
          } else {
            // Self-loop arrow: at the returning end on bottom-left, pointing up toward node
            const nodeRadius = 40;
            const extraBase = linkData.isPath ? 12 : 10;
            const extra = Math.max(0, extraBase - 5);
            const arrowLength = linkData.isPath ? 10 : 8;
            const sideOffset = 6;
            const loopHeight = 80;
            const x = linkData.source.x;
            const y = linkData.source.y;
            const cp2x = x - 25;
            const cp2y = y + nodeRadius + loopHeight;
            const arrowX = x - sideOffset;
            const arrowY = y + (nodeRadius + extra);
            const angle = Math.atan2(arrowY - cp2y, arrowX - cp2x); // tangent at end, about upward
            const x1 = arrowX - Math.cos(angle - Math.PI / 6) * arrowLength;
            const y1 = arrowY - Math.sin(angle - Math.PI / 6) * arrowLength;
            const x2 = arrowX - Math.cos(angle + Math.PI / 6) * arrowLength;
            const y2 = arrowY - Math.sin(angle + Math.PI / 6) * arrowLength;
            g.append("polygon")
              .attr("class", "arrow-group")
              .attr("points", `${arrowX},${arrowY} ${x1},${y1} ${x2},${y2}`)
              .attr("fill", "#FFFFFF")
              .attr("opacity", isLinkVisible(linkData) ? 1 : 0.12)
              .attr("stroke", "none");
          }
        });

        // Position link labels (directed): center along each edge
        linkLabels
          .attr("x", d => {
            const source = typeof d.source === 'string' ? 
              networkData.nodes.find(n => n.id === d.source) : d.source;
            const target = typeof d.target === 'string' ? 
              networkData.nodes.find(n => n.id === d.target) : d.target;
            if (!source || !target) return 0;
            if (source === target) {
              const nodeRadius = 40;
              const margin = d.isPath ? 15 : 17;
              const adjusted = Math.max(0, margin - 5);
              const adjustedStart = Math.max(0, adjusted - 5);
              const sideOffset = 6;
              const loopHeight = 80;
              // left vertical part midpoint
              return source.x - sideOffset - 6;
            }
            const dx = target.x - source.x;
            const dy = target.y - source.y;
            const len = Math.hypot(dx, dy) || 1;
            const nodeRadius = 40;
            const margin = d.isPath ? 15 : 17;
            const adjusted = Math.max(0, margin - 5);
            const adjustedStart = Math.max(0, adjusted - 5);
            const startX = source.x + (dx / len) * (nodeRadius + adjustedStart);
            const endX = target.x - (dx / len) * (nodeRadius + adjusted);
            // midpoint between start and end of the visible segment
            return (startX + endX) / 2;
          })
          .attr("y", d => {
            const source = typeof d.source === 'string' ? 
              networkData.nodes.find(n => n.id === d.source) : d.source;
            const target = typeof d.target === 'string' ? 
              networkData.nodes.find(n => n.id === d.target) : d.target;
            if (!source || !target) return 0;
            if (source === target) {
              const nodeRadius = 40;
              const margin = d.isPath ? 15 : 17;
              const adjusted = Math.max(0, margin - 5);
              const adjustedStart = Math.max(0, adjusted - 5);
              const sideOffset = 6;
              const loopHeight = 80;
              return source.y + (nodeRadius + adjustedStart) + loopHeight / 2;
            }
            const dx = target.x - source.x;
            const dy = target.y - source.y;
            const len = Math.hypot(dx, dy) || 1;
            const nodeRadius = 40;
            const margin = d.isPath ? 15 : 17;
            const adjusted = Math.max(0, margin - 5);
            const adjustedStart = Math.max(0, adjusted - 5);
            const startY = source.y + (dy / len) * (nodeRadius + adjustedStart);
            const endY = target.y - (dy / len) * (nodeRadius + adjusted);
            // midpoint between start and end of the visible segment
            return (startY + endY) / 2;
          })
          // keep label-hit rectangles aligned, and rotate labels parallel to edges
          .attr("text-anchor", _d => 'middle')
          // Rotate labels parallel to the link; keep upright and adjust anchor directionality
          .attr("transform", function(d) {
            const source = typeof d.source === 'string' ? 
              networkData.nodes.find(n => n.id === d.source) : d.source;
            const target = typeof d.target === 'string' ? 
              networkData.nodes.find(n => n.id === d.target) : d.target;
            if (!source || !target) return '';
            if (source === target) {
              const attrX = this && this.getAttribute ? parseFloat(this.getAttribute('x')) : NaN;
              const attrY = this && this.getAttribute ? parseFloat(this.getAttribute('y')) : NaN;
              const x = Number.isFinite(attrX) ? attrX : (source.x - 16);
              const y = Number.isFinite(attrY) ? attrY : (source.y + 50);
              // Align vertically along the left leg
              return `rotate(-90, ${x}, ${y})`;
            }
            const dx = target.x - source.x;
            const dy = target.y - source.y;
            const angle = Math.atan2(dy, dx) * 180 / Math.PI;
            // Use the current midpoint attrs as the rotation center
            const attrX = this && this.getAttribute ? parseFloat(this.getAttribute('x')) : NaN;
            const attrY = this && this.getAttribute ? parseFloat(this.getAttribute('y')) : NaN;
            const x = Number.isFinite(attrX) ? attrX : (source.x + target.x) / 2;
            const y = Number.isFinite(attrY) ? attrY : (source.y + target.y) / 2;
            const adjustedAngle = Math.abs(angle) > 90 ? angle + 180 : angle;
            return `rotate(${adjustedAngle}, ${x}, ${y})`;
          })
          .attr("opacity", d => isLinkVisible(d) ? 1 : 0.2) // Apply filter-based opacity
          .attr("fill", _d => "#FFFFFF");

        // Position and size label hit rects (centered on the edge)
        linkLabelHits
          .attr("x", function(d) {
            const source = typeof d.source === 'string' ? networkData.nodes.find(n => n.id === d.source) : d.source;
            const target = typeof d.target === 'string' ? networkData.nodes.find(n => n.id === d.target) : d.target;
            if (!source || !target) return -9999;
            const dx = target.x - source.x;
            const dy = target.y - source.y;
            const len = Math.hypot(dx, dy) || 1;
            const nodeRadius = 40;
            const margin = d.isPath ? 15 : 17;
            const adjusted = Math.max(0, margin - 5);
            const adjustedStart = Math.max(0, adjusted - 5);
            const startX = source.x + (dx / len) * (nodeRadius + adjustedStart);
            const endX = target.x - (dx / len) * (nodeRadius + adjusted);
            const x = (startX + endX) / 2;
            const w = approximateTextWidth(d.label);
            return x - w / 2 - 4;
          })
          .attr("y", function(d) {
            const source = typeof d.source === 'string' ? networkData.nodes.find(n => n.id === d.source) : d.source;
            const target = typeof d.target === 'string' ? networkData.nodes.find(n => n.id === d.target) : d.target;
            if (!source || !target) return -9999;
            const dx = target.x - source.x;
            const dy = target.y - source.y;
            const len = Math.hypot(dx, dy) || 1;
            const nodeRadius = 40;
            const margin = d.isPath ? 15 : 17;
            const adjusted = Math.max(0, margin - 5);
            const adjustedStart = Math.max(0, adjusted - 5);
            const startY = source.y + (dy / len) * (nodeRadius + adjustedStart);
            const endY = target.y - (dy / len) * (nodeRadius + adjusted);
            const y = (startY + endY) / 2;
            return y - 10; // padding
          })
          .attr("width", d => approximateTextWidth(d.label) + 8)
          .attr("height", 20);
        // Position nodes
        node
          .attr("cx", d => d.x)
          .attr("cy", d => d.y)
          .attr("stroke", d => getNodeStyle(d, selectedNode).stroke)
          .attr("stroke-width", d => getNodeStyle(d, selectedNode).strokeWidth)
          .attr("opacity", d => isNodeVisible(d) ? 1 : 0.2) // Apply filter-based opacity
          .style("cursor", "pointer");

        // Position node labels
        nodeLabels
          .attr("transform", d => `translate(${d.x}, ${d.y})`)
          .attr("opacity", d => isNodeVisible(d) ? 1 : 0.2); // Apply filter-based opacity to labels too

        applyLongPressHandlers();
      };
      // Create simulation for initial positioning only - controlled by shouldRunSimulation flag
      if (shouldRunSimulation) {
        // Apply anti-overlap positioning if nodes don't have valid positions
        const needsPositioning = networkData.nodes.some(node => 
          !node.x || !node.y || node.x < 50 || node.x > width - 50 || node.y < 50 || node.y > height - 50
        );
        
        if (needsPositioning) {
          // Reset positions for anti-overlap system
          networkData.nodes.forEach(node => {
            node.x = 0;
            node.y = 0;
          });
          positionNodesWithoutOverlap(networkData.nodes, width, height);
        }
        
        // Reset simulation properties
        networkData.nodes.forEach(node => {
          node.fx = null;
          node.fy = null;
          node.vx = 0; // Reset velocity
          node.vy = 0;
        });

        try {
          const hasPinnedNodes = networkData.nodes.some(n => n && n.userPlaced);
          // Analyze network structure to adjust force parameters
          const nodeTypes = networkData.nodes.reduce((acc, node) => {
            acc[node.type] = (acc[node.type] || 0) + 1;
            return acc;
          }, {});
          
          const hasOperas = nodeTypes.opera > 0;
          const hasBooks = nodeTypes.book > 0;
          const nodeCount = networkData.nodes.length;
          
          // Adjust forces based on network composition
          let linkDistance = 140;
          let linkStrength = 0.3;
          let chargeStrength = -1000;
          baseChargeStrengthRef.current = chargeStrength;
          let collisionRadius = 60;
          
          if (hasOperas || hasBooks) {
            // Opera/book networks tend to be star-shaped (one center, many connections)
            linkDistance = 180; // Longer links for better spread
            linkStrength = 0.4; // Stronger links to maintain structure
            chargeStrength = -1200; // Stronger repulsion to prevent overlap
            collisionRadius = 65; // Larger collision radius
          }
          
          if (nodeCount > 10) {
            // Larger networks need different parameters
            chargeStrength = Math.max(-1500, chargeStrength - (nodeCount * 30));
            linkDistance = Math.max(120, linkDistance - (nodeCount * 3));
          }
          
          // Additional spacing for better visualization
          if (nodeCount > 5) {
            linkDistance += 20;
            collisionRadius += 10;
          }
          


          // Use different parameters for expansion vs initial simulations
          const isExpansion = isExpansionSimulation;
          const simulationAlphaDecay = isExpansion ? 0.015 : 0.035; // Even slower decay to allow more settling
          const simulationAlphaMin = isExpansion ? 0.003 : 0.008;   // Lower minimum for more iterations
          const simulationVelocityDecay = isExpansion ? 0.35 : 0.55; // Lower velocity decay for smoother settle

          const simulation = d3.forceSimulation(networkData.nodes)
            .force("link", d3.forceLink(networkData.links)
              .id(d => d.id)
              .distance(l => {
                const sPlaced = !!(l.source && l.source.userPlaced);
                const tPlaced = !!(l.target && l.target.userPlaced);
                if (sPlaced || tPlaced) {
                  const stretched = linkDistance * 1.35;
                  return Math.min(stretched, linkDistance + 140);
                }
                return linkDistance;
              })
              .strength(l => {
                const sPlaced = !!(l.source && l.source.userPlaced);
                const tPlaced = !!(l.target && l.target.userPlaced);
                const base = linkStrength;
                if (sPlaced || tPlaced) {
                  return Math.max(0.02, base * 0.35);
                }
                return base;
              }))
            .force("charge", d3.forceManyBody().strength(n => (n && n.userPlaced ? 0 : chargeStrength)))
            .force("center", hasPinnedNodes ? null : d3.forceCenter(width / 2, height / 2))
            .force("collision", d3.forceCollide().radius(collisionRadius))
            .force("x", d3.forceX(width / 2).strength(n => (n && n.userPlaced) ? 0 : 0.1))
            .force("y", d3.forceY(height / 2).strength(n => (n && n.userPlaced) ? 0 : 0.1))
            .alpha(1)
            .alphaDecay(simulationAlphaDecay)
            .alphaMin(simulationAlphaMin)
            .velocityDecay(simulationVelocityDecay);

          simulationRef.current = simulation;
          isSimulationActiveRef.current = true;

          // Let simulation run for optimal balance of speed and quality
          const simulationDuration = isExpansion ? 5500 : 2200; // Longer durations for clearer settling
          const simulationTimeout = setTimeout(() => {
            if (simulation) {
              simulation.stop();
              isSimulationActiveRef.current = false;
              setShouldRunSimulation(false);
              if (isExpansion) {
                setIsExpansionSimulation(false); // Reset expansion flag
              }
              // Reassert current zoom to prevent any implicit resets after layout settles
              try { applyZoomTransformSilently(zoomTransformRef.current || d3.zoomIdentity); } catch (_) {}
              setTimeout(() => {
                try { applyZoomTransformSilently(zoomTransformRef.current || d3.zoomIdentity); } catch (_) {}
              }, 0);
              try {
                networkData.nodes.forEach(node => {
                  if (!node) return;
                  if (!node.userPlaced) {
                    node.fx = node.x;
                    node.fy = node.y;
                  }
                  if (!Number.isFinite(node.homeX)) node.homeX = node.x;
                  if (!Number.isFinite(node.homeY)) node.homeY = node.y;
                });
              } catch (_) {}
            }
          }, simulationDuration);

          // Set up event handlers
          simulation.on("tick", () => {
            renderNetwork();
          });
          
          simulation.on("end", () => {
            clearTimeout(simulationTimeout);
            isSimulationActiveRef.current = false;
            setShouldRunSimulation(false); // Clear flag when simulation ends
            if (isExpansion) {
              setIsExpansionSimulation(false); // Reset expansion flag
            }
            // Ensure whatever zoom user had is preserved post-simulation
            try { applyZoomTransformSilently(zoomTransformRef.current || d3.zoomIdentity); } catch (_) {}
            setTimeout(() => {
              try { applyZoomTransformSilently(zoomTransformRef.current || d3.zoomIdentity); } catch (_) {}
            }, 0);
            try {
              networkData.nodes.forEach(node => {
                if (!node) return;
                if (!node.userPlaced) {
                  node.fx = node.x;
                  node.fy = node.y;
                }
                if (!Number.isFinite(node.homeX)) node.homeX = node.x;
                if (!Number.isFinite(node.homeY)) node.homeY = node.y;
              });
            } catch (_) {}
          });
          
          simulation.restart();
          
        } catch (error) {
          console.error("❌ Error creating simulation:", error);
          isSimulationActiveRef.current = false;
          setShouldRunSimulation(false);
        }
      } else {
        // If we are here, the outer decision chose not to run the full sim (positions exist)
        // Build a dormant simulation so user interactions can reheat it while preserving layout
        try {
          const defaultLinkDistance = 160;
          const defaultLinkStrength = 0.35;
          const defaultChargeStrength = -1100;
          baseChargeStrengthRef.current = defaultChargeStrength;
          const defaultCollisionRadius = 60;
          const hasPinnedNodes = networkData.nodes.some(n => n && n.userPlaced);
          const simulation = d3.forceSimulation(networkData.nodes)
            .force("link", d3.forceLink(networkData.links)
              .id(d => d.id)
              .distance(l => {
                const sPlaced = !!(l.source && l.source.userPlaced);
                const tPlaced = !!(l.target && l.target.userPlaced);
                if (sPlaced || tPlaced) {
                  const stretched = defaultLinkDistance * 1.35;
                  return Math.min(stretched, defaultLinkDistance + 140);
                }
                return defaultLinkDistance;
              })
              .strength(l => {
                const sPlaced = !!(l.source && l.source.userPlaced);
                const tPlaced = !!(l.target && l.target.userPlaced);
                const base = defaultLinkStrength;
                if (sPlaced || tPlaced) {
                  return Math.max(0.02, base * 0.35);
                }
                return base;
              }))
            .force("charge", d3.forceManyBody().strength(n => (n && n.userPlaced ? 0 : Math.round(defaultChargeStrength))))
            .force("center", hasPinnedNodes ? null : d3.forceCenter(width / 2, height / 2))
            .force("collision", d3.forceCollide().radius(defaultCollisionRadius))
            .force("x", d3.forceX(width / 2).strength(n => (n && n.userPlaced) ? 0 : 0.1))
            .force("y", d3.forceY(height / 2).strength(n => (n && n.userPlaced) ? 0 : 0.1))
            .alpha(0)
            .alphaDecay(0.035)
            .alphaMin(0.001)
            .velocityDecay(0.55);
          simulationRef.current = simulation;
          simulation.on("tick", () => { renderNetwork(); });
          simulation.stop();
          try {
            networkData.nodes.forEach(node => {
              if (!node) return;
              if (!node.userPlaced) {
                node.fx = node.x;
                node.fy = node.y;
              }
              if (!Number.isFinite(node.homeX)) node.homeX = node.x;
              if (!Number.isFinite(node.homeY)) node.homeY = node.y;
            });
          } catch (_) {}
        } catch (_) {}
        isSimulationActiveRef.current = false;
        renderNetwork();
      }

      

      const startDragForNode = (dragEvent, node) => {
        if (!node) return;
        dragActiveRef.current = true;
        try { clearLongPress(); } catch (_) {}

        if (simulationRef.current) {
          try {
            const sim = simulationRef.current;
            const base = baseChargeStrengthRef.current || -1000;
            const reduced = Math.round(base * 0.45);
            const neighborIds = new Set();
            (networkData.links || []).forEach(l => {
              const s = typeof l.source === 'string' ? l.source : l.source?.id;
              const t = typeof l.target === 'string' ? l.target : l.target?.id;
              if (s === node.id && t) neighborIds.add(t);
              if (t === node.id && s) neighborIds.add(s);
            });
            sim.force("charge", d3.forceManyBody().strength(n => {
              if (!n) return base;
              if (n.userPlaced) return 0;
              return neighborIds.has(n.id) ? Math.round(base * 0.25) : reduced;
            }));
            sim.alphaTarget(0.35).restart();
          } catch (_) {}
        }

        node.fx = node.x;
        node.fy = node.y;
        node.userPlaced = true;
        node.homeX = node.x;
        node.homeY = node.y;
        const stateNode = networkData.nodes.find(n => n.id === node.id);
        if (stateNode && stateNode !== node) {
          stateNode.fx = node.fx;
          stateNode.fy = node.fy;
          stateNode.x = node.x;
          stateNode.y = node.y;
        }
        if (stateNode) {
          stateNode.homeX = stateNode.x;
          stateNode.homeY = stateNode.y;
        }

        try {
          const neighborIds = new Set();
          (networkData.links || []).forEach(l => {
            const s = typeof l.source === 'string' ? l.source : l.source?.id;
            const t = typeof l.target === 'string' ? l.target : l.target?.id;
            if (s === node.id && t) neighborIds.add(t);
            if (t === node.id && s) neighborIds.add(s);
          });
          networkData.nodes.forEach(n => {
            if (!n || n.id === node.id) return;
            if (!n.userPlaced && !neighborIds.has(n.id)) {
              n.fx = n.x;
              n.fy = n.y;
              n.vx = 0; n.vy = 0;
              n._frozenDuringDrag = true;
              if (!Number.isFinite(n.homeX)) n.homeX = n.x;
              if (!Number.isFinite(n.homeY)) n.homeY = n.y;
            }
          });
        } catch (_) {}

        const origEvt = dragEvent && dragEvent.sourceEvent ? dragEvent.sourceEvent : dragEvent;
        const modifierHeld = !!(origEvt && (origEvt.shiftKey || origEvt.altKey));
        const isPathGroup = !!(node.wasAddedByPath || node.isPath) && modifierHeld;
        dragGroupIdsRef.current = new Set();
        dragGroupInitialPosRef.current = new Map();
        dragLeaderInitialPosRef.current = { x: node.x, y: node.y };
        if (isPathGroup) {
          const pathIds = new Set(
            networkData.nodes.filter(n => n.wasAddedByPath || n.isPath).map(n => n.id)
          );
          const neighbors = new Map();
          networkData.links.forEach(l => {
            const s = typeof l.source === 'string' ? l.source : l.source?.id;
            const t = typeof l.target === 'string' ? l.target : l.target?.id;
            if (!s || !t) return;
            if (!pathIds.has(s) || !pathIds.has(t)) return;
            if (!neighbors.has(s)) neighbors.set(s, new Set());
            if (!neighbors.has(t)) neighbors.set(t, new Set());
            neighbors.get(s).add(t);
            neighbors.get(t).add(s);
          });
          const stack = [node.id];
          const visited = new Set();
          while (stack.length) {
            const id = stack.pop();
            if (visited.has(id)) continue;
            visited.add(id);
            dragGroupIdsRef.current.add(id);
            const nodeObj = networkData.nodes.find(n => n.id === id);
            if (nodeObj) {
              dragGroupInitialPosRef.current.set(id, { x: nodeObj.x, y: nodeObj.y });
            }
            const nbrs = neighbors.get(id);
            if (nbrs) nbrs.forEach(nid => { if (!visited.has(nid)) stack.push(nid); });
          }
        } else {
          dragGroupIdsRef.current.add(node.id);
          dragGroupInitialPosRef.current.set(node.id, { x: node.x, y: node.y });
        }
      };

      function dragstarted(event, d) {
        const srcEvt = event?.sourceEvent;
        const pointerType = srcEvt && (srcEvt.pointerType || (srcEvt.touches ? 'touch' : undefined));
        dragActiveRef.current = false;
        if (pointerType === 'touch') {
          const state = touchDragStateRef.current;
          if (state) {
            state.pendingDragNode = d;
            state.pendingDragEvent = event;
            state.dragInitialized = false;
          }
          return;
        }
        startDragForNode(event, d);
      }

      function dragged(event, d) {
        const srcEvt = event?.sourceEvent;
        const pointerType = srcEvt && (srcEvt.pointerType || (srcEvt.touches ? 'touch' : undefined));
        if (pointerType === 'touch') {
          const state = touchDragStateRef.current;
          if (state) {
            state.hasMoved = true;
            clearLongPress();
            if (state.longPressFired) {
              return;
            }
            if (!state.dragInitialized) {
              startDragForNode(state.pendingDragEvent || event, state.pendingDragNode || d);
              state.dragInitialized = true;
            }
          }
        } else if (!dragActiveRef.current) {
          startDragForNode(event, d);
        }

        if (!dragActiveRef.current) {
          return;
        }

        const dx = event.x - dragLeaderInitialPosRef.current.x;
        const dy = event.y - dragLeaderInitialPosRef.current.y;

        dragGroupIdsRef.current.forEach(id => {
          const initial = dragGroupInitialPosRef.current.get(id);
          if (!initial) return;
          const nodeObj = networkData.nodes.find(n => n.id === id);
          if (!nodeObj) return;
          const nx = initial.x + dx;
          const ny = initial.y + dy;
          nodeObj.fx = nx; nodeObj.fy = ny; nodeObj.x = nx; nodeObj.y = ny;
        });

        if (simulationRef.current) {
          try { simulationRef.current.alpha(0.18); } catch (_) {}
        }
        renderNetwork();
      }

      function dragended(event, d) {
        const srcEvt = event?.sourceEvent;
        const pointerType = srcEvt && (srcEvt.pointerType || (srcEvt.touches ? 'touch' : undefined));
        if (pointerType === 'touch') {
          const state = touchDragStateRef.current;
          resetTouchTracking(srcEvt?.pointerId ?? null);
          if (state && !state.dragInitialized) {
            return;
          }
        }
        if (!dragActiveRef.current) {
          return;
        }
        dragActiveRef.current = false;
        dragGroupIdsRef.current.forEach(id => {
          const nodeObj = networkData.nodes.find(n => n.id === id);
          if (!nodeObj) return;
          nodeObj.fx = nodeObj.x;
          nodeObj.fy = nodeObj.y;
        });
        try {
          networkData.nodes.forEach(n => {
            if (!n || !n._frozenDuringDrag) return;
            if (!n.userPlaced) { n.fx = n.x; n.fy = n.y; }
            n._frozenDuringDrag = false;
            n.vx = 0; n.vy = 0;
            if (!Number.isFinite(n.homeX)) n.homeX = n.x;
            if (!Number.isFinite(n.homeY)) n.homeY = n.y;
          });
        } catch (_) {}
        if (simulationRef.current && !event.active) {
          try {
            const sim = simulationRef.current;
            const base = baseChargeStrengthRef.current || -1000;
            sim.force("charge", d3.forceManyBody().strength(n => (
              n && n.userPlaced ? 0 : Math.round(base)
            )));
            sim.alphaTarget(0);
          } catch (_) {}
        }
      }

      // Initial render (simulation will take over immediately)
      renderNetwork();
      applyLongPressHandlers();

      // Cleanup
      return () => {
        clearLongPress();
        resetTouchTracking();
        if (simulationRef.current) {
          simulationRef.current.stop();
        }
      };
    }, [
      networkData.nodes.length,
      networkData.links.length,
      visualizationHeight,
      birthYearRange[0],
      birthYearRange[1],
      deathYearRange[0],
      deathYearRange[1],
      selectedVoiceTypes,
      filtersVersion,
      currentCenterNode,
      viewportIsPhone
    ]); // Re-run on data, height, or filter changes
    // Guard against outside clicks forcing any transform reset by reapplying zoom
    useEffect(() => {
      const onDocClickCapture = (e) => {
        const container = containerRef.current;
        if (!container) return;
        if (!container.contains(e.target)) {
          // Temporarily lock zoom updates and reapply current transform after handlers run
          try { zoomLockedRef.current = true; } catch (_) {}
          setTimeout(() => {
            try {
              const t = zoomTransformRef.current || d3.zoomIdentity;
              const svgSel = d3.select(svgRef.current);
              svgSel.property('__zoom', t);
              const g = svgSel.select('g');
              g.attr('transform', t);
              // Unlock shortly after to allow normal zooming again
              setTimeout(() => { try { zoomLockedRef.current = false; } catch (_) {} }, 0);
            } catch (_) {}
          }, 0);
        }
      };
      document.addEventListener('mousedown', onDocClickCapture, true);
      // Also handle shortly after in case other handlers mutate transform again
      const onDocClickBubble = (e) => {
        const container = containerRef.current;
        if (!container) return;
        if (!container.contains(e.target)) {
          try { zoomLockedRef.current = true; } catch (_) {}
          setTimeout(() => {
            try {
              const t = zoomTransformRef.current || d3.zoomIdentity;
              const svgSel = d3.select(svgRef.current);
              svgSel.property('__zoom', t);
              const g = svgSel.select('g');
              g.attr('transform', t);
              setTimeout(() => { try { zoomLockedRef.current = false; } catch (_) {} }, 0);
            } catch (_) {}
          }, 0);
        }
      };
      document.addEventListener('mouseup', onDocClickBubble, false);
      return () => {
        document.removeEventListener('mousedown', onDocClickCapture, true);
        document.removeEventListener('mouseup', onDocClickBubble, false);
      };
    }, []);

    return (
      <div
        className={viewportIsPhone ? 'mobile-network-shell' : undefined}
        style={{ position: 'relative' }}
      >
        <div
          ref={containerRef}
          style={{
            position: 'relative',
            width: '100%',
            height: `${visualizationHeight}px`,
            border: viewportIsPhone ? '4px solid #FFFFFF' : '6px solid #FFFFFF',
            borderRadius: '8px',
            backgroundColor: '#3e96e2',
            overflow: 'hidden',
            boxSizing: 'border-box',
            marginBottom: viewportIsPhone ? '24px' : 0
          }}
        >
          <svg ref={svgRef}></svg>

          <ContextMenu />
            {linkContextMenu.show && (
              <div
                className="context-menu"
                style={{
                  position: 'absolute',
                  top: linkContextMenu.y,
                  left: linkContextMenu.x,
                  backgroundColor: 'white',
                  border: '2px solid #3e96e2',
                  borderRadius: '8px',
                  padding: '12px 16px 12px 12px',
                  boxShadow: '0 4px 16px rgba(0,0,0,0.15)',
                  zIndex: 1000,
                  minWidth: '260px',
                  fontFamily: "'Inter', 'Helvetica Neue', Arial, sans-serif",
                  fontSize: '16px'
                }}
                onClick={(e) => e.stopPropagation()}
              >
                <button
                  type="button"
                  onClick={() => setLinkContextMenu({ show: false, x: 0, y: 0, role: '', source: '' })}
                  aria-label="Close relationship menu"
                  style={{
                    position: 'absolute',
                    top: 6,
                    right: 6,
                    border: 'none',
                    background: 'transparent',
                    color: '#6b7280',
                    fontSize: '18px',
                    cursor: 'pointer',
                    padding: 4,
                    lineHeight: 1
                  }}
                >
                  ×
                </button>
                {linkContextMenu.role && (
                  <>
                <div style={{ fontWeight: 600, marginBottom: '6px', color: '#1f2937' }}>
                  Premiered role in
                </div>
                <div style={{ color: '#374151', marginBottom: '4px' }}>
                      <strong>Role:</strong> {linkContextMenu.role}
                </div>
                  </>
                )}
                <div style={{ color: '#374151' }}>
                  <strong>Source:</strong> {linkContextMenu.source || 'Unknown'}
                </div>
              </div>
            )}
            <ProfileCard />
          </div>

          {/** Reusable handler so both button click and Enter key submit run the same code */}
          {(() => {
            // define on every render to capture latest state in closure
            window.__cmg_runFindPath = async () => {
                const from = pathFromValRef.current?.trim();
                const to = pathToValRef.current?.trim();
                if (!from || !to) return;
                try {
                  // Snapshot before mutating the network with path overlay
                  pushHistory('path-find');
                  setLoading(true);
                  // Snapshot current network before overlay so Clear can restore baseline
                  prePathNetworkRef.current = {
                    nodes: networkData.nodes.map(n => ({ ...n })),
                    links: networkData.links.map(l => ({
                      ...l,
                      source: (typeof l.source === 'string' ? l.source : l.source?.id),
                      target: (typeof l.target === 'string' ? l.target : l.target?.id),
                      isPath: false,
                      wasAddedByPath: false
                    }))
                  };
                  const payload = { from, to, maxHops: 8 };
                  const resp = await fetchWithRetry(`${API_BASE}/path/find`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
                    body: JSON.stringify(payload)
                  }, { retries: 2, baseDelay: 600 });
                  const data = await resp.json();
                  if (!resp.ok) throw new Error(data.error || 'Failed');
                  setPathInfo({ nodes: data.nodes, links: data.links, steps: data.steps || [] });
                  // Show the path on the graph by merging nodes/links and highlighting the path
                  const existingNodeMap = new Map();
                  networkData.nodes.forEach(existingNode => {
                    if (existingNode && existingNode.type === 'person') {
                      const normalized = normalizePersonNode(existingNode);
                      if (normalized !== existingNode) {
                        Object.assign(existingNode, normalized);
                      }
                    }
                    if (existingNode?.id) {
                      existingNodeMap.set(existingNode.id, existingNode);
                    }
                  });
                  const existingLinkKeys = new Set(
                    networkData.links.map(l => `${typeof l.source === 'string' ? l.source : l.source?.id}-${typeof l.target === 'string' ? l.target : l.target?.id}-${l.type}`)
                  );
                  const pathNodeIds = new Set(data.nodes.map(n => n.id));

                  // Prepare merged nodes
                  const mergedNodes = [...networkData.nodes];
                  // ensure trackers exist and accumulate across runs
                  if (!pathOverlayRef.current.addedNodeIds) pathOverlayRef.current.addedNodeIds = new Set();
                  if (!pathOverlayRef.current.addedLinkKeys) pathOverlayRef.current.addedLinkKeys = new Set();
                  // Helper: place new node near an existing neighbor if possible
                  const placeNearA = (neighbor) => {
                    const angle = Math.random() * Math.PI * 2;
                    const radius = 90;
                    return { x: (neighbor.x || 300) + Math.cos(angle) * radius, y: (neighbor.y || 300) + Math.sin(angle) * radius };
                  };
                  // Build neighbor map from path links
                  const pathNeighbors = new Map();
                  data.links.forEach(l => {
                    if (!pathNeighbors.has(l.source)) pathNeighbors.set(l.source, new Set());
                    if (!pathNeighbors.has(l.target)) pathNeighbors.set(l.target, new Set());
                    pathNeighbors.get(l.source).add(l.target);
                    pathNeighbors.get(l.target).add(l.source);
                  });

                  // Partition base constellation (non-path) vs path nodes
                  const baseConstellation = mergedNodes.filter(n => !pathNodeIds.has(n.id));
                  const existing = baseConstellation.length ? baseConstellation : mergedNodes;
                  const minX = Math.min(...existing.map(n => n.x || 0));
                  const maxX = Math.max(...existing.map(n => n.x || 0));
                  const minY = Math.min(...existing.map(n => n.y || 0));
                  const maxY = Math.max(...existing.map(n => n.y || 0));
                  const cx = (minX + maxX) / 2;
                  const cy = (minY + maxY) / 2;
                  const bboxMargin = 40;
                  const minDistance = 120;

                  const isInsideBBox = (x, y) => (x > (minX - bboxMargin) && x < (maxX + bboxMargin) && y > (minY - bboxMargin) && y < (maxY + bboxMargin));
                  const collides = (x, y) => existing.some(o => {
                    const ox = o.x || 0, oy = o.y || 0; const dx = x - ox, dy = y - oy; return Math.hypot(dx, dy) < minDistance;
                  });
                  const normalize = (vx, vy) => {
                    const m = Math.hypot(vx, vy) || 1; return { x: vx / m, y: vy / m };
                  };
                  const rotate = (vx, vy, ang) => {
                    const c = Math.cos(ang), s = Math.sin(ang); return { x: vx * c - vy * s, y: vx * s + vy * c };
                  };

                  // Simple merge placement near neighbors (revert behavior)
                  const container = svgRef.current?.parentElement;
                  const width = container ? container.clientWidth : 800;
                  let height = visualizationHeight || 600;
                  const pad = 30;
                  const placeNearB = (neighbor) => {
                    const angle = Math.random() * Math.PI * 2;
                    const radius = 90;
                    return { x: (neighbor.x || 300) + Math.cos(angle) * radius, y: (neighbor.y || 300) + Math.sin(angle) * radius };
                  };
                  data.nodes.forEach(rawNode => {
                    const canonicalNode = normalizePersonNode(rawNode);
                    if (!canonicalNode?.id) return;
                    if (!existingNodeMap.has(canonicalNode.id)) {
                      const neighbors = Array.from(pathNeighbors.get(canonicalNode.id) || []);
                      const existingNeighbor = neighbors.find(id => existingNodeMap.has(id));
                      let x = 300, y = 300;
                      if (existingNeighbor) {
                        const nb = existingNodeMap.get(existingNeighbor);
                        const p = placeNearA(nb);
                        x = p.x; y = p.y;
                      }
                      if (x < pad) x = pad;
                      if (x > width - pad) x = width - pad;
                      if (y < pad) y = pad;
                      if (y > height - pad) height = Math.ceil(y + 60);
                      const newNode = normalizePersonNode({ ...canonicalNode, x, y, isPath: true, wasAddedByPath: true });
                      mergedNodes.push(newNode);
                      existingNodeMap.set(newNode.id, newNode);
                      pathOverlayRef.current.addedNodeIds.add(newNode.id);
                    } else {
                      const ex = existingNodeMap.get(canonicalNode.id);
                      const updated = normalizePersonNode({ ...ex, ...canonicalNode, isPath: true });
                      Object.assign(ex, updated);
                    }
                  });
                  // Fixed height; do not auto-grow the canvas
                  // if (height > (visualizationHeight || 600)) setVisualizationHeight(height);

                  // Anti-overlap nudge for base constellation only (skip path nodes)
                  const containerEl = document.querySelector('div[style*="height:"] > svg')?.parentElement || null;
                  const widthGuess = containerEl ? containerEl.clientWidth : 800;
                  const baseOnly = mergedNodes.filter(n => !n.isPath);
                  positionNodesWithoutOverlap(baseOnly, widthGuess, visualizationHeight || 600);

                  // Prefetch person details for path endpoints so we can attach relationship sources
                  try {
                    const pathPersonNames = Array.from(new Set((data.nodes || [])
                      .filter(n => n && n.type === 'person' && n.id)
                      .map(n => n.id)));
                    await Promise.all(pathPersonNames.map(nm => fetchAndCachePersonDetails(nm)));
                  } catch (_) {}

                  // Helper: resolve relationship source for path links (person→person only)
                  const resolvePathRelSource = (srcName, trgName, relType) => {
                    try {
                      const cache = personCacheRef.current || new Map();
                      const src = cache.get(srcName);
                      const trg = cache.get(trgName);
                      const type = (relType || '').toLowerCase();
                      if (type === 'taught') {
                        const sStudents = (src?.students || []);
                        const tTeachers = (trg?.teachers || []);
                        const a = sStudents.find(x => x?.full_name === trgName);
                        if (a && a.teacher_rel_source) return a.teacher_rel_source;
                        const b = tTeachers.find(x => x?.full_name === srcName);
                        if (b && b.teacher_rel_source) return b.teacher_rel_source;
                      }
                      if (type === 'family' || type === 'parent' || type === 'spouse' || type === 'sibling' || type === 'grandparent') {
                        const sFam = (src?.family || []);
                        const tFam = (trg?.family || []);
                        const a = sFam.find(x => x?.full_name === trgName);
                        if (a && (a.teacher_rel_source || a.source)) return a.teacher_rel_source || a.source;
                        const b = tFam.find(x => x?.full_name === srcName);
                        if (b && (b.teacher_rel_source || b.source)) return b.teacher_rel_source || b.source;
                      }
                    } catch (_) {}
                    return '';
                  };

                  // Prepare merged links with path highlighting
                  const mergedLinks = [...networkData.links];
                  // Build orientation hints from existing graph
                  const teacherPairs = new Set(); // teacher->student
                  const workPairs = new Set(); // person->work
                  const familyPairs = new Set(); // keep as seen
                  mergedLinks.forEach(el => {
                    const s = typeof el.source === 'string' ? el.source : el.source?.id;
                    const t = typeof el.target === 'string' ? el.target : el.target?.id;
                    if (!s || !t) return;
                    if (el.type === 'taught') teacherPairs.add(`${s}->${t}`);
                    if (el.type === 'premiered' || el.type === 'composed' || el.type === 'authored' || el.type === 'edited') workPairs.add(`${s}->${t}`);
                    if (el.type === 'family') familyPairs.add(`${s}->${t}`);
                  });
                  data.links.forEach(l => {
                    let src = l.source;
                    let trg = l.target;
                    const type = l.type;

                    // Enforce canonical orientation per relationship using existing graph as source of truth
                    const sourceNode = existingNodeMap.get(src) || data.nodes.find(n => n.id === src);
                    const targetNode = existingNodeMap.get(trg) || data.nodes.find(n => n.id === trg);
                    const isWorkType = type === 'premiered' || type === 'wrote' || type === 'composed' || type === 'authored' || type === 'edited';
                    if (type === 'taught') {
                      if (teacherPairs.has(`${src}->${trg}`)) {
                        // ok
                      } else if (teacherPairs.has(`${trg}->${src}`)) {
                        const tmp = src; src = trg; trg = tmp;
                      }
                      // else fall back to backend orientation
                    } else if (isWorkType && sourceNode && targetNode) {
                      // Person -> Work (Opera/Book)
                      const sourceIsPerson = sourceNode.type === 'person';
                      const targetIsWork = targetNode.type === 'opera' || targetNode.type === 'book';
                      if (!(sourceIsPerson && targetIsWork)) {
                        const tmp = src; src = trg; trg = tmp;
                      }
                    } else if (type === 'family') {
                      if (familyPairs.has(`${src}->${trg}`)) {
                        // keep
                      } else if (familyPairs.has(`${trg}->${src}`)) {
                        const tmp = src; src = trg; trg = tmp;
                      }
                    }

                    const key = `${src}-${trg}-${type}`;
                    const revKey = `${trg}-${src}-${type}`;
                    const existingIdx = mergedLinks.findIndex(ml => {
                      const s = typeof ml.source === 'string' ? ml.source : ml.source?.id;
                      const t = typeof ml.target === 'string' ? ml.target : ml.target?.id;
                      return `${s}-${t}-${ml.type}` === key || `${s}-${t}-${ml.type}` === revKey;
                    });

                    const srcName = src;
                    const trgName = trg;
                    const computedSourceInfo = resolvePathRelSource(srcName, trgName, type);

                    if (existingIdx >= 0) {
                      // Do NOT change existing orientation; just mark as path to match base graph
                      const cur = mergedLinks[existingIdx];
                      mergedLinks[existingIdx] = {
                        ...cur,
                        isPath: true,
                        sourceInfo: cur.sourceInfo || computedSourceInfo || cur.sourceInfo || ''
                      };
                    } else {
                      mergedLinks.push({ ...l, source: src, target: trg, isPath: true, wasAddedByPath: true, sourceInfo: computedSourceInfo });
                      pathOverlayRef.current.addedLinkKeys.add(key);
                    }
                  });

                  // Mark nodes in path
                  mergedNodes.forEach(n => { if (pathNodeIds.has(n.id)) n.isPath = true; });
                  setNetworkData({ nodes: mergedNodes, links: mergedLinks });
                  const pathPersons = mergedNodes.filter(n => n && n.type === 'person' && pathNodeIds.has(n.id));
                  extendDateRangesForNodes(pathPersons, { resetUserRangeFlags: true });
                  // Enrich newly added path person nodes so CSV has full details
                  const newPersonNames = (data.nodes || [])
                    .filter(n => n && n.type === 'person')
                    .map(n => n.id)
                    .filter(Boolean);
                  enrichPersonNodes(newPersonNames);
                  // After path overlay, briefly run an expansion-style simulation so nodes settle
                  setTimeout(() => {
                    setIsExpansionSimulation(true);
                    setShouldRunSimulation(true);
                  }, 120);
                } catch (e) {
                  setError(e.message || 'Path find failed');
                } finally {
                  setLoading(false);
                  // After request, re-focus last edited field to avoid losing focus
                  if (document.activeElement === document.body) {
                    if (pathToRef.current) pathToRef.current.focus();
                  }
                }
              };
            // Provide a small helper to disable zoom if needed
            window.__cmg_disableZoomWhileScrolling = (disabled) => {
              // We rely on event.stopPropagation on the list, but keep this for future
              // hooks if we want to temporarily unbind zoom.
              // No-op currently.
              return !!disabled;
            };
            return null;
          })()}
          {showPathPanel && (
            <div
              ref={pathPanelRef}
              style={viewportIsPhone ? {
                position: 'fixed',
                left: '16px',
                right: '16px',
                bottom: '112px',
                backgroundColor: 'white',
                border: '2px solid #3e96e2',
                borderRadius: '16px',
                boxShadow: '0 16px 40px rgba(37, 99, 235, 0.25)',
                padding: '16px',
                maxHeight: '60vh',
                overflowY: 'auto',
                zIndex: 1401,
                overscrollBehavior: 'contain'
              } : {
                position: 'absolute',
                top: 12,
                right: 12,
                backgroundColor: 'white',
                border: '2px solid #3e96e2',
                borderRadius: '8px',
                boxShadow: '0 4px 16px rgba(0,0,0,0.1)',
                padding: '12px',
                width: '340px',
                zIndex: 1000,
                overscrollBehavior: 'contain'
              }}
              onWheel={(e) => { e.stopPropagation(); }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                <div style={{ fontWeight: 600, color: '#1f2937' }}>Find path</div>
                <button
                  onClick={() => setShowPathPanel(false)}
                  style={{
                    background: 'none',
                    border: 'none',
                    fontSize: 18,
                    cursor: 'pointer',
                    color: '#666',
                    padding: viewportIsPhone ? '6px' : 0
                  }}
                >
                  ×
                </button>
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: 8 }}>
                <div>
                  <label style={{ display: 'block', fontSize: 12, color: '#6b7280', marginBottom: 4 }}>From (Person full name)</label>
                  <input
                    ref={pathFromRef}
                    defaultValue=""
                    onInput={e => { pathFromValRef.current = e.target.value; }}
                    onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); e.stopPropagation(); window.__cmg_runFindPath && window.__cmg_runFindPath(); } }}
                    autoComplete="off"
                    autoCorrect="off"
                    autoCapitalize="off"
                    spellCheck={false}
                    inputMode="text"
                    name="cmg-path-from"
                    data-lpignore="true"
                    data-1p-ignore
                    style={{
                      width: '100%',
                      padding: viewportIsPhone ? '12px 14px' : '6px 8px',
                      border: '2px solid #3e96e2',
                      borderRadius: viewportIsPhone ? 12 : 4,
                      fontSize: viewportIsPhone ? '16px' : '14px'
                    }}
                  />
                </div>
                <div>
                  <label style={{ display: 'block', fontSize: 12, color: '#6b7280', marginBottom: 4 }}>To (Person full name)</label>
                  <input
                    ref={pathToRef}
                    defaultValue=""
                    onInput={e => { pathToValRef.current = e.target.value; }}
                    onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); e.stopPropagation(); window.__cmg_runFindPath && window.__cmg_runFindPath(); } }}
                    autoComplete="off"
                    autoCorrect="off"
                    autoCapitalize="off"
                    spellCheck={false}
                    inputMode="text"
                    name="cmg-path-to"
                    data-lpignore="true"
                    data-1p-ignore
                    style={{
                      width: '100%',
                      padding: viewportIsPhone ? '12px 14px' : '6px 8px',
                      border: '2px solid #3e96e2',
                      borderRadius: viewportIsPhone ? 12 : 4,
                      fontSize: viewportIsPhone ? '16px' : '14px'
                    }}
                  />
                </div>
                <div style={{ display: 'flex', gap: 8, flexDirection: viewportIsPhone ? 'column' : 'row' }}>
                  <button
                    onClick={() => { window.__cmg_runFindPath && window.__cmg_runFindPath(); }}
                    style={{
                      backgroundColor: '#2563eb',
                      color: 'white',
                      border: '2px solid #3e96e2',
                      padding: viewportIsPhone ? '12px 16px' : '6px 10px',
                      borderRadius: viewportIsPhone ? 12 : 4,
                      cursor: 'pointer',
                      flex: viewportIsPhone ? 1 : 'initial',
                      fontSize: viewportIsPhone ? '16px' : '14px'
                    }}
                  >
                    Find path
                  </button>
                  <button
                    aria-label="Clear path"
                    title="Clear path"
                    onClick={() => {
                      // Clear the panel info
                      setPathInfo(null);
                      // Clear input fields and refs
                      if (pathFromRef.current) pathFromRef.current.value='';
                      if (pathToRef.current) pathToRef.current.value='';
                      pathFromValRef.current='';
                      pathToValRef.current='';
                      // If we have a snapshot from before the path overlay, restore it completely
                      if (prePathNetworkRef.current) {
                        const snapshot = prePathNetworkRef.current;
                        // Reset trackers
                        pathOverlayRef.current.addedNodeIds = new Set();
                        pathOverlayRef.current.addedLinkKeys = new Set();
                        // Restore network as it was before path overlay (and any expansions after)
                        setNetworkData({
                          nodes: snapshot.nodes.map(n => ({ ...n, isPath: false, wasAddedByPath: false })),
                          links: snapshot.links.map(l => ({ ...l, isPath: false, wasAddedByPath: false }))
                        });
                        // Clear snapshot so next path overlay will re-snapshot
                        prePathNetworkRef.current = null;
                        setShouldRunSimulation(false);
                      } else {
                        // Fallback: Remove path overlay from graph based on trackers
                        setNetworkData(prev => {
                          const addedNodeIds = new Set(pathOverlayRef.current.addedNodeIds || []);
                          const addedLinkKeys = new Set(pathOverlayRef.current.addedLinkKeys || []);
                          const remainingNodes = prev.nodes
                            .filter(n => !addedNodeIds.has(n.id) && !n.wasAddedByPath)
                            .map(n => ({ ...n, isPath: false, wasAddedByPath: false }));
                          const remainingLinks = prev.links
                            .filter(l => {
                              const key = `${typeof l.source === 'string' ? l.source : l.source?.id}-${typeof l.target === 'string' ? l.target : l.target?.id}-${l.type}`;
                              return !addedLinkKeys.has(key) && !l.wasAddedByPath;
                            })
                            .map(l => ({ ...l, isPath: false, wasAddedByPath: false }));
                          pathOverlayRef.current.addedNodeIds = new Set();
                          pathOverlayRef.current.addedLinkKeys = new Set();
                          return { nodes: remainingNodes, links: remainingLinks };
                        });
                      }
                    }}
                    style={{
                      backgroundColor: '#f9fafb',
                      color: '#111827',
                      border: '2px solid #3e96e2',
                      padding: viewportIsPhone ? '12px 16px' : '6px 10px',
                      borderRadius: viewportIsPhone ? 12 : 4,
                      cursor: 'pointer',
                      flex: viewportIsPhone ? 1 : 'initial',
                      fontSize: viewportIsPhone ? '16px' : '14px'
                    }}
                  >
                    Clear path
                  </button>
                </div>
              </div>
              {pathInfo && (
                <div
                  ref={pathListRef}
                  style={{ marginTop: 10, maxHeight: 200, overflowY: 'auto', fontSize: 12, color: '#374151', borderTop: '1px solid #eee', paddingTop: 8, overscrollBehavior: 'contain', WebkitOverflowScrolling: 'touch' }}
                  onWheelCapture={(e) => {
                    // Let the browser perform smooth scrolling; just prevent D3 zoom from receiving the wheel
                    e.stopPropagation();
                  }}
                  onMouseEnter={() => {
                    // disable svg zoom while hovering the list
                    try { window.__cmg_disableZoomWhileScrolling && window.__cmg_disableZoomWhileScrolling(true); } catch (_) {}
                  }}
                  onMouseLeave={() => {
                    try { window.__cmg_disableZoomWhileScrolling && window.__cmg_disableZoomWhileScrolling(false); } catch (_) {}
                  }}
                >
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>Path summary</div>
                  <div>Nodes: {pathInfo.nodes.length}, Links: {pathInfo.links.length}</div>
                  {Array.isArray(pathInfo.steps) && pathInfo.steps.length > 0 && (
                    <div style={{ marginTop: 8 }}>
                      {pathInfo.steps.map((step, idx) => (
                        <div
                          key={idx}
                          style={{
                            padding: '6px 8px',
                            borderRadius: 4,
                            marginBottom: 4,
                            border: '2px solid #3e96e2',
                            background: '#fff',
                            cursor: 'default'
                          }}
                          // Hover highlight disabled to keep scroll smooth
                        >
                          <div>
                            <strong>{step.source?.name || step.source?.id}</strong> — {step.label}
                            {step.type === 'premiered' && (
                              <>
                                {' '}(
                                Role: {step.role || 'Unknown'}; Source: {step.sourceInfo || 'Unknown'}
                                )
                              </>
                            )}
                            {' '}→ <strong>{step.target?.name || step.target?.id}</strong>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>
          )}
        
        {/* Resizer removed */}
        
        {/* Height indicator removed */}
      </div>
    );
  };
  // Saved View Dialog
  const SavedViewDialog = () => {
    const [copied, setCopied] = useState(false);
    const [copyMessage, setCopyMessage] = useState('');
    if (!showSavedViewDialog) return null;
    const copyToClipboard = async (text, label = 'Copied to clipboard') => {
      try {
        await navigator.clipboard.writeText(text);
        setCopyMessage(label);
        setCopied(true);
        setTimeout(() => setCopied(false), 1800);
      } catch (_) {}
    };
    const combined = savedViewLabel ? `${savedViewToken}\nLabel: ${savedViewLabel}` : savedViewToken;

    const content = (
      <>
        <p style={{ margin: 0, color: '#555', fontSize: isMobileViewport ? '15px' : '14px' }}>
          Copy the string below to load this view later.
        </p>
        <div style={{ minHeight: 22 }}>
          <span style={{
            padding: '4px 10px',
            borderRadius: 9999,
            border: '2px solid #3e96e2',
            background: '#ecfdf5',
            color: '#065f46',
            fontSize: 12,
            opacity: copied ? 1 : 0,
            transition: 'opacity 200ms ease'
          }}>
            {copyMessage}
          </span>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <label style={{ fontSize: 12, color: '#666' }}>Saved view string</label>
          <div style={{ display: 'flex', flexDirection: isMobileViewport ? 'column' : 'row', gap: 8 }}>
            <textarea
              readOnly
              value={savedViewToken}
              style={{
                flex: 1,
                padding: 10,
                border: '2px solid #3e96e2',
                borderRadius: 10,
                fontFamily: 'monospace',
                fontSize: 13,
                resize: 'vertical',
                minHeight: isMobileViewport ? 100 : 60,
                color: '#111'
              }}
            />
            <button
              onClick={() => copyToClipboard(savedViewToken)}
              style={{
                padding: '12px 16px',
                backgroundColor: '#ffffff',
                color: '#374151',
                border: '2px solid #3e96e2',
                borderRadius: 10,
                cursor: 'pointer',
                fontSize: '16px',
                height: '48px'
              }}
            >
              Copy
            </button>
          </div>
          {savedViewLabel && (
            <>
              <label style={{ fontSize: 12, color: '#666' }}>Label</label>
              <div style={{ display: 'flex', flexDirection: isMobileViewport ? 'column' : 'row', gap: 8 }}>
                <input
                  readOnly
                  value={savedViewLabel}
                  style={{ flex: 1, padding: 10, border: '2px solid #3e96e2', borderRadius: 10, fontSize: 13, color: '#111' }}
                />
                <button
                  onClick={() => copyToClipboard(savedViewLabel)}
                  style={{ padding: '12px 16px', backgroundColor: '#ffffff', color: '#374151', border: '2px solid #3e96e2', borderRadius: 10, cursor: 'pointer', fontSize: '16px', height: '48px' }}
                >
                  Copy
                </button>
              </div>
            </>
          )}
        </div>
      </>
    );

    const actionButtons = (
      <>
        <button
          onClick={() => copyToClipboard(combined)}
          style={{ padding: '12px 16px', backgroundColor: '#ffffff', color: '#374151', border: '2px solid #3e96e2', borderRadius: 10, cursor: 'pointer', fontSize: '16px', height: '48px' }}
        >
          Copy All
        </button>
        <button
          onClick={() => setShowSavedViewDialog(false)}
          style={{ padding: '12px 16px', backgroundColor: '#111827', color: '#ffffff', border: '2px solid #111827', borderRadius: 10, cursor: 'pointer', fontSize: '16px', height: '48px' }}
        >
          Close
        </button>
      </>
    );

    if (isMobileViewport) {
      return (
        <>
          <div
            className="mobile-overlay-backdrop is-open"
            style={{ zIndex: 2000 }}
            onClick={() => setShowSavedViewDialog(false)}
          />
          <div
            className="mobile-sheet is-open"
            style={{ zIndex: 2001, paddingBottom: '24px' }}
            onClick={(e) => e.stopPropagation()}
          >
            <div className="mobile-sheet__header" style={{ paddingBottom: 0 }}>
              <div className="mobile-sheet__handle" />
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12 }}>
                <h3 style={{ margin: 0, fontSize: '20px', fontWeight: 600, color: '#0f172a' }}>Saved View</h3>
                <button
                  onClick={() => setShowSavedViewDialog(false)}
                  style={{
                    background: 'none',
                    border: 'none',
                    color: '#1f2937',
                    fontSize: '24px',
                    cursor: 'pointer',
                    fontWeight: 600,
                    padding: 4,
                    width: 40,
                    height: 40,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center'
                  }}
                  aria-label="Close saved view dialog"
                >
                  ×
                </button>
              </div>
            </div>
            <div className="mobile-sheet__content" style={{ paddingTop: 12 }}>
              {content}
            </div>
            <div className="mobile-sheet__footer">
              {actionButtons}
            </div>
          </div>
        </>
      );
    }

    return (
      <div
        style={{
          position: 'fixed',
          inset: 0,
          backgroundColor: 'rgba(15,23,42,0.45)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          zIndex: 2000
        }}
        onClick={() => setShowSavedViewDialog(false)}
      >
        <div
          style={{
            background: 'white',
            borderRadius: 8,
            padding: 20,
            width: 520,
            boxShadow: '0 18px 44px rgba(15,23,42,0.3)',
            display: 'flex',
            flexDirection: 'column',
            gap: 12
          }}
          onClick={(e) => e.stopPropagation()}
        >
          <h3 style={{ margin: 0 }}>Saved View</h3>
          {content}
          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
            {actionButtons}
          </div>
        </div>
      </div>
    );
  };

  // Function to fetch actual relationship counts from database
  const fetchActualCounts = async (node) => {
    if (actualCounts[node.id]) {
      return actualCounts[node.id];
    }

    try {
      let response, data;
      
      if (node.type === 'person') {
        // All people are persons in Neo4j - use the network endpoint for all persons
        response = await fetchWithRetry(`${API_BASE}/singer/network`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ singerName: node.name, depth: 1 })
        }, { retries: 2, baseDelay: 600 });
      } else if (node.type === 'opera') {
        response = await fetchWithRetry(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ operaName: node.name })
        }, { retries: 2, baseDelay: 600 });
      } else if (node.type === 'book') {
        response = await fetchWithRetry(`${API_BASE}/book/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ bookTitle: node.name })
        }, { retries: 2, baseDelay: 600 });
      }

      if (response && response.ok) {
        data = await response.json();
      } else if (response) {
        // Throw error with status for proper handling
        const error = new Error(`HTTP ${response.status}: ${response.statusText}`);
        error.status = response.status;
        throw error;
      }

      // Process the data and count relationships
      const counts = {
        taughtBy: 0,
        taught: 0,
        authored: 0,
        premieredRoleIn: 0,
        wrote: 0,
        parent: 0,
        parentOf: 0,
        spouse: 0,
        grandparent: 0,
        grandparentOf: 0,
        sibling: 0,
        edited: 0,
        editedBy: 0
      };

      if (node.type === 'person') {
        if (data.teachers) counts.taughtBy = data.teachers.length;
        if (data.students) counts.taught = data.students.length;
        if (data.family) {
          data.family.forEach(relative => {
            const relType = relative.relationship_type?.toLowerCase() || '';
            if (relType.includes('parent') && relType.includes('of')) counts.parentOf++;
            else if (relType.includes('parent')) counts.parent++;
            else if (relType.includes('spouse')) counts.spouse++;
            else if (relType.includes('grandparent') && relType.includes('of')) counts.grandparentOf++;
            else if (relType.includes('grandparent')) counts.grandparent++;
            else if (relType.includes('sibling')) counts.sibling++;
          });
        }
        if (data.works) {
          if (data.works.operas) counts.premieredRoleIn = data.works.operas.length;
          if (data.works.books) counts.authored = data.works.books.length;
        }
        
        // Note: All people are persons in Neo4j regardless of activity (singer, composer, etc.)
        // The API should return comprehensive data for all persons through the network endpoint
      } else if (node.type === 'opera') {
        // Operas have people who premiered roles in them (incoming relationships)
        if (data.premieredRoles) counts.premieredRoleIn = data.premieredRoles.length;
        // Most operas have one composer (outgoing relationship)
        if (data.opera && data.opera.composer) counts.wrote = 1;
      } else if (node.type === 'book') {
        if (data.book && data.book.author) counts.authored = 1;
      }

      // Cache the counts
      setActualCounts(prev => ({ ...prev, [node.id]: counts }));
      return counts;
    } catch (err) {
      // Only log unexpected errors, not 404s for non-existent nodes
      if (err.status !== 404) {
        console.error('Failed to fetch actual counts:', err);
      }
      // Re-throw to allow the caller to handle it
      throw err;
    }

    return {
      taughtBy: 0,
      taught: 0,
      authored: 0,
      premieredRoleIn: 0,
      wrote: 0,
      parent: 0,
      parentOf: 0,
      spouse: 0,
      grandparent: 0,
      grandparentOf: 0,
      sibling: 0,
      edited: 0,
      editedBy: 0
    };
  };

  // Helper function to get expandable relationship counts for a node
  const getExpandableRelationshipCounts = (node) => {
    // If we have actual counts from API, use those and subtract already visible relationships
    if (actualCounts[node.id]) {
      const apiCounts = actualCounts[node.id];
      const visibleCounts = getVisibleRelationshipCounts(node);
      
      return {
        taughtBy: Math.max(0, apiCounts.taughtBy - visibleCounts.taughtBy),
        taught: Math.max(0, apiCounts.taught - visibleCounts.taught),
        authored: Math.max(0, apiCounts.authored - visibleCounts.authored),
        premieredRoleIn: Math.max(0, apiCounts.premieredRoleIn - visibleCounts.premieredRoleIn),
        wrote: Math.max(0, apiCounts.wrote - visibleCounts.wrote),
        parent: Math.max(0, apiCounts.parent - visibleCounts.parent),
        parentOf: Math.max(0, apiCounts.parentOf - visibleCounts.parentOf),
        spouse: Math.max(0, apiCounts.spouse - visibleCounts.spouse),
        grandparent: Math.max(0, apiCounts.grandparent - visibleCounts.grandparent),
        grandparentOf: Math.max(0, apiCounts.grandparentOf - visibleCounts.grandparentOf),
        sibling: Math.max(0, apiCounts.sibling - visibleCounts.sibling),
        edited: Math.max(0, apiCounts.edited - visibleCounts.edited),
        editedBy: Math.max(0, apiCounts.editedBy - visibleCounts.editedBy)
      };
    }
    
    // Fallback: if no API counts yet, only show "All" option
    // Don't show misleading specific relationship options without accurate data
    return {
      taughtBy: 0,
      taught: 0,
      authored: 0,
      premieredRoleIn: 0,
      wrote: 0,
      parent: 0,
      parentOf: 0,
      spouse: 0,
      grandparent: 0,
      grandparentOf: 0,
      sibling: 0,
      edited: 0,
      editedBy: 0
    };
  };
  // Helper function to get visible relationship counts for a node (what's already in the network)
  const getVisibleRelationshipCounts = (node) => {
    const counts = {
      taughtBy: 0,
      taught: 0,
      authored: 0,
      premieredRoleIn: 0,
      wrote: 0,
      parent: 0,
      parentOf: 0,
      spouse: 0,
      grandparent: 0,
      grandparentOf: 0,
      sibling: 0,
      edited: 0,
      editedBy: 0
    };

    if (!networkData.links) return counts;

    // Count relationships based on links
    networkData.links.forEach(link => {
      const sourceId = typeof link.source === 'string' ? link.source : link.source?.id;
      const targetId = typeof link.target === 'string' ? link.target : link.target?.id;
      const isSource = sourceId === node.id;
      const isTarget = targetId === node.id;

      if (isSource) {
        switch (link.type) {
          case 'taught':
            counts.taught++;
            break;
          case 'authored':
            counts.authored++;
            break;
          case 'premiered':
            counts.premieredRoleIn++;
            break;
          case 'wrote':
            counts.wrote++;
            break;
          case 'composed':
            counts.wrote++;
            break;
          case 'family':
            // Parse family relationship types
            const label = link.label?.toLowerCase() || '';
            if (label.includes('parent')) counts.parent++;
            else if (label.includes('spouse')) counts.spouse++;
            else if (label.includes('grandparent')) counts.grandparent++;
            else if (label.includes('sibling')) counts.sibling++;
            break;
          case 'edited':
            counts.edited++;
            break;
        }
      }

      if (isTarget) {
        switch (link.type) {
          case 'taught':
            counts.taughtBy++;
            break;
          case 'authored':
            // Count books that were authored by others
            break;
          case 'premiered':
            // Count performers who premiered in this work (opera as target)
            counts.premieredRoleIn++;
            break;
          case 'wrote':
            counts.wrote++;
            break;
          case 'composed':
            counts.wrote++;
            break;
          case 'family':
            // Parse family relationship types for reverse
            const label = link.label?.toLowerCase() || '';
            if (label.includes('parent')) counts.parentOf++;
            else if (label.includes('spouse')) counts.spouse++;
            else if (label.includes('grandparent')) counts.grandparentOf++;
            else if (label.includes('sibling')) counts.sibling++;
            break;
          case 'edited':
            counts.editedBy++;
            break;
        }
      }
    });

    return counts;
  };
    const ContextMenu = React.memo(() => {
    const node = contextMenu.node;
    
    // Extract stable values
    const nodeId = node?.id;
    const nodeActualCount = actualCounts[nodeId];
    const nodesLength = networkData.nodes.length;
    const linksLength = networkData.links.length;
    
    // Check if node is alone (no other nodes or no relationships)
    const isNodeAlone = React.useMemo(() => {
      return nodesLength === 1 || linksLength === 0;
    }, [nodesLength, linksLength]);
    
    // Get expandable relationship counts (what can actually be expanded)
    const counts = React.useMemo(() => {
      if (!contextMenu.show || !node) return {};
      return getExpandableRelationshipCounts(node);
    }, [contextMenu.show, nodeId, nodeActualCount, linksLength, networkData.nodes.length]);
    
    // Calculate total relationships for "All" option
    const totalRelationships = React.useMemo(() => {
      const specificCounts = Object.values(counts).reduce((sum, count) => sum + (typeof count === 'number' ? count : 0), 0);
      
      // Show "All" if there are specific expandable relationships OR if we don't have API data yet
      const hasApiData = nodeActualCount;
      return specificCounts > 0 || !hasApiData ? 1 : 0;
    }, [counts, nodeActualCount]);
    
    const menuItems = React.useMemo(() => {
      if (!contextMenu.show || !node) return [];
      const hasAnyExpandable = Object.values(counts).some(v => (typeof v === 'number' ? v : 0) > 0);
      return [
      {
        label: 'Full information',
        action: () => {
          showFullInformation(node);
          setContextMenu({ show: false, x: 0, y: 0, node: null });
        }
      },
      {
        label: 'Expand',
        disabled: !hasAnyExpandable,
        hasSubmenu: hasAnyExpandable,
        submenu: [
          {
            label: 'All',
            action: () => {
              expandAllRelationships(node);
              setContextMenu({ show: false, x: 0, y: 0, node: null });
            }
          },
          ...(node?.type === 'person' ? [
            ...(counts.taughtBy > 0 ? [{
              label: `<- Taught - (${counts.taughtBy} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'taughtBy');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.authored > 0 ? [{
              label: `- Authored -> (${counts.authored} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'authored');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.premieredRoleIn > 0 ? [{
              label: `- Premiered role in -> (${counts.premieredRoleIn} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'premieredRoleIn');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.taught > 0 ? [{
              label: `- Taught -> (${counts.taught} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'taught');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.parent > 0 ? [{
              label: `- Parent -> (${counts.parent} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'parent');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.parentOf > 0 ? [{
              label: `<- Parent - (${counts.parentOf} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'parentOf');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.spouse > 0 ? [{
              label: `- Spouse -> (${counts.spouse} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'spouse');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.spouse > 0 ? [{
              label: `<- Spouse - (${counts.spouse} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'spouseOf');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.grandparent > 0 ? [{
              label: `- Grandparent -> (${counts.grandparent} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'grandparent');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.grandparentOf > 0 ? [{
              label: `<- Grandparent - (${counts.grandparentOf} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'grandparentOf');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.sibling > 0 ? [{
              label: `- Sibling - (${counts.sibling} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'sibling');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : [])
          ] : []),
          ...(node?.type === 'opera' ? [
            ...(counts.premieredRoleIn > 0 ? [{
              label: `<- Premiered role in - (${counts.premieredRoleIn} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'premieredRoleIn');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.wrote > 0 ? [{
              label: `<- Wrote - (${counts.wrote} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'wrote');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : [])
          ] : []),
          ...(node?.type === 'book' ? [] : [])
        ]
      },
      {
         label: 'Dismiss other nodes',
         action: () => {
           dismissOtherNodes(node);
           setContextMenu({ show: false, x: 0, y: 0, node: null });
         }
       },
       {
         label: 'Dismiss',
         action: () => {
           dismissNode(node);
           setContextMenu({ show: false, x: 0, y: 0, node: null });
         }
       }
     ];
   }, [nodeId, isNodeAlone, counts]);

   if (!contextMenu.show) return null;

   const dismissMenu = () => {
     setContextMenu({ show: false, x: 0, y: 0, node: null });
     setExpandSubmenu(null);
   };

   return (
      <div
        className="context-menu"
        style={{
          position: 'absolute',
          top: contextMenu.y,
          left: contextMenu.x,
          backgroundColor: 'white',
          border: '2px solid #3e96e2',
          borderRadius: '8px',
          padding: '4px 0',
          boxShadow: '0 4px 16px rgba(0,0,0,0.15)',
          zIndex: 1000,
          minWidth: '220px',
          maxWidth: '300px',
          fontFamily: "'Inter', 'Helvetica Neue', Arial, sans-serif",
          fontSize: '16px'
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <button
          type="button"
          onClick={dismissMenu}
          aria-label="Close menu"
          style={{
            position: 'absolute',
            top: 6,
            right: 6,
            border: 'none',
            background: 'transparent',
            color: '#6b7280',
            fontSize: '18px',
            cursor: 'pointer',
            padding: 4,
            lineHeight: 1
          }}
        >
          ×
        </button>
        {/* Header */}
        <div style={{ 
          padding: '8px 12px 8px 12px', 
          fontWeight: '600', 
          borderBottom: '1px solid #e5e7eb',
          color: '#1f2937',
          fontSize: '13px',
          paddingRight: '36px'
        }}>
          {node?.name}
        </div>

        {/* Menu Items */}
        {menuItems.map((item, index) => (
          <div key={index} style={{ position: 'relative' }}>
            <div
              style={{
                padding: '8px 12px',
                cursor: item.disabled ? 'not-allowed' : 'pointer',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                color: item.disabled ? '#9ca3af' : '#374151',
                transition: 'background-color 0.1s'
              }}
                             onMouseEnter={(e) => {
                 e.target.style.backgroundColor = '#f3f4f6';
                 if (item.hasSubmenu) {
                   // Clear any existing timeout
                   if (submenuTimeoutRef.current) {
                     clearTimeout(submenuTimeoutRef.current);
                   }
                   setExpandSubmenu(index);
                 }
               }}
               onMouseLeave={(e) => {
                 e.target.style.backgroundColor = 'transparent';
                 if (item.hasSubmenu) {
                   // Set timeout to close submenu
                   submenuTimeoutRef.current = setTimeout(() => {
                     setExpandSubmenu(null);
                   }, 300);
                 }
               }}
              onClick={() => {
                if (item.disabled) return;
                if (!item.hasSubmenu && typeof item.action === 'function') {
                  item.action();
                }
              }}
            >
              <span>{item.label}</span>
              {item.hasSubmenu && <span style={{ color: '#9ca3af' }}>▶</span>}
            </div>

                        {/* Submenu */}
            {item.hasSubmenu && expandSubmenu === index && (
              <div
                style={{
                  position: 'absolute',
                  left: '100%',
                  top: '0',
                  backgroundColor: 'white',
                  border: '2px solid #3e96e2',
                  borderRadius: '8px',
                  padding: '8px 0',
                  boxShadow: '0 4px 16px rgba(0,0,0,0.15)',
                  minWidth: '280px',
                  maxWidth: '400px',
                  zIndex: 1001,
                  fontSize: '16px'
                }}
                onMouseEnter={() => {
                  // Clear any existing timeout when entering submenu
                  if (submenuTimeoutRef.current) {
                    clearTimeout(submenuTimeoutRef.current);
                  }
                  setExpandSubmenu(index);
                }}
                onMouseLeave={() => {
                  // Set timeout to close submenu when leaving
                  submenuTimeoutRef.current = setTimeout(() => {
                    setExpandSubmenu(null);
                  }, 300);
                }}
                onClick={(e) => e.stopPropagation()}
              >
                {item.submenu.map((subItem, subIndex) => (
                  <div
                    key={subIndex}
                    style={{
                      padding: '8px 12px',
                      cursor: 'pointer',
                      color: '#374151',
                      fontSize: '16px',
                      fontFamily: "'Inter', 'Helvetica Neue', Arial, sans-serif",
                      whiteSpace: 'nowrap',
                      minHeight: '24px',
                      display: 'flex',
                      alignItems: 'center'
                    }}
                    onMouseEnter={(e) => e.target.style.backgroundColor = '#f3f4f6'}
                    onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
                    onClick={() => {
                      if (typeof subItem.action === 'function') subItem.action();
                    }}
                                      >
                      {subItem.label}
                    </div>
                ))}
              </div>
            )}
          </div>
        ))}
      </div>
    );
  });
  // Filter Panel Component
  const FilterPanel = () => {
    const { totalNodes, visibleNodes } = getFilterCounts();
    const { birthRange, deathRange } = getDateRanges();
    const hasVoiceFilters = selectedVoiceTypes.size > 0;
    const hasBirthFilter = birthRangeIsUserSet;
    const hasDeathFilter = deathRangeIsUserSet;
    const hasBirthplaceFilters = selectedBirthplaces.size > 0;
    const hasAnyFilters = hasVoiceFilters || hasBirthFilter || hasDeathFilter || hasBirthplaceFilters;

    // Local input state to prevent re-renders during typing
    const [birthMinInput, setBirthMinInput] = useState(String(birthYearRange[0]));
    const [birthMaxInput, setBirthMaxInput] = useState(String(birthYearRange[1]));
    const [deathMinInput, setDeathMinInput] = useState(String(deathYearRange[0]));
    const [deathMaxInput, setDeathMaxInput] = useState(String(deathYearRange[1]));
    const contentRef = useRef(null);
    const [isVoiceOpen, setIsVoiceOpen] = useState(false);
    const [isBirthOpen, setIsBirthOpen] = useState(false);
    const [isDeathOpen, setIsDeathOpen] = useState(false);
    const [isBirthplacesOpen, setIsBirthplacesOpen] = useState(false);

    useLayoutEffect(() => {
      // Sync inputs when ranges or panel visibility changes
      setBirthMinInput(String(birthYearRange[0]));
      setBirthMaxInput(String(birthYearRange[1]));
      setDeathMinInput(String(deathYearRange[0]));
      setDeathMaxInput(String(deathYearRange[1]));
    }, [birthYearRange, deathYearRange, showFilterPanel]);

    const clamp = (value, min, max) => Math.max(min, Math.min(value, max));

    const applyBirthRange = () => {
      const scrollEl = contentRef.current;
      const prevTop = scrollEl ? scrollEl.scrollTop : null;
      const winY = typeof window !== 'undefined' ? window.scrollY : null;
      const minBound = getDateRanges().birthRange[0];
      const maxBound = getDateRanges().birthRange[1];
      const parsedMin = parseInt(birthMinInput, 10);
      const parsedMax = parseInt(birthMaxInput, 10);
      let nextMin = isNaN(parsedMin) ? birthYearRange[0] : clamp(parsedMin, minBound, maxBound);
      let nextMax = isNaN(parsedMax) ? birthYearRange[1] : clamp(parsedMax, minBound, maxBound);
      if (nextMax < nextMin) nextMax = nextMin;
      updateBirthYearRange([nextMin, nextMax]);
      setBirthRangeIsUserSet(true);
      setBirthMinInput(String(nextMin));
      setBirthMaxInput(String(nextMax));
      if (prevTop !== null && scrollEl) {
        requestAnimationFrame(() => {
          scrollEl.scrollTop = prevTop;
          if (winY !== null) window.scrollTo(0, winY);
          requestAnimationFrame(() => {
            scrollEl.scrollTop = prevTop;
            if (winY !== null) window.scrollTo(0, winY);
          });
        });
      }
    };

    const applyDeathRange = () => {
      const scrollEl = contentRef.current;
      const prevTop = scrollEl ? scrollEl.scrollTop : null;
      const winY = typeof window !== 'undefined' ? window.scrollY : null;
      const minBound = getDateRanges().deathRange[0];
      const maxBound = getDateRanges().deathRange[1];
      const parsedMin = parseInt(deathMinInput, 10);
      const parsedMax = parseInt(deathMaxInput, 10);
      let nextMin = isNaN(parsedMin) ? deathYearRange[0] : clamp(parsedMin, minBound, maxBound);
      let nextMax = isNaN(parsedMax) ? deathYearRange[1] : clamp(parsedMax, minBound, maxBound);
      if (nextMax < nextMin) nextMax = nextMin;
      updateDeathYearRange([nextMin, nextMax]);
      setDeathRangeIsUserSet(true);
      setDeathMinInput(String(nextMin));
      setDeathMaxInput(String(nextMax));
      if (prevTop !== null && scrollEl) {
        requestAnimationFrame(() => {
          scrollEl.scrollTop = prevTop;
          if (winY !== null) window.scrollTo(0, winY);
          requestAnimationFrame(() => {
            scrollEl.scrollTop = prevTop;
            if (winY !== null) window.scrollTo(0, winY);
          });
        });
      }
    };

    const applyAllFilters = () => {
      applyBirthRange();
      applyDeathRange();
      setFiltersVersion(v => v + 1);
    };

    return (
      <>
        {/* Overlay */}
        <div
          className={`mobile-overlay-backdrop${showFilterPanel ? ' is-open' : ''}`}
          onClick={() => setShowFilterPanel(false)}
        />

        {/* Filter Panel */}
        <div
          className={isMobileViewport ? `mobile-sheet${showFilterPanel ? ' is-open' : ''}` : undefined}
          style={isMobileViewport ? undefined : {
            position: 'fixed',
            top: 0,
            left: showFilterPanel ? 0 : -350,
            width: '350px',
            height: '100vh',
            backgroundColor: 'white',
            boxShadow: '2px 0 10px rgba(0, 0, 0, 0.1)',
            zIndex: 1000,
            transition: 'left 0.3s ease',
            display: 'flex',
            flexDirection: 'column',
            pointerEvents: showFilterPanel ? 'auto' : 'none'
          }}
          role="dialog"
          aria-modal="true"
        >
          {/* Header */}
          <div
            className={isMobileViewport ? 'mobile-sheet__header' : undefined}
            style={isMobileViewport ? undefined : {
              padding: '20px',
              borderBottom: '1px solid #e5e7eb',
              backgroundColor: '#f8f9fa'
            }}
          >
            {isMobileViewport && <div className="mobile-sheet__handle" />}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <h3 style={{ margin: 0, fontSize: '18px', fontWeight: '600', color: '#1f2937' }}>
                Filters
              </h3>
              <button
                onClick={() => setShowFilterPanel(false)}
                style={{
                  background: 'none',
                  border: 'none',
                  fontSize: isMobileViewport ? '28px' : '24px',
                  cursor: 'pointer',
                  color: '#6b7280',
                  padding: isMobileViewport ? '4px' : '0',
                  width: isMobileViewport ? '40px' : '32px',
                  height: isMobileViewport ? '40px' : '32px',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center'
                }}
              >
                ×
              </button>
            </div>
            
            {/* Clear/Apply Buttons */}
            <div
              className={isMobileViewport ? 'mobile-sheet__footer' : undefined}
              style={isMobileViewport ? undefined : {
                display: 'flex',
                gap: '8px',
                marginTop: '12px'
              }}
            >
              {hasAnyFilters && (
                <button
                  onClick={clearAllFilters}
                  style={{
                    background: 'none',
                    border: '1px solid #dc2626',
                    color: '#dc2626',
                    padding: isMobileViewport ? '12px 16px' : '6px 12px',
                    borderRadius: '8px',
                    fontSize: '16px',
                    cursor: 'pointer'
                  }}
                >
                  Clear All Filters
                </button>
              )}
              <button
                onClick={applyAllFilters}
                style={{
                  backgroundColor: '#2563eb',
                  color: 'white',
                  border: '2px solid #3e96e2',
                  padding: isMobileViewport ? '12px 16px' : '6px 12px',
                  borderRadius: '8px',
                  fontSize: '16px',
                  cursor: 'pointer'
                }}
              >
                Apply Filters
              </button>
            </div>
            
            {/* Filter Count Display */}
            {totalNodes > 0 && (
              <div style={{
                marginTop: hasAnyFilters ? '8px' : '12px',
                padding: isMobileViewport ? '12px 14px' : '8px 12px',
                backgroundColor: hasAnyFilters ? '#f0f9ff' : '#f9fafb',
                border: hasAnyFilters ? '1px solid #0ea5e9' : '1px solid #e5e7eb',
                borderRadius: '8px',
                fontSize: '16px',
                color: hasAnyFilters ? '#0c4a6e' : '#374151'
              }}>
                {hasAnyFilters ? (
                  <>
                    <strong>{visibleNodes}</strong> of <strong>{totalNodes}</strong> nodes match current filters
                    {visibleNodes !== totalNodes && (
                      <div style={{ fontSize: '12px', opacity: 0.8, marginTop: '2px' }}>
                        {totalNodes - visibleNodes} nodes filtered out
                      </div>
                    )}
                  </>
                ) : (
                  <>Showing all <strong>{totalNodes}</strong> nodes</>
                )}
              </div>
            )}
          </div>

          {/* Filter Content */}
          <div 
            className={isMobileViewport ? 'mobile-sheet__content' : undefined}
            style={isMobileViewport ? undefined : {
              flex: 1,
              overflowY: 'auto',
              padding: '20px',
              overflowAnchor: 'none'
            }}
            ref={contentRef}
          >
            {/* Voice Type Section */}
            <div style={{ marginBottom: '24px' }}>
              <button
                type="button"
                onClick={(e) => { e.stopPropagation(); setIsVoiceOpen(open => !open); }}
                onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); e.stopPropagation(); setIsVoiceOpen(open => !open); } }}
                style={{ display: 'flex', alignItems: 'center', cursor: 'pointer', padding: '8px 0', gap: '10px', background: 'none', border: 'none', width: '100%', textAlign: 'left' }}
              >
                <span style={{ color: '#374151', fontSize: '22px', lineHeight: 1 }}>{isVoiceOpen ? '▾' : '▸'}</span>
                <h4 style={{ margin: 0, fontSize: '16px', fontWeight: '600', color: '#374151' }}>Voice Type</h4>
              </button>
              {isVoiceOpen && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                  {getVisibleVoiceTypes().map(voiceType => (
                    <label
                      key={voiceType.name}
                      style={{
                        display: 'flex',
                        alignItems: 'center',
                        cursor: 'pointer',
                        padding: '6px 8px',
                        borderRadius: '8px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseEnter={(e) => e.target.style.backgroundColor = '#f3f4f6'}
                      onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
                    >
                      <input
                        type="checkbox"
                        checked={selectedVoiceTypes.has(voiceType.name)}
                        onChange={() => toggleVoiceTypeFilter(voiceType.name)}
                        style={{
                          marginRight: '10px',
                          width: '14px',
                          height: '14px',
                          accentColor: voiceType.color
                        }}
                      />
                      <div
                        style={{
                          width: '14px',
                          height: '14px',
                          backgroundColor: voiceType.color,
                          borderRadius: '50%',
                          marginRight: '10px',
                          border: '2px solid #3e96e2',
                          boxShadow: '0 0 0 1px rgba(0,0,0,0.1)'
                        }}
                      />
                      <span style={{
                        fontSize: '13px',
                        color: '#374151',
                        fontWeight: selectedVoiceTypes.has(voiceType.name) ? '600' : '400'
                      }}>
                        {voiceType.name} ({voiceType.count})
                      </span>
                    </label>
                  ))}
                </div>
              )}
            </div>

            {/* Birthplace Section */}
            <div style={{ marginBottom: '24px' }}>
              <button
                type="button"
                onClick={(e) => { e.stopPropagation(); setIsVoiceOpen(open => open); }}
                style={{ display: 'none' }}
              />
              <button
                type="button"
                onClick={(e) => { e.stopPropagation(); setIsBirthOpen(open => open); }}
                style={{ display: 'none' }}
              />
              <button
                type="button"
                onClick={(e) => { e.stopPropagation(); setIsDeathOpen(open => open); }}
                style={{ display: 'none' }}
              />
              <button
                type="button"
                onClick={(e) => { e.stopPropagation(); setIsBirthplacesOpen(open => !open); }}
                onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); e.stopPropagation(); setIsBirthplacesOpen(open => !open); } }}
                style={{ display: 'flex', alignItems: 'center', cursor: 'pointer', padding: '8px 0', gap: '10px', background: 'none', border: 'none', width: '100%', textAlign: 'left' }}
              >
                <span style={{ color: '#374151', fontSize: '22px', lineHeight: 1 }}>{isBirthplacesOpen ? '▾' : '▸'}</span>
                <h4 style={{ margin: 0, fontSize: '16px', fontWeight: '600', color: '#374151' }}>Birthplace</h4>
              </button>
              {isBirthplacesOpen && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                  {getVisibleBirthplaces().map(bp => (
                    <label key={bp.name} style={{ display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer' }}>
                      <input
                        type="checkbox"
                        checked={selectedBirthplaces.has(normalizePlaceName(bp.name))}
                        onChange={() => toggleBirthplaceFilter(bp.name)}
                      />
                      <span style={{ fontSize: '13px', color: '#374151' }}>{bp.name} ({bp.count})</span>
                    </label>
                  ))}
                  {getVisibleBirthplaces().length === 0 && (
                    <div style={{ fontSize: '12px', color: '#6b7280' }}>No birthplaces in current view</div>
                  )}
                </div>
              )}
            </div>

            {/* Birth Year Range Section */}
            <div style={{ marginBottom: '24px' }}>
              <button
                type="button"
                onClick={(e) => { e.stopPropagation(); setIsBirthOpen(open => !open); }}
                onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); e.stopPropagation(); setIsBirthOpen(open => !open); } }}
                style={{ display: 'flex', alignItems: 'center', cursor: 'pointer', padding: '8px 0', gap: '10px', background: 'none', border: 'none', width: '100%', textAlign: 'left' }}
              >
                <span style={{ color: '#374151', fontSize: '22px', lineHeight: 1 }}>{isBirthOpen ? '▾' : '▸'}</span>
                <h4 style={{ margin: 0, fontSize: '16px', fontWeight: '600', color: '#374151' }}>Birth Year Range</h4>
              </button>
              {isBirthOpen && (
              <div style={{ display: 'flex', gap: '8px' }}>
                <div style={{ flex: 1 }}>
                  <label style={{ display: 'block', fontSize: '12px', color: '#6b7280', marginBottom: '4px' }}>From</label>
                  <input
                    type="text"
                    inputMode="numeric"
                    value={birthMinInput}
                    onChange={(e) => setBirthMinInput(e.target.value)}
                    onBlur={() => {}}
                    onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); e.stopPropagation(); } }}
                    onMouseDown={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    onTouchStart={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    onFocus={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    style={{ width: '100%', padding: '6px 8px', border: '2px solid #3e96e2', borderRadius: '8px' }}
                  />
                </div>
                <div style={{ flex: 1 }}>
                  <label style={{ display: 'block', fontSize: '12px', color: '#6b7280', marginBottom: '4px' }}>To</label>
                  <input
                    type="text"
                    inputMode="numeric"
                    value={birthMaxInput}
                    onChange={(e) => setBirthMaxInput(e.target.value)}
                    onBlur={() => {}}
                    onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); e.stopPropagation(); } }}
                    onMouseDown={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    onTouchStart={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    onFocus={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    style={{ width: '100%', padding: '6px 8px', border: '2px solid #3e96e2', borderRadius: '8px' }}
                  />
                </div>
              </div>
              )}
            </div>
            {/* Death Year Range Section */}
            <div style={{ marginBottom: '24px' }}>
              <button
                type="button"
                onClick={(e) => { e.stopPropagation(); setIsDeathOpen(open => !open); }}
                onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); e.stopPropagation(); setIsDeathOpen(open => !open); } }}
                style={{ display: 'flex', alignItems: 'center', cursor: 'pointer', padding: '8px 0', gap: '10px', background: 'none', border: 'none', width: '100%', textAlign: 'left' }}
              >
                <span style={{ color: '#374151', fontSize: '22px', lineHeight: 1 }}>{isDeathOpen ? '▾' : '▸'}</span>
                <h4 style={{ margin: 0, fontSize: '16px', fontWeight: '600', color: '#374151' }}>Death Year Range</h4>
              </button>
              {isDeathOpen && (
              <div style={{ display: 'flex', gap: '8px' }}>
                <div style={{ flex: 1 }}>
                  <label style={{ display: 'block', fontSize: '12px', color: '#6b7280', marginBottom: '4px' }}>From</label>
                  <input
                    type="text"
                    inputMode="numeric"
                    value={deathMinInput}
                    onChange={(e) => setDeathMinInput(e.target.value)}
                    onBlur={() => {}}
                    onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); e.stopPropagation(); } }}
                    onMouseDown={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    onTouchStart={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    onFocus={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    style={{ width: '100%', padding: '6px 8px', border: '2px solid #3e96e2', borderRadius: '8px' }}
                  />
                </div>
                <div style={{ flex: 1 }}>
                  <label style={{ display: 'block', fontSize: '12px', color: '#6b7280', marginBottom: '4px' }}>To</label>
                  <input
                    type="text"
                    inputMode="numeric"
                    value={deathMaxInput}
                    onChange={(e) => setDeathMaxInput(e.target.value)}
                    onBlur={() => {}}
                    onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); e.stopPropagation(); } }}
                    onMouseDown={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    onTouchStart={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    onFocus={(e) => {
                      const scrollContainer = e.target.closest('div[style*="overflowY"]') || e.target.closest('[style*="overflow-y"]');
                      if (scrollContainer) {
                        const currentScrollTop = scrollContainer.scrollTop;
                        setTimeout(() => {
                          if (scrollContainer.scrollTop !== currentScrollTop) {
                            scrollContainer.scrollTop = currentScrollTop;
                          }
                        }, 0);
                      }
                    }}
                    style={{ width: '100%', padding: '6px 8px', border: '2px solid #3e96e2', borderRadius: '8px' }}
                  />
                </div>
              </div>
              )}
            </div>
          </div>
        </div>
      </>
    );
  };
  const ProfileCard = () => {
    if (!profileCard.show || !profileCard.data) return null;

    const data = profileCard.data;
    const isSinger = !!data?.full_name;
    const isOpera = !!data?.opera_name;
    const isBook = !!data?.title && !isSinger && !isOpera;

    const cardStyle = isMobileViewport ? {
      position: 'fixed',
      left: '50%',
      bottom: `calc(${showPathPanel ? '190px' : '120px'} + var(--cmg-mobile-block-padding-end))`,
      transform: 'translateX(-50%)',
      zIndex: 1000,
      fontFamily: "'Inter', 'Helvetica Neue', Arial, sans-serif"
    } : {
      position: 'absolute',
      bottom: '20px',
      left: '20px',
      width: '300px',
      maxHeight: '400px',
      backgroundColor: 'white',
      borderRadius: '8px',
      boxShadow: '0 8px 32px rgba(0,0,0,0.15)',
      border: '2px solid #3e96e2',
      zIndex: 1000,
      fontFamily: "'Inter', 'Helvetica Neue', Arial, sans-serif",
      overflow: 'hidden'
    };

    const cardClassName = isMobileViewport ? 'mobile-profile-card' : undefined;

    const contentStyle = isMobileViewport ? {
      padding: '20px 24px',
      maxHeight: 'calc(70vh - 72px)',
      overflowY: 'auto'
    } : {
      padding: '16px',
      maxHeight: '340px',
      overflowY: 'auto'
    };

    return (
      <div className={cardClassName} style={cardStyle}>
        {/* Header */}
        <div style={{
          padding: isMobileViewport ? '18px 24px 14px' : '16px',
          backgroundColor: '#f9fafb',
          borderBottom: '1px solid #e5e7eb',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          position: isMobileViewport ? 'relative' : undefined
        }}>
          {isMobileViewport && (
            <div style={{
              position: 'absolute',
              left: '50%',
              top: 10,
              transform: 'translateX(-50%)',
              width: 48,
              height: 4,
              borderRadius: 9999,
              backgroundColor: '#d1d5db'
            }} />
          )}
          <h3 style={{
            margin: 0,
            fontSize: isMobileViewport ? '18px' : '16px',
            fontWeight: '600',
            color: '#1f2937'
          }}>
            {isSinger ? '👤 Singer Profile' : isOpera ? '🎵 Opera Profile' : isBook ? '📚 Book Profile' : 'Profile'}
          </h3>
          <button
            onClick={() => setProfileCard({ show: false, data: null })}
            style={{
              background: 'none',
              border: 'none',
              fontSize: isMobileViewport ? '24px' : '18px',
              cursor: 'pointer',
              color: '#6b7280',
              padding: isMobileViewport ? '4px' : '0',
              width: isMobileViewport ? '40px' : '24px',
              height: isMobileViewport ? '40px' : '24px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center'
            }}
          >
            ×
          </button>
        </div>

        {/* Content */}
        <div style={contentStyle}>
          {isSinger && (
            <>
          <div style={{ marginBottom: '12px' }}>
            <strong style={{ color: '#1f2937' }}>Name:</strong>
            <div style={{ color: '#374151', marginTop: '2px' }}>
              {data.full_name}
            </div>
          </div>

          {data.voice_type && (
            <div style={{ marginBottom: '12px' }}>
              <strong style={{ color: '#1f2937' }}>Voice type:</strong>
              <div style={{ color: '#374151', marginTop: '2px' }}>
                {data.voice_type}
              </div>
            </div>
          )}

          {(data.birth_year || data.death_year || data.birth || data.death) && (
            <div style={{ marginBottom: '12px' }}>
              <strong style={{ color: '#1f2937' }}>Dates:</strong>
              <div style={{ color: '#374151', marginTop: '2px' }}>
                {data.birth_year && data.death_year
                  ? `${data.birth_year} - ${data.death_year}`
                  : data.birth_year
                  ? `${data.birth_year} - `
                  : data.death_year
                  ? ` - ${data.death_year}`
                  : (data.birth && data.death)
                  ? `${data.birth.low} - ${data.death.low}`
                  : data.birth
                  ? `${data.birth.low} - `
                  : data.death
                  ? ` - ${data.death.low}`
                  : ''}
              </div>
            </div>
          )}

              {(data.birthplace || data.citizen) && (
            <div style={{ marginBottom: '12px' }}>
              <strong style={{ color: '#1f2937' }}>Birthplace:</strong>
              <div style={{ color: '#374151', marginTop: '2px' }}>
                    {data.birthplace || data.citizen}
              </div>
            </div>
          )}

          {data.underrepresented_group && (
            <div style={{ marginBottom: '12px' }}>
              <strong style={{ color: '#1f2937' }}>Underrepresented group:</strong>
              <div style={{ color: '#374151', marginTop: '2px' }}>
                {data.underrepresented_group}
              </div>
            </div>
          )}

          {data.roles && Array.isArray(data.roles) && data.roles.length > 0 && (
            <div style={{ marginBottom: '12px' }}>
              <strong style={{ color: '#1f2937' }}>Roles premiered:</strong>
              <ul style={{ marginTop: '6px', paddingLeft: '18px', color: '#374151' }}>
                {data.roles.map((r, i) => (
                  <li key={i}>{r}</li>
                ))}
              </ul>
            </div>
          )}

          {(data.spotify_link || data.youtube_search) && (
            <div style={{ marginBottom: '12px' }}>
              <strong style={{ color: '#1f2937' }}>Spotify:</strong>
              <div style={{ marginTop: '2px' }}>
                <a
                  href={data.spotify_link || data.youtube_search}
                  target="_blank"
                  rel="noopener noreferrer"
                  style={{ color: '#2563eb', textDecoration: 'underline', fontSize: '16px', overflowWrap: 'anywhere', wordBreak: 'break-word', display: 'inline-block' }}
                  onMouseOver={(e) => (e.target.style.color = '#1d4ed8')}
                  onMouseOut={(e) => (e.target.style.color = '#2563eb')}
                >
                  {data.spotify_link || data.youtube_search}
                </a>
              </div>
            </div>
          )}

              <div style={{ marginTop: '16px', paddingTop: '16px', borderTop: '1px solid #e5e7eb' }}>
                <strong style={{ color: '#1f2937', fontSize: '16px' }}>Sources:</strong>
            <div style={{ marginTop: '8px' }}>
              {data.spelling_source && (
                    <div style={{ fontSize: '12px', color: '#6b7280', marginBottom: '4px' }}>
                  <strong>Spelling:</strong> {data.spelling_source}
                </div>
              )}
              {data.voice_type_source && (
                    <div style={{ fontSize: '12px', color: '#6b7280', marginBottom: '4px' }}>
                  <strong>Voice type:</strong> {data.voice_type_source}
                </div>
              )}
              {data.dates_source && (
                    <div style={{ fontSize: '12px', color: '#6b7280', marginBottom: '4px' }}>
                  <strong>Dates:</strong> {data.dates_source}
                </div>
              )}
                  {data.birthplace_source && (
                    <div style={{ fontSize: '12px', color: '#6b7280', marginBottom: '4px' }}>
                      <strong>Birthplace:</strong> {data.birthplace_source}
                </div>
              )}
            </div>
          </div>
            </>
          )}

          {isOpera && (
            <>
              <div style={{ marginBottom: '12px' }}>
                <strong style={{ color: '#1f2937' }}>Title:</strong>
                <div style={{ color: '#374151', marginTop: '2px' }}>
                  {data.opera_name}
                </div>
              </div>
              {data.composer && (
                <div style={{ marginBottom: '12px' }}>
                  <strong style={{ color: '#1f2937' }}>Composer:</strong>
                  <div style={{ color: '#374151', marginTop: '2px' }}>
                    {data.composer}
                  </div>
                </div>
              )}
            </>
          )}

          {isBook && (
            <>
              <div style={{ marginBottom: '12px' }}>
                <strong style={{ color: '#1f2937' }}>Title:</strong>
                <div style={{ color: '#374151', marginTop: '2px' }}>
                  {data.title}
                </div>
              </div>
              {data.type && (
                <div style={{ marginBottom: '12px' }}>
                  <strong style={{ color: '#1f2937' }}>Type:</strong>
                  <div style={{ color: '#374151', marginTop: '2px' }}>
                    {data.type}
                  </div>
                </div>
              )}
              {data.link && (
                <div style={{ marginBottom: '12px' }}>
                  <strong style={{ color: '#1f2937' }}>Link:</strong>
                  <a href={data.link} target="_blank" rel="noopener noreferrer" style={{ color: '#059669', textDecoration: 'underline', marginLeft: '4px' }}>
                    View Book
                  </a>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    );
  };
  const AuthForm = () => {
    const [isLogin, setIsLogin] = useState(true);
    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');
    const [showTerms, setShowTerms] = useState(false);
    const [termsChecked, setTermsChecked] = useState(false);

    useEffect(() => {
      try {
        const accepted = localStorage.getItem('tosAccepted') === '1';
        if (accepted) setTermsChecked(true);
      } catch (_) {}
    }, []);
    


    const handleSubmit = () => {
      // Add some basic validation
      if (!email || !password) {
        alert('Please enter both email and password');
        return;
      }

      // Require terms acknowledgement before proceeding
      if (!termsChecked) {
        setShowTerms(true);
        return;
      }
      
      if (isLogin) {
        login(email, password);
      } else {
        register(email, password);
      }
    };



    // Auto-fill test credentials
    const handleAutoFill = () => {
      setEmail('test@example.com');
      setPassword('password123');
    };

    const outerStyle = isMobileViewport ? {
      minHeight: backgroundMinHeight,
      backgroundImage: 'url(/aspens.jpg)',
      backgroundSize: 'cover',
      backgroundPosition: 'center center',
      backgroundRepeat: 'no-repeat',
      backgroundAttachment: backgroundAttachmentMode,
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'stretch',
      justifyContent: 'flex-start'
    } : {
      minHeight: backgroundMinHeight,
      backgroundImage: 'url(/aspens.jpg)',
      backgroundSize: 'cover',
      backgroundPosition: 'center center',
      backgroundRepeat: 'no-repeat',
      backgroundAttachment: backgroundAttachmentMode,
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      padding: '20px'
    };

    const wrapperProps = {
      className: isMobileViewport ? 'mobile-auth-wrapper' : undefined,
      style: isMobileViewport ? undefined : { maxWidth: '460px', width: '100%' }
    };

    const cardProps = {
      className: isMobileViewport ? 'mobile-auth-card' : undefined,
      style: isMobileViewport ? undefined : {
        maxWidth: '460px',
        width: '100%',
        backgroundColor: 'rgba(255,255,255,0.6)',
        borderRadius: '8px',
        boxShadow: '0 10px 25px rgba(0,0,0,0.08)',
        padding: '32px'
      }
    };

    const inputStyle = isMobileViewport ? {
      width: '100%',
      padding: '12px 14px',
      border: '2px solid #3e96e2',
      borderRadius: '12px',
      fontSize: '16px',
      boxSizing: 'border-box'
    } : {
      width: '100%',
      padding: '10px',
      border: '2px solid #3e96e2',
      borderRadius: '8px',
      fontSize: '16px',
      boxSizing: 'border-box'
    };
  return (
    <div style={outerStyle}>
      <div {...wrapperProps}>
        <div {...cardProps}>
          <div className={isMobileViewport ? 'mobile-auth-title' : undefined} style={isMobileViewport ? undefined : { textAlign: 'center', marginBottom: '18px' }}>
            <h1 style={{ fontSize: isMobileViewport ? '26px' : '28px', fontWeight: 'bold', color: '#111', margin: '8px 0 16px 0' }}>
              The Opera Singer <br /> Aspen Grove
            </h1>
            <div style={{ display: 'inline-block', backgroundColor: '#ffffff', padding: isMobileViewport ? '8px 14px' : '8px 12px', borderRadius: isMobileViewport ? '12px' : '8px' }}>
              <div style={{ fontSize: '16px', fontWeight: '600', color: '#111', lineHeight: 1.35, textAlign: 'center' }}>
                Discover Connections Among Classical Singers, Opera Premieres, and Vocal Pedagogy Books
              </div>
            </div>
          </div>
  
          <div className={isMobileViewport ? 'mobile-auth-field' : undefined} style={isMobileViewport ? undefined : { marginBottom: '20px' }}>
            <label style={{ fontWeight: '500', fontSize: isMobileViewport ? '15px' : '16px' }}>Email</label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              style={inputStyle}
              placeholder="your@email.com"
              autoComplete="email"
            />
          </div>
  
          <div className={isMobileViewport ? 'mobile-auth-field' : undefined} style={isMobileViewport ? undefined : { marginBottom: '20px' }}>
            <label style={{ fontWeight: '500', fontSize: isMobileViewport ? '15px' : '16px' }}>Password</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              style={inputStyle}
              placeholder="••••••••"
            />
          </div>
  
          {error && (
            <div style={{
              backgroundColor: '#fee',
              border: '2px solid #3e96e2',
              color: '#c33',
              padding: '10px',
              borderRadius: isMobileViewport ? '12px' : '8px'
            }}>
              {error}
            </div>
          )}
  
          <div
            className={isMobileViewport ? 'mobile-auth-inline' : undefined}
            style={isMobileViewport ? undefined : { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}
          >
            <button
              type="button"
              onClick={() => setShowTerms(true)}
              style={{
                background: 'none',
                border: 'none',
                color: '#111',
                textDecoration: 'underline',
                cursor: 'pointer',
                fontSize: isMobileViewport ? '14px' : '12px',
                padding: 0
              }}
            >
              View disclaimer
            </button>
            <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: isMobileViewport ? '14px' : '12px', color: '#111' }}>
              <input
                type="checkbox"
                checked={termsChecked}
                onChange={(e) => {
                  const v = e.target.checked;
                  setTermsChecked(v);
                  try {
                    v ? localStorage.setItem('tosAccepted', '1') : localStorage.removeItem('tosAccepted');
                  } catch (_) {}
                }}
              />
              I acknowledge the disclaimer
            </label>
          </div>
  
          <button
            onClick={handleSubmit}
            disabled={loading}
            style={{
              width: '100%',
              backgroundColor: '#667eea',
              color: '#ffffff',
              padding: isMobileViewport ? '14px' : '12px',
              border: 'none',
              borderRadius: isMobileViewport ? '12px' : '8px',
              fontSize: '16px',
              cursor: loading ? 'not-allowed' : 'pointer',
              opacity: loading ? 0.7 : 1
            }}
          >
            {loading ? 'Loading...' : (isLogin ? 'Sign In' : 'Create Account')}
          </button>
  
          {isMobileViewport ? (
            <div className="mobile-auth-actions">
              <button
                type="button"
                onClick={() => setIsLogin(!isLogin)}
                style={{
                  backgroundColor: '#f3f4f6',
                  color: '#0f172a',
                  border: '2px solid #3e96e2',
                  borderRadius: '12px',
                  fontWeight: 600
                }}
              >
                {isLogin ? "Don't have an account? Sign up!" : "Already have an account? Sign in"}
              </button>
              <div className="mobile-auth-note">
                <strong>Testing credentials:</strong><br />
                Email: test@example.com<br />
                Password: password123
              </div>
              <button
                type="button"
                onClick={handleAutoFill}
                style={{
                  backgroundColor: '#2563eb',
                  color: '#ffffff',
                  border: '2px solid #2563eb',
                  borderRadius: '12px',
                  fontSize: '15px'
                }}
              >
                Auto-fill credentials
              </button>
            </div>
          ) : (
            <>
              <div style={{ marginTop: '10px', textAlign: 'center' }}>
                <div style={{ display: 'inline-block', backgroundColor: 'rgba(255,255,255,0.6)', padding: '6px 10px', borderRadius: '8px' }}>
                  <button
                    onClick={() => setIsLogin(!isLogin)}
                    style={{
                      background: 'none',
                      border: 'none',
                      color: '#111',
                      cursor: 'pointer',
                      fontWeight: 600,
                      textDecoration: 'underline'
                    }}
                  >
                    {isLogin ? "Don't have an account? Sign up!" : "Already have an account? Sign in"}
                  </button>
                </div>
              </div>
              <div style={{ marginTop: '15px', padding: '10px', backgroundColor: '#f8f9fa', borderRadius: '8px', fontSize: '12px', color: '#666' }}>
                <div style={{ marginBottom: '8px' }}>
                  <strong>For testing:</strong><br />
                  Email: test@example.com<br />
                  Password: password123
                </div>
                <button
                  onClick={handleAutoFill}
                  style={{
                    fontSize: '11px',
                    padding: '4px 8px',
                    backgroundColor: '#6c757d',
                    color: 'white',
                    border: 'none',
                    borderRadius: '8px',
                    cursor: 'pointer'
                  }}
                >
                  Auto-fill credentials
                </button>
              </div>
            </>
          )}
        </div>
      </div>
  
      {showTerms && (
        isMobileViewport ? (
          <>
            <div
              className="mobile-overlay-backdrop is-open"
              style={{ zIndex: 3000 }}
              onClick={() => setShowTerms(false)}
            />
            <div
              className="mobile-sheet is-open"
              style={{ zIndex: 3001, paddingBottom: '24px' }}
              onClick={(e) => e.stopPropagation()}
            >
              <div className="mobile-sheet__header" style={{ paddingBottom: 0 }}>
                <div className="mobile-sheet__handle" />
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12 }}>
                  <h3 style={{ margin: 0, fontSize: '20px', fontWeight: 600, color: '#0f172a' }}>
                    The Aspen Grove of Opera Singers, Disclaimer
                  </h3>
                  <button
                    onClick={() => setShowTerms(false)}
                    style={{
                      background: 'none',
                      border: 'none',
                      color: '#1f2937',
                      fontSize: '24px',
                      cursor: 'pointer',
                      fontWeight: 600,
                      padding: 4,
                      width: 40,
                      height: 40,
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center'
                    }}
                    aria-label="Close disclaimer"
                  >
                    ×
                  </button>
                </div>
              </div>
              <div className="mobile-sheet__content" style={{ paddingTop: 12 }}>
                <p className="mobile-auth-note">
                  Welcome to The Aspen Grove of Opera Singers, and thank you for your interest in this project.
                </p>
                <p className="mobile-auth-note">
                  I have spent the last three summers and this current sabbatical collecting information for this database. I have endeavored to include "successful" opera singers and their teachers. Success can, of course, be defined many ways. For the purposes of this tool, I have chosen to include singers who have sung roles at A- and B-level houses and their equivalents, singers who are managed, and singers who have been documented in reference books and websites specializing in classical singing and teaching history. Though the information is vast (14,000+ singers, 3,500 relationships), it is far from exhaustive. I can make no claims about the quality of the teaching or of the quality of the relationship between teacher and student. Further, there is no guarantee that any teacher's methods carry forward to their students or the next generation, or to those that follow.
                </p>
                <p className="mobile-auth-note">
                  If you would like some or all of your personal information to be removed from the dataset for any reason, please <a href="mailto:classicalsinginghumanitieslab@gmail.com">let me know.</a> I will happily remove anyone's information. If you have a correction from a credible source, I will happily incorporate that too. If you have information to add that meets the criteria described above, please fill out <a href="https://forms.gle/TZmuaPpMUu9ob4jT8" target="_blank" rel="noopener noreferrer">this form</a>, and I will incorporate it as quickly as I can.
                </p>
                <label style={{ display: 'flex', alignItems: 'flex-start', gap: 10, fontSize: '14px', color: '#111' }}>
                  <input
                    type="checkbox"
                    checked={termsChecked}
                    onChange={(e) => {
                      const v = e.target.checked;
                      setTermsChecked(v);
                      try {
                        v ? localStorage.setItem('tosAccepted', '1') : localStorage.removeItem('tosAccepted');
                      } catch (_) {}
                    }}
                  />
                  <span className="mobile-auth-note" style={{ color: '#111', fontSize: '14px' }}>
                    By checking this box, I acknowledge the extent of the site's current contents and limitations expressed in this disclaimer.
                  </span>
                </label>
              </div>
              <div className="mobile-sheet__footer">
                <button
                  onClick={() => setShowTerms(false)}
                  style={{
                    padding: '12px 16px',
                    backgroundColor: '#ffffff',
                    color: '#374151',
                    border: '2px solid #3e96e2',
                    borderRadius: '12px'
                  }}
                >
                  Close
                </button>
                <button
                  onClick={() => {
                    try { localStorage.setItem('tosAccepted', '1'); } catch (_) {}
                    setTermsChecked(true);
                    setShowTerms(false);
                  }}
                  disabled={!termsChecked}
                  style={{
                    padding: '12px 16px',
                    backgroundColor: termsChecked ? '#2563eb' : '#93c5fd',
                    color: '#ffffff',
                    border: '2px solid #2563eb',
                    borderRadius: '12px',
                    cursor: termsChecked ? 'pointer' : 'not-allowed'
                  }}
                >
                  Agree & Continue
                </button>
              </div>
            </div>
          </>
        ) : (
          <div style={{ position: 'fixed', inset: 0, backgroundColor: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 3000 }}>
            <div style={{ backgroundColor: 'white', width: 'min(720px, 92vw)', maxHeight: '80vh', overflowY: 'auto', borderRadius: 10, boxShadow: '0 20px 40px rgba(0,0,0,0.25)', padding: 20 }}>
              <h3 style={{ margin: 0, marginBottom: 10, fontSize: 18, color: '#111' }}>The Aspen Grove of Opera Singers, Disclaimer</h3>
              <p style={{ fontSize: 14, color: '#333', lineHeight: 1.6 }}>
                Welcome to The Aspen Grove of Opera Singers, and thank you for your interest in this project.
              </p>
              <p style={{ fontSize: 14, color: '#333', lineHeight: 1.6 }}>
                I have spent the last three summers and this current sabbatical collecting information for this database. I have endeavored to include "successful" opera singers and their teachers. Success can, of course, be defined many ways. For the purposes of this tool, I have chosen to include singers who have sung roles at A- and B-level houses and their equivalents, singers who are managed, and singers who have been documented in reference books and websites specializing in classical singing and teaching history. Though the information is vast (14,000+ singers, 3,500 relationships), it is far from exhaustive. I can make no claims about the quality of the teaching or of the quality of the relationship between teacher and student. Further, there is no guarantee that any teacher's methods carry forward to their students or the next generation, or to those that follow.
              </p>
              <p style={{ fontSize: 14, color: '#333', lineHeight: 1.6 }}>
                If you would like some or all of your personal information to be removed from the dataset for any reason, please <a href="mailto:classicalsinginghumanitieslab@gmail.com">let me know.</a>. I will happily remove anyone's information. If you have a correction from a credible source, I will happily incorporate that too. If you have information to add that meets the criteria described above, please fill out <a href="https://forms.gle/TZmuaPpMUu9ob4jT8" target="_blank">this form</a>, and I will incorporate it as quickly as I can.
              </p>
              <div style={{ marginTop: 12, paddingTop: 12, borderTop: '1px solid #eee', display: 'flex', flexDirection: 'column', gap: 12 }}>
                <label style={{ display: 'flex', alignItems: 'flex-start', gap: 8, fontSize: 14, color: '#111' }}>
                  <input type="checkbox" checked={termsChecked} onChange={(e) => { const v = e.target.checked; setTermsChecked(v); try { v ? localStorage.setItem('tosAccepted', '1') : localStorage.removeItem('tosAccepted'); } catch(_){} }} />
                  <span>By checking this box, I acknowledge the extent of the site's current contents and limitations expressed in this disclaimer.</span>
                </label>
                <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
                  <button onClick={() => setShowTerms(false)} style={{ padding: '8px 12px', border: '2px solid #3e96e2', backgroundColor: '#fafafa', color: '#374151', borderRadius: 6, cursor: 'pointer' }}>Close</button>
                  <button onClick={() => { try { localStorage.setItem('tosAccepted','1'); } catch(_){} setTermsChecked(true); setShowTerms(false); }} disabled={!termsChecked} style={{ padding: '8px 12px', border: '2px solid #3e96e2', backgroundColor: '#ffffff', color: '#374151', borderRadius: 6, cursor: termsChecked ? 'pointer' : 'not-allowed' }}>Agree & Continue</button>
                </div>
              </div>
            </div>
          </div>
        )
      )}
    </div>
  );
  };

  if (!token) {
    return <AuthForm />;
  }
  return (
    <div style={{
      minHeight: backgroundMinHeight,
      backgroundImage: 'url(/aspens.jpg)',
      backgroundSize: 'cover',
      backgroundPosition: 'center center',
      backgroundRepeat: 'no-repeat',
      backgroundAttachment: backgroundAttachmentMode
    }}>
      {false && (<header style={{
        backgroundColor: 'white',
        borderBottom: '1px solid #dee2e6',
        padding: '15px 0'
      }}>
        <div style={{
          maxWidth: '1240px',
          margin: '0 auto',
          padding: '0 20px',
          display: 'flex',
          justifyContent: 'flex-start',
          alignItems: 'center',
          gap: 12
        }}>
          <h1 style={{ fontSize: '20px', fontWeight: 'bold', color: '#333' }}>
            Classical Singer Family Tree
          </h1>
          
          <div style={{ display: 'flex', alignItems: 'stretch', gap: '8px', flex: 1, justifyContent: 'flex-start' }}>
            

            {/* Saved view quick loader moved below Save/Export and Logout */}

            {currentView === 'network' && null}
            
            {currentView === 'network' && (
              <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-start', alignItems: 'stretch' }}>
                {/* Full menu: Search, Back, Forward, Filters, Path */}
            <button
              onClick={() => setCurrentView('search')}
              style={{
                padding: '8px 16px',
                    backgroundColor: '#f3f4f6',
                    color: '#374151',
                    border: '2px solid #3e96e2',
                borderRadius: '8px',
                    cursor: 'pointer',
                    fontSize: '16px'
              }}
            >
              Search
            </button>
                <button
                  onClick={() => { goBack(); }}
                  disabled={historyCounts.past === 0}
                  title={historyCounts.past ? `Back (${historyCounts.past})` : 'Back'}
                  style={{
                    padding: '8px 12px',
                    backgroundColor: historyCounts.past ? '#f3f4f6' : '#fafafa',
                    color: '#374151',
                    border: '2px solid #3e96e2',
                    borderRadius: '8px',
                    cursor: historyCounts.past ? 'pointer' : 'not-allowed',
                    fontSize: '16px'
                  }}
                >
                  <span style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', lineHeight: 1 }}>
                    <span>Back</span>
                    <span style={{ fontSize: 12 }}>←</span>
                  </span>
                </button>
                <button
                  onClick={() => { goForward(); }}
                  disabled={historyCounts.future === 0}
                  title={historyCounts.future ? `Forward (${historyCounts.future})` : 'Forward'}
                  style={{
                    padding: '8px 12px',
                    backgroundColor: historyCounts.future ? '#f3f4f6' : '#fafafa',
                    color: '#374151',
                    border: '2px solid #3e96e2',
                    borderRadius: '8px',
                    cursor: historyCounts.future ? 'pointer' : 'not-allowed',
                    fontSize: '16px'
                  }}
                >
                  <span style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', lineHeight: 1 }}>
                    <span>Forward</span>
                    <span style={{ fontSize: 12 }}>→</span>
                  </span>
                </button>
              <button
                onClick={() => setShowFilterPanel(true)}
                style={{
                  padding: '8px 16px',
                  backgroundColor: selectedVoiceTypes.size > 0 ? '#e3f2fd' : 'transparent',
                  color: selectedVoiceTypes.size > 0 ? '#1976d2' : '#666',
                  border: '2px solid #3e96e2',
                  borderRadius: '8px',
                  cursor: 'pointer',
                  fontSize: '16px',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '6px'
                }}
              >
                🔍 Filters
                {selectedVoiceTypes.size > 0 && (
                  <span style={{
                    backgroundColor: '#1976d2',
                    color: 'white',
                    borderRadius: '8px',
                    padding: '2px 6px',
                    fontSize: '12px',
                    fontWeight: 'bold'
                  }}>
                    {selectedVoiceTypes.size}
                  </span>
                )}
              </button>
              <button
                onClick={() => { setCurrentView('network'); setShowPathPanel(v => !v); }}
                style={{
                  padding: '8px 16px',
                  backgroundColor: '#ffffff',
                  color: '#374151',
                  border: '2px solid #3e96e2',
                  borderRadius: '8px',
                  cursor: 'pointer',
                  fontSize: '16px',
                  opacity: 1
                }}
              >
                Path
              </button>
              </div>
            )}

            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginLeft: 'auto' }}>
            {/* Save/Export dropdown (right-aligned with viz) */}
            <div style={{ position: 'relative' }}>
              <button
                onClick={() => {
                  if (!isSaveExportEligible) return;
                  setShowSaveExportMenu(v => !v);
                }}
                disabled={!isSaveExportEligible}
                style={{
                  padding: '8px 16px',
                  backgroundColor: showSaveExportMenu ? '#f3f4f6' : '#fafafa',
                  color: '#374151',
                  border: '2px solid #3e96e2',
                  borderRadius: '8px',
                  cursor: isSaveExportEligible ? 'pointer' : 'not-allowed',
                  fontSize: '16px',
                  lineHeight: '20px',
                  height: '48px',
                  display: 'inline-flex',
                  alignItems: 'center',
                  boxSizing: 'border-box',
                  opacity: isSaveExportEligible ? 1 : 0.6
                }}
              >
                Save/Export ▾
              </button>
              {showSaveExportMenu && (
                <div
                  style={{ position: 'absolute', right: 0, top: '110%', backgroundColor: 'white', border: '2px solid #3e96e2', borderRadius: 6, boxShadow: '0 8px 20px rgba(0,0,0,0.12)', padding: 12, minWidth: 260, zIndex: 1000 }}
                  onMouseLeave={() => setShowSaveExportMenu(false)}
                >
                  {renderSaveExportFields()}
                </div>
              )}
            </div>
            <button
              onClick={() => {
                setToken('');
                clearStoredToken();
                setCurrentView('search');
              }}
              style={{
                backgroundColor: '#ffffff',
                color: '#374151',
                padding: '8px 16px',
                height: '48px',
                border: '2px solid #3e96e2',
                borderRadius: '8px',
                cursor: 'pointer',
                display: 'inline-flex',
                alignItems: 'center',
                boxSizing: 'border-box',
                fontSize: '16px'
              }}
            >
              Logout
            </button>
          </div>
        </div>
        </div>
      </header>)}
      <SavedViewDialog />
      <main
        style={{
          maxWidth: '1240px',
          margin: '0 auto',
          padding: '30px 20px',
          paddingBottom: isMobileViewport
            ? (currentView === 'network' ? '220px' : '140px')
            : '30px'
        }}
      >
        {/* Former header content moved into main */}
        <div
          ref={headerContainerRef}
          className={isHeaderMobile ? 'mobile-header-card' : undefined}
          style={{
            backgroundColor: 'rgba(255,255,255,0.9)',
            padding: isHeaderMobile
              ? 'calc(var(--cmg-mobile-block-padding) + 12px) max(16px, var(--cmg-mobile-inline-padding-end)) 20px max(16px, var(--cmg-mobile-inline-padding))'
              : '12px 16px',
            borderRadius: isHeaderMobile ? '18px' : '8px',
            marginBottom: 15,
            position: 'relative',
            boxShadow: isHeaderMobile ? '0 14px 32px rgba(15, 23, 42, 0.18)' : undefined,
            border: isHeaderMobile ? '2px solid #3e96e2' : undefined,
            paddingRight: isHeaderMobile ? undefined : 440,
            minHeight: isHeaderMobile ? 'auto' : 140
          }}
        >
          <div
            className={isHeaderMobile ? 'mobile-stack' : undefined}
            style={{
              maxWidth: '1240px',
              margin: '0 auto',
              padding: isHeaderMobile ? '0 calc(var(--cmg-mobile-inline-padding-end) + 32px) 0 0' : '0 20px',
              display: 'flex',
              flexDirection: 'column',
              justifyContent: 'flex-start',
              alignItems: 'flex-start',
              gap: isHeaderMobile ? 16 : 12
            }}
          >
            <div
              role="group"
              aria-label="Site Titles"
              style={{
                display: 'flex',
                flexDirection: 'column',
                alignItems: isHeaderMobile ? 'flex-start' : 'flex-start',
                gap: 4,
                flex: 1,
                minWidth: 0
              }}
            >
              <h1
                className={isHeaderMobile ? 'mobile-heading' : undefined}
                style={{
                  fontSize: isHeaderMobile ? undefined : '40px',
                  fontWeight: 'bold',
                  color: isHeaderMobile ? undefined : '#333',
                  margin: 0,
                  whiteSpace: 'normal',
                  overflow: 'visible',
                  lineHeight: isHeaderMobile ? undefined : 1.2
                }}
              >
                The Aspen Grove of Opera Singers
              </h1>
              <h2
                className={isHeaderMobile ? 'mobile-subheading mobile-muted' : undefined}
                style={{
                  fontSize: isHeaderMobile ? undefined : '24px',
                  fontWeight: 600,
                  color: isHeaderMobile ? undefined : '#374151',
                  margin: 0,
                  whiteSpace: 'normal',
                  overflow: 'visible',
                  lineHeight: isHeaderMobile ? undefined : 1.4
                }}
              >
                Discover Connections Among Classical Singers, <br/> Opera Premieres, and Vocal Pedagogy Books
              </h2>
            </div>
            {!isHeaderMobile && (
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px', justifyContent: 'flex-start', flex: '0 0 auto' }} />
            )}
            {/* Right controls moved to absolute group */}
          </div>
          {/* Absolute top-right controls group */}
          <div
            style={{
              position: 'absolute',
              top: isHeaderMobile ? 'max(12px, var(--cmg-mobile-block-padding))' : 12,
              right: isHeaderMobile ? 'max(12px, var(--cmg-mobile-inline-padding-end))' : 20,
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'flex-end',
              gap: isHeaderMobile ? 6 : 8
            }}
          >
            {isHeaderMobile ? (
              <button
                type="button"
                className="mobile-tap-target"
                onClick={() => {
                  setShowSaveExportMenu(false);
                  setShowHeaderMenu(prev => !prev);
                }}
                style={{
                  padding: '10px 12px',
                  backgroundColor: '#ffffff',
                  color: '#374151',
                  border: '2px solid #3e96e2',
                  borderRadius: '12px',
                  cursor: 'pointer',
                  fontSize: '20px',
                  lineHeight: 1,
                  width: '48px',
                  height: '48px',
                  display: 'inline-flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  boxSizing: 'border-box',
                  fontWeight: 700
                }}
                aria-label={showHeaderMenu ? 'Close menu' : 'Open menu'}
                aria-expanded={showHeaderMenu}
              >
                ☰
              </button>
            ) : (
              <>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <button
                    onClick={() => { setCurrentView('help'); setShowSaveExportMenu(false); try { window.__cmg_reapplyZoom && window.__cmg_reapplyZoom(); } catch (_) {} }}
                    style={{
                      padding: '8px 16px',
                      backgroundColor: '#ffffff',
                      color: '#374151',
                      border: '2px solid #3e96e2',
                      borderRadius: '8px',
                      cursor: 'pointer',
                      fontSize: '16px',
                      lineHeight: '20px',
                      height: '48px',
                      display: 'inline-flex',
                      alignItems: 'center',
                      boxSizing: 'border-box'
                    }}
                  >
                    Help center
                  </button>
                  {hasSearchResults && (
                    <div style={{ position: 'relative' }}>
                      <button
                        ref={saveExportBtnRef}
                        onMouseDown={(e) => { if (isSaveExportEligible) { e.stopPropagation(); try { window.__cmg_reapplyZoom && window.__cmg_reapplyZoom(); } catch (_) {} }}}
                        onClick={(e) => {
                          if (!isSaveExportEligible) return;
                          e.stopPropagation();
                          setShowSaveExportMenu(v => !v);
                        }}
                        disabled={!isSaveExportEligible}
                        style={{
                          padding: '8px 16px',
                          backgroundColor: '#ffffff',
                          color: '#374151',
                          border: '2px solid #3e96e2',
                          borderRadius: '8px',
                          cursor: isSaveExportEligible ? 'pointer' : 'not-allowed',
                          fontSize: '16px',
                          lineHeight: '20px',
                          height: '48px',
                          display: 'inline-flex',
                          alignItems: 'center',
                          boxSizing: 'border-box',
                          opacity: isSaveExportEligible ? 1 : 0.6
                        }}
                      >
                        Save/Export ▾
                      </button>
                      {showSaveExportMenu && (
                        <div
                          style={{ position: 'absolute', right: 0, top: '110%', backgroundColor: 'white', border: '2px solid #3e96e2', borderRadius: 8, boxShadow: '0 8px 20px rgba(0,0,0,0.12)', padding: 12, minWidth: 260, zIndex: 1000 }}
                          onMouseLeave={() => setShowSaveExportMenu(false)}
                          onMouseDown={(e) => { e.stopPropagation(); try { window.__cmg_reapplyZoom && window.__cmg_reapplyZoom(); } catch (_) {} }}
                          onClick={(e) => e.stopPropagation()}
                        >
                          {renderSaveExportFields()}
                        </div>
                      )}
                    </div>
                  )}
                  <button
                    ref={logoutBtnRef}
                    onClick={() => { setToken(''); clearStoredToken(); setCurrentView('search'); }}
                    style={{ backgroundColor: '#ffffff', color: '#374151', padding: '8px 16px', height: '48px', border: '2px solid #3e96e2', borderRadius: '8px', cursor: 'pointer', display: 'inline-flex', alignItems: 'center', boxSizing: 'border-box', opacity: 1, fontSize: '16px' }}
                  >
                    Logout
                  </button>
                </div>
                {!hasSearchResults && currentView !== 'network' && (
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <input
                      placeholder="Paste saved view string here"
                      value={loadToken}
                      onChange={e => setLoadToken(e.target.value)}
                      onKeyDown={(e) => { if (e.key === 'Enter') { attemptLoadSavedView(); } }}
                    style={{ padding: '6px 8px', border: '2px solid #3e96e2', backgroundColor: '#ffffff', color: '#374151', borderRadius: 8, width: savedInputBelowWidth, height: '48px', boxSizing: 'border-box', fontSize: '16px', textAlign: 'center' }}
                    />
                    <button
                      ref={openBtnBelowRef}
                      onClick={attemptLoadSavedView}
                      disabled={!token || !loadToken || isLoadingView}
                      style={{ padding: '8px 12px', backgroundColor: '#ffffff', color: '#374151', border: '2px solid #3e96e2', borderRadius: '8px', cursor: (token && loadToken && !isLoadingView) ? 'pointer' : 'not-allowed', fontSize: '16px', opacity: 1, height: '48px', display: 'inline-flex', alignItems: 'center', boxSizing: 'border-box' }}
                    >
                      {isLoadingView ? 'Opening…' : 'Open'}
                    </button>
                  </div>
                )}
                {currentView === 'network' && (
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <button onClick={() => setCurrentView('search')} style={{ padding: '8px 16px', backgroundColor: '#f3f4f6', color: '#374151', border: '2px solid #3e96e2', borderRadius: '8px', cursor: 'pointer', fontSize: '16px', height: '48px', display: 'inline-flex', alignItems: 'center', boxSizing: 'border-box' }}>Search</button>
                    <button onClick={() => { goBack(); }} disabled={historyCounts.past === 0} title={historyCounts.past ? `Back (${historyCounts.past})` : 'Back'} style={{ padding: '8px 12px', backgroundColor: '#ffffff', color: '#374151', border: '2px solid #3e96e2', borderRadius: '8px', cursor: historyCounts.past ? 'pointer' : 'not-allowed', fontSize: '16px', opacity: 1, height: '48px', display: 'inline-flex', alignItems: 'center', boxSizing: 'border-box' }}>
                      <span style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', lineHeight: 1 }}>
                        <span>Back</span>
                        <span style={{ fontSize: 12 }}>←</span>
                      </span>
                    </button>
                    <button onClick={() => { goForward(); }} disabled={historyCounts.future === 0} title={historyCounts.future ? `Forward (${historyCounts.future})` : 'Forward'} style={{ padding: '8px 12px', backgroundColor: '#ffffff', color: '#374151', border: '2px solid #3e96e2', borderRadius: '8px', cursor: historyCounts.future ? 'pointer' : 'not-allowed', fontSize: '16px', opacity: 1, height: '48px', display: 'inline-flex', alignItems: 'center', boxSizing: 'border-box' }}>
                      <span style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', lineHeight: 1 }}>
                        <span>Forward</span>
                        <span style={{ fontSize: 12 }}>→</span>
                      </span>
                    </button>
                    <button onClick={() => setShowFilterPanel(true)} style={{ padding: '8px 16px', backgroundColor: '#ffffff', color: selectedVoiceTypes.size > 0 ? '#1976d2' : '#666', border: '2px solid #3e96e2', borderRadius: '8px', cursor: 'pointer', fontSize: '16px', display: 'flex', alignItems: 'center', gap: '6px', opacity: 1, height: '48px', boxSizing: 'border-box' }}>
                      🔍 Filters
                      {selectedVoiceTypes.size > 0 && (
                        <span style={{ backgroundColor: '#1976d2', color: 'white', borderRadius: '8px', padding: '2px 6px', fontSize: '12px', fontWeight: 'bold' }}>
                          {selectedVoiceTypes.size}
                        </span>
                      )}
                    </button>
                    <button onClick={() => { setCurrentView('network'); setShowPathPanel(v => !v); }} style={{ padding: '8px 16px', backgroundColor: '#ffffff', color: '#374151', border: '2px solid #3e96e2', borderRadius: '8px', cursor: 'pointer', fontSize: '16px', opacity: 1, height: '48px', display: 'inline-flex', alignItems: 'center', boxSizing: 'border-box' }}>
                      Path
                    </button>
                  </div>
                )}
              </>
            )}
          </div>
        </div>
        {isHeaderMobile && showHeaderMenu && (
          <div
            className="mobile-header-overlay"
            style={{
              position: 'fixed',
              inset: 0,
              backgroundColor: 'rgba(15,23,42,0.45)',
              zIndex: 2000,
              display: 'flex',
              justifyContent: 'center'
            }}
            onClick={() => setShowHeaderMenu(false)}
          >
            <div
              className="mobile-panel"
              role="dialog"
              aria-modal="true"
              aria-label="Navigation menu"
              style={{
                marginTop: 'calc(var(--cmg-mobile-block-padding) + 48px)'
              }}
              onClick={(e) => e.stopPropagation()}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <span style={{ fontSize: '18px', fontWeight: 600, color: '#0f172a' }}>Menu</span>
                <button
                  type="button"
                  onClick={() => setShowHeaderMenu(false)}
                  style={{
                    background: 'none',
                    border: 'none',
                    fontSize: '24px',
                    color: '#1f2937',
                    cursor: 'pointer',
                    fontWeight: 600
                  }}
                  aria-label="Close menu"
                >
                  ×
                </button>
              </div>
              <button
                type="button"
                onClick={() => {
                  setCurrentView('help');
                  setShowHeaderMenu(false);
                }}
                style={{
                  padding: '12px 16px',
                  border: '2px solid #3e96e2',
                  borderRadius: 12,
                  backgroundColor: '#ffffff',
                  color: '#374151',
                  fontSize: '16px',
                  fontWeight: 600,
                  cursor: 'pointer'
                }}
              >
                Help center
              </button>
              {(!hasSearchResults && currentView !== 'network') && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                  <input
                    placeholder="Paste saved view string here"
                    value={loadToken}
                    onChange={e => setLoadToken(e.target.value)}
                    onKeyDown={(e) => { if (e.key === 'Enter') { attemptLoadSavedView(); } }}
                    style={{ padding: '6px 8px', border: '2px solid #3e96e2', borderRadius: 12, height: '48px', boxSizing: 'border-box', fontSize: '16px', textAlign: 'center' }}
                  />
                  <button
                    type="button"
                    onClick={attemptLoadSavedView}
                    disabled={!token || !loadToken || isLoadingView}
                    style={{ padding: '12px 16px', border: '2px solid #3e96e2', borderRadius: 12, backgroundColor: '#ffffff', color: '#374151', fontSize: '16px', fontWeight: 600, cursor: (token && loadToken && !isLoadingView) ? 'pointer' : 'not-allowed', opacity: (token && loadToken && !isLoadingView) ? 1 : 0.6 }}
                  >
                    {isLoadingView ? 'Opening…' : 'Open'}
                  </button>
                </div>
              )}
              {hasSearchResults && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                  {renderSaveExportFields({ isMobileLayout: true })}
                </div>
              )}
              {currentView === 'network' && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                  <button type="button" onClick={() => { setCurrentView('search'); setShowHeaderMenu(false); }} style={{ padding: '12px 16px', border: '2px solid #3e96e2', borderRadius: 12, backgroundColor: '#ffffff', color: '#374151', fontSize: '16px', fontWeight: 600, cursor: 'pointer' }}>Search</button>
                  <button type="button" onClick={() => { goBack(); }} disabled={historyCounts.past === 0} style={{ padding: '12px 16px', border: '2px solid #3e96e2', borderRadius: 12, backgroundColor: '#ffffff', color: '#374151', fontSize: '16px', fontWeight: 600, cursor: historyCounts.past ? 'pointer' : 'not-allowed', opacity: historyCounts.past ? 1 : 0.6 }}>Back</button>
                  <button type="button" onClick={() => { goForward(); }} disabled={historyCounts.future === 0} style={{ padding: '12px 16px', border: '2px solid #3e96e2', borderRadius: 12, backgroundColor: '#ffffff', color: '#374151', fontSize: '16px', fontWeight: 600, cursor: historyCounts.future ? 'pointer' : 'not-allowed', opacity: historyCounts.future ? 1 : 0.6 }}>Forward</button>
                  <button type="button" onClick={() => { setShowFilterPanel(true); setShowHeaderMenu(false); }} style={{ padding: '12px 16px', border: '2px solid #3e96e2', borderRadius: 12, backgroundColor: '#ffffff', color: selectedVoiceTypes.size > 0 ? '#1976d2' : '#374151', fontSize: '16px', fontWeight: 600, cursor: 'pointer', display: 'flex', justifyContent: 'center', gap: 8 }}>
                    🔍 Filters
                    {selectedVoiceTypes.size > 0 && (
                      <span style={{ backgroundColor: '#1976d2', color: '#ffffff', borderRadius: 8, padding: '2px 8px', fontSize: 12, fontWeight: 700 }}>
                        {selectedVoiceTypes.size}
                      </span>
                    )}
                  </button>
                  <button type="button" onClick={() => { setShowPathPanel(v => !v); setCurrentView('network'); setShowHeaderMenu(false); }} style={{ padding: '12px 16px', border: '2px solid #3e96e2', borderRadius: 12, backgroundColor: '#ffffff', color: '#374151', fontSize: '16px', fontWeight: 600, cursor: 'pointer' }}>Path</button>
                </div>
              )}
              <button
                type="button"
                onClick={() => {
                  setToken('');
                  clearStoredToken();
                  setCurrentView('search');
                  setShowHeaderMenu(false);
                }}
                style={{
                  padding: '12px 16px',
                  border: '2px solid #3e96e2',
                  borderRadius: 12,
                  backgroundColor: '#ffffff',
                  color: '#cb1f1f',
                  fontSize: '16px',
                  fontWeight: 600,
                  cursor: 'pointer'
                }}
              >
                Logout
              </button>
            </div>
          </div>
        )}
        {/* Removed duplicate tagline block below header as titles are now in the header group */}

        {currentView !== 'help' && (
          <>
            <div style={{
              display: 'flex',
              flexDirection: 'row',
              alignItems: 'center',
              gap: '10px',
              marginBottom: '20px',
              justifyContent: 'center'
            }}>
              {[
                { key: 'singers', label: 'People', icon: '👤' },
                { key: 'operas', label: 'Operas', icon: '🎵' },
                { key: 'books', label: 'Books', icon: '📚' }
              ].map(type => (
                <button
                  key={type.key}
                  onClick={() => {
                    setSearchType(type.key);
                    setSearchResults([]);
                    setCurrentView('search');
                    setItemDetails(null);
                    setSelectedItem(null);
                    setError('');
                    setSearchQuery('');
                    setNetworkData({ nodes: [], links: [] });
                  }}
                  style={{
                    padding: '10px 16px',
                    backgroundColor: 'white',
                    color: '#333',
                    border: searchType === type.key ? '4px solid #3e96e2' : '2px solid #3e96e2',
                    borderRadius: '8px',
                    cursor: 'pointer',
                    fontSize: '14px',
                    fontWeight: (searchType === type.key ? '600' : '500'),
                    opacity: 1
                  }}
                >
                  {type.icon} {type.label}
                </button>
              ))}
            </div>

            <div style={{ maxWidth: '600px', margin: '0 auto 30px', width: '100%' }}>
              <div style={{ display: 'flex', flexDirection: isHeaderMobile ? 'column' : 'row', gap: '15px', width: '100%' }}>
                <input
                  type="text"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  onKeyPress={(e) => e.key === 'Enter' && performSearch()}
                  placeholder={
                    searchType === 'singers'
                      ? (isHeaderMobile ? 'Search people (e.g., Garcia II)' : 'Search for opera singers and teachers... (e.g., Garcia II)')
                      : searchType === 'operas'
                        ? 'Search for operas... (e.g., La Traviata)'
                        : (isHeaderMobile ? 'Search ped books (Vocal Wisdom)' : 'Search for vocal pedagogy books... (e.g., Vocal wisdom, c1931)')
                  }
                  spellCheck="false"
                  style={{
                    flex: 1,
                    padding: '15px',
                    border: '2px solid #3e96e2',
                    borderRadius: '8px',
                    fontSize: '16px'
                  }}
                />
                <button
                  onClick={performSearch}
                  disabled={loading || !searchQuery.trim()}
                  style={{
                    backgroundColor: 'white',
                    color: '#333',
                    padding: '12px 20px',
                    border: '2px solid #3e96e2',
                    borderRadius: '8px',
                    cursor: loading || !searchQuery.trim() ? 'not-allowed' : 'pointer',
                    fontSize: '16px',
                    fontWeight: 500,
                    width: isHeaderMobile ? '100%' : 'auto'
                  }}
                >
                  Search
                </button>
              </div>
            </div>
          </>
        )}

        {currentView === 'search' && (
          <>
            <div style={{ marginTop: isHeaderMobile ? '48px' : '100px', width: '100%', display: 'flex', justifyContent: 'center' }}>
              <div
                className={isHeaderMobile ? 'mobile-stack mobile-search-hero' : undefined}
                style={{
                  width: isHeaderMobile ? '100%' : 790,
                  display: 'flex',
                  flexDirection: 'column',
                  gap: isHeaderMobile ? '20px' : '18px',
                  padding: isHeaderMobile ? '0 var(--cmg-mobile-inline-padding)' : 0
                }}
              >
                <div style={{ textAlign: isHeaderMobile ? 'center' : 'left' }}>
                  <h3 style={{ display: 'inline-block', backgroundColor: '#ffffff', padding: '6px 10px', borderRadius: '8px' }}>Examples:</h3>
                </div>
                <div
                  className={isHeaderMobile ? 'mobile-search-example-grid' : undefined}
                  style={{
                    display: isHeaderMobile ? 'grid' : 'flex',
                    gridTemplateColumns: isHeaderMobile ? '1fr' : undefined,
                    gap: isHeaderMobile ? '16px' : '20px',
                    justifyContent: isHeaderMobile ? 'stretch' : 'flex-start',
                    alignItems: 'flex-start',
                    flexDirection: isHeaderMobile ? undefined : 'row',
                    width: '100%'
                  }}
                >
                  {[{
                    key: 'ailyn',
                    label: 'Ailyn Pérez',
                    image: '/Ailyn.png',
                    token: 'd4240ab6-d2a5-4199-8e06-6d24c01e3ad7'
                  }, {
                    key: 'longest',
                    label: 'Longest Path',
                    image: '/Longest.png',
                    token: '97a81f43-ac2f-4b1b-9403-326f2453b4fb'
                  }, {
                    key: 'books-premieres',
                    label: 'Books &\nPremieres',
                    image: '/BnP.png',
                    token: 'b5715a67-28a4-4add-975b-7a682bc64f4a'
                  }].map(example => (
                    <button
                      key={example.key}
                      type="button"
                      onClick={() => loadViewByToken(example.token, { treatAsSearch: true })}
                      style={{
                        width: isHeaderMobile ? '100%' : 250,
                        height: isHeaderMobile ? 200 : 170,
                        padding: 0,
                        border: 'none',
                        background: 'none',
                        position: 'relative',
                        cursor: 'pointer',
                        borderRadius: '12px',
                        overflow: 'hidden',
                        boxShadow: '0 8px 18px rgba(0,0,0,0.25)'
                      }}
                      aria-label={`Load example view for ${example.label.replace(/\n/g, ' ')}`}
                    >
                      <img
                        src={example.image}
                        alt={example.label}
                        style={{
                          width: '100%',
                          height: '100%',
                          objectFit: 'cover',
                          display: 'block',
                          borderRadius: '12px',
                          border: '4px solid #ffffff'
                        }}
                      />
                      <span
                        style={{
                          position: 'absolute',
                          left: '50%',
                          top: '50%',
                          transform: 'translate(-50%, -50%)',
                          color: '#111827',
                          fontSize: '24px',
                          fontWeight: 700,
                          textShadow: '0 0 6px rgba(255,255,255,0.95), 0 1px 10px rgba(255,255,255,0.85)',
                          whiteSpace: 'nowrap'
                        }}
                      >
                        <span style={{ display: 'inline-block', textAlign: 'center', whiteSpace: 'pre-line' }}>
                          {example.label}
                        </span>
                      </span>
                    </button>
                  ))}
                </div>
              </div>
            </div>

            {!isHeaderMobile && (
              <div style={{ maxWidth: '1200px', margin: '20px auto 0', padding: '0 20px', display: 'flex', justifyContent: 'flex-end' }}>
                <img
                  src="/paypal.png"
                  alt="Support via PayPal"
                  style={{
                    width: 160,
                    height: 160,
                    objectFit: 'contain',
                    border: '2px solid #3e96e2',
                    borderRadius: '12px',
                    padding: '8px',
                    backgroundColor: '#f9fafb'
                  }}
                />
              </div>
            )}
            {isHeaderMobile && (
              <div style={{ margin: '12px auto 0', display: 'flex', justifyContent: 'center', width: '100%' }}>
                <a
                  className="mobile-donate-card"
                  href="https://www.paypal.biz/sethkeetonvoice"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Link to donate on PayPal
                </a>
              </div>
            )}
          </>
        )}

        {currentView === 'search' && (
          isMobileViewport ? (
            <>
              <div
                className={`mobile-overlay-backdrop${showSupportPanel ? ' is-open' : ''}`}
                onClick={() => setShowSupportPanel(false)}
                style={{ zIndex: 1199 }}
              />
              <div
                className={`mobile-sheet${showSupportPanel ? ' is-open' : ''}`}
                style={{ paddingBottom: '24px', zIndex: 1200 }}
                aria-label="Support panel"
              >
                <div className="mobile-sheet__header" style={{ paddingBottom: 0 }}>
                  <div className="mobile-sheet__handle" />
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12 }}>
                    <h3 style={{ margin: 0, fontSize: '20px', fontWeight: 600, color: '#0f172a' }}>
                      Support The Aspen Grove of Opera Singers
                    </h3>
                    <button
                      onClick={() => setShowSupportPanel(false)}
                      style={{
                        background: 'none',
                        border: 'none',
                        color: '#1f2937',
                        fontSize: '22px',
                        cursor: 'pointer',
                        fontWeight: 600,
                        padding: 4,
                        width: 40,
                        height: 40,
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center'
                      }}
                      aria-label="Hide support panel"
                    >
                      ×
                    </button>
                  </div>
                </div>
                <div className="mobile-sheet__content" style={{ paddingTop: 12 }}>
                  <p style={{ margin: 0, fontSize: '16px', color: '#374151', lineHeight: 1.5 }}>
                    We depend on your support to maintain and grow the Aspen Grove. Proceeds from your donation pay for hosting costs and the ability to hire an assistant. Server costs are modest, but ongoing. To help with these and to keep the site ad free, a suggested $10/year donation is incredibly appreciated. Any amount is a great help. Thank you!
                  </p>
                </div>
                <div className="mobile-sheet__footer">
                  <a
                    className="mobile-donate-card"
                    href="https://www.paypal.biz/sethkeetonvoice"
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    Link to donate on PayPal
                  </a>
                </div>
              </div>
            </>
          ) : (
            <div
              style={{
                position: 'fixed',
                left: '50%',
                bottom: showSupportPanel ? '40px' : '-1000px',
                transform: 'translateX(-50%)',
                width: headerWidth ? `${headerWidth}px` : 'min(1200px, calc(100vw - 40px))',
                backgroundColor: '#ffffff',
                borderRadius: '16px',
                border: '2px solid #3e96e2',
                boxShadow: '0 18px 44px rgba(0,0,0,0.25)',
                padding: '24px 28px',
                display: 'grid',
                gridTemplateColumns: 'minmax(0, 1fr) auto auto',
                columnGap: '24px',
                rowGap: '16px',
                alignItems: 'start',
                transition: 'bottom 0.6s cubic-bezier(0.22, 1, 0.36, 1)',
                zIndex: 1200,
                opacity: showSupportPanel ? 1 : 0,
                pointerEvents: showSupportPanel ? 'auto' : 'none'
              }}
            >
              <div
                style={{
                  gridColumn: '1',
                  gridRow: '1',
                  display: 'flex',
                  flexDirection: 'column',
                  gap: '12px',
                  minWidth: 0
                }}
              >
                <h3 style={{ margin: 0, fontSize: '22px', color: '#0f172a' }}>
                  Support The Aspen Grove of Opera Singers
                </h3>
                <p style={{ margin: 0, fontSize: '16px', color: '#374151', lineHeight: 1.5 }}>
                  We depend on your support to maintain and grow the Aspen Grove. Proceeds from your donation pay for hosting costs and the ability to hire an assistant. Server costs are modest, but ongoing. To help with these and to keep the site ad free, a suggested $10/year donation is incredibly appreciated. Any amount is a great help. Thank you!
                </p>
              </div>
              <img
                src="/paypal.png"
                alt="Support via PayPal"
                style={{
                  gridColumn: '2',
                  gridRow: '1',
                  width: '160px',
                  height: '160px',
                  objectFit: 'contain',
                  border: '2px solid #3e96e2',
                  borderRadius: '12px',
                  padding: '8px',
                  backgroundColor: '#f9fafb',
                  justifySelf: 'end'
                }}
              />
              <button
                onClick={() => setShowSupportPanel(false)}
                style={{
                  gridColumn: '3',
                  gridRow: '1',
                  justifySelf: 'end',
                  alignSelf: 'start',
                  background: 'none',
                  border: 'none',
                  color: '#1f2937',
                  fontSize: '22px',
                  cursor: 'pointer',
                  fontWeight: 600,
                  padding: 0
                }}
                aria-label="Hide support panel"
              >
                ×
              </button>
            </div>
          )
        )}

        {currentView === 'help' && (
          <div style={{
            marginTop: '80px',
            display: 'flex',
            justifyContent: 'center'
          }}>
            <div style={{
              width: 'min(960px, 92vw)',
              backgroundColor: 'rgba(255,255,255,0.9)',
              borderRadius: '18px',
              border: '2px solid #3e96e2',
              boxShadow: '0 24px 48px rgba(0,0,0,0.25)',
              padding: '36px',
              color: '#1f2937',
              fontSize: '18px',
              lineHeight: 1.6,
              display: 'flex',
              flexDirection: 'column',
              gap: '28px'
            }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 16 }}>
                <h2 style={{ margin: 0, fontSize: '34px', color: '#0f172a' }}>Help Center</h2>
                <button
                  onClick={() => setCurrentView('search')}
                  style={{
                    padding: '8px 16px',
                    backgroundColor: '#ffffff',
                    color: '#374151',
                    border: '2px solid #3e96e2',
                    borderRadius: '8px',
                    cursor: 'pointer',
                    fontSize: '16px'
                  }}
                >
                  ← Back to Search
                </button>
              </div>

              <section>
                <h3 style={{ margin: '0 0 12px 0', fontSize: '22px', color: '#0f172a' }}>About</h3>
                <details style={{ backgroundColor: '#f6faff', border: '2px solid #cbdaf7', borderRadius: '12px', padding: '16px 20px' }}>
                  <summary style={{ cursor: 'pointer', fontWeight: 600, color: '#1d4ed8', fontSize: '18px' }}>Project overview</summary>
                  <p style={{ marginTop: '12px', color: '#374151' }}>
                    As a lover of nature, when my family and I moved to the west, I was amazed to learn that aspen trees in a grove are all one organism. They are rhizomatic - that is, they all grow from the same system of roots. We can trace the beginning of trained, classical singing to the Florentine Camerata, and in this manner of thinking, all classical singers share the same origins. This site is my attempt to show our vast interconnectedness.
                  </p>
                  <p style={{ marginTop: '12px', color: '#374151' }}>
                    From 2022 to 2025, I worked to create a ‘family tree’ of successful opera singers and those who taught them. I was motivated to do this work because I frequently marveled at highly skilled classical singers and wondered who these incredible teachers were. Like <a href="https://www.songhelix.com" target="_blank">SongHelix</a> (released in 2019), I wanted to make a useful tool that allowed for deep and broad insights. I hoped to create a tool that would allow singers, fans of classical singing, and scholars a simple way to discover teacher-singer lineage. I have pulled data from a variety of online and print sources. Each of those can be investigated on examination of any piece of data on the site. When Wikipedia is cited, it can refer to any language version of the student or teacher's Wikipedia site. Frequenlty another language's version will have different information from the English version.
                  </p>
                  <p style={{ marginTop: '12px', color: '#374151' }}>
                    I have used various methods for gathering data at scale including querying Wikidata, webscraping, and using python scripts. While Artificial Intelligence has helped me gather information (and to code the entire website(!)), no information has been <i>created</i> through the use of AI.
                  </p>
                  <p style={{ marginTop: '12px', color: '#374151' }}>
                    For any questions regarding the tool's creation, the data collection methods, to license the background systems for a similar site of your own, or to send any comments, please contact me <a href="mailto:classicalsinginghumanitieslab@gmail.com">here</a>.
                  </p>
                  <p style={{ marginTop: '12px', color: '#374151',  textAlign: "right"}}>
                   - Seth Keeton, founder <br/>
                   Classical Singing Humanities Lab
                  </p>
                </details>
              </section>

              <section>
                <h3 style={{ margin: '0 0 18px 0', fontSize: '22px', color: '#0f172a' }}>Videos</h3>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '16px' }}>
                  {[1, 2, 3, 4].map((slot) => (
                    <div
                      key={slot}
                      style={{
                        height: 140,
                        backgroundColor: '#e0edff',
                        border: '2px dashed #3e96e2',
                        borderRadius: '12px',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        color: '#1d4ed8',
                        fontWeight: 600
                      }}
                    >
                      Video {slot}
                    </div>
                  ))}
                </div>
              </section>

              <section>
                <h3 style={{ margin: '0 0 12px 0', fontSize: '22px', color: '#0f172a' }}>Acknowledgements</h3>
                <p style={{ margin: 0, color: '#374151' }}>
                  Placeholder text for credits, institutional support, and contributor recognition will appear here. Use this space to thank collaborators, data partners, and supporters who make the project possible.
                </p>
              </section>
            </div>
          </div>
        )}

        {/* Active Filter Indicators - show when in network view and filters are active */}
        {currentView === 'network' && selectedVoiceTypes.size > 0 && (
          <div style={{
            maxWidth: '600px',
            margin: '0 auto 20px',
            display: 'flex',
            alignItems: 'center',
            gap: '8px',
            flexWrap: isMobileViewport ? 'nowrap' : 'wrap',
            overflowX: isMobileViewport ? 'auto' : 'visible',
            paddingBottom: isMobileViewport ? '6px' : 0
          }}>
            <span style={{
              fontSize: '16px',
              color: '#6b7280',
              fontWeight: '500'
            }}>
              Active filters:
            </span>
            {Array.from(selectedVoiceTypes).map(voiceType => {
              const voiceTypeConfig = VOICE_TYPES.find(vt => vt.name === voiceType);
              return (
                <div
                  key={voiceType}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    backgroundColor: '#e3f2fd',
                    border: '1px solid #bbdefb',
                    borderRadius: '8px',
                    padding: '4px 8px 4px 6px',
                    fontSize: '16px',
                    gap: '6px',
                    flex: '0 0 auto'
                  }}
                >
                  <div
                    style={{
                      width: '12px',
                      height: '12px',
                      backgroundColor: voiceTypeConfig?.color || '#6b7280',
                      borderRadius: '50%',
                      border: '2px solid #3e96e2',
                      boxShadow: '0 0 0 1px rgba(0,0,0,0.1)'
                    }}
                  />
                  <span style={{ color: '#1976d2', fontWeight: '500' }}>
                    {voiceType}
                  </span>
                  <button
                    onClick={() => toggleVoiceTypeFilter(voiceType)}
                    style={{
                      background: 'none',
                      border: 'none',
                      color: '#666',
                      cursor: 'pointer',
                      fontSize: '16px',
                      lineHeight: '1',
                      padding: '0',
                      width: '16px',
                      height: '16px',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center'
                    }}
                    title={`Remove ${voiceType} filter`}
                  >
                    ×
                  </button>
                </div>
              );
            })}
            <button
              onClick={clearAllFilters}
              style={{
                background: 'none',
                border: '1px solid #dc2626',
                color: '#dc2626',
                padding: '4px 8px',
                borderRadius: '8px',
                fontSize: '12px',
                cursor: 'pointer',
                fontWeight: '500'
              }}
            >
              Clear all
            </button>
          </div>
        )}

        {error && (
          <div style={{
            maxWidth: '600px',
            margin: '0 auto',
            backgroundColor: '#fee',
            border: '2px solid #3e96e2',
            color: '#c33',
            padding: '15px',
            borderRadius: '8px'
          }}>
            {error}
          </div>
        )}

        {currentView === 'results' && searchResults.length > 0 && (
          <div style={{ marginBottom: '30px', padding: isHeaderMobile ? '0 var(--cmg-mobile-inline-padding)' : 0 }}>
            <h3 style={{ display: 'inline-block', backgroundColor: '#ffffff', padding: '6px 10px', borderRadius: '8px' }}>Search Results ({searchResults.length})</h3>
            <div
              className={isHeaderMobile ? 'mobile-search-results-grid' : undefined}
              style={{
                display: 'grid',
                gridTemplateColumns: isHeaderMobile ? '1fr' : 'repeat(auto-fill, minmax(300px, 1fr))',
                gap: isHeaderMobile ? '16px' : '20px'
              }}
            >
              {searchResults.map((item, index) => (
                <div
                  key={index}
                  onClick={() => getItemDetails(item)}
                  className={isHeaderMobile ? 'mobile-card mobile-search-result-card' : undefined}
                  style={{
                    backgroundColor: 'white',
                    borderRadius: '8px',
                    padding: isHeaderMobile ? '18px' : '20px',
                    boxShadow: showResultsHalo ? '0 0 12px 2px rgba(255,255,255,0.85), 0 0 18px 6px rgba(62,150,226,0.45), 0 0 22px 9px rgba(228,162,1,0.35), 0 0 28px 12px rgba(62,150,226,0.25)' : '0 2px 4px rgba(0,0,0,0.1)',
                    border: '2px solid #3e96e2',
                    cursor: 'pointer',
                    transition: 'box-shadow 0.35s ease'
                  }}
                  onMouseOver={(e) => e.currentTarget.style.boxShadow = showResultsHalo ? '0 0 12px 2px rgba(255,255,255,0.90), 0 0 22px 8px rgba(62,150,226,0.50), 0 0 26px 10px rgba(228,162,1,0.40), 0 10px 22px rgba(0,0,0,0.12)' : '0 4px 8px rgba(0,0,0,0.15)'}
                  onMouseOut={(e) => e.currentTarget.style.boxShadow = showResultsHalo ? '0 0 12px 2px rgba(255,255,255,0.85), 0 0 18px 6px rgba(62,150,226,0.45), 0 0 22px 9px rgba(228,162,1,0.35), 0 0 28px 12px rgba(62,150,226,0.25)' : '0 2px 4px rgba(0,0,0,0.1)'}
                >
                  <h4>{searchType === 'singers' ? (item.name || item.properties.full_name) : searchType === 'operas' ? item.properties.opera_name : item.properties.title}</h4>
                  {searchType === 'operas' && item.properties.version && (
                    <p style={{ margin: '2px 0 6px 0', fontSize: '16px', color: '#555', fontWeight: 400 }}>
                      {item.properties.version}
                    </p>
                  )}
                  {searchType === 'singers' && item.properties.voice_type && (
                    <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                      <strong>Voice type:</strong> {item.properties.voice_type}
                    </p>
                  )}
                  {searchType === 'singers' && (item.properties.birth_year || item.properties.death_year) && (
                    <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                      <strong>Dates:</strong> {
                        item.properties.birth_year && item.properties.death_year
                          ? `${item.properties.birth_year} - ${item.properties.death_year}`
                          : item.properties.birth_year
                          ? `${item.properties.birth_year} - `
                          : item.properties.death_year
                          ? ` - ${item.properties.death_year}`
                          : ''
                      }
                    </p>
                  )}
                  {searchType === 'operas' && item.properties.composer && (
                    <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                      <strong>Composer:</strong> {item.properties.composer}
                    </p>
                  )}
                  {searchType === 'books' && item.properties.author && (
                    <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                      <strong>Author:</strong> {item.properties.author}
                    </p>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}

        {(currentView === 'results' || currentView === 'network') && networkData.nodes.length > 0 && (
          <div
            className={isMobileViewport ? 'mobile-safe-area-inline' : undefined}
            style={{ width: '100%', marginBottom: '30px' }}
          >
            <div
              style={{
                display: 'flex',
                justifyContent: isMobileViewport ? 'center' : 'flex-end',
                alignItems: 'center',
                marginBottom: '15px'
              }}
            >
              <div className="network-hint">
                Drag nodes to reposition • Scroll to zoom • Drag to pan
              </div>
            </div>
            <NetworkVisualization viewport={viewport} />
          </div>
        )}

        {currentView === 'results' && searchResults.length === 0 && (
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', padding: '20px' }}>
            <div style={{ display: 'inline-block', backgroundColor: '#ffffff', color: '#000000', padding: '10px 16px', borderRadius: '8px' }}>
              <p style={{ fontSize: '18px', color: '#000000', margin: 0 }}>No search results to display.</p>
            </div>
            {/* Back to Search button removed as requested */}
          </div>
        )}
        {currentView === 'network' && itemDetails && (
          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '30px' }}>
              <h2 style={{ fontSize: '24px', fontWeight: 'bold', backgroundColor: '#ffffff', padding: '6px 10px', borderRadius: '8px' }}>
                {searchType === 'singers' && itemDetails.center ? itemDetails.center.full_name : 
                 searchType === 'operas' && itemDetails.opera ? itemDetails.opera.opera_name :
                 searchType === 'books' && itemDetails.book ? itemDetails.book.title :
                 selectedItem.name || (selectedItem.properties && selectedItem.properties.title)} - Details
              </h2>
            </div>

            <div style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))',
              gap: '20px'
            }}>
              {searchType === 'singers' && itemDetails.center && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                    👤 Singer Profile
                  </h3>
                  <p style={{ margin: '8px 0' }}>
                    <strong>Name:</strong> {itemDetails.center.full_name}
                  </p>
                  {itemDetails.center.voice_type && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Voice type:</strong> {itemDetails.center.voice_type}
                    </p>
                  )}
                  {(itemDetails.center.birth_year || itemDetails.center.death_year || itemDetails.center.birth || itemDetails.center.death) && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Dates:</strong> {
                        itemDetails.center.birth_year && itemDetails.center.death_year
                          ? `${itemDetails.center.birth_year} - ${itemDetails.center.death_year}`
                          : itemDetails.center.birth_year
                          ? `${itemDetails.center.birth_year} - `
                          : itemDetails.center.death_year
                          ? ` - ${itemDetails.center.death_year}`
                          : (itemDetails.center.birth && itemDetails.center.death)
                          ? `${itemDetails.center.birth.low} - ${itemDetails.center.death.low}`
                          : itemDetails.center.birth
                          ? `${itemDetails.center.birth.low} - `
                          : itemDetails.center.death
                          ? ` - ${itemDetails.center.death.low}`
                          : ''
                      }
                    </p>
                  )}
                  {(itemDetails.center.birthplace || itemDetails.center.citizen) && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Birthplace:</strong> {itemDetails.center.birthplace || itemDetails.center.citizen}
                    </p>
                  )}
                  {itemDetails.center.underrepresented_group && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Underrepresented group:</strong> {itemDetails.center.underrepresented_group}
                    </p>
                  )}
                  {(itemDetails.center.spotify_link || itemDetails.center.youtube_search) && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Spotify:</strong>{' '}
                      <a
                        href={itemDetails.center.spotify_link || itemDetails.center.youtube_search}
                        target="_blank"
                        rel="noopener noreferrer"
                        style={{
                          color: '#2563eb',
                          textDecoration: 'underline',
                          overflowWrap: 'anywhere',
                          wordBreak: 'break-word',
                          display: 'inline-block'
                        }}
                        onMouseOver={(e) => (e.target.style.color = '#1d4ed8')}
                        onMouseOut={(e) => (e.target.style.color = '#2563eb')}
                      >
                        {itemDetails.center.spotify_link || itemDetails.center.youtube_search}
                      </a>
                    </p>
                  )}
                  
                  {/* Sources section */}
                  <div style={{ marginTop: '15px', paddingTop: '15px', borderTop: '1px solid #e5e7eb' }}>
                    {itemDetails.center.spelling_source && (
                      <p style={{ margin: '4px 0', fontSize: '12px', color: '#888'}}>
                        Spelling source: {itemDetails.center.spelling_source}
                      </p>
                    )}
                    {itemDetails.center.voice_type_source && (
                      <p style={{ margin: '4px 0', fontSize: '12px', color: '#888'}}>
                        Voice type source: {itemDetails.center.voice_type_source}
                      </p>
                    )}
                    {itemDetails.center.dates_source && (
                      <p style={{ margin: '4px 0', fontSize: '12px', color: '#888'}}>
                        Dates source: {itemDetails.center.dates_source}
                      </p>
                    )}
                    {itemDetails.center.birthplace_source && (
                      <p style={{ margin: '4px 0', fontSize: '12px', color: '#888'}}>
                        Birthplace source: {itemDetails.center.birthplace_source}
                      </p>
                    )}
                  </div>
                  </div>
                </div>
              )}

              {searchType === 'singers' && itemDetails.teachers && itemDetails.teachers.length > 0 && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                  👤 Teachers ({itemDetails.teachers.length})
                  </h3>
                  {itemDetails.teachers.map((teacher, index) => (
                    <div 
                      key={index} 
                      style={{ 
                        marginBottom: '12px', 
                        paddingBottom: '12px', 
                        borderBottom: index < itemDetails.teachers.length - 1 ? '1px solid #e5e7eb' : 'none',
                        cursor: 'pointer',
                        padding: '8px',
                        borderRadius: '8px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={() => {
                        pushHistory('card-click-teacher');
                        // Set search type to singers to maintain consistent styling
                        setSearchType('singers');
                        searchForPerson(teacher.full_name);
                      }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{teacher.full_name}</p>
                      {teacher.voice_type && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Voice type:</strong> {teacher.voice_type}
                        </p>
                      )}
                      {(teacher.birth_year || teacher.death_year) && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Dates:</strong> {
                            teacher.birth_year && teacher.death_year
                              ? `${teacher.birth_year} - ${teacher.death_year}`
                              : teacher.birth_year
                              ? `${teacher.birth_year} - `
                              : teacher.death_year
                              ? ` - ${teacher.death_year}`
                              : ''
                          }
                        </p>
                      )}
                      <p style={{ margin: '4px 0', fontSize: '12px', color: '#888', fontStyle: 'italic' }}>
                        Relationship source: {teacher.teacher_rel_source || 'Unknown'}
                      </p>
                    </div>
                  ))}
                  </div>
                </div>
              )}

              {searchType === 'singers' && itemDetails.students && itemDetails.students.length > 0 && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                  👤 Students ({itemDetails.students.length})
                  </h3>
                  {itemDetails.students.map((student, index) => (
                    <div 
                      key={index} 
                      style={{ 
                        marginBottom: '12px', 
                        paddingBottom: '12px', 
                        borderBottom: index < itemDetails.students.length - 1 ? '1px solid #e5e7eb' : 'none',
                        cursor: 'pointer',
                        padding: '8px',
                        borderRadius: '8px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={() => {
                        pushHistory('card-click-student');
                        // Set search type to singers to maintain consistent styling
                        setSearchType('singers');
                        searchForPerson(student.full_name);
                      }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{student.full_name}</p>
                      {student.voice_type && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Voice type:</strong> {student.voice_type}
                        </p>
                      )}
                      {(student.birth_year || student.death_year) && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Dates:</strong> {
                            student.birth_year && student.death_year
                              ? `${student.birth_year} - ${student.death_year}`
                              : student.birth_year
                              ? `${student.birth_year} - `
                              : student.death_year
                              ? ` - ${student.death_year}`
                              : ''
                          }
                        </p>
                      )}
                      <p style={{ margin: '4px 0', fontSize: '12px', color: '#888', fontStyle: 'italic' }}>
                        Relationship source: {student.teacher_rel_source || 'Unknown'}
                      </p>
                    </div>
                  ))}
                  </div>
                </div>
              )}

              {(() => { const fam = itemDetails ? (itemDetails.family || itemDetails.center?.family || []) : []; return fam.length > 0; })() && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                    {(() => { const fam = itemDetails ? (itemDetails.family || itemDetails.center?.family || []) : []; return `👨‍👩‍👧‍👦 Family (${fam.length})`; })()}
                  </h3>
                  {(() => { const fam = itemDetails ? (itemDetails.family || itemDetails.center?.family || []) : []; return fam; })().map((relative, index) => (
                    <div 
                      key={index} 
                      style={{ 
                        marginBottom: '12px', 
                        paddingBottom: '12px', 
                        borderBottom: (() => { const fam = itemDetails ? (itemDetails.family || itemDetails.center?.family || []) : []; return index < fam.length - 1 ? '1px solid #e5e7eb' : 'none'; })(),
                        cursor: 'pointer',
                        padding: '8px',
                        borderRadius: '8px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={() => {
                        pushHistory('card-click-family');
                        // Set search type to singers to maintain consistent styling
                        setSearchType('singers');
                        searchForPerson(relative.full_name);
                      }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{relative.full_name}</p>
                      {relative.relationship_type && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Relationship:</strong> {relative.relationship_type}
                        </p>
                      )}
                      {relative.voice_type && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Voice type:</strong> {relative.voice_type}
                        </p>
                      )}
                      {(relative.birth_year || relative.death_year) && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Dates:</strong> {
                            relative.birth_year && relative.death_year
                              ? `${relative.birth_year} - ${relative.death_year}`
                              : relative.birth_year
                              ? `${relative.birth_year} - `
                              : relative.death_year
                              ? ` - ${relative.death_year}`
                              : ''
                          }
                        </p>
                      )}
                      {(relative.teacher_rel_source || relative.source) && (
                        <p style={{ margin: '4px 0', fontSize: '12px', color: '#888', fontStyle: 'italic' }}>
                          Relationship source: {relative.teacher_rel_source || relative.source || 'Unknown'}
                        </p>
                      )}
                    </div>
                  ))}
                  </div>
                </div>
              )}

              {/* Roles premiered card */}
              {searchType === 'singers' && itemDetails.premieredRoles && itemDetails.premieredRoles.length > 0 && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                    🎭 Operas Premiered ({itemDetails.premieredRoles.length})
                  </h3>
                  {itemDetails.premieredRoles.map((role, index) => (
                    <div 
                      key={index} 
                      style={{ 
                        marginBottom: '12px', 
                        paddingBottom: '12px', 
                        borderBottom: index < itemDetails.premieredRoles.length - 1 ? '1px solid #e5e7eb' : 'none',
                        cursor: 'pointer',
                        padding: '8px',
                        borderRadius: '8px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={async () => {
                        pushHistory('card-click-opera-premiered');
                        try {
                          setLoading(true);
                          const response = await fetch(`${API_BASE}/opera/details`, {
                            method: 'POST',
                            headers: {
                              'Content-Type': 'application/json',
                              'Authorization': `Bearer ${token}`
                            },
                            body: JSON.stringify({ operaName: role.opera_name })
                          });

                          const data = await response.json();
                          if (response.ok) {
                            setItemDetails(data);
                            setSelectedItem({ properties: { opera_name: role.opera_name } });
                            setSearchType('operas');
                            setCurrentView('network');
                            generateNetworkFromDetails(data, role.opera_name, 'operas');
                            setShouldRunSimulation(true);
                          } else {
                            setError(data.error);
                          }
                        } catch (err) {
                          setError('Failed to fetch opera details');
                        } finally {
                          setLoading(false);
                        }
                      }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{role.opera_name}</p>
                      {role.role && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Role premiered:</strong> {role.role}
                        </p>
                      )}
                      {role.source && (
                        <p style={{ margin: '4px 0', fontSize: '12px', color: '#888', fontStyle: 'italic' }}>
                          Source: {role.source || 'Unknown'}
                        </p>
                      )}
                    </div>
                  ))}
                  </div>
                </div>
              )}
              {searchType === 'singers' && itemDetails.works && (
                <>

                  {itemDetails.works.books && itemDetails.works.books.length > 0 && (
                    <div style={{
                      backgroundColor: 'white',
                      borderRadius: '8px',
                      padding: 0,
                      boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                      border: '2px solid #3e96e2',
                      height: '300px',
                      overflow: 'hidden'
                    }}>
                      <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                      <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                        📚 Books ({itemDetails.works.books.length})
                      </h3>
                      {itemDetails.works.books.map((book, index) => (
                        <div 
                          key={index} 
                          style={{ 
                            marginBottom: '12px', 
                            paddingBottom: '12px', 
                            borderBottom: index < itemDetails.works.books.length - 1 ? '1px solid #e5e7eb' : 'none',
                            cursor: 'pointer',
                            padding: '8px',
                            borderRadius: '8px',
                            transition: 'background-color 0.2s'
                          }}
                          onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                          onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                          onClick={async () => {
                            pushHistory('card-click-book');
                            try {
                              setLoading(true);
                              const response = await fetch(`${API_BASE}/book/details`, {
                                method: 'POST',
                                headers: {
                                  'Content-Type': 'application/json',
                                  'Authorization': `Bearer ${token}`
                                },
                                body: JSON.stringify({ bookTitle: book.title })
                              });

                              const data = await response.json();
                              if (response.ok) {
                                setItemDetails(data);
                                setSelectedItem({ properties: { title: book.title } });
                                setSearchType('books');
                                setCurrentView('network');
                                generateNetworkFromDetails(data, book.title, 'books');
                                setShouldRunSimulation(true); // Trigger simulation for clicked book
                              } else {
                                setError(data.error);
                              }
                            } catch (err) {
                              setError('Failed to fetch book details');
                            } finally {
                              setLoading(false);
                            }
                          }}
                        >
                          <p style={{ margin: '4px 0', fontWeight: '500' }}>{book.title}</p>
                          {book.source && (
                            <p style={{ margin: '4px 0', fontSize: '12px', color: '#888', fontStyle: 'italic' }}>
                              Source: {book.source || 'Unknown'}
                            </p>
                          )}
                        </div>
                      ))}
                      </div>
                    </div>
                  )}

                  {itemDetails.works.composedOperas && itemDetails.works.composedOperas.length > 0 && (
                    <div style={{
                      backgroundColor: 'white',
                      borderRadius: '8px',
                      padding: '20px',
                      boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                      height: '300px',
                      overflowY: 'auto'
                    }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                        🎼 Composed Operas ({itemDetails.works.composedOperas.length})
                      </h3>
                      {itemDetails.works.composedOperas.map((opera, index) => (
                        <div 
                          key={index} 
                          style={{ 
                            marginBottom: '12px', 
                            paddingBottom: '12px', 
                            borderBottom: index < itemDetails.works.composedOperas.length - 1 ? '1px solid #e5e7eb' : 'none',
                            cursor: 'pointer',
                            padding: '8px',
                            borderRadius: '8px',
                            transition: 'background-color 0.2s'
                          }}
                          onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                          onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                          onClick={async () => {
                            pushHistory('card-click-opera-composed');
                            try {
                              setLoading(true);
                              const response = await fetch(`${API_BASE}/opera/details`, {
                                method: 'POST',
                                headers: {
                                  'Content-Type': 'application/json',
                                  'Authorization': `Bearer ${token}`
                                },
                                body: JSON.stringify({ operaName: opera.title })
                              });

                              const data = await response.json();
                              if (response.ok) {
                                setItemDetails(data);
                                setSelectedItem({ properties: { title: opera.title } });
                                setSearchType('operas');
                                setCurrentView('network');
                                generateNetworkFromDetails(data, opera.title, 'operas');
                                setShouldRunSimulation(true); // Trigger simulation for clicked composed opera
                              } else {
                                setError(data.error);
                              }
                            } catch (err) {
                              setError('Failed to fetch opera details');
                            } finally {
                              setLoading(false);
                            }
                          }}
                        >
                          <p style={{ margin: '4px 0', fontWeight: '500' }}>{opera.title}</p>
                          {opera.source && (
                            <p style={{ margin: '4px 0', fontSize: '12px', color: '#888', fontStyle: 'italic' }}>
                              Source: {opera.source || 'Unknown'}
                            </p>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                </>
              )}

              {/* Opera detail cards */}
              {searchType === 'operas' && itemDetails.opera && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#a09602', marginBottom: '15px' }}>
                    🎵 Opera Profile
                  </h3>
                  <p style={{ margin: '8px 0' }}>
                    <strong>Title:</strong> {itemDetails.opera.opera_name}
                  </p>
                  {itemDetails.opera.composer && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Composer:</strong> 
                      <span 
                        style={{ 
                          color: '#059669', 
                          cursor: 'pointer', 
                          textDecoration: 'underline',
                          marginLeft: '4px'
                        }}
                        onClick={() => {
                          // Store the current opera for context
                          const currentOpera = {
                            name: itemDetails.opera.opera_name,
                            composer: itemDetails.opera.composer
                          };
                          searchForPersonFromOpera(itemDetails.opera.composer, currentOpera);
                        }}
                        onMouseOver={(e) => e.target.style.color = '#047857'}
                        onMouseOut={(e) => e.target.style.color = '#059669'}
                      >
                        {itemDetails.opera.composer}
                      </span>
                    </p>
                  )}
                  {itemDetails.opera.premiere_year && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Premiere:</strong> {itemDetails.opera.premiere_year}
                    </p>
                  )}
                  </div>
                </div>
              )}

              {/* Opera Composer card (Wrote) */}
              {searchType === 'operas' && ((itemDetails.opera && itemDetails.opera.composer) || (Array.isArray(itemDetails.wrote) && itemDetails.wrote.length > 0)) && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: '20px',
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflowY: 'auto'
                }}>
                <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                    ✍️ Wrote ({Array.isArray(itemDetails.wrote) && itemDetails.wrote.length > 0 ? itemDetails.wrote.length : 1})
                  </h3>
                  {Array.isArray(itemDetails.wrote) && itemDetails.wrote.length > 0 ? (
                    itemDetails.wrote.map((row, index) => (
                      <div key={index}
                        style={{ marginBottom: '12px', paddingBottom: '12px', borderBottom: index < itemDetails.wrote.length - 1 ? '1px solid #e5e7eb' : 'none', cursor: 'pointer', padding: '8px', borderRadius: '8px', transition: 'background-color 0.2s' }}
                        onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                        onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                        onClick={() => {
                          const name = row && (row.composer || row.name || row.full_name);
                          if (!name) return;
                          setSearchType('singers');
                          searchForPerson(name);
                        }}
                      >
                        <p style={{ margin: '4px 0', fontWeight: '500' }}>{row && (row.composer || row.name || row.full_name)}</p>
                      </div>
                    ))
                  ) : (
                    <div 
                      style={{ marginBottom: '12px', paddingBottom: '12px', borderBottom: 'none', cursor: 'pointer', padding: '8px', borderRadius: '8px', transition: 'background-color 0.2s' }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={() => { setSearchType('singers'); searchForPerson(itemDetails.opera.composer); }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{itemDetails.opera.composer}</p>
                    </div>
                  )}
                </div>
              )}


              {/* Opera Roles premiered card */}
              {searchType === 'operas' && itemDetails.premieredRoles && itemDetails.premieredRoles.length > 0 && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                  👤 Roles Premiered ({itemDetails.premieredRoles.length})
                  </h3>
                  {itemDetails.premieredRoles.map((performer, index) => (
                    <div 
                      key={index} 
                      style={{ 
                        marginBottom: '12px', 
                        paddingBottom: '12px', 
                        borderBottom: index < itemDetails.premieredRoles.length - 1 ? '1px solid #e5e7eb' : 'none',
                        cursor: 'pointer',
                        padding: '8px',
                        borderRadius: '8px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={() => {
                        // Set search type to singers to maintain consistent styling
                        setSearchType('singers');
                        searchForPerson(performer.singer);
                      }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{performer.singer}</p>
                      {performer.role && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Role premiered:</strong> {performer.role}
                        </p>
                      )}
                      {performer.voice_type && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Voice type:</strong> {performer.voice_type}
                        </p>
                      )}
                      {performer.source && (
                        <p style={{ margin: '4px 0', fontSize: '12px', color: '#888', fontStyle: 'italic' }}>
                          Source: {performer.source || 'Unknown'}
                        </p>
                      )}
                    </div>
                  ))}
                  </div>
                </div>
              )}
              {/* Book detail cards */}
              {searchType === 'books' && itemDetails.book && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                    📚 Book Profile
                  </h3>
                  <p style={{ margin: '8px 0' }}>
                    <strong>Title:</strong> {itemDetails.book.title}
                  </p>
                  {itemDetails.book.type && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Type:</strong> {itemDetails.book.type}
                    </p>
                  )}
                  {itemDetails.book.link && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Link:</strong> 
                      <a 
                        href={itemDetails.book.link} 
                        target="_blank" 
                        rel="noopener noreferrer"
                        style={{ 
                          color: '#059669', 
                          textDecoration: 'underline',
                          marginLeft: '4px'
                        }}
                      >
                        View Book
                      </a>
                    </p>
                  )}
                  </div>
                </div>
              )}

              {/* Book Authors card */}
              {searchType === 'books' && itemDetails.authors && itemDetails.authors.length > 0 && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#6a7304', marginBottom: '15px' }}>
                    ✍️ Authors ({itemDetails.authors.length})
                  </h3>
                  {itemDetails.authors.map((author, index) => (
                    <div 
                      key={index} 
                      style={{ 
                        marginBottom: '12px', 
                        paddingBottom: '12px', 
                        borderBottom: index < itemDetails.authors.length - 1 ? '1px solid #e5e7eb' : 'none',
                        cursor: 'pointer',
                        padding: '8px',
                        borderRadius: '8px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={() => {
                        // Set search type to singers to maintain consistent styling
                        setSearchType('singers');
                        searchForPerson(author.author);
                      }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{author.author}</p>
                      {author.voice_type && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Voice type:</strong> {author.voice_type}
                        </p>
                      )}
                    </div>
                  ))}
                  </div>
                </div>
              )}

              {/* Book Editors card */}
              {searchType === 'books' && itemDetails.editors && itemDetails.editors.length > 0 && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: 0,
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  border: '2px solid #3e96e2',
                  height: '300px',
                  overflow: 'hidden'
                }}>
                  <div style={{ height: '100%', overflowY: 'auto', padding: '20px 16px 20px 20px' }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#a09602', marginBottom: '15px' }}>
                    ✏️ Editors ({itemDetails.editors.length})
                  </h3>
                  {itemDetails.editors.map((editor, index) => (
                    <div 
                      key={index} 
                      style={{ 
                        marginBottom: '12px', 
                        paddingBottom: '12px', 
                        borderBottom: index < itemDetails.editors.length - 1 ? '1px solid #e5e7eb' : 'none',
                        cursor: 'pointer',
                        padding: '8px',
                        borderRadius: '8px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={() => {
                        // Set search type to singers to maintain consistent styling
                        setSearchType('singers');
                        searchForPerson(editor.editor);
                      }}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{editor.editor}</p>
                      {editor.voice_type && (
                        <p style={{ margin: '4px 0', fontSize: '16px', color: '#666' }}>
                          <strong>Voice type:</strong> {editor.voice_type}
                        </p>
                      )}
                      {editor.source && (
                        <p style={{ margin: '4px 0', fontSize: '12px', color: '#888', fontStyle: 'italic' }}>
                          Source: {editor.source || 'Unknown'}
                        </p>
                      )}
                    </div>
                  ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        )}
      </main>
      {currentView === 'network' && isMobileViewport && (
        <>
          <div className="mobile-toolbar" role="toolbar">
            <button
              type="button"
              className="mobile-toolbar__button"
              onPointerDown={handleToolbarPointerDown}
              onPointerUp={(e) => handleToolbarPointerUp(e, () => {
                if (historyCounts.past > 0) {
                  goBack();
                }
              })}
              onClick={handleToolbarClick(() => {
                if (historyCounts.past > 0) {
                  goBack();
                }
              })}
              disabled={historyCounts.past === 0}
              style={{
                opacity: historyCounts.past === 0 ? 0.5 : 1,
                cursor: historyCounts.past === 0 ? 'not-allowed' : 'pointer'
              }}
            >
              Back
            </button>
            <button
              type="button"
              className="mobile-toolbar__button"
              onPointerDown={handleToolbarPointerDown}
              onPointerUp={(e) => handleToolbarPointerUp(e, () => {
                if (historyCounts.future > 0) {
                  goForward();
                }
              })}
              onClick={handleToolbarClick(() => {
                if (historyCounts.future > 0) {
                  goForward();
                }
              })}
              disabled={historyCounts.future === 0}
              style={{
                opacity: historyCounts.future === 0 ? 0.5 : 1,
                cursor: historyCounts.future === 0 ? 'not-allowed' : 'pointer'
              }}
            >
              Forward
            </button>
            <button
              type="button"
              className="mobile-toolbar__button"
              onPointerDown={handleToolbarPointerDown}
              onPointerUp={(e) => handleToolbarPointerUp(e, () => {
                setShowFilterPanel(true);
              })}
              onClick={handleToolbarClick(() => {
                setShowFilterPanel(true);
              })}
            >
              Filters
            </button>
            <button
              type="button"
              className="mobile-toolbar__button"
              onPointerDown={handleToolbarPointerDown}
              onPointerUp={(e) => handleToolbarPointerUp(e, () => {
                setCurrentView('network');
                setShowPathPanel(v => !v);
              })}
              onClick={handleToolbarClick(() => {
                setCurrentView('network');
                setShowPathPanel(v => !v);
              })}
            >
              Path
            </button>
          </div>
        </>
      )}
      {currentView === 'search' && !isMobileViewport && (
        <footer style={{ position: 'fixed', bottom: 0, left: 0, width: '100%', padding: '8px 12px', color: '#e5e7eb', fontSize: 12, textAlign: 'right' }}>
          Photo by <a href="https://unsplash.com/@fortuitousfoto?utm_content=creditCopyText&utm_medium=referral&utm_source=unsplash" target="_blank" rel="noopener noreferrer" style={{ color: '#e5e7eb', textDecoration: 'underline' }}>Richard Hedrick</a> on <a href="https://unsplash.com/photos/a-group-of-tall-trees-with-yellow-leaves-VcrxHU4iSgM?utm_content=creditCopyText&utm_medium=referral&utm_source=unsplash" target="_blank" rel="noopener noreferrer" style={{ color: '#e5e7eb', textDecoration: 'underline' }}>Unsplash</a>
        </footer>
      )}
      {/* Filter Panel */}
      <FilterPanel />

      
    </div>
  );
};

export default ClassicalMusicGenealogy;
