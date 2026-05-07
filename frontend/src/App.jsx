import React, { useState, useEffect, useRef, useLayoutEffect, Suspense, useCallback } from 'react';
import * as d3 from 'd3';
import useViewport from './useViewport';
import useDebounce from './useDebounce';
import initTouchInteractions from './touchInteractions';
import { VOICE_TYPES, TYPE_FILTER_COLORS } from './constants/voiceTypes';
import { RELATIONSHIP_SOURCE_FIELDS, SOURCE_FIELD_KEYS } from './constants/fieldNames';
import {
  SESSION_SNAPSHOT_KEY,
  SESSION_SNAPSHOT_FILTERLESS_KEY,
  DEFAULT_BIRTH_RANGE,
  DEFAULT_DEATH_RANGE,
} from './constants/defaults';
import {
  normalizeSourceValue,
  extractTextValue,
  collapsePlaceWhitespace,
  canonicalizePlaceText,
  deriveRelationshipSourceText,
  deriveOperaName,
  normalizeLinks,
  normalizeDetailsRelationshipSources
} from './utils/normalization';
import {
  URL_DETECT_REGEX,
  TRAILING_PUNCTUATION_REGEX,
  WWW_URL_REGEX,
  DOMAIN_ONLY_REGEX,
  DOMAIN_DETECT_REGEX,
  isDebugRelSourcesEnabled,
  isProbablyHttpUrl,
  sanitizeUrlCandidate,
  extractFirstUrlFromValue,
  deriveRelationshipSourceUrl
} from './utils/urlUtils';
import {
  computeCenteredTransform,
  isLayoutDebug,
  debugLog,
  computeGraphBBox,
  stableAngleFromString,
  computeRingRadius,
  getExpansionRingConfig
} from './utils/graphLayout';
import {
  toTitleCase,
  formatRelationshipTypeLabel,
  isPersonOperaPair,
  normalizeNodeId,
  buildNodeAliasKey,
  collectNodeAliasValues,
  registerNodeAliases,
  resolveAliasIdFromMap,
  OPERA_FORBIDDEN_FIELDS,
  BOOK_FORBIDDEN_FIELDS,
  GRAPH_BASE_KEYS,
  stripOperaBookFields,
  copyGraphBaseProps
} from './utils/nodeUtils';
import { createLinkContextMenuState, buildLinkContextSource } from './utils/linkUtils';
import { parseTypedId, createOperaNodePayload, createBookNodePayload, resolveLinkEndpointId, isPlaceholderName } from './utils/nodeFactory';
import { mergeNodeAttributes, finalizeNodeCandidate, sanitizeGraphData, sanitizeIncrementalGraph, createLinkKey, buildLinkKeySet, normalizeLinkForMerge, mergeNetworkUpdates } from './utils/graphMerge';
import useRateLimit from './hooks/useRateLimit';
import useFilters from './hooks/useFilters';
import useSaveExport from './hooks/useSaveExport';
import useSnapshot from './hooks/useSnapshot';
import useAuth from './hooks/useAuth';
import { resolveApiBase } from './utils/apiBase';
import { renderRelationshipSourceLink } from './utils/renderHelpers';
import ProfileCard from './components/ProfileCard';
import SavedViewDialog from './components/SavedViewDialog';
import Landing from './components/Landing';
import ContextMenu from './components/ContextMenu';
import FilterPanel from './components/FilterPanel';
import PathPanelContent from './components/PathPanelContent';
const NetworkVisualization = React.lazy(() => import('./components/NetworkVisualization'));
import DisclaimerPage from './components/DisclaimerPage';
import SupportPanel from './components/SupportPanel';
import ActiveFilterBar from './components/ActiveFilterBar';
import SearchResults from './components/SearchResults';
import NetworkDetailCards from './components/NetworkDetailCards';
const HelpCenter = React.lazy(() => import('./HelpCenter'));

// Global console helpers (defined at module load) to avoid undefined in console
if (typeof window !== 'undefined') {
  try {
    window.__CMG_DEBUG_EVENTS = window.__CMG_DEBUG_EVENTS || [];
    if (typeof window.__CMG_dumpLongEdges !== 'function') {
      window.__CMG_dumpLongEdges = (threshold = (window.__CMG_LONG_EDGE_THRESHOLD || 260)) => {
        const snap = window.__CMG_lastNetwork || window.__cmg_lastNetwork || {};
        const nodes = Array.isArray(snap.nodes) ? snap.nodes : [];
        const links = Array.isArray(snap.links) ? snap.links : [];
        const idTo = new Map(nodes.map(n => [n.id, n]));
        const rows = [];
        links.forEach(l => {
          const s = (typeof l.source === 'string' ? idTo.get(l.source) : idTo.get(l?.source?.id) || l.source);
          const t = (typeof l.target === 'string' ? idTo.get(l.target) : idTo.get(l?.target?.id) || l.target);
          if (!s || !t) return;
          const len = Math.hypot((Number(t.x)||0)-(Number(s.x)||0), (Number(t.y)||0)-(Number(s.y)||0));
          if (Number.isFinite(len) && len >= threshold) rows.push({ s: s.id, t: t.id, type: l.type, len: Math.round(len) });
        });
        rows.sort((a,b) => b.len - a.len);
        try { console.table(rows.slice(0, 50)); } catch (_) { console.log(rows.slice(0, 50)); }
        return rows;
      };
    }
    if (typeof window.__CMG_findNodeIdByName !== 'function') {
      window.__CMG_findNodeIdByName = (name) => {
        const snap = window.__CMG_lastNetwork || window.__cmg_lastNetwork || {};
        const nodes = Array.isArray(snap.nodes) ? snap.nodes : [];
        const nm = String(name || '').trim().toLowerCase();
        const n = nodes.find(x => String(x?.name||'').toLowerCase() === nm);
        return n ? n.id : null;
      };
    }
  } catch (_) {}
}


const ClassicalMusicGenealogy = () => {
  // Minimal ToS/Disclaimer route to support Auth0 Post-Login Redirect Action
  const isDisclaimerRoute = (typeof window !== 'undefined') && (window.location.pathname.replace(/\/+$/, '') === '/disclaimer');
  if (isDisclaimerRoute) {
    return <DisclaimerPage />;
  }
const viewport = useViewport();
const { width: viewportWidth, height: viewportHeight, isTablet, isPhone } = viewport;
const isMobileViewport = !!isPhone;
const viewportIsPhone = !!isPhone;
const viewportIsTablet = !!isTablet;
const isHeaderMobile = !!isPhone || (viewportWidth > 0 && viewportWidth <= 600);
  const debouncedViewportHeight = useDebounce(viewportHeight, 150);
  // Stabilize mobile viewport height to avoid background jumps when browser chrome shows/hides
  useEffect(() => {
    try {
      const setStableVh = () => {
        const vh = Math.max(320, Math.floor((window.innerHeight || 0))) * 0.01; // floor for stability
        document.documentElement.style.setProperty('--cmg-vh', `${vh}px`);
      };
      setStableVh();
      window.addEventListener('resize', setStableVh, { passive: true });
      window.addEventListener('orientationchange', setStableVh, { passive: true });
      return () => {
        window.removeEventListener('resize', setStableVh);
        window.removeEventListener('orientationchange', setStableVh);
      };
    } catch (_) {}
  }, []);
  const backgroundMinHeight = isMobileViewport ? 'calc(var(--cmg-vh, 1vh) * 100)' : '100vh';
  const backgroundAttachmentMode = isMobileViewport ? 'fixed' : 'fixed';

  const [currentView, setCurrentView] = useState('search');
  const [searchType, setSearchType] = useState('singers');
  const [originalSearchType, setOriginalSearchType] = useState('singers');
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [originalSearchResults, setOriginalSearchResults] = useState([]);
  const [hasExecutedSearch, setHasExecutedSearch] = useState(false);
  const [selectedItem, setSelectedItem] = useState(null);
  const [itemDetails, setItemDetails] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [networkData, setNetworkData] = useState({ nodes: [], links: [] });
  useEffect(() => {
    try {
      if (typeof window !== 'undefined') {
        // Update both legacy and new snapshots for console helpers
        window.__cmg_lastNetwork = networkData;
        try {
          window.__CMG_lastNetwork = {
            nodes: Array.isArray(networkData?.nodes) ? networkData.nodes.map(n => ({ ...n })) : [],
            links: Array.isArray(networkData?.links) ? networkData.links.map(l => ({ ...l })) : []
          };
        } catch (_) {}
        window.dumpLatestNetwork = (options = {}) => {
          const graph = window.__cmg_lastNetwork;
          if (!graph || !Array.isArray(graph?.nodes) || !Array.isArray(graph?.links)) {
            console.warn('[cmg-debug] No network captured yet.');
            return null;
          }
          const includeNodes = options.includeNodes !== false;
          const includeLinks = options.includeLinks !== false;
          console.log(
            `[cmg-debug] Latest network snapshot: ${graph.nodes.length} nodes, ${graph.links.length} links`
          );
          const summary = {
            nodeCount: graph.nodes.length,
            linkCount: graph.links.length
          };
          if (includeNodes) {
            const nodeSample = graph.nodes.slice(0, 50).map(node => ({
              id: node?.id ?? '',
              name: node?.name ?? '',
              type: node?.type ?? '',
              x: Number.isFinite(node?.x) ? Math.round(node.x) : null,
              y: Number.isFinite(node?.y) ? Math.round(node.y) : null
            }));
            console.table(nodeSample);
            summary.nodes = nodeSample;
          }
          if (includeLinks) {
            const linkSample = graph.links.slice(0, 100).map(link => ({
              source: resolveLinkEndpointId(link?.source),
              target: resolveLinkEndpointId(link?.target),
              type: link?.type ?? '',
              label: link?.label ?? link?.relationshipSourceDisplay ?? '',
              role: link?.role ?? ''
            }));
            console.table(linkSample);
            summary.links = linkSample;
          }
          return summary;
        };
        window.dumpNetworkOnError = () => {
          const snapshot = window.__cmg_lastNetworkOnError;
          if (!snapshot) {
            console.warn('[cmg-debug] No error snapshot captured.');
            return null;
          }
          console.log(
            `[cmg-debug] Last error snapshot: ${snapshot.nodes?.length || 0} nodes, ${snapshot.links?.length || 0} links`
          );
          window.__cmg_lastNetwork = snapshot;
          return window.dumpLatestNetwork();
        };
      }
    } catch (_) {}
  }, [networkData]);
  useEffect(() => {
    try {
      const nodes = Array.isArray(networkData?.nodes) ? networkData.nodes : [];
      const placeholders = nodes
        .filter(node => isPlaceholderName(node?.id ?? node?.name))
        .map(node => node?.id ?? node?.name);
      if (placeholders.length > 0) {
        console.warn('[cmg-debug] Placeholder nodes detected after sanitization', placeholders);
      }
      const links = Array.isArray(networkData?.links) ? networkData.links : [];
      const badLinks = [];
      links.forEach(link => {
        const sourceId = resolveLinkEndpointId(link?.source);
        const targetId = resolveLinkEndpointId(link?.target);
        if (isPlaceholderName(sourceId) || isPlaceholderName(targetId)) {
          badLinks.push({ sourceId, targetId, type: link?.type });
        }
      });
      if (badLinks.length > 0) {
        console.warn('[cmg-debug] Links referencing placeholder endpoints detected', badLinks);
      }
    } catch (_) {}
  }, [networkData]);
  const [shouldRunSimulation, setShouldRunSimulation] = useState(false);
  const [contextMenu, setContextMenu] = useState({ show: false, x: 0, y: 0, node: null });
  const [linkContextMenu, setLinkContextMenu] = useState(createLinkContextMenuState);
  const [visualizationHeight, setVisualizationHeight] = useState(550);
  // Visualization height adapts for smaller viewports; defaults to 550px on desktop
  const [selectedNode, setSelectedNode] = useState(null);
  // Align Saved view token + Open with Save/Export
  const saveExportBtnRef = useRef(null);
  const openBtnBelowRef = useRef(null);
  const [savedInputBelowWidth, setSavedInputBelowWidth] = useState(160);
  const [rightGroupWidthPx, setRightGroupWidthPx] = useState(0);
  const [expandSubmenu, setExpandSubmenu] = useState(null);
  const [profileCard, setProfileCard] = useState({ show: false, data: null });
  const relationshipCountsUnavailableRef = useRef(false);
  const pathApiUnavailableRef = useRef(false);
  const [actualCounts, setActualCounts] = useState({});
  const [fetchingCounts, setFetchingCounts] = useState({});
  const [failedFetches, setFailedFetches] = useState({}); // Track nodes that failed to fetch
  const [currentCenterNode, setCurrentCenterNode] = useState(null); // Track current center to prevent re-triggering
  const [isExpansionSimulation, setIsExpansionSimulation] = useState(false); // Track if simulation is for expansion
  // Disable global click outside handlers while any path input is focused
  const [pathInputFocused, setPathInputFocused] = useState(false);
  // Path panel toggle (default off)
  const [showPathPanel, setShowPathPanel] = useState(false);
  const [showMobileToolbarMenu, setShowMobileToolbarMenu] = useState(false);
  const [showHeaderMenu, setShowHeaderMenu] = useState(false);
  const [showSupportPanel, setShowSupportPanel] = useState(false);

  const {
    rateLimitedUntil, rateLimitedUntilRef, scheduleRateLimitCooldown, handleRateLimitResponse,
    isRateLimitMessage, formatRateLimitWaitMessage, sleep, fetchWithRetry,
  } = useRateLimit({ setError });

  const {
    selectedVoiceTypes, setSelectedVoiceTypes,
    selectedBirthplaces, setSelectedBirthplaces,
    birthYearRange, setBirthYearRange,
    deathYearRange, setDeathYearRange,
    birthRangeIsUserSet, setBirthRangeIsUserSet,
    deathRangeIsUserSet, setDeathRangeIsUserSet,
    birthRangeIsUserSetRef, deathRangeIsUserSetRef,
    showFilterPanel, setShowFilterPanel,
    toggleFilterPanel,
    filterSectionsOpen, setFilterSectionsOpen,
    filtersVersion, setFiltersVersion,
    updateSelectedVoiceTypes,
    updateBirthYearRange,
    updateDeathYearRange,
    normalizePersonNode,
    extendDateRangesForNodes,
    toggleVoiceTypeFilter,
    normalizePlaceName,
    toggleBirthplaceFilter,
    computeRangesFromNodes,
    resetFiltersForNodeSet,
    clearFiltersForNewSearch,
    clearAllFilters,
    getDateRanges,
    getVisibleBirthplaces,
    isNodeVisibleWithoutVoiceFilter,
    isNodeVisibleWithoutBirthplaceFilter,
    getVisibleVoiceTypes,
    resetDateRanges,
    isNodeVisible,
    isLinkVisible,
    getNodeOpacity,
    getFilterCounts,
  } = useFilters({ networkData });

  // Login removal (2026-05-06): only handleUnauthorized is still consumed —
  // it covers the "session expired" 401/403 case from older tokens. The hook
  // itself is left running so its module-load side effects (localStorage
  // reads, etc.) stay intact, but its other returns are no longer wired.
  const {
    handleUnauthorized,
  } = useAuth({ setError, setHasExecutedSearch, setShowSupportPanel });
  // Login removal (2026-05-06): the app is public. `token` is a constant
  // sentinel so existing `if (!token)` and `disabled={!token}` guards keep
  // evaluating as authenticated. userEmail is empty since there's no user.
  const token = 'public';
  const userEmail = '';
  // Disclaimer acceptance — single localStorage flag. First visit shows the
  // Landing card; subsequent visits go straight into the app.
  const [hasAcceptedDisclaimer, setHasAcceptedDisclaimer] = useState(() => {
    try { return localStorage.getItem('tosAccepted') === '1'; } catch (_) { return false; }
  });

  const pathFromRef = useRef(null);
  const pathToRef = useRef(null);
  const pathFromValRef = useRef('');
  const pathToValRef = useRef('');
  const pathOverlayRef = useRef({ addedNodeIds: new Set(), addedLinkKeys: new Set() });
  const prePathNetworkRef = useRef(null);
  const pathPanelRef = useRef(null);
  const pathListRef = useRef(null);
  const pathPreviousViewRef = useRef(null);
  const nodeClickTimeoutRef = useRef(null);
  const expansionAbortControllerRef = useRef(null);
const helperMessageTimeoutRef = useRef(null);
const vocalizingTimeoutRef = useRef(null);
const pendingHelperMessageRef = useRef(null);
  const lastTappedNodeIdRef = useRef(null);
  const suppressNextClickRef = useRef(false);
  const [pathInfo, setPathInfo] = useState(null);
  const [helperMessage, setHelperMessage] = useState('');
  // Centered "Vocalizing…" overlay shown while the network is fetched +
  // animated. Cleared by NetworkVisualization's onPhysicsStabilized callback
  // when the FULL graph (nodes > 1) settles, with an 8s safety fallback.
  const [isVocalizing, setIsVocalizing] = useState(false);
  const handleClearPath = () => {
    try { setPathInfo(null); } catch (_) {}
    try { if (pathFromRef.current) pathFromRef.current.value = ''; } catch (_) {}
    try { if (pathToRef.current) pathToRef.current.value = ''; } catch (_) {}
    try { pathFromValRef.current = ''; } catch (_) {}
    try { pathToValRef.current = ''; } catch (_) {}
    if (prePathNetworkRef.current) {
      const snapshot = prePathNetworkRef.current;
      pathOverlayRef.current.addedNodeIds = new Set();
      pathOverlayRef.current.addedLinkKeys = new Set();
      try {
        setNetworkData(
          sanitizeGraphData({
            nodes: snapshot.nodes.map((n) => ({ ...n, isPath: false, wasAddedByPath: false })),
            links: snapshot.links.map((l) => ({ ...l, isPath: false, wasAddedByPath: false }))
          })
        );
      } catch (_) {}
      prePathNetworkRef.current = null;
      try { setShouldRunSimulation(false); } catch (_) {}
    } else {
      setNetworkData((prev) => {
        const addedNodeIds = new Set(pathOverlayRef.current.addedNodeIds || []);
        const addedLinkKeys = new Set(pathOverlayRef.current.addedLinkKeys || []);
        const remainingNodes = prev.nodes
          .filter((n) => !addedNodeIds.has(n.id) && !n.wasAddedByPath)
          .map((n) => ({ ...n, isPath: false, wasAddedByPath: false }));
        const remainingLinks = prev.links
          .filter((l) => {
            const key = `${typeof l.source === 'string' ? l.source : l.source?.id}-${typeof l.target === 'string' ? l.target : l.target?.id}-${l.type}`;
            return !addedLinkKeys.has(key) && !l.wasAddedByPath;
          })
          .map((l) => ({ ...l, isPath: false, wasAddedByPath: false }));
        pathOverlayRef.current.addedNodeIds = new Set();
        pathOverlayRef.current.addedLinkKeys = new Set();
        return sanitizeGraphData({ nodes: remainingNodes, links: remainingLinks });
      });
    }
  };

const svgRef = useRef(null);
const centerOnNodeRef = useRef(null);
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
  const dragSuppressClickRef = useRef(false);
  const dragStartPosRef = useRef({ x: 0, y: 0 });
  const longPressClickSuppressRef = useRef(false);
  // Temporary halo effect for search result cards after a search
  const [showResultsHalo, setShowResultsHalo] = useState(false);
  const resultsHaloTimeoutRef = useRef(null);
  // Cache person details fetched during expansions/path overlays so nodes can be enriched immediately
  const personCacheRef = useRef(new Map());
  const isSearchingRef = useRef(false);
  const supportPanelLoginFlagRef = useRef(false);
  const sessionRestoredRef = useRef(false);
  const sessionPersistReadyRef = useRef(false);
  const filtersResetRef = useRef(false);
  const headerContainerRef = useRef(null);
  const [headerWidth, setHeaderWidth] = useState(null);

const hasSearchResults = Array.isArray(searchResults) && searchResults.length > 0;

const isSaveExportEligible = hasExecutedSearch && Array.isArray(networkData?.nodes) && networkData.nodes.length > 0;
  const API_BASE = resolveApiBase();
  const showHelperMessage = (message, duration = 3200) => {
    if (helperMessageTimeoutRef.current) {
      clearTimeout(helperMessageTimeoutRef.current);
      helperMessageTimeoutRef.current = null;
    }
    setHelperMessage(message);
    if (message && duration > 0) {
      helperMessageTimeoutRef.current = setTimeout(() => {
        setHelperMessage('');
        helperMessageTimeoutRef.current = null;
      }, duration);
    }
  };

  // ── Vocalizing overlay helpers (2026-05-06) ─────────────────────────────────
  // startVocalizing(): show the centered overlay and arm an 8s safety timer
  //   that clears it even if 'stabilized' never fires.
  // stopVocalizing(): clear the overlay and the safety timer. Called from
  //   NetworkVisualization's onPhysicsStabilized prop when the full graph
  //   has settled (nodes > 1 to skip the temporary single-node placeholder).
  const startVocalizing = () => {
    if (vocalizingTimeoutRef.current) {
      clearTimeout(vocalizingTimeoutRef.current);
    }
    // Clear any stale result helperMessage (e.g. "No additional related nodes.")
    // so it doesn't peek through after the new in-progress overlay clears.
    if (helperMessageTimeoutRef.current) {
      clearTimeout(helperMessageTimeoutRef.current);
      helperMessageTimeoutRef.current = null;
    }
    setHelperMessage('');
    setIsVocalizing(true);
    vocalizingTimeoutRef.current = setTimeout(() => {
      vocalizingTimeoutRef.current = null;
      setIsVocalizing(false);
    }, 8000);
  };
  const stopVocalizing = () => {
    if (vocalizingTimeoutRef.current) {
      clearTimeout(vocalizingTimeoutRef.current);
      vocalizingTimeoutRef.current = null;
    }
    setIsVocalizing(false);
  };

  // Rate-limit gate (2026-05-06): if a cooldown is currently active, re-surface
  // the live countdown banner and return true so the caller can fail-fast on
  // user-initiated actions (expand, search, path-find, etc.). Returns false if
  // the user is free to proceed. Calling scheduleRateLimitCooldown with the
  // remaining time refreshes the visible countdown without extending the
  // cooldown window (it max's against the existing untilTs).
  const checkAndEnforceRateLimit = () => {
    const until = rateLimitedUntilRef.current || 0;
    const nowTs = Date.now();
    if (until && nowTs < until) {
      scheduleRateLimitCooldown(until - nowTs);
      return true;
    }
    return false;
  };



  const {
    historyCounts,
    setHistoryCounts,
    createSnapshot,
    applySnapshot,
    pushHistory,
    goBack,
    goForward,
  } = useSnapshot({
    networkData,
    itemDetails,
    searchResults,
    originalSearchResults,
    pathInfo,
    selectedVoiceTypes,
    selectedBirthplaces,
    birthYearRange,
    deathYearRange,
    currentView,
    searchType,
    searchQuery,
    showFilterPanel,
    showPathPanel,
    hasExecutedSearch,
    selectedNode,
    currentCenterNode,
    visualizationHeight,
    selectedItem,
    uiZoomRef,
    svgRef,
    historyRef,
    setNetworkData,
    setShouldRunSimulation,
    setFiltersVersion,
    setCurrentView,
    setSearchType,
    setSelectedNode,
    setCurrentCenterNode,
    setItemDetails,
    setSelectedItem,
    setSearchQuery,
    setSearchResults,
    setOriginalSearchResults,
    setHasExecutedSearch,
    setSelectedVoiceTypes,
    setSelectedBirthplaces,
    setBirthYearRange,
    setDeathYearRange,
    setShowFilterPanel,
    setShowPathPanel,
    setPathInfo,
    resetDateRanges,
  });

  const {
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
  } = useSaveExport({
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
  });


const ensureNetworkView = () => {
  if (currentView !== 'network') {
    setCurrentView('network');
  }
};

const openPathPanel = () => {
  if (pathPreviousViewRef.current == null && currentView !== 'network') {
    pathPreviousViewRef.current = currentView;
  }
  ensureNetworkView();
  setShowSaveExportMenu(false);
  if (currentView === 'network') {
    setShowPathPanel(true);
  } else {
    setTimeout(() => {
      setShowPathPanel(true);
    }, 0);
  }
};

const togglePathPanel = () => {
  if (showPathPanel) {
    closePathPanel();
  } else {
    openPathPanel();
  }
};

const closePathPanel = () => {
  setShowSaveExportMenu(false);
  setShowPathPanel(false);
  pathPreviousViewRef.current = null;
};

const runPathFind = async () => {
  if (checkAndEnforceRateLimit()) return;
  const fromName = (pathFromValRef.current || '').trim();
  const toName = (pathToValRef.current || '').trim();
  if (!fromName || !toName) {
    setError('Please enter both From and To names.');
    return;
  }
  if (fromName.toLowerCase() === toName.toLowerCase()) {
    setError('From and To must be different.');
    return;
  }
  if (!token) {
    setError('Not signed in.');
    return;
  }
  setError('');
  setLoading(true);
  // ── pill→overlay (2026-05-06): in-progress states route through Vocalizing
  startVocalizing();
  try {
    const response = await fetchWithRetry(`${API_BASE}/path/find`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`
      },
      body: JSON.stringify({ from: fromName, to: toName, maxHops: 25 })
    }, { retries: 2, baseDelay: 600 });

    if (response.status === 404) {
      let errMsg = `No path found between "${fromName}" and "${toName}".`;
      try { const data = await response.json(); if (data?.error) errMsg = data.error; } catch (_) {}
      setError(errMsg);
      stopVocalizing();
      return;
    }
    if (!response.ok) {
      if (handleUnauthorized(response)) return;
      let errMsg = `Path search failed (${response.status})`;
      try { const data = await response.json(); if (data?.error) errMsg = data.error; } catch (_) {}
      setError(errMsg);
      stopVocalizing();
      return;
    }
    const data = await response.json();
    if (!Array.isArray(data?.nodes) || data.nodes.length === 0
        || !Array.isArray(data?.links) || data.links.length === 0) {
      setError(`No path found between "${fromName}" and "${toName}".`);
      stopVocalizing();
      return;
    }

    // Stash the current graph so handleClearPath / closePathPanel can restore.
    if (!prePathNetworkRef.current) {
      prePathNetworkRef.current = {
        nodes: networkData.nodes.map((n) => ({ ...n })),
        links: networkData.links.map((l) => ({ ...l }))
      };
    }

    pushHistory('path-find');

    // Normalize path nodes through the same factories regular graphs use so
    // they have identical structure (opera_id/opera_name/version, book_id/
    // title/link, etc.) — keeps right-click expand/profile-card behavior
    // consistent regardless of how the node arrived in the graph.
    const normalizedNodes = data.nodes.map((n) => {
      if (n.type === 'opera') {
        return createOperaNodePayload({
          id: n.id,
          name: n.name,
          opera_id: n.opera_id,
          opera_name: n.opera_name || n.name,
          version: n.version,
          composer: n.composer,
        });
      }
      if (n.type === 'book') {
        return createBookNodePayload({
          id: n.id,
          name: n.name,
          book_id: n.book_id,
          title: n.title || n.name,
          link: n.link,
        });
      }
      // Person — backend now returns voice_type/birth_year/death_year/
      // birthplace/source citations, so colors and filters work on first paint.
      return {
        id: n.id,
        name: n.name,
        type: 'person',
        voiceType: n.voice_type || null,
        birthYear: n.birth_year ?? null,
        deathYear: n.death_year ?? null,
        birthplace: n.birthplace || null,
        spelling_source: n.spelling_source || null,
        voice_type_source: n.voice_type_source || null,
        dates_source: n.dates_source || null,
        birthplace_source: n.birthplace_source || null,
        x: 0,
        y: 0,
      };
    });

    // Initial scatter (anti-overlap), then let physics settle into a chain
    // shape — same approach generateNetworkFromDetails uses, so the path
    // visualization "feels" like any other graph rather than a pinned line.
    positionNodesWithoutOverlap(normalizedNodes);

    clearFiltersForNewSearch(normalizedNodes);
    setItemDetails(null);
    setSelectedItem(null);
    setSelectedNode(null);
    setCurrentCenterNode(null);
    setProfileCard({ show: false, data: null });
    setNetworkData(sanitizeGraphData({ nodes: normalizedNodes, links: data.links }));
    setPathInfo(data);
    setHasExecutedSearch(true);
    setShouldRunSimulation(true);
    if (currentView !== 'network') setCurrentView('network');
    // Path-find success: physics on the new path graph will fire 'stabilized'
    // and clear the overlay. stopVocalizing here is a defensive backstop.
    stopVocalizing();
  } catch (err) {
    setError(err?.message || 'Path search failed');
    stopVocalizing();
  } finally {
    setLoading(false);
  }
};

useEffect(() => {
  if (showPathPanel && pathFromRef.current && !viewportIsPhone) {
    try {
      pathFromRef.current.focus();
    } catch (_) {}
  }
}, [showPathPanel, viewportIsPhone]);

useEffect(() => {
  }, [showPathPanel, currentView]);

  useEffect(() => {
    // Expose simple debug helpers in the browser console
    try {
      if (typeof window !== 'undefined') {
        // Keep a lightweight snapshot of the latest network so console helpers
        // can read state even outside React closures.
        try {
          window.__CMG_lastNetwork = {
            nodes: Array.isArray(networkData?.nodes) ? networkData.nodes.map(n => ({ ...n })) : [],
            links: Array.isArray(networkData?.links) ? networkData.links.map(l => ({ ...l })) : []
          };
        } catch (_) {}
        window.__CMG_setLayoutDebug = (on) => (window.__CMG_DEBUG_LAYOUT = !!on);
        window.__CMG_setLongEdgeThreshold = (px) => (window.__CMG_LONG_EDGE_THRESHOLD = Number(px) || 260);
        // Define helpers if missing; operate on the latest snapshot
        if (typeof window.__CMG_dumpLongEdges !== 'function') {
          window.__CMG_dumpLongEdges = (threshold = (window.__CMG_LONG_EDGE_THRESHOLD || 260)) => {
            const snap = window.__CMG_lastNetwork || {};
            const nodes = Array.isArray(snap.nodes) ? snap.nodes : [];
            const links = Array.isArray(snap.links) ? snap.links : [];
            const idTo = new Map(nodes.map(n => [n.id, n]));
            const rows = [];
            links.forEach(l => {
              const s = (typeof l.source === 'string' ? idTo.get(l.source) : idTo.get(l?.source?.id) || l.source);
              const t = (typeof l.target === 'string' ? idTo.get(l.target) : idTo.get(l?.target?.id) || l.target);
              if (!s || !t) return;
              const len = Math.hypot((Number(t.x)||0)-(Number(s.x)||0), (Number(t.y)||0)-(Number(s.y)||0));
              if (Number.isFinite(len) && len >= threshold) rows.push({ s: s.id, t: t.id, type: l.type, len: Math.round(len) });
            });
            rows.sort((a,b) => b.len - a.len);
            try { console.table(rows.slice(0, 50)); } catch (_) { console.log(rows.slice(0, 50)); }
            return rows;
          };
        }
        if (typeof window.__CMG_findNodeIdByName !== 'function') {
          window.__CMG_findNodeIdByName = (name) => {
            const snap = window.__CMG_lastNetwork || {};
            const nodes = Array.isArray(snap.nodes) ? snap.nodes : [];
            const nm = String(name || '').trim().toLowerCase();
            const n = nodes.find(x => String(x?.name||'').toLowerCase() === nm);
            return n ? n.id : null;
          };
        }
        window.__CMG_debugTrackNode = (id) => {
          window.__CMG_DEBUG_TRACK_NODE = String(id || '').trim();
          return window.__CMG_DEBUG_TRACK_NODE;
        };
      }
    } catch (_) {}
    return () => {
      if (nodeClickTimeoutRef.current) {
        clearTimeout(nodeClickTimeoutRef.current);
        nodeClickTimeoutRef.current = null;
      }
      if (helperMessageTimeoutRef.current) {
        clearTimeout(helperMessageTimeoutRef.current);
        helperMessageTimeoutRef.current = null;
      }
      if (vocalizingTimeoutRef.current) {
        clearTimeout(vocalizingTimeoutRef.current);
        vocalizingTimeoutRef.current = null;
      }
      lastTappedNodeIdRef.current = null;
      suppressNextClickRef.current = false;
    };
  }, []);

  const renderSaveExportFields = ({ containerStyle = {}, isMobileLayout = false } = {}) => {
  const disabledSave = !isSaveExportEligible || !token || isSavingView;
  const disabledExport = !isSaveExportEligible || isExporting;
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
          {isExporting ? 'Exporting…' : 'Export text file'}
        </button>
      </div>
    );
};

const renderSaveExportToggle = ({
  containerStyle = {},
  buttonStyle = {},
    menuStyle = {},
    align = 'right',
    isMobileLayout = false
  } = {}) => {
    const justify = align === 'left' ? { left: 0, right: 'auto' } : { right: 0, left: 'auto' };
    return (
      <div style={{ position: 'relative', ...containerStyle }}>
        <button
          ref={saveExportBtnRef}
          onMouseDown={(e) => {
            if (isSaveExportEligible) {
              e.stopPropagation();
              try { window.__cmg_reapplyZoom && window.__cmg_reapplyZoom(); } catch (_) {}
            }
          }}
          onClick={(e) => {
            if (!isSaveExportEligible) return;
            e.stopPropagation();
            setShowSaveExportMenu((v) => !v);
          }}
          disabled={!isSaveExportEligible}
          style={{
            padding: '8px 16px',
            backgroundColor: showSaveExportMenu ? '#f3f4f6' : '#ffffff',
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
            opacity: isSaveExportEligible ? 1 : 0.6,
            ...buttonStyle
          }}
        >
          Save/Export ▾
        </button>
        {showSaveExportMenu && (
          <div
            style={{
              position: 'absolute',
              top: '110%',
              border: '2px solid #3e96e2',
              borderRadius: 8,
              boxShadow: '0 8px 20px rgba(0,0,0,0.12)',
              padding: 12,
              minWidth: isMobileLayout ? 'min(280px, 80vw)' : 260,
              backgroundColor: 'white',
              zIndex: 1000,
              ...justify,
              ...menuStyle
            }}
            onMouseLeave={() => setShowSaveExportMenu(false)}
            onMouseDown={(e) => { e.stopPropagation(); try { window.__cmg_reapplyZoom && window.__cmg_reapplyZoom(); } catch (_) {} }}
            onClick={(e) => e.stopPropagation()}
          >
            {renderSaveExportFields({ isMobileLayout })}
          </div>
        )}
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

  // Prevent body scroll from stealing touch scroll when filter sheet is open on mobile
  useEffect(() => {
    try {
      if (typeof document !== 'undefined' && isMobileViewport) {
        const prev = document.body.style.overflow;
        if (showFilterPanel) {
          document.body.style.overflow = 'hidden';
        } else {
          document.body.style.overflow = prev || '';
        }
        return () => { document.body.style.overflow = prev || ''; };
      }
    } catch (_) {}
  }, [isMobileViewport, showFilterPanel]);

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
      if (handleRateLimitResponse(resp)) return null;
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

  // Enhanced filter setter functions
  // Auto-show-SupportPanel-after-login effect removed (2026-05-06)
  // along with the login flow.

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
    // Clean-start on hard refresh (2026-05-06): bypass session-snapshot
    // restoration and reset filters so every fresh mount lands on a clean
    // search start page rather than the previously-loaded graph. The persist
    // effect below still writes snapshots; only the read path is disabled.
    // To restore prior behavior: re-enable the localStorage read + applySnapshot
    // block that previously lived here (see git history pre-2026-05-06).
    try { localStorage.removeItem(SESSION_SNAPSHOT_KEY); } catch (_) {}
    try { clearAllFilters(); } catch (_) {}
    filtersResetRef.current = true;
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



  // Close transient overlays when clicking anywhere on the page background.
  // Each overlay (context menu, edge tooltip, profile card, path panel) stops
  // propagation on its own root so clicks inside them DON'T bubble up to here.
  // The buttons that OPEN these overlays must also stopPropagation, otherwise
  // the same click that opens them would bubble up and immediately close them.
  useEffect(() => {
    const handleClick = (event) => {
      // Don't interfere with form inputs / auth, or while a path input has focus.
      if (event.target.tagName === 'INPUT' ||
          event.target.tagName === 'TEXTAREA' ||
          event.target.closest('form') ||
          !token) {
        return;
      }

      try { window.__cmg_reapplyZoom && window.__cmg_reapplyZoom(); } catch (_) {}
      setContextMenu({ show: false, x: 0, y: 0, node: null });
      setLinkContextMenu(createLinkContextMenuState());
      setExpandSubmenu(null);
      setProfileCard({ show: false, data: null });
      if (showPathPanel) closePathPanel();
      if (submenuTimeoutRef.current) {
        clearTimeout(submenuTimeoutRef.current);
      }
    };

    if (token && !pathInputFocused) {
      document.addEventListener('click', handleClick);
    }

    return () => document.removeEventListener('click', handleClick);
  }, [token, pathInputFocused, showPathPanel]);

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
    if (!token || loading || isSearchingRef.current || currentView !== 'network') return;
    const cooldown = rateLimitedUntilRef.current || 0;
    if (cooldown && Date.now() < cooldown) return;
    const currentIds = new Set(networkData.nodes.map(n => n.id));
    const isExpansion = Boolean(isExpansionSimulation || shouldRunSimulation);
    const newlyAddedIds = [];
    currentIds.forEach((id) => {
      if (!prevNodeIdsRef.current.has(id)) newlyAddedIds.push(id);
    });
    prevNodeIdsRef.current = currentIds;

    if (newlyAddedIds.length === 0 || isExpansion) return;

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

  // Close context menu when clicking outside it (never recenter or modify zoom).
  // ONLY listen to 'click' — not 'contextmenu'. Right-clicks on the canvas are
  // handled by vis-network's oncontext (open / replace the menu); listening to
  // contextmenu here would close the menu in the same event tick that just
  // opened it, since the bubble-phase document listener fires after vis-network
  // sets state but before React commits.
  useEffect(() => {
    const handleClickOutside = (event) => {
      const isFormTarget = event.target.tagName === 'INPUT' || event.target.tagName === 'TEXTAREA' || event.target.closest('form');
      if (isFormTarget || !token) return;
      try { window.__cmg_reapplyZoom && window.__cmg_reapplyZoom(); } catch (_) {}
      if (contextMenu.show && !event.target.closest('.context-menu')) {
        setContextMenu({ show: false, x: 0, y: 0, node: null });
      }
      if (linkContextMenu.show && !event.target.closest('.context-menu')) {
        setLinkContextMenu(createLinkContextMenuState());
      }
    };

    const shouldBind = (contextMenu.show || linkContextMenu.show) && token && !pathInputFocused;
    if (shouldBind) {
      document.addEventListener('click', handleClickOutside);
    }

    return () => {
      document.removeEventListener('click', handleClickOutside);
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

  // Local registration is removed; all sign-up flows go through Auth0.

  const performSearch = async () => {
    if (!searchQuery.trim() || searchQuery.length < 2) {
      setError('Please enter at least 2 characters');
      return;
    }

    try {
      try { window.__cmg_resetZoom && window.__cmg_resetZoom(); } catch (_) {}
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
      // If we're currently under a global rate-limit cooldown, fail fast.
      if (checkAndEnforceRateLimit()) {
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
        // Push history BEFORE replacing state so Back can recover whatever
        // graph/results were on screen before this search. Skip if there's
        // nothing meaningful to restore (e.g. very first search after login).
        const hadPriorContent = (Array.isArray(networkData?.nodes) && networkData.nodes.length > 0)
          || (Array.isArray(searchResults) && searchResults.length > 0);
        if (hadPriorContent) pushHistory('search');

        setSearchResults(data[searchType] || []);
        setOriginalSearchResults(data[searchType] || []);
        setOriginalSearchType(searchType);
        setCurrentView('results');
        setHasExecutedSearch(true);
        // Clear stale drilldown state from a previous search so click-vs-expand
        // detection doesn't see leftover itemDetails / cards.
        setItemDetails(null);
        setSelectedItem(null);
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
  const resolveFallbackConfig = (relationshipType = '', anchorType = '') => {
    const normalizedLabel = String(relationshipType || '').trim().toLowerCase();
    const base = {
      source: null,
      type: null,
      label: ''
    };

    if (!normalizedLabel) return base;
    if (normalizedLabel.includes('taughtby')) {
      return { source: 'node', type: 'taught', label: 'taught' };
    }
    if (normalizedLabel.includes('taught')) {
      return { source: 'anchor', type: 'taught', label: 'taught' };
    }
    if (normalizedLabel.includes('parentof')) {
      return { source: 'anchor', type: 'family', label: 'parent' };
    }
    if (normalizedLabel.includes('parent')) {
      return { source: 'node', type: 'family', label: 'parent' };
    }
    if (normalizedLabel.includes('spouseof')) {
      return { source: 'node', type: 'family', label: 'spouse' };
    }
    if (normalizedLabel.includes('spouse')) {
      return { source: 'anchor', type: 'family', label: 'spouse' };
    }
    if (normalizedLabel.includes('grandparentof')) {
      return { source: 'anchor', type: 'family', label: 'grandparent' };
    }
    if (normalizedLabel.includes('grandparent')) {
      return { source: 'node', type: 'family', label: 'grandparent' };
    }
    if (normalizedLabel.includes('sibling')) {
      return { source: 'anchor', type: 'family', label: 'sibling' };
    }
    if (normalizedLabel.includes('premieredrolein') || normalizedLabel.includes('premiered')) {
      return anchorType === 'opera'
        ? { source: 'node', type: 'premiered', label: 'premiered role in' }
        : { source: 'anchor', type: 'premiered', label: 'premiered role in' };
    }
    if (normalizedLabel.includes('wrote') || normalizedLabel.includes('composed')) {
      return anchorType === 'opera'
        ? { source: 'node', type: 'wrote', label: 'wrote' }
        : { source: 'anchor', type: 'wrote', label: 'wrote' };
    }
    if (normalizedLabel.includes('authored')) {
      return anchorType === 'book'
        ? { source: 'node', type: 'authored', label: 'authored' }
        : { source: 'anchor', type: 'authored', label: 'authored' };
    }
    if (normalizedLabel.includes('edited')) {
      return anchorType === 'book'
        ? { source: 'node', type: 'edited', label: 'edited' }
        : { source: 'anchor', type: 'edited', label: 'edited' };
    }

    return base;
  };
  const attachNewNodesToAnchor = ({
    anchorId,
    anchorType,
    relationshipType,
    newNodes,
    newLinks,
    existingLinks,
    addLink
  }) => {
    if (!anchorId || !Array.isArray(newNodes) || newNodes.length === 0) return;

    const normalize = (value) => normalizeNodeId(value);
    const attachedToAnchor = new Set();
    const registerAttachment = (sourceId, targetId) => {
      const src = normalize(sourceId);
      const tgt = normalize(targetId);
      if (src === anchorId && tgt) attachedToAnchor.add(tgt);
      if (tgt === anchorId && src) attachedToAnchor.add(src);
    };

    (existingLinks || []).forEach(link => {
      const sourceId = typeof link.source === 'string' ? link.source : link.source?.id;
      const targetId = typeof link.target === 'string' ? link.target : link.target?.id;
      registerAttachment(sourceId, targetId);
    });
    (newLinks || []).forEach(link => {
      registerAttachment(link.source, link.target);
    });

    const fallbackConfig = resolveFallbackConfig(relationshipType, anchorType);
    const fallbackTypeKey = typeof fallbackConfig.type === 'string' ? fallbackConfig.type.toLowerCase() : '';
    if (!fallbackTypeKey || fallbackTypeKey === 'related') {
      return;
    }

    newNodes.forEach(node => {
      const nodeId = normalize(node?.id ?? node?.name);
      if (!nodeId || attachedToAnchor.has(nodeId)) return;
      if (isPlaceholderName(nodeId)) return;

      const sourceId = fallbackConfig.source === 'node' ? nodeId : anchorId;
      const targetId = fallbackConfig.source === 'node' ? anchorId : nodeId;
      if (!sourceId || !targetId) return;

      const linkKey = `${sourceId}|${targetId}|${fallbackTypeKey}`;
      // addLink handles deduplication via existingLinks set, so just call it
      addLink(sourceId, targetId, fallbackConfig.type, { label: fallbackConfig.label });
      attachedToAnchor.add(nodeId);
    });
  };
  const ensureNodeConnectivity = (
    nodes,
    links,
    {
      primaryId = null,
      fallbackType = null,
      fallbackLabel = ''
    } = {}
  ) => {
    const allowedFallbackTypes = new Set([
      'taught',
      'family',
      'spouse',
      'sibling',
      'parent',
      'grandparent',
      'wrote',
      'composed',
      'premiered',
      'premiered_role_in',
      'authored',
      'edited'
    ]);
    const fallbackTypeKey = typeof fallbackType === 'string' ? fallbackType.toLowerCase().trim() : '';
    if (!fallbackTypeKey || fallbackTypeKey === 'related' || !allowedFallbackTypes.has(fallbackTypeKey)) {
      return;
    }
    const normalize = (value) => {
      if (value === null || value === undefined) return '';
      return String(value).replace(/\s+/g, ' ').trim();
    };
    const idToNode = new Map();
    (nodes || []).forEach((node) => {
      const id = normalize(node?.id ?? node?.name);
      if (!id || isPlaceholderName(id)) return;
      if (!idToNode.has(id)) {
        idToNode.set(id, node);
      }
    });
    const nodeIds = nodes
      .map((node) => normalize(node?.id ?? node?.name))
      .filter(id => id && !isPlaceholderName(id));
    if (nodeIds.length === 0) return;
    const nodeIdSet = new Set(nodeIds);
    const adjacency = new Map();
    nodeIds.forEach((id) => adjacency.set(id, 0));
    const linkKeys = new Set();
    links.forEach((link) => {
      const sourceId = normalize(
        typeof link.source === 'string'
          ? link.source
          : link.source?.id ?? link.source?.name
      );
      const targetId = normalize(
        typeof link.target === 'string'
          ? link.target
          : link.target?.id ?? link.target?.name
      );
      if (!sourceId || !targetId) return;
      const key = `${sourceId}|${targetId}|${String(link.type || '').toLowerCase()}`;
      linkKeys.add(key);
      if (adjacency.has(sourceId)) {
        adjacency.set(sourceId, (adjacency.get(sourceId) || 0) + 1);
      }
      if (adjacency.has(targetId)) {
        adjacency.set(targetId, (adjacency.get(targetId) || 0) + 1);
      }
    });
    const normalizedPrimaryId = normalize(primaryId);
    const pickFallback = (nodeId) => {
      if (
        normalizedPrimaryId &&
        normalizedPrimaryId !== nodeId &&
        nodeIdSet.has(normalizedPrimaryId)
      ) {
        return normalizedPrimaryId;
      }
      for (const candidate of nodeIds) {
        if (candidate && candidate !== nodeId) {
          return candidate;
        }
      }
      return null;
    };
    nodeIds.forEach((nodeId) => {
      if (!nodeId) return;
      if ((adjacency.get(nodeId) || 0) > 0) return;
      let fallbackId = pickFallback(nodeId);
      const isSelfLink = !fallbackId || fallbackId === nodeId;
      if (!fallbackId) {
        fallbackId = nodeId;
      }
      if (!nodeIdSet.has(nodeId) || (fallbackId && !nodeIdSet.has(fallbackId))) {
        console.warn('[ensureNodeConnectivity] skipped fallback link; missing node', { nodeId, fallbackId });
        return;
      }
      const key = `${fallbackId}|${nodeId}|${fallbackTypeKey}`;
      if (linkKeys.has(key)) return;
      const sourceNode = idToNode.get(fallbackId);
      const targetNode = idToNode.get(nodeId);
      links.push({
        source: fallbackId,
        target: nodeId,
        type: fallbackType,
        label: fallbackLabel,
        relationshipSourceDisplay: fallbackLabel,
        sourceInfo: fallbackLabel,
        relationship_source: fallbackLabel,
        // Treat fallback attachments (created during expansions) as expansion-internal
        // so the main simulation keeps the cluster compact around the anchor.
        expansionInternal: true
      });
      linkKeys.add(key);
      if (!isSelfLink) {
        adjacency.set(fallbackId, (adjacency.get(fallbackId) || 0) + 1);
      }
      adjacency.set(nodeId, (adjacency.get(nodeId) || 0) + 1);
    });
  };
  const generateNetworkFromSearchResults = (results, type) => {
    const nodes = [];
    const links = [];
    
  results.forEach((item, index) => {
    if (type === 'singers') {
      const name = item.name || item.properties.full_name || `Unknown Singer ${index + 1}`;
      const typedIdRaw = item.id || (item.person_id ? `person:${item.person_id}` : '');
      const personId = normalizeNodeId(typedIdRaw || name);
      nodes.push({
        id: personId,
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
        const operaName = deriveOperaName(item.properties, `Unknown Opera ${index + 1}`);
        const typedIdRaw = item.id || (item.opera_id ? `opera:${item.opera_id}` : '');
        nodes.push(createOperaNodePayload({
          id: typedIdRaw || operaName,
          name: operaName,
          opera_id: item.opera_id || item.properties?.opera_id,
          opera_name: operaName,
          version: item.properties?.version,
          x: 0, // Will be positioned by anti-overlap system
          y: 0
        }));
      } else if (type === 'books') {
        const bookTitle = item.properties.title || `Unknown Book ${index + 1}`;
        const typedIdRaw = item.id || (item.book_id ? `book:${item.book_id}` : '');
        nodes.push(createBookNodePayload({
          id: typedIdRaw || bookTitle,
          name: bookTitle,
          book_id: item.book_id || item.properties?.book_id,
          title: bookTitle,
          link: item.properties?.link,
          x: 0, // Will be positioned by anti-overlap system
          y: 0
        }));
      }
  });

  // Apply anti-overlap positioning to all nodes
  positionNodesWithoutOverlap(nodes);

  setNetworkData(sanitizeGraphData({ nodes, links }));
    resetFiltersForNodeSet(nodes);
    setShowFilterPanel(false);
    setCurrentCenterNode(null); // Reset center tracking for search results
    setShouldRunSimulation(true); // Trigger simulation for search results
  };
  const generateNetworkFromDetails = (details, centerName, type, options = {}) => {
    const nodes = [];
    const links = [];
    const addedNodes = new Set(); // Track which people have been added
    
    // Helper function to add a person node only if not already added
  const addPersonNode = (person, defaultX, defaultY) => {
    const candidateName = person?.full_name || person?.name;
    const typedIdRaw = person?.id || (person?.person_id ? `person:${person.person_id}` : '');
    const normalizedId = normalizeNodeId(typedIdRaw || candidateName);
    if (!normalizedId || isPlaceholderName(normalizedId)) return null;
    const displayName = String(candidateName || normalizedId).trim() || normalizedId;
    if (!addedNodes.has(normalizedId)) {
      nodes.push({
        id: normalizedId,
        name: displayName,
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
      addedNodes.add(normalizedId);
    }
    return normalizedId;
  };
    
  const centerData = details?.center || {};
  const centerIdCandidates = [
    centerData?.id,
    centerData?.person_id ? `person:${centerData.person_id}` : '',
    type === 'operas' ? details?.opera?.id : '',
    type === 'operas' && details?.opera?.opera_id ? `opera:${details.opera.opera_id}` : '',
    type === 'books' ? details?.book?.id : '',
    type === 'books' && details?.book?.book_id ? `book:${details.book.book_id}` : ''
  ];
  const centerTypedIdRaw = centerIdCandidates.find((candidate) => typeof candidate === 'string' && candidate.trim()) || '';
  const fallbackCenterName = centerData?.full_name || centerData?.name ||
    (type === 'operas' ? details?.opera?.opera_name : type === 'books' ? details?.book?.title : '') ||
    centerName || '';
  const rawCenterName = String(fallbackCenterName || '').trim();
  const centerId = normalizeNodeId(centerTypedIdRaw || rawCenterName);
  if (!centerId || isPlaceholderName(centerId)) {
    console.warn('[generateNetworkFromDetails] Skipping network with placeholder center', { centerName });
    return;
  }
  const centerLabel = rawCenterName || centerId;

  let centerNode;
  if (type === 'singers') {
    centerNode = {
      id: centerId,
      name: centerLabel,
      type: 'person',
      isCenter: true,
      x: 400,
      y: 300
    };
    if (details.center) {
      centerNode.voiceType = details.center.voice_type;
      centerNode.birthYear = (details.center.birth_year ?? (details.center.birth && (details.center.birth.low ?? details.center.birth))) || null;
      centerNode.deathYear = (details.center.death_year ?? (details.center.death && (details.center.death.low ?? details.center.death))) || null;
      centerNode.birthplace = details.center.birthplace || details.center.citizen || null;
    }
  } else if (type === 'operas') {
    centerNode = createOperaNodePayload({
      id: centerTypedIdRaw || centerId,
      name: centerLabel,
      isCenter: true,
      x: 400,
      y: 300,
      opera_id: details?.opera?.opera_id || centerData?.opera_id,
      opera_name: details?.opera?.opera_name || centerLabel,
      version: details?.opera?.version
    });
  } else if (type === 'books') {
    centerNode = createBookNodePayload({
      id: centerTypedIdRaw || centerId,
      name: centerLabel,
      isCenter: true,
      x: 400,
      y: 300,
      book_id: details?.book?.book_id || centerData?.book_id,
      title: details?.book?.title || centerLabel,
      link: details?.book?.link || centerData?.link
    });
  } else {
    centerNode = {
      id: centerId,
      name: centerLabel,
      type: 'person',
      isCenter: true,
      x: 400,
      y: 300
    };
  }

  nodes.push(centerNode);
  addedNodes.add(centerId);

  // Add composer(s) for opera center via wrote list; fallback to single property if present
    if (type === 'operas') {
      const wroteList = Array.isArray(details.wrote) ? details.wrote : [];
    if (wroteList.length > 0) {
      wroteList.forEach((row, idx) => {
        const composerRaw = row && (row.composer || row.name || row.full_name);
        const composerId = normalizeNodeId(composerRaw);
        if (!composerId || isPlaceholderName(composerId)) return;
        const composerName = String(composerRaw || composerId).trim() || composerId;
        if (!addedNodes.has(composerId)) {
          nodes.push({ id: composerId, name: composerName, type: 'person', x: 250 + (idx * 40), y: 180 });
          addedNodes.add(composerId);
        }
        const relationshipSource = deriveRelationshipSourceText(row?.source, row?.relationship_source);
        links.push({
          source: composerId,
          target: centerId,
          type: 'wrote',
          label: 'wrote',
          relationshipSourceDisplay: relationshipSource,
          sourceInfo: relationshipSource,
          relationship_source: row?.relationship_source || null
        });
      });
    } else if (type === 'singers' && Array.isArray(details.works?.composedOperas) && details.works.composedOperas.length > 0) {
      details.works.composedOperas.forEach((operaEntry, idx) => {
        const fallbackLabel = `Unknown Opera ${idx + 1}`;
        const name = deriveOperaName(operaEntry, fallbackLabel);
        const typedIdRaw = operaEntry?.id || (operaEntry?.opera_id ? `opera:${operaEntry.opera_id}` : '');
        const operaId = normalizeNodeId(typedIdRaw || name);
        if (!operaId || isPlaceholderName(operaId)) return;
        const displayName = name || operaId;
        if (!addedNodes.has(operaId)) {
          nodes.push(createOperaNodePayload({
            id: typedIdRaw || displayName,
            name: displayName,
            opera_id: operaEntry?.opera_id,
            opera_name: displayName,
            version: operaEntry?.version,
            x: 150 + idx * 80,
            y: 420
          }));
          addedNodes.add(operaId);
        }
        const relationshipSource = deriveRelationshipSourceText(operaEntry?.source, operaEntry?.opera_source_text, operaEntry?.relationship_source);
        links.push({
          source: centerId,
          target: operaId,
          type: 'wrote',
          label: 'wrote',
          relationshipSourceDisplay: relationshipSource,
          sourceInfo: relationshipSource,
          relationship_source: operaEntry?.relationship_source || null
        });
      });
    } else if (details.opera && details.opera.composer) {
      const composerRaw = details.opera.composer;
      const composerId = normalizeNodeId(composerRaw);
      if (composerId && !isPlaceholderName(composerId)) {
        const composerName = String(composerRaw || composerId).trim() || composerId;
        if (!addedNodes.has(composerId)) {
          nodes.push({ id: composerId, name: composerName, type: 'person', x: 250, y: 180 });
          addedNodes.add(composerId);
        }
        const relationshipSource = deriveRelationshipSourceText(details.opera?.source, details.opera?.relationship_source);
        links.push({
          source: composerId,
          target: centerId,
          type: 'wrote',
          label: 'wrote',
          relationshipSourceDisplay: relationshipSource,
          sourceInfo: relationshipSource,
          relationship_source: details.opera?.relationship_source || null
        });
      }
    }
  } else if (options?.context === 'composer') {
    if (details.works?.composedOperas && details.works.composedOperas.length > 0) {
      details.works.composedOperas.forEach((opera, index) => {
        const fallbackLabel = `Unknown Opera ${index + 1}`;
        const operaName = deriveOperaName(opera, fallbackLabel);
        const typedIdRaw = opera?.id || (opera?.opera_id ? `opera:${opera.opera_id}` : '');
        const operaId = normalizeNodeId(typedIdRaw || operaName);
        if (!operaId || isPlaceholderName(operaId)) return;
        const displayName = operaName || operaId;
        if (!addedNodes.has(operaId)) {
          nodes.push(createOperaNodePayload({
            id: typedIdRaw || displayName,
            name: displayName,
            opera_id: opera?.opera_id,
            opera_name: displayName,
            version: opera?.version,
            x: 120 + (index * 100),
            y: 420
          }));
          addedNodes.add(operaId);
        }
        const relationshipSource = deriveRelationshipSourceText(opera.opera_source_text, opera.source);
        links.push({
          source: centerId,
          target: operaId,
          type: 'wrote',
          label: 'wrote',
          relationshipSourceDisplay: relationshipSource,
          sourceInfo: relationshipSource,
          relationship_source: opera.relationship_source || null
        });
      });
    }
  }

  // Add teachers
  if (details.teachers) {
    details.teachers.forEach((teacher, index) => {
      const teacherId = addPersonNode(teacher, 200 + (index * 50), 150);
      if (!teacherId) return;
      const relationshipSource = deriveRelationshipSourceText(teacher.teacher_rel_source_text, teacher.relationshipSourceDisplay, teacher.teacher_rel_source, teacher.relationship_source, teacher.source);
      links.push({
        source: teacherId,
        target: centerId,
        type: 'taught',
        label: 'taught',
        relationshipSourceDisplay: relationshipSource,
        sourceInfo: relationshipSource,
        teacher_rel_source: teacher.teacher_rel_source || null,
        teacher_rel_source_text: teacher.teacher_rel_source_text || relationshipSource || null,
        teacher_rel_source_url: teacher.teacher_rel_source_url || null,
        relationship_source: teacher.relationship_source || null
      });
    });
  }

  // Add students
  if (details.students) {
    details.students.forEach((student, index) => {
      const studentId = addPersonNode(student, 200 + (index * 50), 450);
      if (!studentId) return;
      const relationshipSource = deriveRelationshipSourceText(student.teacher_rel_source_text, student.relationshipSourceDisplay, student.teacher_rel_source, student.relationship_source, student.source);
      links.push({
        source: centerId,
        target: studentId,
        type: 'taught',
        label: 'taught',
        relationshipSourceDisplay: relationshipSource,
        sourceInfo: relationshipSource,
        teacher_rel_source: student.teacher_rel_source || null,
        teacher_rel_source_text: student.teacher_rel_source_text || relationshipSource || null,
        teacher_rel_source_url: student.teacher_rel_source_url || null,
        relationship_source: student.relationship_source || null
      });
    });
  }

    // Add family (fallback: some responses may nest under center)
    const familyList = (details.family || details.center?.family || []);
    if (familyList && familyList.length > 0) {
      familyList.forEach((relative, index) => {
        const relName = relative.full_name || relative.name;
        const relativeNorm = { ...relative, full_name: relName };
        const relativeId = addPersonNode(relativeNorm, 600 + (index * 50), 200 + (index * 50));
        if (!relativeId) return;
        const relDisplay = String(relName || relativeId).trim() || relativeId;
        
        // Determine correct direction based on relationship_type from backend
        const relType = (relative.relationship_type || '').toLowerCase();
        let src = centerId;
        let tgt = relativeId;
        if ((relType.includes('parent') && !relType.includes('of')) ||
            (relType.includes('grandparent') && !relType.includes('of'))) {
          // relative is ancestor of center
          src = relativeId;
          tgt = centerId;
        } else if (relType.includes('parentof') || relType.includes('grandparentof')) {
          // center is ancestor of relative (already src=center, tgt=relative)
        } // spouse/sibling/default keep center -> relative for determinism
        
        const relationshipSource = deriveRelationshipSourceText(relative.teacher_rel_source_text, relative.relationshipSourceDisplay, relative.teacher_rel_source, relative.relationship_source, relative.source);
        links.push({
          source: src,
          target: tgt,
          type: 'family',
          label: relative.relationship_type || relDisplay,
          relationshipSourceDisplay: relationshipSource,
          sourceInfo: relationshipSource,
          teacher_rel_source: relative.teacher_rel_source || null,
          teacher_rel_source_text: relative.teacher_rel_source_text || relationshipSource || null,
          teacher_rel_source_url: relative.teacher_rel_source_url || null,
          relationship_source: relative.relationship_source || null
        });
      });
    }

  // Add works
    if (details.works) {
      // Add operas
      if (details.works.operas) {
        details.works.operas.forEach((opera, index) => {
          const fallbackLabel = `Unknown Opera ${index + 1}`;
          const operaName = deriveOperaName(opera, fallbackLabel);
          const typedIdRaw = opera?.id || (opera?.opera_id ? `opera:${opera.opera_id}` : '');
          const operaId = normalizeNodeId(typedIdRaw || operaName);
          if (!operaId || isPlaceholderName(operaId)) return;
          nodes.push(createOperaNodePayload({
            id: typedIdRaw || operaId,
            name: operaName || operaId,
            opera_id: opera?.opera_id,
            opera_name: operaName || operaId,
            version: opera?.version,
            x: 100 + (index * 80),
            y: 500
          }));
          
        const relationshipSource = deriveRelationshipSourceText(opera.opera_source_text, opera.relationshipSourceDisplay, opera.source, opera.relationship_source);
        links.push({
          source: centerId,
          target: operaId,
            type: 'premiered',
            label: 'premiered role in',
            role: opera.role,
            relationshipSourceDisplay: relationshipSource,
            sourceInfo: relationshipSource,
            relationship_source: opera.relationship_source || null,
            opera_source_text: opera.opera_source_text || relationshipSource || null,
            opera_source_url: opera.opera_source_url || null
          });
        });
      }

      // Add books
      if (details.works.books) {
        details.works.books.forEach((book, index) => {
          const typedIdRaw = book?.id || (book?.book_id ? `book:${book.book_id}` : '');
          const bookLabel = String(book.title || `Unknown Book ${index + 1}`).trim();
          const bookId = normalizeNodeId(typedIdRaw || bookLabel);
          if (!bookId || isPlaceholderName(bookId)) return;
          nodes.push(createBookNodePayload({
            id: typedIdRaw || bookId,
            name: bookLabel || bookId,
            book_id: book?.book_id,
            title: bookLabel || bookId,
            link: book?.link,
            x: 500 + (index * 80),
            y: 500
          }));
          
          const relationshipSource = null;
          links.push({
            source: centerId,
            target: bookId,
            type: 'authored',
            label: 'authored',
            relationshipSourceDisplay: relationshipSource,
            sourceInfo: relationshipSource,
            relationship_source: null
          });
        });
      }

      // Add edited books
      if (details.works.editedBooks) {
        details.works.editedBooks.forEach((book, index) => {
          const typedIdRaw = book?.id || (book?.book_id ? `book:${book.book_id}` : '');
          const bookLabel = String(book.title || `Unknown Book ${index + 1}`).trim();
          const bookId = normalizeNodeId(typedIdRaw || bookLabel);
          if (!bookId || isPlaceholderName(bookId)) return;
          nodes.push(createBookNodePayload({
            id: typedIdRaw || bookId,
            name: bookLabel || bookId,
            book_id: book?.book_id,
            title: bookLabel || bookId,
            link: book?.link,
            x: 500 + (index * 80),
            y: 560
          }));

          const relationshipSource = deriveRelationshipSourceText(book.source, book.relationship_source);
          links.push({
            source: centerId,
            target: bookId,
            type: 'edited',
            label: 'edited',
            relationshipSourceDisplay: relationshipSource,
            sourceInfo: relationshipSource,
            relationship_source: book.relationship_source || null
          });
        });
      }

      // Add composed operas (person center) - keep 'composed' label for legacy, but also treat as 'wrote'
      if (details.works.composedOperas) {
        details.works.composedOperas.forEach((opera, index) => {
          const fallbackLabel = `Unknown Opera ${index + 1}`;
          const operaName = deriveOperaName(opera, fallbackLabel);
          const typedIdRaw = opera?.id || (opera?.opera_id ? `opera:${opera.opera_id}` : '');
          const operaId = normalizeNodeId(typedIdRaw || operaName);
          if (!operaId || isPlaceholderName(operaId)) return;
          nodes.push(createOperaNodePayload({
            id: typedIdRaw || operaId,
            name: operaName || operaId,
            opera_id: opera?.opera_id,
            opera_name: operaName || operaId,
            version: opera?.version,
            x: 100 + (index * 80),
            y: 400
          }));
          
          const relationshipSource = deriveRelationshipSourceText(opera.opera_source_text, opera.relationshipSourceDisplay, opera.source, opera.relationship_source);
          links.push({
            source: centerId,
            target: operaId,
            type: 'wrote',
            label: 'wrote',
            relationshipSourceDisplay: relationshipSource,
            sourceInfo: relationshipSource,
            relationship_source: opera.relationship_source || null,
            opera_source_text: opera.opera_source_text || relationshipSource || null,
            opera_source_url: opera.opera_source_url || null
          });
        });
      }
    }

    // Add premiered roles - behavior depends on center type
  if (details.premieredRoles) {
    if (type === 'operas') {
      // For opera networks: show people who premiered roles in this opera
      details.premieredRoles.forEach((role, index) => {
        const fallbackSinger = `Unknown Singer ${index}`;
        const singerRaw = role?.singer || fallbackSinger;
        const singerId = normalizeNodeId(singerRaw);
        if (!singerId || isPlaceholderName(singerId)) return;
        const singerName = String(singerRaw || singerId).trim() || singerId;
        if (!addedNodes.has(singerId)) {
          nodes.push({
            id: singerId,
            name: singerName,
            type: 'person',
            voiceType: role.voice_type,
            x: 300 + (index * 60),
            y: 400
          });
          addedNodes.add(singerId);
        }
        
        const relationshipSource = deriveRelationshipSourceText(role.opera_source_text, role.relationshipSourceDisplay, role.source, role.relationship_source);
        links.push({
          source: singerId,
          target: centerId,
          type: 'premiered',
          label: 'premiered role in',
          role: role.role,
          relationshipSourceDisplay: relationshipSource,
          sourceInfo: relationshipSource,
          relationship_source: role.relationship_source || null,
          opera_source_text: role.opera_source_text || relationshipSource || null,
          opera_source_url: role.opera_source_url || null
        });
      });
    } else if (type === 'singers') {
      // For person networks: show operas the person premiered roles in
      // This data should be in works.operas, not premieredRoles.
      // Skip processing premieredRoles for person networks to avoid confusion.
    }
  }

  // Add authors for books
  if (details.authors) {
    details.authors.forEach((author, index) => {
      const fallbackAuthor = `Unknown Author ${index}`;
      const authorRaw = author?.author || author?.name || fallbackAuthor;
      const authorId = normalizeNodeId(authorRaw);
      if (!authorId || isPlaceholderName(authorId)) return;
      const authorName = String(authorRaw || authorId).trim() || authorId;
      if (!addedNodes.has(authorId)) {
        nodes.push({
          id: authorId,
          name: authorName,
          type: 'person',
          voiceType: author.voice_type,
          x: 200 + (index * 60),
          y: 200
        });
        addedNodes.add(authorId);
      }
      
      const relationshipSource = null;
      links.push({
        source: authorId,
        target: centerId,
        type: 'authored',
        label: 'authored',
        relationshipSourceDisplay: relationshipSource,
        sourceInfo: relationshipSource,
        relationship_source: null
      });
    });
  }

  // Add editors for books
  if (details.editors) {
    details.editors.forEach((editor, index) => {
      const fallbackEditor = `Unknown Editor ${index}`;
      const editorRaw = editor?.editor || editor?.name || fallbackEditor;
      const editorId = normalizeNodeId(editorRaw);
      if (!editorId || isPlaceholderName(editorId)) return;
      const editorName = String(editorRaw || editorId).trim() || editorId;
      if (!addedNodes.has(editorId)) {
        nodes.push({
          id: editorId,
          name: editorName,
          type: 'person',
          voiceType: editor.voice_type,
          x: 600 + (index * 60),
          y: 200
        });
        addedNodes.add(editorId);
      }
      
      const relationshipSource = null;
      links.push({
        source: editorId,
        target: centerId,
        type: 'edited',
        label: 'edited',
        relationshipSourceDisplay: relationshipSource,
        sourceInfo: relationshipSource,
        relationship_source: null
      });
    });
  }

  // Apply anti-overlap positioning to all nodes
  positionNodesWithoutOverlap(nodes);
  ensureNodeConnectivity(nodes, links, {
    primaryId: centerId
  });

  clearFiltersForNewSearch(nodes);

  setNetworkData(sanitizeGraphData({ nodes, links }));
  setCurrentCenterNode(centerLabel); // Set the center node for this network
  setShouldRunSimulation(true); // Trigger force simulation for new network
  // Vocalizing overlay clears when physics stabilizes on the new graph.
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
        const { type: typedType, value: typedValue } = parseTypedId(node.id);
        const payload = { operaName: node.name };
        if (typedType === 'opera' && typedValue) {
          payload.operaId = typedValue;
          payload.opera_id = typedValue;
        }
        response = await fetchWithRetry(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify(payload)
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

  useEffect(() => {
    const nodesList = Array.isArray(networkData.nodes) ? networkData.nodes : [];
    const placeholderNodes = nodesList.filter(node => {
      const id = normalizeNodeId(node?.id ?? node?.name);
      return isPlaceholderName(id);
    });
    const nodeIdSet = new Set(
      nodesList
        .map(node => normalizeNodeId(node?.id ?? node?.name))
        .filter(Boolean)
    );
    const invalidLinks = [];
    (networkData.links || []).forEach(link => {
      const sourceId = resolveLinkEndpointId(link?.source);
      const targetId = resolveLinkEndpointId(link?.target);
      if (
        !sourceId ||
        !targetId ||
        isPlaceholderName(sourceId) ||
        isPlaceholderName(targetId) ||
        !nodeIdSet.has(sourceId) ||
        !nodeIdSet.has(targetId)
      ) {
        invalidLinks.push({ sourceId, targetId });
      }
    });
    if (!placeholderNodes.length && invalidLinks.length === 0) return;
    try {
      if (placeholderNodes.length > 0) {
        console.warn('[sanitizeGraphData] Removing placeholder nodes before simulation:', placeholderNodes.map(n => n?.id ?? n?.name));
      }
      if (invalidLinks.length > 0) {
        console.warn('[sanitizeGraphData] Detected invalid links; sanitizing graph', invalidLinks);
      }
    } catch (_) {}
    setNetworkData(prev => sanitizeGraphData(prev));
  }, [networkData]);

  // Function to expand all relationships for a node
  const expandAllRelationships = async (node, prefetch = null) => {
    try {
      // Camera recenter is now triggered from clearExpansionBoost (after physics
      // settles) so the contract + pan read as one motion instead of three.
      pushHistory('expand-all');
      setLoading(true);
      pendingHelperMessageRef.current = null;
      const relationshipType = 'all';
      const expansionBatchId = Date.now();
      let response, data;

      if (prefetch) {
        try {
          response = await prefetch;
        } catch (err) {
          if (err && err.name === 'AbortError') { setLoading(false); return; }
          throw err;
        }
      } else if (node.type === 'person') {
        response = await fetch(`${API_BASE}/singer/network`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ singerName: node.name, depth: 5 })
        });
      } else if (node.type === 'opera') {
        const { type: typedType, value: typedValue } = parseTypedId(node.id);
        const payload = { operaName: node.name };
        if (typedType === 'opera' && typedValue) {
          payload.operaId = typedValue;
          payload.opera_id = typedValue;
        }
        response = await fetch(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify(payload)
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

      if (response && handleRateLimitResponse(response)) {
        setLoading(false);
        return;
      }
      if (response) {
        let responseText = '';
        try {
          responseText = await response.text();
        } catch (parseErr) {
          console.warn('[expandAll] Failed to read response text', parseErr);
        }
        try {
          data = responseText ? JSON.parse(responseText) : {};
        } catch (parseErr) {
          console.warn('[expandAll] Failed to parse response JSON', parseErr, { responseText });
          data = {};
        }

        if (!response.ok) {
          if (handleUnauthorized(response)) return;
          if (response.status === 404) {
            // End the in-progress overlay before showing the result message
            // so the result text isn't masked by the "Vocalizing…" overlay.
            stopVocalizing();
            showHelperMessage('No additional related nodes.');
            setError('');
            return;
          }
          const errorMessage = (data && (data.error || data.message))
            ? String(data.error || data.message)
            : `Failed to expand (${response.status})`;
          console.warn('[expandAll] Non-OK response', { status: response.status, statusText: response.statusText, errorMessage });
          setError(errorMessage);
          return;
        }

        if (response.ok) {
          // Merge new data with existing network
          const existingNodes = new Set(
            (networkData.nodes || []).map(n => normalizeNodeId(n.id ?? n.name)).filter(Boolean)
          );
          const existingLinks = buildLinkKeySet(networkData.links);
          const aliasLookup = new Map();
          (networkData.nodes || []).forEach(nodeEntry => registerNodeAliases(aliasLookup, nodeEntry));
          const nodeUpdates = new Map();
          
          let newNodes = [];
          let newLinks = [];
          const anchorId = normalizeNodeId(node.id ?? node.name);
          const anchorX = Number.isFinite(node?.x) ? node.x : 400;
          const anchorY = Number.isFinite(node?.y) ? node.y : 300;

          const registerNode = (payload) => {
            if (!payload) return null;
            const basePayload = {
              ...payload,
              x: Number.isFinite(payload?.x) ? payload.x : anchorX,
              y: Number.isFinite(payload?.y) ? payload.y : anchorY
            };
            const resolvedId = resolveAliasIdFromMap(aliasLookup, basePayload);
            if (resolvedId) {
              basePayload.id = resolvedId;
            }
            const payloadType = (basePayload.type || '').toLowerCase();
            let preparedPayload = basePayload;
            if (payloadType === 'opera') {
              preparedPayload = createOperaNodePayload(basePayload);
            } else if (payloadType === 'book') {
              preparedPayload = createBookNodePayload(basePayload);
            }
            const candidate = finalizeNodeCandidate(preparedPayload);
            if (!candidate) return null;
            registerNodeAliases(aliasLookup, candidate);
            const key = candidate.id;
            if (existingNodes.has(key)) {
              const currentPatch = nodeUpdates.get(key) || { id: key };
              nodeUpdates.set(key, mergeNodeAttributes(currentPatch, candidate));
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
            if (isPlaceholderName(sourceId) || isPlaceholderName(targetId)) return;
            const linkKey = createLinkKey(sourceId, targetId, type);
            if (existingLinks.has(linkKey)) return;
            existingLinks.add(linkKey);
            const cleanedExtra = { ...extra };
            const teacherRelSource = typeof cleanedExtra.teacher_rel_source === 'string' ? cleanedExtra.teacher_rel_source : null;
            const relationshipSourceProp = typeof cleanedExtra.relationship_source === 'string' ? cleanedExtra.relationship_source : null;
            const relationshipSourceCandidates = Array.isArray(cleanedExtra.relationshipSourceCandidates)
              ? cleanedExtra.relationshipSourceCandidates
              : [];
            delete cleanedExtra.relationshipSourceCandidates;
            delete cleanedExtra.teacher_rel_source;
            delete cleanedExtra.relationship_source;
            delete cleanedExtra.source;
            delete cleanedExtra.sourceInfo;
            delete cleanedExtra.target;
            delete cleanedExtra.sourceDisplay;
            const linkPayload = {
              source: sourceId,
              target: targetId,
              type,
              ...cleanedExtra
            };
            const relationshipSource = deriveRelationshipSourceText(
              linkPayload.relationshipSourceDisplay,
              linkPayload.sourceInfo,
              ...relationshipSourceCandidates
            );
            linkPayload.relationshipSourceDisplay = relationshipSource;
            linkPayload.teacher_rel_source = teacherRelSource;
            linkPayload.relationship_source = relationshipSourceProp;
            linkPayload.sourceInfo = relationshipSource;
            newLinks.push(linkPayload);
          };
          
          // Handle different node types and their data structures
          if (node.type === 'person') {
            // Add new nodes from the expanded data for people
              if (data.teachers) {
                data.teachers.forEach(teacher => {
                  const teacherName = teacher.full_name || teacher.name || teacher.label || 'Unknown Teacher';
                  const typedIdRaw = teacher?.id || (teacher?.person_id ? `person:${teacher.person_id}` : '');
                  const teacherId = registerNode({
                    id: typedIdRaw || teacherName,
                    name: teacherName,
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
                  relationshipSourceCandidates: [teacher.teacher_rel_source, teacher.relationship_source, teacher.source]
                });
              });
              // Enrich teacher nodes with full details for CSV immediately
              enrichPersonNodes((data.teachers || []).map(t => t.full_name));
            }
            
              if (data.students) {
                data.students.forEach(student => {
                  const studentName = student.full_name || student.name || student.label || 'Unknown Student';
                  const typedIdRaw = student?.id || (student?.person_id ? `person:${student.person_id}` : '');
                  const studentId = registerNode({
                    id: typedIdRaw || studentName,
                    name: studentName,
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
                  relationshipSourceCandidates: [student.teacher_rel_source, student.relationship_source, student.source]
                });
              });
              // Enrich student nodes with full details for CSV immediately
              enrichPersonNodes((data.students || []).map(s => s.full_name));
            }
            
            {
              const familyList = (data.family || data.center?.family || []);
              if (familyList && familyList.length > 0) familyList.forEach(relative => {
                const relName = relative.full_name || relative.name || relative.label || 'Unknown Relative';
                const typedIdRaw = relative?.id || (relative?.person_id ? `person:${relative.person_id}` : '');
                const relId = registerNode({
                  id: typedIdRaw || relName,
                  name: relName,
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
                  relationshipSourceCandidates: [relative.teacher_rel_source, relative.relationship_source, relative.source]
                });
              });
              // Enrich family person nodes for CSV immediately
              if (familyList && familyList.length > 0) enrichPersonNodes(familyList.map(r => r.full_name));
            }
            
            if (data.works) {
              if (data.works.operas) {
                data.works.operas.forEach(opera => {
                  const displayName = deriveOperaName(opera, 'Unknown Opera');
                  const typedIdRaw = opera?.id || (opera?.opera_id ? `opera:${opera.opera_id}` : '');
                  const operaId = registerNode(createOperaNodePayload({
                    id: typedIdRaw || displayName,
                    name: displayName,
                    opera_id: opera?.opera_id,
                    opera_name: displayName,
                    version: opera?.version
                  }));
                  if (!operaId) return;
                  addLink(anchorId, operaId, 'premiered', {
                    label: 'premiered role in',
                    role: opera.role,
                    relationshipSourceCandidates: [opera.source, opera.relationship_source]
                  });
                });
              }

              if (Array.isArray(data.works.composedOperas) && data.works.composedOperas.length > 0) {
                data.works.composedOperas.forEach(opera => {
                  const displayName = deriveOperaName(opera, 'Unknown Opera');
                  const typedIdRaw = opera?.id || (opera?.opera_id ? `opera:${opera.opera_id}` : '');
                  const operaId = registerNode(createOperaNodePayload({
                    id: typedIdRaw || displayName,
                    name: displayName,
                    opera_id: opera?.opera_id,
                    opera_name: displayName,
                    version: opera?.version
                  }));
                  if (!operaId) return;
                  addLink(anchorId, operaId, 'wrote', {
                    label: 'wrote',
                    relationshipSourceCandidates: [
                      opera.opera_source_text,
                      opera.opera_source,
                      opera.source,
                      opera.relationship_source
                    ]
                  });
                });
              }

              if (data.works.books) {
                data.works.books.forEach(book => {
                  const bookLabel = String(book.title || book.name || '').trim() || 'Unknown Book';
                  const typedIdRaw = book?.id || (book?.book_id ? `book:${book.book_id}` : '');
                  const bookId = registerNode(createBookNodePayload({
                    id: typedIdRaw || bookLabel,
                    name: bookLabel,
                    book_id: book?.book_id,
                    title: bookLabel,
                    link: book?.link
                  }));
                  if (!bookId) return;
                  addLink(anchorId, bookId, 'authored', {
                    label: 'authored',
                    relationshipSourceCandidates: [book.source, book.relationship_source]
                  });
                });
              }
              if (data.works.editedBooks) {
                data.works.editedBooks.forEach(book => {
                  const bookLabel = String(book.title || book.name || '').trim() || 'Unknown Book';
                  const typedIdRaw = book?.id || (book?.book_id ? `book:${book.book_id}` : '');
                  const bookId = registerNode(createBookNodePayload({
                    id: typedIdRaw || bookLabel,
                    name: bookLabel,
                    book_id: book?.book_id,
                    title: bookLabel,
                    link: book?.link
                  }));
                  if (!bookId) return;
                  addLink(anchorId, bookId, 'edited', {
                    label: 'edited',
                    relationshipSourceCandidates: [book.source, book.relationship_source]
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
                  relationshipSourceCandidates: [role.source, role.relationship_source]
                });
              });
            }
            
            let composerLinked = false;
            if (Array.isArray(data.wrote) && data.wrote.length > 0) {
              data.wrote.forEach(entry => {
                const composerName = entry.composer || entry.name;
                if (!composerName) return;
                const composerId = registerNode({
                  id: composerName,
                  name: composerName,
                  type: 'person',
                  voiceType: 'Composer'
                });
                if (!composerId) return;
                addLink(composerId, anchorId, 'wrote', {
                  label: 'wrote',
                  relationshipSourceCandidates: [
                    entry.opera_source_text,
                    entry.source,
                    entry.relationship_source,
                    entry.opera_source_url
                  ]
                });
                composerLinked = true;
              });
            }

            if (!composerLinked && data.opera && data.opera.composer) {
              const fallbackComposerId = registerNode({
                id: data.opera.composer,
                name: data.opera.composer,
                type: 'person',
                voiceType: 'Composer'
              });
              if (fallbackComposerId) {
                addLink(fallbackComposerId, anchorId, 'wrote', {
                  label: 'wrote',
                  relationshipSourceCandidates: [data.opera?.source, data.opera?.relationship_source]
                });
              }
            }
          } else if (node.type === 'book') {
            const authors = Array.isArray(data.authors) && data.authors.length > 0
              ? data.authors.map(entry => ({
                  name: entry.author,
                  voice_type: entry.voice_type,
                  relationship_source: entry.relationship_source
                }))
              : (data.book && data.book.author ? [{ name: data.book.author }] : []);
            authors.forEach(author => {
              if (!author?.name) return;
              const authorId = registerNode({
                id: author.name,
                name: author.name,
                type: 'person',
                voiceType: author.voice_type
              });
              if (!authorId) return;
              addLink(authorId, anchorId, 'authored', {
                label: 'authored',
                relationshipSourceCandidates: [
                  author.relationship_source,
                  data.book?.source,
                  data.book?.relationship_source
                ]
              });
            });

            if (Array.isArray(data.editors) && data.editors.length > 0) {
              data.editors.forEach(editor => {
                const editorName = editor.editor || editor.name;
                if (!editorName) return;
                const editorId = registerNode({
                  id: editorName,
                  name: editorName,
                  type: 'person',
                  voiceType: editor.voice_type
                });
                if (!editorId) return;
                addLink(editorId, anchorId, 'edited', {
                  label: 'edited',
                  relationshipSourceCandidates: [
                    editor.relationship_source,
                    data.book?.source,
                    data.book?.relationship_source
                  ]
                });
              });
            }
          }

          const sanitizedAdditions = sanitizeIncrementalGraph(newNodes, newLinks, {
            anchorId,
            existingNodeIds: existingNodes
          });
          newNodes = sanitizedAdditions.nodes || [];
          newLinks = sanitizedAdditions.links || [];
          newNodes.forEach(nodeEntry => registerNodeAliases(aliasLookup, nodeEntry));

          if (newNodes.length > 0) {
            newNodes.forEach(n => {
              if (!n) return;
              n.recentlyExpandedAt = expansionBatchId;
              n.expansionBatchId = expansionBatchId;
            });
          }

          attachNewNodesToAnchor({
            anchorId,
            anchorType: node.type,
            relationshipType,
            newNodes,
            newLinks,
            existingLinks: networkData.links,
            addLink
          });

          // Mark expansion-internal links so the main simulation keeps the entire batch compact
          if (newNodes.length > 0 && Array.isArray(newLinks)) {
            const addedIdSet = new Set(newNodes.map(n => n && n.id).filter(Boolean));
            let flagged = 0;
            newLinks.forEach(l => {
              const s = resolveLinkEndpointId(l?.source);
              const t = resolveLinkEndpointId(l?.target);
              if (!s || !t) return;
              // Anchor <-> newly added nodes
              if ((s === anchorId && addedIdSet.has(t)) || (t === anchorId && addedIdSet.has(s))) {
                if (!l.expansionInternal) { l.expansionInternal = true; flagged += 1; }
                return;
              }
              // Links entirely within the newly-added batch
              if (addedIdSet.has(s) && addedIdSet.has(t)) {
                if (!l.expansionInternal) { l.expansionInternal = true; flagged += 1; }
              }
            });
            debugLog('expand-flag-internal-links', { anchorId, newCount: newNodes.length, linkCount: newLinks.length, flagged });
          }

          if (newNodes.length > 0) {
            const { min: ringMin, max: ringMax, spacing: ringSpacing } = getExpansionRingConfig(newNodes.length);
            const ringRadius = computeRingRadius(newNodes.length, ringMin, ringMax, ringSpacing);
            debugLog('expand-spawn', { anchorId, anchorX, anchorY, ringRadius, newCount: newNodes.length });
            newNodes.forEach((n, idx) => {
              if (!n) return;
              const angle = (idx / newNodes.length) * Math.PI * 2;
              n.x = anchorX + ringRadius * Math.cos(angle);
              n.y = anchorY + ringRadius * Math.sin(angle);
            });
            extendDateRangesForNodes(newNodes);
          }

          // Pin the expanded node so the simulation can't drag it away while new nodes settle
          const anchorNodeObj = (networkData.nodes || []).find(n => normalizeNodeId(n?.id ?? n?.name) === anchorId);
          if (anchorNodeObj) { anchorNodeObj.__pinDuringExpansion = true; }

          setNetworkData(prev => {
            const merged = mergeNetworkUpdates(prev, newNodes, newLinks, nodeUpdates);
            const fallback = resolveFallbackConfig(relationshipType, node.type);
            const ensuredLinks = Array.isArray(merged.links) ? [...merged.links] : [];
            ensureNodeConnectivity(merged.nodes, ensuredLinks, {
              primaryId: anchorId,
              fallbackType: fallback.type,
              fallbackLabel: fallback.label || ''
            });
            const next = sanitizeGraphData({ nodes: merged.nodes, links: ensuredLinks });
            if (isLayoutDebug()) {
              try {
                const anchor = (next.nodes || []).find(n => n && n.id === anchorId);
                const flagged = (next.links || []).reduce((acc,l)=>acc+(l.expansionInternal?1:0),0);
                debugLog('post-merge-anchor', { anchorId, x: anchor?.x, y: anchor?.y, flaggedLinks: flagged, totalLinks: next.links?.length });
              } catch (_) {}
            }
            return next;
          });
          const pendingHelperMessage = newNodes.length === 0
            ? { text: 'No additional related nodes.', duration: 3200 }
            : { text: '', duration: 0 };
          pendingHelperMessageRef.current = pendingHelperMessage;
          // Refresh counts for the expanded node to keep context menu accurate
          try {
            const updatedCounts = await fetchActualCounts(node);
            setActualCounts(prev => ({ ...prev, [node.id]: updatedCounts }));
          } catch (e) {}

          if (newNodes.length > 0) {
            setIsExpansionSimulation(true);
            setShouldRunSimulation(true);
          }

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
    if (checkAndEnforceRateLimit()) return;
    try {
      // Recenter handled by clearExpansionBoost (post-stabilize), see expandAll.
      pushHistory(`expand-${relationshipType}`);
      setLoading(true);
      const expansionBatchId = Date.now();
      pendingHelperMessageRef.current = null;
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
        const { type: typedType, value: typedValue } = parseTypedId(node.id);
        const payload = { operaName: node.name };
        if (typedType === 'opera' && typedValue) {
          payload.operaId = typedValue;
          payload.opera_id = typedValue;
        }
        response = await fetchWithRetry(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify(payload)
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
        let responseText = '';
        try {
          responseText = await response.text();
        } catch (parseErr) {
          console.warn('[expandSpecific] Failed to read response text', parseErr);
        }
        try {
          data = responseText ? JSON.parse(responseText) : {};
        } catch (parseErr) {
          console.warn('[expandSpecific] Failed to parse response JSON', parseErr, { responseText });
          data = {};
        }

        if (!response.ok) {
          if (handleUnauthorized(response)) return;
          if (response.status === 404) {
            // End the in-progress overlay before showing the result message
            // so the result text isn't masked by the "Vocalizing…" overlay.
            stopVocalizing();
            showHelperMessage('No additional related nodes.');
            setError('');
            return;
          }
          const errorMessage = (data && (data.error || data.message))
            ? String(data.error || data.message)
            : `Failed to expand (${response.status})`;
          console.warn('[expandSpecific] Non-OK response', { status: response.status, statusText: response.statusText, errorMessage });
          setError(errorMessage);
          return;
        }

        if (response.ok) {
          // Merge new data with existing network
          const existingNodes = new Set(
            (networkData.nodes || []).map(n => normalizeNodeId(n.id ?? n.name)).filter(Boolean)
          );
          const existingLinks = buildLinkKeySet(networkData.links);
          const aliasLookup = new Map();
          (networkData.nodes || []).forEach(nodeEntry => registerNodeAliases(aliasLookup, nodeEntry));
          const nodeUpdates = new Map();

          let newNodes = [];
          let newLinks = [];
          const anchorId = normalizeNodeId(node.id ?? node.name);
          const anchorX = Number.isFinite(node?.x) ? node.x : 400;
          const anchorY = Number.isFinite(node?.y) ? node.y : 300;

          const registerNode = (payload) => {
            if (!payload) return null;
            const basePayload = {
              ...payload,
              x: Number.isFinite(payload?.x) ? payload.x : anchorX,
              y: Number.isFinite(payload?.y) ? payload.y : anchorY
            };
            const resolvedId = resolveAliasIdFromMap(aliasLookup, basePayload);
            if (resolvedId) {
              basePayload.id = resolvedId;
            }
            const payloadType = (basePayload.type || '').toLowerCase();
            let preparedPayload = basePayload;
            if (payloadType === 'opera') {
              preparedPayload = createOperaNodePayload(basePayload);
            } else if (payloadType === 'book') {
              preparedPayload = createBookNodePayload(basePayload);
            }
            const candidate = finalizeNodeCandidate(preparedPayload);
            if (!candidate) return null;
            registerNodeAliases(aliasLookup, candidate);
            const key = candidate.id;
            if (existingNodes.has(key)) {
              const currentPatch = nodeUpdates.get(key) || { id: key };
              nodeUpdates.set(key, mergeNodeAttributes(currentPatch, candidate));
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
            if (isPlaceholderName(sourceId) || isPlaceholderName(targetId)) return;
            const linkKey = createLinkKey(sourceId, targetId, type);
            if (existingLinks.has(linkKey)) return;
            existingLinks.add(linkKey);
            const cleanedExtra = { ...extra };
            const teacherRelSource = typeof cleanedExtra.teacher_rel_source === 'string' ? cleanedExtra.teacher_rel_source : null;
            const relationshipSourceProp = typeof cleanedExtra.relationship_source === 'string' ? cleanedExtra.relationship_source : null;
            const relationshipSourceCandidates = Array.isArray(cleanedExtra.relationshipSourceCandidates)
              ? cleanedExtra.relationshipSourceCandidates
              : [];
            delete cleanedExtra.relationshipSourceCandidates;
            delete cleanedExtra.teacher_rel_source;
            delete cleanedExtra.relationship_source;
            delete cleanedExtra.source;
            delete cleanedExtra.sourceInfo;
            const linkPayload = {
              source: sourceId,
              target: targetId,
              type,
              ...cleanedExtra
            };
            const relationshipSource = deriveRelationshipSourceText(
              linkPayload.relationshipSourceDisplay,
              linkPayload.sourceInfo,
              ...relationshipSourceCandidates
            );
            linkPayload.relationshipSourceDisplay = relationshipSource;
            linkPayload.teacher_rel_source = teacherRelSource;
            linkPayload.relationship_source = relationshipSourceProp;
            linkPayload.sourceInfo = relationshipSource;
            newLinks.push(linkPayload);
          };
          
          // Handle specific relationship types for people
          if (node.type === 'person') {
            if (relationshipType === 'taughtBy' && data.teachers) {
              data.teachers.forEach(teacher => {
                const teacherName = teacher.full_name || teacher.name || teacher.label || 'Unknown Teacher';
                const typedIdRaw = teacher?.id || (teacher?.person_id ? `person:${teacher.person_id}` : '');
                const teacherId = registerNode({
                  id: typedIdRaw || teacherName,
                  name: teacherName,
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
                  relationshipSourceCandidates: [teacher.teacher_rel_source, teacher.relationship_source, teacher.source]
                });
              });
              enrichPersonNodes((data.teachers || []).map(t => t.full_name));
            }
            
            if (relationshipType === 'taught' && data.students) {
              data.students.forEach(student => {
                const studentName = student.full_name || student.name || student.label || 'Unknown Student';
                const typedIdRaw = student?.id || (student?.person_id ? `person:${student.person_id}` : '');
                const studentId = registerNode({
                  id: typedIdRaw || studentName,
                  name: studentName,
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
                  relationshipSourceCandidates: [student.teacher_rel_source, student.relationship_source, student.source]
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
                  const relName = relative.full_name || relative.name || relative.label || 'Unknown Relative';
                  const typedIdRaw = relative?.id || (relative?.person_id ? `person:${relative.person_id}` : '');
                  const relId = registerNode({
                    id: typedIdRaw || relName,
                    name: relName,
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
                    relationshipSourceCandidates: [relative.teacher_rel_source, relative.relationship_source, relative.source]
                  });
                }
              });
              enrichPersonNodes((data.family || []).map(r => r.full_name));
            }
            
            if (relationshipType === 'authored' && data.works && data.works.books) {
              data.works.books.forEach(book => {
                const bookLabel = String(book.title || book.name || '').trim() || 'Unknown Book';
                const typedIdRaw = book?.id || (book?.book_id ? `book:${book.book_id}` : '');
                const bookId = registerNode(createBookNodePayload({
                  id: typedIdRaw || bookLabel,
                  name: bookLabel,
                  book_id: book?.book_id,
                  title: bookLabel,
                  link: book?.link
                }));
                if (!bookId) return;
                addLink(anchorId, bookId, 'authored', {
                  label: 'authored',
                  relationshipSourceCandidates: [book.source, book.relationship_source]
                });
              });
            }

            if (relationshipType === 'edited' && data.works && data.works.editedBooks) {
              data.works.editedBooks.forEach(book => {
                const bookLabel = String(book.title || book.name || '').trim() || 'Unknown Book';
                const typedIdRaw = book?.id || (book?.book_id ? `book:${book.book_id}` : '');
                const bookId = registerNode(createBookNodePayload({
                  id: typedIdRaw || bookLabel,
                  name: bookLabel,
                  book_id: book?.book_id,
                  title: bookLabel,
                  link: book?.link
                }));
                if (!bookId) return;
                addLink(anchorId, bookId, 'edited', {
                  label: 'edited',
                  relationshipSourceCandidates: [book.source, book.relationship_source]
                });
              });
            }
            
            if (relationshipType === 'premieredRoleIn' && data.works && data.works.operas) {
              data.works.operas.forEach(opera => {
                const displayName = deriveOperaName(opera, 'Unknown Opera');
                const typedIdRaw = opera?.id || (opera?.opera_id ? `opera:${opera.opera_id}` : '');
                const operaId = registerNode(createOperaNodePayload({
                  id: typedIdRaw || displayName,
                  name: displayName,
                  opera_id: opera?.opera_id,
                  opera_name: displayName,
                  version: opera?.version
                }));
                if (!operaId) return;
                addLink(anchorId, operaId, 'premiered', {
                  label: 'premiered role in',
                  role: opera.role,
                  relationshipSourceCandidates: [opera.source, opera.relationship_source]
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
                  relationshipSourceCandidates: [role.source, role.relationship_source]
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
                  label: 'composed',
                  relationshipSourceCandidates: [data.opera?.source, data.opera?.relationship_source]
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
                  relationshipSourceCandidates: [data.book?.source, data.book?.relationship_source]
                });
              }
            }
          }
          
          const sanitizedAdditions = sanitizeIncrementalGraph(newNodes, newLinks, {
            anchorId,
            existingNodeIds: existingNodes
          });
          newNodes = sanitizedAdditions.nodes || [];
          newLinks = sanitizedAdditions.links || [];
          newNodes.forEach(nodeEntry => registerNodeAliases(aliasLookup, nodeEntry));

          if (newNodes.length > 0) {
            newNodes.forEach(n => {
              if (!n) return;
              n.recentlyExpandedAt = expansionBatchId;
              n.expansionBatchId = expansionBatchId;
            });
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

            const relationshipLabel = typeof relationshipType === 'string' && relationshipType.trim()
              ? relationshipType.trim()
              : '';
            const normalizedLabel = relationshipLabel.toLowerCase();
            const fallbackType = normalizedLabel.includes('parent') ||
              normalizedLabel.includes('sibling') ||
              normalizedLabel.includes('grandparent') ||
              normalizedLabel.includes('spouse')
              ? 'family'
              : normalizedLabel.includes('taught')
              ? 'taught'
              : normalizedLabel.includes('premiered')
              ? 'premiered'
              : normalizedLabel.includes('wrote') || normalizedLabel.includes('composed')
              ? 'wrote'
              : normalizedLabel.includes('authored')
              ? 'authored'
              : normalizedLabel.includes('edited')
              ? 'edited'
              : '';

            newNodes.forEach(n => {
              const nodeId = normalizeNodeId(n.id);
              if (!nodeId || attachedToAnchor.has(nodeId)) return;
              if (!fallbackType) return;
              addLink(anchorId, nodeId, fallbackType, { label: relationshipLabel || fallbackType });
              attachedToAnchor.add(nodeId);
            });

            // Mark expansion-internal links so global simulation keeps the entire batch compact
            if (Array.isArray(newLinks) && newNodes.length > 0) {
              const addedIdSet = new Set(newNodes.map(n => n && n.id).filter(Boolean));
              newLinks.forEach(l => {
                const s = resolveLinkEndpointId(l?.source);
                const t = resolveLinkEndpointId(l?.target);
                if (!s || !t) return;
                // Anchor <-> newly added nodes
                if ((s === anchorId && addedIdSet.has(t)) || (t === anchorId && addedIdSet.has(s))) {
                  l.expansionInternal = true;
                  return;
                }
                // Links entirely within the newly-added batch
                if (addedIdSet.has(s) && addedIdSet.has(t)) {
                  l.expansionInternal = true;
                }
              });
            }

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
            const { min: simRingMin, max: simRingMax, spacing: simRingSpacing } = getExpansionRingConfig(newNodes.length);
            const initialRadius = computeRingRadius(newNodes.length, simRingMin, simRingMax, simRingSpacing);
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
                .force('link', d3.forceLink(simLinks).distance(140).strength(0.9))
                .force('charge', d3.forceManyBody().strength(-240))
                .force('collision', d3.forceCollide().radius(80))
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

          // Pin the expanded node so the simulation can't drag it away while new nodes settle
          const anchorNodeObj = (networkData.nodes || []).find(n => normalizeNodeId(n?.id ?? n?.name) === anchorId);
          if (anchorNodeObj) { anchorNodeObj.__pinDuringExpansion = true; }

          setNetworkData(prev => {
            const merged = mergeNetworkUpdates(prev, newNodes, newLinks, nodeUpdates);
            const fallback = resolveFallbackConfig(relationshipType, node.type);
            const ensuredLinks = Array.isArray(merged.links) ? [...merged.links] : [];
            ensureNodeConnectivity(merged.nodes, ensuredLinks, {
              primaryId: anchorId,
              fallbackType: fallback.type,
              fallbackLabel: fallback.label || ''
            });
            const next = sanitizeGraphData({ nodes: merged.nodes, links: ensuredLinks });
            if (isLayoutDebug()) {
              try {
                const anchor = (next.nodes || []).find(n => n && n.id === anchorId);
                const flagged = (next.links || []).reduce((acc,l)=>acc+(l.expansionInternal?1:0),0);
                debugLog('post-merge-anchor', { anchorId, x: anchor?.x, y: anchor?.y, flaggedLinks: flagged, totalLinks: next.links?.length });
              } catch (_) {}
            }
            return next;
          });

      const pendingHelperMessage = newNodes.length === 0
        ? { text: 'No additional related nodes.', duration: 3200 }
        : { text: '', duration: 0 };
      pendingHelperMessageRef.current = pendingHelperMessage;

      if (newNodes.length > 0) {
        setIsExpansionSimulation(true);
        setShouldRunSimulation(true);
      }
        } else {
          setError(data.error);
        }
      }
    } catch (err) {
      console.error('[expandSpecific] Failed to expand', { nodeId: node?.id, relationshipType, err });
      if (err && err.name === 'AbortError') {
        return;
      }
      const message = typeof err?.message === 'string' && err.message.trim()
        ? err.message.trim()
        : 'Failed to expand specific relationship';
      setError(message.includes('Too many requests') ? message : 'Failed to expand specific relationship');
    } finally {
      setLoading(false);
    }
  };

  function flushPendingHelperMessage() {
    const pending = pendingHelperMessageRef.current;
    pendingHelperMessageRef.current = null;
    if (!pending) return;
    const text = typeof pending.text === 'string' ? pending.text : '';
    const duration = Number.isFinite(pending.duration) ? pending.duration : 0;
    showHelperMessage(text, duration);
  }

  const clearPendingNodeAction = () => {
    if (nodeClickTimeoutRef.current) {
      clearTimeout(nodeClickTimeoutRef.current);
      nodeClickTimeoutRef.current = null;
      setLoading(false);
    }
    if (expansionAbortControllerRef.current) {
      try { expansionAbortControllerRef.current.abort(); } catch (_) {}
      expansionAbortControllerRef.current = null;
    }
  };

  const closeNodeMenusAndCards = () => {
    try { setContextMenu({ show: false, x: 0, y: 0, node: null }); } catch (_) {}
    try { setLinkContextMenu(createLinkContextMenuState()); } catch (_) {}
    try { setProfileCard({ show: false, data: null }); } catch (_) {}
  };

  const selectNodeForFeedback = (node) => {
    if (!node) return;
    setTimeout(() => {
      setSelectedNode(node);
    }, 0);
  };

  const startExpansionFetch = (node) => {
    const controller = new AbortController();
    expansionAbortControllerRef.current = controller;
    const headers = {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`
    };
    const { signal } = controller;
    if (node.type === 'person') {
      return fetch(`${API_BASE}/singer/network`, {
        method: 'POST', headers, signal,
        body: JSON.stringify({ singerName: node.name, depth: 5 })
      });
    }
    if (node.type === 'opera') {
      const { type: typedType, value: typedValue } = parseTypedId(node.id);
      const payload = { operaName: node.name };
      if (typedType === 'opera' && typedValue) {
        payload.operaId = typedValue;
        payload.opera_id = typedValue;
      }
      return fetch(`${API_BASE}/opera/details`, {
        method: 'POST', headers, signal,
        body: JSON.stringify(payload)
      });
    }
    if (node.type === 'book') {
      return fetch(`${API_BASE}/book/details`, {
        method: 'POST', headers, signal,
        body: JSON.stringify({ bookTitle: node.name })
      });
    }
    return null;
  };

  const scheduleNodeExpansion = (node) => {
    if (checkAndEnforceRateLimit()) return;
    clearPendingNodeAction();
    setLoading(true);
    // \u2500\u2500 pill\u2192overlay (2026-05-06): in-progress states route through Vocalizing
    startVocalizing();
    const prefetch = startExpansionFetch(node);
    nodeClickTimeoutRef.current = setTimeout(() => {
      nodeClickTimeoutRef.current = null;
      lastTappedNodeIdRef.current = null;
      expansionAbortControllerRef.current = null;
      expandAllRelationships(node, prefetch);
    }, 220);
  };

  const handleNodeSingleActivation = (node) => {
    // On the search-results view, a node in the canvas mirrors a text result card:
    // click it to drop the rest of the result nodes and load that item's network.
    // Detect this two ways — currentView and a structural check (canvas matches
    // the search-result set) — so timing/closure quirks don't drop us into the
    // expansion path when we're actually still showing search results.
    const currentNodeCount = Array.isArray(networkData?.nodes) ? networkData.nodes.length : 0;
    const resultsCount = Array.isArray(searchResults) ? searchResults.length : 0;
    const showingSearchResults =
      currentView === 'results' ||
      (resultsCount > 0 && currentNodeCount === resultsCount && currentNodeCount > 1);
    if (showingSearchResults) {
      handleNodeDoubleActivation(node);
      return;
    }
    closeNodeMenusAndCards();
    selectNodeForFeedback(node);
    scheduleNodeExpansion(node);
  };

  const handleNodeDoubleActivation = (node) => {
    closeNodeMenusAndCards();
    selectNodeForFeedback(node);
    clearPendingNodeAction();
    lastTappedNodeIdRef.current = null;
    pushHistory('node-search');
    triggerNodeSearch(node);
  };

  const triggerNodeSearch = (node) => {
    if (!node) return;
    if (checkAndEnforceRateLimit()) return;
    // Immediate visual feedback: drop all other nodes from the canvas right away
    // so the user doesn't watch the old result set linger while getItemDetails
    // fetches. The full target network fills in when the response arrives.
    setNetworkData(prev => ({
      nodes: (prev?.nodes || []).filter(n => n.id === node.id),
      links: [],
    }));
    // Don't fit on the old graph — it's about to be replaced. The post-stabilization
    // fit on the new graph (NetworkVisualization) handles framing.
    // Show the centered "Vocalizing…" overlay; cleared by onPhysicsStabilized
    // when the new graph settles, or by the 8s safety timer.
    startVocalizing();
    if (currentCenterNode !== node.id) {
      setCurrentCenterNode(node.id);
    }
    if (node.type === 'person') {
      const mockSearchItem = {
        name: node.name,
        properties: {
          full_name: node.name,
          voice_type: node.voiceType,
          birth_year: node.birthYear,
          death_year: node.deathYear
        }
      };
      setSearchType('singers');
      getItemDetails(mockSearchItem, 'singers');
    } else if (node.type === 'opera') {
      const mockSearchItem = {
        properties: {
          title: node.opera_name || node.name,
          opera_name: node.opera_name || node.name,
          opera_id: node.opera_id,
          version: node.version
        }
      };
      setSearchType('operas');
      getItemDetails(mockSearchItem, 'operas');
    } else if (node.type === 'book') {
      const mockSearchItem = {
        properties: {
          title: node.title || node.name,
          book_id: node.book_id,
          link: node.link
        }
      };
      setSearchType('books');
      getItemDetails(mockSearchItem, 'books');
    }
    setTimeout(() => {
      setSelectedNode(node);
    }, 0);
  };

  // Function to dismiss other nodes (keep only the selected node, no relationships)
  const dismissOtherNodes = (selectedNode) => {
    const filteredNodes = networkData.nodes.filter(node => node.id === selectedNode.id);
    pushHistory('dismiss-others');
    // Remove all relationships - this creates a new visualization starting from this node
    setNetworkData(sanitizeGraphData({
      nodes: filteredNodes,
      links: [] // No relationships - clean slate
    }));
  };

  // Function to dismiss the selected node
  const dismissNode = (nodeToRemove) => {
    const filteredNodes = networkData.nodes.filter(node => node.id !== nodeToRemove.id);
    const filteredLinks = networkData.links.filter(link => {
      const sourceId = typeof link.source === 'string' ? link.source : link.source?.id;
      const targetId = typeof link.target === 'string' ? link.target : link.target?.id;
      return sourceId !== nodeToRemove.id && targetId !== nodeToRemove.id;
    });
    pushHistory('dismiss-node');
    setNetworkData(sanitizeGraphData({
      nodes: filteredNodes,
      links: filteredLinks
    }));
  };

  const getItemDetails = async (item, itemType = null) => {
    if (checkAndEnforceRateLimit()) return;
    try {
      // Skip fitting the old graph — it's about to be replaced. The viz component
      // refits once the new graph stabilizes.
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
        const normalized = normalizeDetailsRelationshipSources(data);
          setItemDetails(normalized);
          setCurrentView('network');
          generateNetworkFromDetails(normalized, item.name, 'singers');
        } else {
          setError(data.error || `Failed (${response.status})`);
        }
      } else if (typeToUse === 'operas') {
        const { type: typedType, value: typedValue } = parseTypedId(item.id);
        const payload = {
          operaName: item.properties.opera_name || item.properties.title
        };
        if (typedType === 'opera' && typedValue) {
          payload.operaId = typedValue;
          payload.opera_id = typedValue;
        }
        const response = await fetchWithRetry(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify(payload)
        }, { retries: 2, baseDelay: 600 });

        const text = await response.text();
        let data; try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { error: text || 'Invalid response' }; }
        if (response.ok) {
          data = await enrichWithFamily(data, item.properties.opera_name || item.properties.title);
          const normalized = normalizeDetailsRelationshipSources(data);
          setItemDetails(normalized);
          setCurrentView('network');
          generateNetworkFromDetails(normalized, item.properties.opera_name || item.properties.title, 'operas');
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
          const normalized = normalizeDetailsRelationshipSources(data);
          setItemDetails(normalized);
          setCurrentView('network');
          generateNetworkFromDetails(normalized, item.properties.title, 'books');
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
        const normalized = normalizeDetailsRelationshipSources(withFam);
        setItemDetails(normalized);
        setSelectedItem({ name: personName });
        setSearchType('singers');
        setCurrentView('network');
        generateNetworkFromDetails(normalized, personName, 'singers');
        setShouldRunSimulation(true); // Trigger simulation for person search
        setHasExecutedSearch(true);
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
        const normalized = normalizeDetailsRelationshipSources(withFam);
        setItemDetails(normalized);
        setSelectedItem({ name: personName });
        setSearchType('singers');
        setCurrentView('network');
        generateNetworkFromDetails(normalized, personName, 'singers');
        setShouldRunSimulation(true); // Trigger simulation for person search
        setHasExecutedSearch(true);
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
  // Saved View Dialog

  // Function to fetch actual relationship counts from database
  const fetchActualCounts = async (node) => {
    if (actualCounts[node.id]) {
      return actualCounts[node.id];
    }

    const coerceCount = (value) => {
      const num = Number(value);
      return Number.isFinite(num) && num >= 0 ? num : 0;
    };

    try {
      let countsFromCountsEndpoint = null;

      if (
        !relationshipCountsUnavailableRef.current &&
        (node.type === 'person' || node.type === 'opera' || node.type === 'book')
      ) {
        try {
          const response = await fetchWithRetry(`${API_BASE}/node/relationship-counts`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${token}`
            },
            body: JSON.stringify({ nodeType: node.type, nodeName: node.name })
          }, { retries: 1, baseDelay: 400 });

          if (response) {
            if (response.status === 404) {
              relationshipCountsUnavailableRef.current = true;
            } else if (response.ok) {
              const payload = await response.json();
              if (payload && payload.counts) {
                const countsPayload = payload.counts;
                countsFromCountsEndpoint = {
                  taughtBy: coerceCount(countsPayload.taughtBy),
                  taught: coerceCount(countsPayload.taught),
                  authored: coerceCount(countsPayload.authored),
                  premieredRoleIn: coerceCount(countsPayload.premieredRoleIn),
                  wrote: coerceCount(countsPayload.wrote),
                  parent: coerceCount(countsPayload.parent),
                  parentOf: coerceCount(countsPayload.parentOf),
                  spouse: coerceCount(countsPayload.spouse),
                  grandparent: coerceCount(countsPayload.grandparent),
                  grandparentOf: coerceCount(countsPayload.grandparentOf),
                  sibling: coerceCount(countsPayload.sibling),
                  edited: coerceCount(countsPayload.edited),
                  editedBy: coerceCount(countsPayload.editedBy)
                };
              }
            }
          }
        } catch (countErr) {
          if (countErr && countErr.status === 404) {
            relationshipCountsUnavailableRef.current = true;
          }
          // Fall through to legacy detail endpoints on failures
        }
      }

      if (countsFromCountsEndpoint) {
        setActualCounts(prev => ({ ...prev, [node.id]: countsFromCountsEndpoint }));
        return countsFromCountsEndpoint;
      }

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
        const { type: typedType, value: typedValue } = parseTypedId(node.id);
        const payload = { operaName: node.name };
        if (typedType === 'opera' && typedValue) {
          payload.operaId = typedValue;
          payload.opera_id = typedValue;
        }
        response = await fetchWithRetry(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify(payload)
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
        const composedOperas = Array.isArray(data.works?.composedOperas) ? data.works.composedOperas : [];
        const wroteRelationships = Array.isArray(data.wrote) ? data.wrote : [];

        if (data.works) {
          if (Array.isArray(data.works.operas)) counts.premieredRoleIn = data.works.operas.length;
          if (Array.isArray(data.works.books)) counts.authored = data.works.books.length;
          if (Array.isArray(data.works.editedBooks)) counts.edited = data.works.editedBooks.length;
        }

        const combinedWrote = [...composedOperas, ...wroteRelationships];
        if (combinedWrote.length > 0) {
          const uniqueWrote = new Set(
            combinedWrote
              .map(entry => {
                const candidate =
                  entry?.title ||
                  entry?.opera_name ||
                  entry?.name ||
                  entry?.operaTitle ||
                  entry?.label ||
                  entry?.display_name;
                if (candidate) {
                  return normalizeNodeId(candidate);
                }
                const derived = deriveOperaName(entry, '');
                return normalizeNodeId(derived);
              })
              .filter(Boolean)
          );
          counts.wrote = uniqueWrote.size || combinedWrote.length;
        }
        
        // Note: All people are persons in Neo4j regardless of activity (singer, composer, etc.)
        // The API should return comprehensive data for all persons through the network endpoint
      } else if (node.type === 'opera') {
        // Operas have people who premiered roles in them (incoming relationships)
        if (Array.isArray(data.premieredRoles)) counts.premieredRoleIn = data.premieredRoles.length;
        const composerRows = Array.isArray(data.wrote) ? data.wrote : [];
        if (composerRows.length > 0) {
          const uniqueComposers = new Set(
            composerRows
              .map(row => normalizeNodeId(row?.composer || row?.name || row?.full_name))
              .filter(Boolean)
          );
          counts.wrote = uniqueComposers.size || composerRows.length;
        } else if (data.opera && data.opera.composer) {
          counts.wrote = 1;
        }
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
  // Filter Panel Component

  const appBackgroundStyle = isMobileViewport ? {
    minHeight: backgroundMinHeight,
    width: '100%',
    backgroundImage: 'url(/aspens_2000.jpg)',
    backgroundSize: 'cover',
    backgroundPosition: 'center center',
    backgroundRepeat: 'no-repeat',
    backgroundAttachment: 'fixed',
    paddingLeft: 'var(--cmg-mobile-inline-padding)',
    paddingRight: 'var(--cmg-mobile-inline-padding-end)',
    paddingTop: 'var(--cmg-mobile-block-padding)',
    paddingBottom: 'var(--cmg-mobile-block-padding-end)'
  } : {
    minHeight: backgroundMinHeight,
    backgroundImage: 'url(/aspens_2000.jpg)',
    backgroundSize: 'cover',
    backgroundPosition: 'center center',
    backgroundRepeat: 'no-repeat',
    backgroundAttachment: backgroundAttachmentMode
  };

  if (!hasAcceptedDisclaimer) {
    return (
      <Landing
        isMobileViewport={isMobileViewport}
        onEnter={() => setHasAcceptedDisclaimer(true)}
      />
    );
  }
  const shouldBreakTitle = isHeaderMobile || (Number.isFinite(headerWidth) && headerWidth < 720);
  return (
    <div style={appBackgroundStyle}>
      <SavedViewDialog
        show={showSavedViewDialog}
        onClose={() => setShowSavedViewDialog(false)}
        savedViewToken={savedViewToken}
        savedViewLabel={savedViewLabel}
        isMobileViewport={isMobileViewport}
      />
      {/* Mobile Find-path overlay — fixed-position floating card so the */}
      {/* hamburger's Path button has somewhere to render. The desktop */}
      {/* version of this is rendered next to the Path button at L4790. */}
      {viewportIsPhone && showPathPanel && (
        <div
          style={{
            position: 'fixed',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
            width: 'calc(100% - 32px)',
            maxWidth: 420,
            backgroundColor: '#ffffff',
            border: '2px solid #3e96e2',
            borderRadius: 16,
            boxShadow: '0 14px 32px rgba(15, 23, 42, 0.22)',
            padding: 16,
            zIndex: 2000,
            boxSizing: 'border-box'
          }}
          onClick={(e) => e.stopPropagation()}
        >
          <PathPanelContent
            isMobile={true}
            pathFromRef={pathFromRef}
            pathToRef={pathToRef}
            pathFromValRef={pathFromValRef}
            pathToValRef={pathToValRef}
            pathInfo={pathInfo}
            pathListRef={pathListRef}
            handleClearPath={handleClearPath}
            onFindPath={runPathFind}
            onClose={closePathPanel}
          />
        </div>
      )}
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
            minHeight: isHeaderMobile ? 'auto' : 170
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
              justifyContent: 'center',
              alignItems: 'flex-start',
              gap: isHeaderMobile ? 16 : 12,
              // Match the white card's available inner height (170 minHeight
              // minus 12px top + 12px bottom padding) so justifyContent:center
              // has space to work without growing the card. (2026-05-06)
              minHeight: isHeaderMobile ? undefined : 146
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
                {shouldBreakTitle ? (
                  <>The Aspen Grove<br />of Opera Singers</>
                ) : (
                  'The Aspen Grove of Opera Singers'
                )}
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
            {/* Right controls moved to absolute group; previous empty */}
            {/* placeholder removed (2026-05-06) so the title group centers */}
            {/* cleanly within the wrapper's flex column. */}
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
            {/* Welcome line above buttons / hamburger (hide on phones to avoid crowding) */}
            {/* Welcome banner removed (2026-05-06) along with login. */}
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
                {/* Top row (2026-05-07): Help center, Path, Save/Export. */}
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 8
                  }}
                >
                  <div>
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
                  </div>
                  <div style={{ position: 'relative' }}>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        if (showPathPanel) {
                          closePathPanel();
                        } else {
                          openPathPanel();
                        }
                      }}
                      style={{
                        padding: '8px 16px',
                        backgroundColor: showPathPanel ? '#f3f4f6' : '#ffffff',
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
                      Path ▾
                    </button>
                    {!viewportIsPhone && showPathPanel && (
                      <div
                        ref={pathPanelRef}
                        style={{
                          position: 'absolute',
                          top: '110%',
                          left: 0,
                          border: '2px solid #3e96e2',
                          borderRadius: 8,
                          boxShadow: '0 8px 20px rgba(0,0,0,0.12)',
                          padding: 12,
                          backgroundColor: 'white',
                          minWidth: 320,
                          zIndex: 1100
                        }}
                        onClick={(e) => e.stopPropagation()}
                      >
                        <PathPanelContent
                          isMobile={false}
                          pathFromRef={pathFromRef}
                          pathToRef={pathToRef}
                          pathFromValRef={pathFromValRef}
                          pathToValRef={pathToValRef}
                          pathInfo={pathInfo}
                          pathListRef={pathListRef}
                          handleClearPath={handleClearPath}
                          onFindPath={runPathFind}
                          onClose={closePathPanel}
                        />
                      </div>
                    )}
                  </div>
                  {isSaveExportEligible && (
                    <div>
                      {renderSaveExportToggle({ isMobileLayout: isHeaderMobile })}
                    </div>
                  )}
                </div>
                {/* Bottom row (2026-05-07): Back, Forward, Filters (network view only). */}
                {currentView === 'network' && (
                  <div
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: 8,
                      flexWrap: 'wrap'
                    }}
                  >
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
                    <button onClick={() => toggleFilterPanel()} style={{ padding: '8px 16px', backgroundColor: '#ffffff', color: selectedVoiceTypes.size > 0 ? '#1976d2' : '#666', border: '2px solid #3e96e2', borderRadius: '8px', cursor: 'pointer', fontSize: '16px', display: 'flex', alignItems: 'center', gap: '6px', opacity: 1, height: '48px', boxSizing: 'border-box' }}>
                      🔍 Filters
                      {selectedVoiceTypes.size > 0 && (
                        <span style={{ backgroundColor: '#1976d2', color: 'white', borderRadius: '8px', padding: '2px 6px', fontSize: '12px', fontWeight: 'bold' }}>
                          {selectedVoiceTypes.size}
                        </span>
                      )}
                    </button>
                  </div>
                )}
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
              {/* Signed-in identity for mobile, shown inside the menu */}
              {userEmail && (
                <div style={{ marginTop: 8, marginBottom: 8, color: '#374151', fontSize: 14, fontWeight: 600 }}>
                  {`Signed in as ${userEmail.split('@')[0]}`}
                </div>
              )}
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
              <button
                type="button"
                onClick={() => {
                  if (showPathPanel) {
                    closePathPanel();
                  } else {
                    openPathPanel();
                  }
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
                Path
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
                  <button type="button" onClick={() => { goBack(); }} disabled={historyCounts.past === 0} style={{ padding: '12px 16px', border: '2px solid #3e96e2', borderRadius: 12, backgroundColor: '#ffffff', color: '#374151', fontSize: '16px', fontWeight: 600, cursor: historyCounts.past ? 'pointer' : 'not-allowed', opacity: historyCounts.past ? 1 : 0.6 }}>Back</button>
                  <button type="button" onClick={() => { goForward(); }} disabled={historyCounts.future === 0} style={{ padding: '12px 16px', border: '2px solid #3e96e2', borderRadius: 12, backgroundColor: '#ffffff', color: '#374151', fontSize: '16px', fontWeight: 600, cursor: historyCounts.future ? 'pointer' : 'not-allowed', opacity: historyCounts.future ? 1 : 0.6 }}>Forward</button>
                  <button type="button" onClick={() => { toggleFilterPanel(); setShowHeaderMenu(false); }} style={{ padding: '12px 16px', border: '2px solid #3e96e2', borderRadius: 12, backgroundColor: '#ffffff', color: selectedVoiceTypes.size > 0 ? '#1976d2' : '#374151', fontSize: '16px', fontWeight: 600, cursor: 'pointer', display: 'flex', justifyContent: 'center', gap: 8 }}>
                    🔍 Filters
                    {selectedVoiceTypes.size > 0 && (
                      <span style={{ backgroundColor: '#1976d2', color: '#ffffff', borderRadius: 8, padding: '2px 8px', fontSize: 12, fontWeight: 700 }}>
                        {selectedVoiceTypes.size}
                      </span>
                    )}
                  </button>
                </div>
              )}
              {/* Logout button removed (2026-05-07) along with login. */}
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
                    // Snapshot the current graph/results before wiping so Back
                    // can recover. Skip when there's nothing on screen anyway.
                    const hadPriorContent = (Array.isArray(networkData?.nodes) && networkData.nodes.length > 0)
                      || (Array.isArray(searchResults) && searchResults.length > 0);
                    if (hadPriorContent) pushHistory('tab-clear');
                    setSearchType(type.key);
                    setSearchResults([]);
                    setCurrentView('search');
                    setItemDetails(null);
                    setSelectedItem(null);
                    setError('');
                    setSearchQuery('');
                    setNetworkData(sanitizeGraphData({ nodes: [], links: [] }));
                    setHasExecutedSearch(false);
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
                    // On mobile (stacked), keep the button narrower than the input and center it
                    width: isHeaderMobile ? 'auto' : 'auto',
                    alignSelf: isHeaderMobile ? 'center' : undefined
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
                    label: 'Back almost 500 years',
                    image: '/Longest.png',
                    token: '20df2cc8-8add-45e4-a49c-5440d6347715'
                  }, {
                    key: 'books-premieres',
                    label: 'Books &\nPremieres',
                    image: '/BnP.png',
                    token: 'b5715a67-28a4-4add-975b-7a682bc64f4a'
                  }].map(example => (
                    <button
                      key={example.key}
                      type="button"
                      onClick={() => loadViewByToken(example.token, { treatAsSearch: true, centerOnLoad: isHeaderMobile })}
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
              <div style={{ maxWidth: '1200px', margin: '20px auto 0', padding: '0 20px', display: 'flex', justifyContent: 'flex-end', gap: '16px', alignItems: 'center' }}>
                <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '12px' }}>
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
                  <a
                    href="https://www.paypal.biz/sethkeetonvoice"
                    target="_blank"
                    rel="noopener noreferrer"
                    style={{
                      width: '160px',
                      display: 'inline-flex',
                      justifyContent: 'center',
                      alignItems: 'center',
                      padding: '12px 16px',
                      backgroundColor: '#ffffff',
                      color: '#374151',
                      border: '2px solid #3e96e2',
                      borderRadius: '8px',
                      cursor: 'pointer',
                      fontSize: '16px',
                      fontWeight: 600,
                      textDecoration: 'none',
                      boxSizing: 'border-box'
                    }}
                  >
                    Link to Paypal
                  </a>
                </div>
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
          <SupportPanel
            show={showSupportPanel}
            onClose={() => setShowSupportPanel(false)}
            isMobileViewport={isMobileViewport}
            headerWidth={headerWidth}
          />
        )}

        {currentView === 'help' && (
          <Suspense fallback={<div style={{ color: '#fff', textAlign: 'center', marginTop: 40 }}>Loading help…</div>}>
            <HelpCenter onBack={() => setCurrentView('search')} />
          </Suspense>
        )}

        {/* removed legacy help_bak_never_shown block */}

        {/* Active Filter Indicators - show when in network view and filters are active */}
        {currentView === 'network' && (
          <ActiveFilterBar
            selectedVoiceTypes={selectedVoiceTypes}
            toggleVoiceTypeFilter={toggleVoiceTypeFilter}
            clearAllFilters={clearAllFilters}
            isMobileViewport={isMobileViewport}
          />
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

        {/* Old helperMessage pill removed (2026-05-06): unified centered */}
        {/* overlay now lives inside the visualization wrapper below.       */}

        {currentView === 'results' && (
          <SearchResults
            searchResults={searchResults}
            searchType={searchType}
            showResultsHalo={showResultsHalo}
            isHeaderMobile={isHeaderMobile}
            onSelectItem={getItemDetails}
          />
        )}

        {((currentView === 'results' || currentView === 'network') && (networkData.nodes.length > 0 || showPathPanel) || isVocalizing || helperMessage) && (
          <div
            style={{
              position: 'relative',
              width: '100%',
              marginBottom: '30px',
              paddingLeft: 0,
              paddingRight: 0,
              // When the canvas isn't shown but an overlay is, give the
              // wrapper a sensible height so the overlay sits in the same
              // vertical region the canvas would occupy. (2026-05-06)
              minHeight:
                ((currentView === 'results' || currentView === 'network') && (networkData.nodes.length > 0 || showPathPanel))
                  ? undefined
                  : 200,
            }}
          >
            {((currentView === 'results' || currentView === 'network') && (networkData.nodes.length > 0 || showPathPanel)) && (
            <>
            <Suspense fallback={null}>
            <NetworkVisualization
              viewport={viewport}
              setSearchType={setSearchType}
              setHasExecutedSearch={setHasExecutedSearch}
              setLoading={setLoading}
              onPhysicsStabilized={stopVocalizing}
              error={error}
              setError={setError}
              networkData={networkData}
              setNetworkData={setNetworkData}
              shouldRunSimulation={shouldRunSimulation}
              setShouldRunSimulation={setShouldRunSimulation}
              contextMenu={contextMenu}
              setContextMenu={setContextMenu}
              linkContextMenu={linkContextMenu}
              setLinkContextMenu={setLinkContextMenu}
              visualizationHeight={visualizationHeight}
              setVisualizationHeight={setVisualizationHeight}
              selectedNode={selectedNode}
              expandSubmenu={expandSubmenu}
              setExpandSubmenu={setExpandSubmenu}
              profileCard={profileCard}
              setProfileCard={setProfileCard}
              actualCounts={actualCounts}
              currentCenterNode={currentCenterNode}
              isExpansionSimulation={isExpansionSimulation}
              setIsExpansionSimulation={setIsExpansionSimulation}
              showPathPanel={showPathPanel}
              pathInfo={pathInfo}
              setPathInfo={setPathInfo}
              filtersVersion={filtersVersion}
              token={token}
              selectedVoiceTypes={selectedVoiceTypes}
              selectedBirthplaces={selectedBirthplaces}
              birthYearRange={birthYearRange}
              deathYearRange={deathYearRange}
              showFilterPanel={showFilterPanel}
              fetchWithRetry={fetchWithRetry}
              pathApiUnavailableRef={pathApiUnavailableRef}
              pathFromRef={pathFromRef}
              pathToRef={pathToRef}
              pathFromValRef={pathFromValRef}
              pathToValRef={pathToValRef}
              pathOverlayRef={pathOverlayRef}
              prePathNetworkRef={prePathNetworkRef}
              pathPanelRef={pathPanelRef}
              pathListRef={pathListRef}
              nodeClickTimeoutRef={nodeClickTimeoutRef}
              lastTappedNodeIdRef={lastTappedNodeIdRef}
              suppressNextClickRef={suppressNextClickRef}
              simulationRef={simulationRef}
              submenuTimeoutRef={submenuTimeoutRef}
              dragGroupIdsRef={dragGroupIdsRef}
              dragGroupInitialPosRef={dragGroupInitialPosRef}
              dragLeaderInitialPosRef={dragLeaderInitialPosRef}
              dragActiveRef={dragActiveRef}
              svgRef={svgRef}
              centerOnNodeRef={centerOnNodeRef}
              uiZoomRef={uiZoomRef}
              dragSuppressClickRef={dragSuppressClickRef}
              dragStartPosRef={dragStartPosRef}
              longPressClickSuppressRef={longPressClickSuppressRef}
              personCacheRef={personCacheRef}
              handleClearPath={handleClearPath}
              closePathPanel={closePathPanel}
              runPathFind={runPathFind}
              fetchAndCachePersonDetails={fetchAndCachePersonDetails}
              enrichPersonNodes={enrichPersonNodes}
              pushHistory={pushHistory}
              normalizePersonNode={normalizePersonNode}
              extendDateRangesForNodes={extendDateRangesForNodes}
              isNodeVisible={isNodeVisible}
              isLinkVisible={isLinkVisible}
              getNodeOpacity={getNodeOpacity}
              positionNodesWithoutOverlap={positionNodesWithoutOverlap}
              showFullInformation={showFullInformation}
              resolveLinkEndpointId={resolveLinkEndpointId}
              isPlaceholderName={isPlaceholderName}
              sanitizeGraphData={sanitizeGraphData}
              expandAllRelationships={expandAllRelationships}
              expandSpecificRelationship={expandSpecificRelationship}
              clearPendingNodeAction={clearPendingNodeAction}
              handleNodeSingleActivation={handleNodeSingleActivation}
              handleNodeDoubleActivation={handleNodeDoubleActivation}
              dismissOtherNodes={dismissOtherNodes}
              dismissNode={dismissNode}
              getItemDetails={getItemDetails}
              getNodeStyle={getNodeStyle}
              getAccessibleTextColor={getAccessibleTextColor}
              getExpandableRelationshipCounts={getExpandableRelationshipCounts}
              flushPendingHelperMessage={flushPendingHelperMessage}
            />
            </Suspense>
            <div style={{ display: 'flex', justifyContent: 'center', marginTop: '12px' }}>
              <div
                className="network-hint"
                style={{
                  textAlign: 'center',
                  display: 'inline-block',
                  backgroundColor: 'rgba(255,255,255,0.9)',
                  border: 'none',
                  borderRadius: 12,
                  padding: '8px 12px',
                  color: '#111827'
                }}
              >
                {isMobileViewport ? (
                  <>
                    Drag nodes to reposition • Pinch to zoom • Drag to pan • Long-press a node or relationship for more information
                  </>
                ) : (
                  <>
                    Drag nodes to reposition • Scroll to zoom • Drag to pan
                    <span style={{ display: 'block', marginTop: 4 }}>
                      Single-click to expand a node
                    </span>
                    <span style={{ display: 'block', marginTop: 4 }}>
                      Right-click on a node or relationship for more information
                    </span>
                  </>
                )}
              </div>
            </div>
            </>
            )}
            {/* Unified centered status overlay (2026-05-06) */}
            {(isVocalizing || helperMessage) && (
              <div
                style={{
                  position: 'absolute',
                  top: '50%',
                  left: '50%',
                  transform: 'translate(-50%, -50%)',
                  background: 'rgba(255, 255, 255, 0.78)',
                  border: '2px solid #3e96e2',
                  borderRadius: 12,
                  padding: '14px 28px',
                  fontSize: 18,
                  fontWeight: 600,
                  color: '#0f172a',
                  letterSpacing: 0.3,
                  boxShadow: '0 4px 16px rgba(0,0,0,0.12)',
                  pointerEvents: 'none',
                  zIndex: 1500,
                  textAlign: 'center',
                }}
                aria-live="polite"
              >
                {isVocalizing ? 'Vocalizing…' : helperMessage}
              </div>
            )}
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
          <NetworkDetailCards
            itemDetails={itemDetails}
            searchType={searchType}
            selectedItem={selectedItem}
            pushHistory={pushHistory}
            searchForPerson={searchForPerson}
            searchForPersonFromOpera={searchForPersonFromOpera}
            setSearchType={setSearchType}
            setLoading={setLoading}
            setError={setError}
            setItemDetails={setItemDetails}
            setSelectedItem={setSelectedItem}
            setCurrentView={setCurrentView}
            generateNetworkFromDetails={generateNetworkFromDetails}
            setShouldRunSimulation={setShouldRunSimulation}
            API_BASE={API_BASE}
            token={token}
            handleRateLimitResponse={handleRateLimitResponse}
            parseTypedId={parseTypedId}
          />
        )}
      </main>
      {currentView === 'network' && isMobileViewport && !(showFilterPanel || showPathPanel) && (
        <>
          <div className="mobile-toolbar" role="toolbar">
            <button
              type="button"
              className="mobile-toolbar__button"
              onClick={() => {
                if (historyCounts.past > 0) {
                  goBack();
                }
              }}
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
              onClick={() => {
                if (historyCounts.future > 0) {
                  goForward();
                }
              }}
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
              onClick={() => {
                toggleFilterPanel();
              }}
            >
              Filters
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
      <FilterPanel
        getFilterCounts={getFilterCounts}
        getDateRanges={getDateRanges}
        selectedVoiceTypes={selectedVoiceTypes}
        birthRangeIsUserSet={birthRangeIsUserSet}
        deathRangeIsUserSet={deathRangeIsUserSet}
        selectedBirthplaces={selectedBirthplaces}
        filterSectionsOpen={filterSectionsOpen}
        setFilterSectionsOpen={setFilterSectionsOpen}
        birthYearRange={birthYearRange}
        deathYearRange={deathYearRange}
        birthRangeIsUserSetRef={birthRangeIsUserSetRef}
        deathRangeIsUserSetRef={deathRangeIsUserSetRef}
        setBirthRangeIsUserSet={setBirthRangeIsUserSet}
        setDeathRangeIsUserSet={setDeathRangeIsUserSet}
        updateBirthYearRange={updateBirthYearRange}
        updateDeathYearRange={updateDeathYearRange}
        setFiltersVersion={setFiltersVersion}
        showFilterPanel={showFilterPanel}
        setShowFilterPanel={setShowFilterPanel}
        isMobileViewport={isMobileViewport}
        clearAllFilters={clearAllFilters}
        getVisibleVoiceTypes={getVisibleVoiceTypes}
        toggleVoiceTypeFilter={toggleVoiceTypeFilter}
        getVisibleBirthplaces={getVisibleBirthplaces}
        normalizePlaceName={normalizePlaceName}
        toggleBirthplaceFilter={toggleBirthplaceFilter}
      />

      
    </div>
  );
};


export default ClassicalMusicGenealogy;
