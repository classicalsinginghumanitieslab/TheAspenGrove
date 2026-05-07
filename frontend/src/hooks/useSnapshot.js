// Snapshot, history, and undo/redo management for the network visualization.

import { useState } from 'react';
import { DEFAULT_BIRTH_RANGE, DEFAULT_DEATH_RANGE } from '../constants/defaults';
import { normalizeLinks, normalizeDetailsRelationshipSources } from '../utils/normalization';
import { sanitizeGraphData } from '../utils/graphMerge';

const useSnapshot = ({
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
} = {}) => {
  const [historyCounts, setHistoryCounts] = useState({ past: 0, future: 0 });

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
    const birthRangeSnap = Array.isArray(birthYearRange) ? [...birthYearRange] : [...DEFAULT_BIRTH_RANGE];
    const deathRangeSnap = Array.isArray(deathYearRange) ? [...deathYearRange] : [...DEFAULT_DEATH_RANGE];
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
      hasExecutedSearch,
      selectedNodeId: selectedNode ? selectedNode.id : null,
      currentCenterNode,
      visualizationHeight,
      itemDetails: detailsSnap,
      selectedItem: selectedItem ? { ...selectedItem } : null,
      zoom,
    };
  };

  const applySnapshot = (snap, options = {}) => {
    const { restoreFilters = true } = options;
    if (!snap) return;
    // Pin nodes that come with valid saved positions so vis-network's physics
    // doesn't squish them out of the layout that was current when the snapshot
    // was captured. Without this, hitting Back can re-trigger Barnes-Hut on the
    // restored set and compress the graph into an unrecognizable cluster.
    const clonedNodes = snap.nodes.map(n => {
      const copy = { ...n };
      if (Number.isFinite(copy.x) && Number.isFinite(copy.y)) copy.__pinned = true;
      return copy;
    });
    const clonedLinks = snap.links.map(l => ({ ...l }));
    // Normalize link endpoints to string ids to avoid stale object refs
    const normalizedLinks = normalizeLinks(
      clonedLinks.map(l => ({
        ...l,
        source: (typeof l.source === 'string' ? l.source : (l.source && l.source.id) || l.source),
        target: (typeof l.target === 'string' ? l.target : (l.target && l.target.id) || l.target)
      }))
    );
    // Tell NetworkVisualization's sync effect to skip its auto-fit on this
    // setNetworkData call — we're about to push the saved camera state and
    // don't want fit() to overwrite it with a centered framing.
    try {
      if (typeof window !== 'undefined' && snap.zoom) {
        window.__cmg_suppressNextFit = true;
      }
    } catch (_) {}
    setNetworkData(sanitizeGraphData({ nodes: clonedNodes, links: normalizedLinks }));
    // If the snapshot contains explicit positions, preserve layout and build a dormant sim
    const hasPositions = Array.isArray(clonedNodes) && clonedNodes.length > 0 && clonedNodes.every(n => Number.isFinite(n.x) && Number.isFinite(n.y));
    try { setShouldRunSimulation(!hasPositions); } catch (_) {}
    // Force a re-sync of vis-network's DataSet
    try { setFiltersVersion(v => v + 1); } catch (_) {}
    try { setCurrentView(snap.currentView); } catch (_) {}
    try { setSearchType(snap.searchType); } catch (_) {}
    try { setSelectedNode(null); } catch (_) {}
    try { setCurrentCenterNode(snap.currentCenterNode || null); } catch (_) {}
    // Restore saved camera (vis-network scale + view position). Run on a
    // microtask so it lands AFTER setNetworkData's sync has put the nodes
    // in place — moveTo on missing nodes does nothing useful.
    // Force every restored node to its saved position via network.moveNode,
    // because vis-network treats nodes.update({x,y}) on existing nodes as a
    // physics-target hint rather than an authoritative teleport. Without this,
    // restored nodes can end up at their pre-Back positions even when we set
    // x/y in the DataSet update.
    const positionMap = {};
    clonedNodes.forEach((n) => {
      if (n && n.id != null && Number.isFinite(n.x) && Number.isFinite(n.y)) {
        positionMap[n.id] = { x: n.x, y: n.y };
      }
    });
    try {
      const z = snap.zoom;
      if (uiZoomRef && uiZoomRef.current !== undefined && z) uiZoomRef.current = { k: z.k, x: z.x, y: z.y };
      try { if (z) window.__cmg_zoomTransform = { k: z.k, x: z.x, y: z.y }; } catch (_) {}
      // Defer to a microtask so setNetworkData's sync effect has populated the
      // vis-network DataSet first; moveNode and moveTo only work on present nodes.
      setTimeout(() => {
        try {
          if (typeof window !== 'undefined' && typeof window.__cmg_applyPositions === 'function') {
            window.__cmg_applyPositions(positionMap);
          }
          if (z && typeof window !== 'undefined' && typeof window.__cmg_applyZoom === 'function') {
            window.__cmg_applyZoom(z);
          }
        } catch (_) {}
      }, 0);
    } catch (_) {}
    // Restore detail cards state
    try {
      setItemDetails(snap.itemDetails ? normalizeDetailsRelationshipSources(snap.itemDetails) : null);
    } catch (_) {}
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
    try {
      if (typeof snap.hasExecutedSearch === 'boolean') {
        setHasExecutedSearch(snap.hasExecutedSearch);
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
      try { setBirthYearRange([...DEFAULT_BIRTH_RANGE]); } catch (_) {}
      try { setDeathYearRange([...DEFAULT_DEATH_RANGE]); } catch (_) {}
      try { setShowFilterPanel(false); } catch (_) {}
      setTimeout(() => {
        try { resetDateRanges(); } catch (_) {}
      }, 0);
    }
    try { setShowPathPanel(!!snap.showPathPanel); } catch (_) {}
    try { setPathInfo(snap.pathInfo || null); } catch (_) {}
  };

  const pushHistory = (label) => {
    try {
      const snap = createSnapshot();
      historyRef.current.past.push(snap);
      historyRef.current.future = [];
      setHistoryCounts({ past: historyRef.current.past.length, future: 0 });
    } catch (_) {}
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

  return {
    historyCounts,
    setHistoryCounts,
    createSnapshot,
    applySnapshot,
    pushHistory,
    goBack,
    goForward,
  };
};

export default useSnapshot;
