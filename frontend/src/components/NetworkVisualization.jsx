/**
 * NetworkVisualization — vis-network implementation
 * Replaces the previous D3/SVG version. Accepts the same props from App.jsx.
 */
import React, { useEffect, useRef, useCallback } from 'react';
import { Network } from 'vis-network';
import { DataSet } from 'vis-data';
import ContextMenu from './ContextMenu';
import ProfileCard from './ProfileCard';
import { VOICE_TYPES } from '../constants/voiceTypes';
import { computeRingRadius, getExpansionRingConfig } from '../utils/graphLayout';
import { renderRelationshipSourceLink } from '../utils/renderHelpers';

// ─── Color utilities ──────────────────────────────────────────────────────────

const VOICE_COLOR_MAP = Object.fromEntries(VOICE_TYPES.map(v => [v.name, v.color]));

const getNodeFill = (node) => {
  if (node.type === 'opera' || node.type === 'book') return '#9CA3AF';
  if (!node.voiceType) return '#8cc400';
  return VOICE_COLOR_MAP[node.voiceType] || '#6B7280';
};

const darken = (hex, amount = 0.5) => {
  const m = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  if (!m) return hex;
  return '#' + m.slice(1).map(x =>
    Math.max(0, Math.round(parseInt(x, 16) * (1 - amount))).toString(16).padStart(2, '0')
  ).join('');
};

const getTextColor = (hex) => {
  const m = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  if (!m) return '#ffffff';
  const [r, g, b] = m.slice(1).map(x => {
    const c = parseInt(x, 16) / 255;
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  });
  const L = 0.2126 * r + 0.7152 * g + 0.0722 * b;
  return L > 0.179 ? '#111827' : '#ffffff';
};

// ─── Edge preprocessing ───────────────────────────────────────────────────────

const RIGHT_CLICK_DISABLED_TYPES = new Set(['authored', 'edited', 'wrote']);
const SYMMETRIC_TYPES = new Set(['sibling', 'spouse']);

// Dynamic spring length based on endpoint degrees.
// Low-degree nodes (e.g. 4 connections) stay compact; high-degree nodes spread out.
// Scale: deg=1 → 155px, deg=4 → 185px, deg=71 → 300px (sqrt curve for perceptual evenness).
const edgeSpringLength = (degA, degB) => {
  const maxDeg = Math.max(degA || 1, degB || 1);
  const t = Math.min(Math.sqrt(maxDeg - 1) / Math.sqrt(70), 1);
  return Math.round(155 + t * 145);
};

// Minimum edge length needed for the label (centered at the edge midpoint) to
// clear both endpoint circles. Approximates pixel width as charCount × 7px at
// the 11px font, plus 2× node radius and a small buffer. Multi-line labels use
// the longest line. Returns 0 for empty labels so the spring length wins.
// `circle` shape pads the label by ~8px on each side, so the rendered radius
// sits slightly above the configured size:40 — use 48 to stay outside that.
const NODE_RADIUS_PX = 48;
// Average char width is ~7px at 11px sans-serif but wide letters (m/w) and
// physics-induced edge contraction (centralGravity pulls chains tighter than
// the spring length) push us toward the conservative 8px estimate.
const LABEL_CHAR_PX = 8;
// Breathing room beyond the node circles. Larger here means labels stay clear
// even when physics settles the edge a bit shorter than the spring target.
const LABEL_BUFFER_PX = 40;
const labelClearanceLength = (label) => {
  if (!label) return 0;
  const longestLine = String(label)
    .split('\n')
    .reduce((max, line) => Math.max(max, line.length), 0);
  if (!longestLine) return 0;
  return longestLine * LABEL_CHAR_PX + 2 * NODE_RADIUS_PX + LABEL_BUFFER_PX;
};

const resolveId = (endpoint) =>
  typeof endpoint === 'string' ? endpoint : endpoint?.id;

// expansionCtx = { anchorId, boostLength, newNodeIds } while an expansion is settling.
// Edge IDs are content-derived and stable across runs, so the sync effect can do
// differential add/remove/update on the DataSet instead of clear+add (which would
// restart physics on every rerun).
const buildVisEdges = (links, degreeMap = new Map(), expansionCtx = null) => {
  const groups = new Map();

  links.forEach(link => {
    const src = resolveId(link.source);
    const tgt = resolveId(link.target);
    if (!src || !tgt) return;
    const isSelf = src === tgt;
    const key = isSelf
      ? `${src}::self`
      : [src, tgt].sort().join('::');
    if (!groups.has(key)) groups.set(key, { isSelf, links: [] });
    groups.get(key).links.push({ ...link, _src: src, _tgt: tgt });
  });

  const edges = [];

  groups.forEach(({ isSelf, links: grp }, key) => {
    if (isSelf) {
      // Sort self-loops deterministically so identical graphs produce identical IDs
      const sorted = [...grp].sort((a, b) =>
        (a.type || a.label || '').localeCompare(b.type || b.label || '')
      );
      sorted.forEach((link, idx) => {
        edges.push({
          id: `self|${link._src}|${link.type || link.label || ''}|${idx}`,
          from: link._src,
          to: link._tgt,
          label: link.label || link.type || '',
          _originalLinks: [link],
          _rightClickDisabled: RIGHT_CLICK_DISABLED_TYPES.has(link.type),
          selfReference: { size: 20, angle: Math.PI / 4, renderBehindTheNode: true },
          arrows: { to: { enabled: true, scaleFactor: 0.8 } },
          smooth: { type: 'curvedCW', roundness: 0.5 },
        });
      });
      return;
    }

    const [nodeA, nodeB] = key.split('::');
    const forward = grp.filter(l => l._src === nodeA);
    const backward = grp.filter(l => l._src === nodeB);
    const hasBoth = forward.length > 0 && backward.length > 0;

    if (forward.length > 0) {
      const isSymmetric = forward.every(l => SYMMETRIC_TYPES.has(l.type) || SYMMETRIC_TYPES.has((l.label || '').toLowerCase()));
      const baseLength = edgeSpringLength(degreeMap.get(nodeA) || 1, degreeMap.get(nodeB) || 1);
      // Boost any edge where either endpoint is the anchor OR a brand-new node.
      // This prevents cross-cluster pulls (e.g. new node that also connects to an
      // already-visible node) from fighting the expansion and causing early stabilisation.
      const isBoostEdge = expansionCtx && (
        nodeA === expansionCtx.anchorId || nodeB === expansionCtx.anchorId ||
        expansionCtx.newNodeIds?.has(nodeA) || expansionCtx.newNodeIds?.has(nodeB)
      );
      const fwdLabel = forward.map(l => l.label || l.type || '').join('\n');
      const length = Math.max(
        isBoostEdge ? Math.max(baseLength, expansionCtx.boostLength) : baseLength,
        labelClearanceLength(fwdLabel),
      );
      edges.push({
        id: `fwd|${nodeA}|${nodeB}`,
        from: nodeA,
        to: nodeB,
        label: fwdLabel,
        _originalLinks: forward,
        _rightClickDisabled: forward.every(l => RIGHT_CLICK_DISABLED_TYPES.has(l.type)),
        smooth: hasBoth ? { type: 'curvedCW', roundness: 0.2 } : { enabled: false },
        arrows: { to: { enabled: !isSymmetric, scaleFactor: 0.8 } },
        length,
      });
    }

    if (backward.length > 0) {
      const isSymmetric = backward.every(l => SYMMETRIC_TYPES.has(l.type) || SYMMETRIC_TYPES.has((l.label || '').toLowerCase()));
      const baseLength = edgeSpringLength(degreeMap.get(nodeB) || 1, degreeMap.get(nodeA) || 1);
      const isBoostEdge = expansionCtx && (
        nodeA === expansionCtx.anchorId || nodeB === expansionCtx.anchorId ||
        expansionCtx.newNodeIds?.has(nodeA) || expansionCtx.newNodeIds?.has(nodeB)
      );
      const bwdLabel = backward.map(l => l.label || l.type || '').join('\n');
      const length = Math.max(
        isBoostEdge ? Math.max(baseLength, expansionCtx.boostLength) : baseLength,
        labelClearanceLength(bwdLabel),
      );
      edges.push({
        id: `bwd|${nodeA}|${nodeB}`,
        from: nodeB,
        to: nodeA,
        label: bwdLabel,
        _originalLinks: backward,
        _rightClickDisabled: backward.every(l => RIGHT_CLICK_DISABLED_TYPES.has(l.type)),
        smooth: hasBoth ? { type: 'curvedCCW', roundness: 0.2 } : { enabled: false },
        arrows: { to: { enabled: !isSymmetric, scaleFactor: 0.8 } },
        length,
      });
    }
  });

  return edges;
};

// Diff two edge arrays and apply to a DataSet, minimising physics restarts:
//  - edges that disappeared → remove
//  - edges that appeared → add
//  - edges whose length/label changed → update
//  - unchanged edges → left alone (vis-network preserves their physics state)
const applyEdgeDiff = (edgesDs, prevEdgeStateMap, nextEdges) => {
  if (!edgesDs) return new Map();
  const nextById = new Map(nextEdges.map(e => [e.id, e]));
  const toRemoveIds = [];
  prevEdgeStateMap.forEach((_, id) => { if (!nextById.has(id)) toRemoveIds.push(id); });
  const toAddEdges = [];
  const toUpdateEdges = [];
  nextById.forEach((edge, id) => {
    const prev = prevEdgeStateMap.get(id);
    if (!prev) {
      toAddEdges.push(edge);
    } else if (prev.length !== edge.length || prev.label !== edge.label) {
      toUpdateEdges.push(edge);
    }
  });
  try {
    if (toRemoveIds.length) edgesDs.remove(toRemoveIds);
    if (toAddEdges.length) edgesDs.add(toAddEdges);
    if (toUpdateEdges.length) edgesDs.update(toUpdateEdges);
  } catch (_) {}
  // Build fresh state map for next diff
  const nextState = new Map();
  nextEdges.forEach(e => nextState.set(e.id, { length: e.length, label: e.label }));
  return nextState;
};

// ─── Node building ────────────────────────────────────────────────────────────

// Truncate so vis-network's `circle` shape (which auto-sizes to label) stays uniform.
// Soft-wraps to roughly two lines at widthConstraint:80 + 11px font, then ellipsises.
const MAX_LABEL_CHARS = 24;
const truncateLabel = (s) => {
  const str = String(s || '');
  return str.length > MAX_LABEL_CHARS ? str.slice(0, MAX_LABEL_CHARS - 1).trimEnd() + '…' : str;
};

const buildVisNode = (node, isSelected, opacity) => {
  const fill = getNodeFill(node);
  const textColor = getTextColor(fill);

  // Hyphen-aware line break (2026-05-06): any hyphen followed by non-whitespace
  // becomes "hyphen + newline" so hyphenated names like "Jean-Claude" wrap as
  // "Jean-" / "Claude". The lookahead skips trailing hyphens and the ellipsis
  // produced by truncateLabel, so "X-…" stays on one line.
  const displayName = truncateLabel(node.name).replace(/-(?=[^\s…])/g, '-\n');
  let label;
  if (node.type === 'opera') label = `🎵\n${displayName}`;
  else if (node.type === 'book') label = `📚\n${displayName}`;
  else label = displayName;

  const out = {
    id: node.id,
    label,
    title: undefined,
    color: {
      background: fill,
      border: fill,                                      // no border
      highlight: { background: darken(fill, 0.15), border: fill },
      hover: { background: fill, border: fill },
    },
    borderWidth: 0,
    opacity: typeof opacity === 'number' ? opacity : 1,
    shape: 'circle',                                     // uniform circles, label inside
    size: 40,
    font: { size: 11, color: textColor, align: 'center', multi: false, strokeWidth: 0 },
    widthConstraint: { minimum: 80, maximum: 80 },       // force uniform size
    physics: !node.__pinDuringExpansion && !node.__pinned,
  };

  if (Number.isFinite(node.x)) out.x = node.x;
  if (Number.isFinite(node.y)) out.y = node.y;
  // __pinned means "user placed — don't let physics move it" but DO leave it draggable.
  // Using fixed:true here would block user re-drag, which isn't the intent.
  if (node.__pinned) out.fixed = { x: false, y: false };

  return out;
};

// ─── vis-network options ──────────────────────────────────────────────────────

const VIS_OPTIONS = {
  physics: {
    enabled: true,
    solver: 'barnesHut',
    barnesHut: {
      gravitationalConstant: -3000, // overridden dynamically per graph density
      centralGravity: 0.05,
      springLength: 200,            // overridden per-edge based on endpoint degrees
      springConstant: 0.04,
      damping: 0.4,
      avoidOverlap: 1,
    },
    // fit:false — we run our own fit() in the stabilizationIterationsDone handler
    // so it only fires when we want it (replacement transitions). Letting vis-network
    // also auto-fit caused racing camera animations after data changes.
    stabilization: { enabled: true, iterations: 200, updateInterval: 50, fit: false },
    minVelocity: 0.75,
  },
  interaction: {
    hover: true,
    multiselect: false,
    selectConnectedEdges: false,
    tooltipDelay: 99999,
    navigationButtons: false,
    keyboard: false,
    zoomView: true,
    dragView: true,
    dragNodes: true,
  },
  nodes: {
    shape: 'circle',
    size: 40,
    font: { size: 11, align: 'center', multi: false },
    borderWidth: 0,
    chosen: false,
    widthConstraint: { minimum: 80, maximum: 80 },
  },
  edges: {
    arrows: { to: { enabled: true, scaleFactor: 0.8 } },
    arrowStrikethrough: false,
    color: { color: '#ffffff', highlight: '#ffffff', hover: '#ffffff' },
    font: { size: 11, align: 'top', color: '#ffffff', strokeWidth: 0 },
    smooth: { enabled: false },
    chosen: false,
    width: 1.5,
    selectionWidth: 2.5,
  },
  layout: { randomSeed: 42, improvedLayout: false },
};

// ─── Component ────────────────────────────────────────────────────────────────

const NetworkVisualization = (props) => {
  const {
    networkData = { nodes: [], links: [] },
    setNetworkData,
    handleNodeSingleActivation,
    handleNodeDoubleActivation,
    isNodeVisible,
    getNodeOpacity,
    selectedNode,
    contextMenu = { show: false },
    setContextMenu,
    linkContextMenu = { show: false },
    setLinkContextMenu,
    profileCard = { show: false },
    setProfileCard,
    expandSubmenu,
    setExpandSubmenu,
    actualCounts = {},
    visualizationHeight = 600,
    token,
    selectedVoiceTypes,
    selectedBirthplaces,
    birthYearRange,
    deathYearRange,
    filtersVersion,
    simulationRef,
    uiZoomRef,
    showFullInformation,
    expandAllRelationships,
    expandSpecificRelationship,
    clearPendingNodeAction,
    dismissOtherNodes,
    dismissNode,
    getExpandableRelationshipCounts,
    pushHistory,
    showHelperMessage,
    flushPendingHelperMessage,
    viewport = {},
    submenuTimeoutRef,
    closePathPanel,
    showPathPanel,
    // onPhysicsStabilized (2026-05-06): App.jsx wires the centered status
    // overlay to clear via this callback once the FULL graph (nodes > 1)
    // finishes its physics pass.
    onPhysicsStabilized,
    // unused D3-era refs accepted to avoid prop warnings
    svgRef, centerOnNodeRef, dragSuppressClickRef, dragStartPosRef,
    longPressClickSuppressRef, dragGroupIdsRef, dragGroupInitialPosRef,
    dragLeaderInitialPosRef, dragActiveRef, nodeClickTimeoutRef,
    lastTappedNodeIdRef, suppressNextClickRef, personCacheRef,
  } = props;

  const containerRef = useRef(null);
  const networkRef = useRef(null);
  const nodesDatasetRef = useRef(null);
  const edgesDatasetRef = useRef(null);
  const prevNodesRef = useRef(new Map()); // id → node data snapshot
  const expansionBoostRef = useRef(new Set()); // edge IDs with temporary length boost
  const expansionContextRef = useRef(null);    // { anchorId, boostLength } — persists across rerenders
  const boostStartTimeRef = useRef(0);         // timestamp when boost was last applied
  const tempUnpinnedRef = useRef([]);          // [{ id, wasUserPinned }] neighbors freed during expansion
  const prevSyncSigRef = useRef('');           // last sync signature — skip redundant reruns
  const prevEdgeStateRef = useRef(new Map());  // id → { length, label } for differential edge updates
  const pendingFitRef = useRef(false);         // true when a graph replacement is awaiting fit-on-stabilize
  const pendingFitTimerRef = useRef(null);     // safety-net timer id for the above
  const draggedNodeIdRef = useRef(null);       // id of node currently being dragged, for top-of-stack redraw
  const justDraggedAtRef = useRef(0);          // timestamp of last dragEnd, used to suppress trailing clicks
  const clearPendingNodeActionRef = useRef(null);

  // Stable refs for callbacks (avoids stale closures in vis-network handlers)
  const networkDataRef = useRef(networkData);
  const handleSingleRef = useRef(handleNodeSingleActivation);
  const handleDoubleRef = useRef(handleNodeDoubleActivation);
  const isNodeVisibleRef = useRef(isNodeVisible);
  const getNodeOpacityRef = useRef(getNodeOpacity);
  const selectedNodeRef = useRef(selectedNode);
  const onPhysicsStabilizedRef = useRef(onPhysicsStabilized);

  // Update refs during render so vis-network event handlers always see the
  // latest props/closures. useEffect-based updates leave a tiny gap between
  // commit and effect-fire where a click handler could read a stale closure.
  networkDataRef.current = networkData;
  handleSingleRef.current = handleNodeSingleActivation;
  handleDoubleRef.current = handleNodeDoubleActivation;
  isNodeVisibleRef.current = isNodeVisible;
  getNodeOpacityRef.current = getNodeOpacity;
  selectedNodeRef.current = selectedNode;
  clearPendingNodeActionRef.current = clearPendingNodeAction;
  onPhysicsStabilizedRef.current = onPhysicsStabilized;

  // ── Sync zoom state for useSaveExport ────────────────────────────────────────
  const syncZoom = useCallback(() => {
    const net = networkRef.current;
    if (!net) return;
    try {
      const scale = net.getScale();
      const pos = net.getViewPosition();
      const z = { k: scale, x: Math.round(pos.x), y: Math.round(pos.y) };
      if (uiZoomRef) uiZoomRef.current = z;
      window.__cmg_zoomTransform = z;
    } catch (_) {}
  }, [uiZoomRef]);

  // ── Mirror per-node opacity onto edges + labels (2026-05-06) ────────────────
  // Filtered-out nodes (opacity 0.15) shouldn't have full-opacity edges or
  // edge labels drawing attention to them. Each edge gets opacity = min of
  // its two endpoints; the label color is set as rgba so its alpha tracks
  // the same dimming.
  const syncEdgeOpacity = () => {
    const edges = edgesDatasetRef.current;
    if (!edges || !isNodeVisible || !getNodeOpacity) return;
    const currNodes = networkDataRef.current?.nodes || [];
    const opacityMap = new Map();
    currNodes.forEach(n => {
      const op = isNodeVisible(n) ? getNodeOpacity(n) : 0.15;
      opacityMap.set(n.id, op);
    });
    const allEdges = edges.get() || [];
    const updates = allEdges.map(e => {
      const fromOp = opacityMap.has(e.from) ? opacityMap.get(e.from) : 1;
      const toOp = opacityMap.has(e.to) ? opacityMap.get(e.to) : 1;
      const op = Math.min(fromOp, toOp);
      return {
        id: e.id,
        color: { color: '#ffffff', highlight: '#ffffff', hover: '#ffffff', opacity: op },
        font: { size: 11, align: 'top', color: `rgba(255, 255, 255, ${op})`, strokeWidth: 0 },
      };
    });
    if (updates.length) {
      try { edges.update(updates); } catch (_) {}
    }
  };

  // ── Sync node positions back into networkData objects (for useSaveExport) ───
  const syncPositions = useCallback(() => {
    const net = networkRef.current;
    if (!net) return;
    try {
      const positions = net.getPositions();
      (networkDataRef.current.nodes || []).forEach(n => {
        if (positions[n.id]) {
          n.x = positions[n.id].x;
          n.y = positions[n.id].y;
        }
      });
    } catch (_) {}
  }, []);

  // ── Initialize vis-network (once) ────────────────────────────────────────────
  useEffect(() => {
    if (!containerRef.current) return;

    const nodes = new DataSet([]);
    const edges = new DataSet([]);
    nodesDatasetRef.current = nodes;
    edgesDatasetRef.current = edges;

    const net = new Network(containerRef.current, { nodes, edges }, VIS_OPTIONS);
    networkRef.current = net;

    // Expose simulation adapter for App.jsx (freezes physics when menus open)
    if (simulationRef) {
      simulationRef.current = {
        stop: () => { try { net.stopSimulation(); } catch (_) {} },
        alphaTarget: () => ({ restart: () => { try { net.startSimulation(); } catch (_) {} } }),
      };
    }

    // Expose center-on-node for App.jsx expansion calls
    if (centerOnNodeRef) {
      centerOnNodeRef.current = (nodeId, opts = {}) => {
        try {
          const positions = net.getPositions([nodeId]);
          if (positions[nodeId]) {
            net.moveTo({
              position: positions[nodeId],
              scale: net.getScale(),
              animation: { duration: opts.duration || 500, easingFunction: 'easeInOutQuad' },
            });
          }
        } catch (_) {}
      };
    }

    // Expose center function for path finding and external callers
    window.__cmg_centerGraph = () => {
      try { net.fit({ animation: { duration: 500, easingFunction: 'easeInOutQuad' } }); } catch (_) {}
    };
    // Apply a snapshot's saved camera state (k=scale, x/y=view-position).
    // Used by useSnapshot.applySnapshot when navigating Back/Forward so the
    // restored view matches what the user had on screen at snapshot time.
    window.__cmg_applyZoom = (z) => {
      if (!z || typeof z.k !== 'number' || typeof z.x !== 'number' || typeof z.y !== 'number') return;
      try {
        net.moveTo({ position: { x: z.x, y: z.y }, scale: z.k, animation: false });
        syncZoom();
      } catch (_) {}
    };
    // Force-apply node positions via moveNode (which always teleports the node)
    // because nodes.update({x,y}) on an already-existing node is treated by
    // vis-network as a physics target rather than an authoritative teleport.
    // Used by useSnapshot.applySnapshot after setNetworkData to put the graph
    // back to exactly the saved layout.
    window.__cmg_applyPositions = (positionMap) => {
      if (!positionMap || typeof positionMap !== 'object') return;
      try {
        const ids = Object.keys(positionMap);
        ids.forEach((id) => {
          const p = positionMap[id];
          if (!p || !Number.isFinite(p.x) || !Number.isFinite(p.y)) return;
          if (!net.body?.nodes?.[id]) return;
          try { net.moveNode(id, p.x, p.y); } catch (e) { /* ignore */ }
        });
        // Pin the restored layout: stop any in-flight simulation and disable
        // physics globally so Barnes-Hut can't drift the just-teleported nodes
        // into a new equilibrium. The next user action (expand/click/drag) goes
        // through the existing path that re-enables physics on demand.
        try { net.stopSimulation(); } catch (_) {}
        try { net.setOptions({ physics: { enabled: false } }); } catch (_) {}
      } catch (_) {}
    };
    window.__cmg_resetZoom = () => {
      try { net.fit({ animation: { duration: 300, easingFunction: 'easeInOutQuad' } }); } catch (_) {}
    };

    // After physics settles, retract expansion-boosted edge lengths back to normal.
    // Guard: ignore stabilized events that fire within 1200ms of boost being applied —
    // the sanitizeGraphData re-render in App.jsx can restart physics almost immediately,
    // causing a premature stabilized event before nodes have had time to spread out.
    //
    // ── BEGIN smooth-retraction lerp (2026-05-05) ─────────────────────────────
    // Originally clearExpansionBoost did a single-shot edge rebuild from
    // boostLength → baseLength, which makes physics jump and reads as a
    // separate "phase 2" animation. The version below lerps boostLength → 0
    // over RETRACTION_MS so spring contraction is gradual and overlaps the
    // 400ms camera moveTo into a single visible motion. Steady-state edge
    // values at the end of the lerp are identical to the old single-shot
    // rebuild, so this is a visual change only.
    //
    // Closure variable that lets a rapid second expansion implicitly cancel
    // the in-flight lerp via the `expansionContextRef.current` check inside
    // tick(); also cleared at unmount (see RAF-cancel in the unmount return).
    let boostRetractionRaf = null;

    const clearExpansionBoost = () => {
      // Cancel any prior in-flight retraction lerp before starting a new one.
      if (boostRetractionRaf != null) {
        cancelAnimationFrame(boostRetractionRaf);
        boostRetractionRaf = null;
      }

      // Capture context BEFORE clearing the ref. We need the original
      // boostLength as the lerp's start value, plus anchorId / newNodeIds so
      // the synthetic ctx we pass to buildVisEdges during the lerp matches
      // the original boost shape (same edges treated as boost edges).
      const ctx = expansionContextRef.current;
      const anchorId = ctx?.anchorId || null;
      const startBoostLength = ctx?.boostLength || 0;
      const newNodeIds = ctx?.newNodeIds || new Set();

      expansionContextRef.current = null;
      expansionBoostRef.current.clear();
      tempUnpinnedRef.current = [];

      const edgesDs = edgesDatasetRef.current;
      if (!edgesDs) return;

      // Helper: rebuild + apply edges with whichever expansion-context shape
      // the caller passes. Used both for the lerp ticks and the final
      // unboosted rebuild.
      const buildAndApply = (synthCtx) => {
        const currLinks = networkDataRef.current.links || [];
        const dm = new Map();
        currLinks.forEach(link => {
          const s = resolveId(link.source), t = resolveId(link.target);
          if (s && t && s !== t) {
            dm.set(s, (dm.get(s) || 0) + 1);
            dm.set(t, (dm.get(t) || 0) + 1);
          }
        });
        const rebuilt = buildVisEdges(currLinks, dm, synthCtx);
        prevEdgeStateRef.current = applyEdgeDiff(edgesDs, prevEdgeStateRef.current, rebuilt);
      };

      // Pan camera to the anchor in parallel with the spring retraction.
      // moveTo's 400ms duration matches RETRACTION_MS so they end together.
      if (anchorId) {
        try {
          const positions = net.getPositions([anchorId]);
          if (positions[anchorId]) {
            net.moveTo({
              position: positions[anchorId],
              scale: net.getScale(),
              animation: { duration: 400, easingFunction: 'easeInOutQuad' },
            });
          }
        } catch (_) {}
      }

      // Nothing to retract — produce unboosted edges in one shot and exit.
      // Matches the old single-shot behavior for the no-boost case.
      if (startBoostLength <= 0) {
        buildAndApply(null);
        return;
      }

      const RETRACTION_MS = 400;
      const start = performance.now();

      const tick = (now) => {
        // Self-abort if a new expansion has taken over while we were lerping.
        // The new boost edges shouldn't be overwritten by stale anchor data.
        if (expansionContextRef.current) {
          boostRetractionRaf = null;
          return;
        }
        const elapsed = now - start;
        const tRaw = Math.min(1, elapsed / RETRACTION_MS);
        // easeInOutQuad — matches the camera moveTo's easing.
        const t = tRaw < 0.5 ? 2 * tRaw * tRaw : 1 - Math.pow(-2 * tRaw + 2, 2) / 2;
        const currBoost = startBoostLength * (1 - t);
        const synthCtx = currBoost > 0
          ? { anchorId, boostLength: currBoost, newNodeIds }
          : null;
        buildAndApply(synthCtx);
        if (tRaw < 1) {
          boostRetractionRaf = requestAnimationFrame(tick);
        } else {
          boostRetractionRaf = null;
        }
      };
      boostRetractionRaf = requestAnimationFrame(tick);
    };
    // ── END smooth-retraction lerp ────────────────────────────────────────────

    net.on('stabilized', () => {
      // Always capture the truly-final positions back into networkData. The
      // 'stabilizationIterationsDone' event fires after the initial 200-iter
      // fast-forward, but vis-network keeps integrating until minVelocity is
      // reached — and 'stabilized' is the event that fires at THAT moment.
      // Without this sync, networkData.nodes hold the post-fast-forward
      // (still-drifting) positions, so snapshots captured later for Back/
      // Forward and Save reflect a layout that was never actually on screen.
      syncPositions();
      // Notify the Vocalizing overlay that the FULL graph has settled. Skip
      // the temporary single-node placeholder triggerNodeSearch sets while
      // it's fetching — it would clear the overlay before the real graph
      // arrives. (2026-05-06)
      try {
        const nodeCount = (networkDataRef.current?.nodes || []).length;
        if (nodeCount > 1 && typeof onPhysicsStabilizedRef.current === 'function') {
          onPhysicsStabilizedRef.current();
        }
      } catch (_) {}
      if (!expansionContextRef.current) return;
      const elapsed = Date.now() - boostStartTimeRef.current;
      const MIN_BOOST_MS = 1000;
      if (elapsed >= MIN_BOOST_MS) {
        clearExpansionBoost();
      } else {
        setTimeout(clearExpansionBoost, MIN_BOOST_MS - elapsed);
      }
    });

    // ── Event: single click ────────────────────────────────────────────────────
    // Suppression window: Hammer.js (vis-network's gesture lib) can fire BOTH a
    // drag pair (dragStart → dragEnd) AND a 'tap' for the same gesture when the
    // total movement is small. vis-network forwards that tap as a 'click' event,
    // so a real drag that didn't move far ends up triggering the click handler
    // (expansion / drilldown). Ignore clicks that arrive within 250ms of a
    // dragEnd or while a drag is still in progress.
    const POST_DRAG_CLICK_GUARD_MS = 250;
    net.on('click', (params) => {
      if (draggedNodeIdRef.current) return;
      if (Date.now() - justDraggedAtRef.current < POST_DRAG_CLICK_GUARD_MS) return;
      if (params.nodes.length === 1) {
        const nodeId = params.nodes[0];
        const node = (networkDataRef.current.nodes || []).find(n => n.id === nodeId);
        if (node && handleSingleRef.current) {
          handleSingleRef.current(node);
        }
        return;
      }
      // Background click — close all transient overlays: context menu, edge
      // tooltip, profile card, and the path-finding panel + summary.
      if (params.nodes.length === 0 && params.edges.length === 0) {
        if (setContextMenu) setContextMenu({ show: false });
        if (setLinkContextMenu) setLinkContextMenu({ show: false });
        if (setProfileCard) setProfileCard({ show: false, data: null });
        if (closePathPanel) closePathPanel();
      }
    });

    // ── Event: double click ────────────────────────────────────────────────────
    net.on('doubleClick', (params) => {
      if (params.nodes.length === 1) {
        const nodeId = params.nodes[0];
        const node = (networkDataRef.current.nodes || []).find(n => n.id === nodeId);
        if (node && handleDoubleRef.current) {
          handleDoubleRef.current(node);
        }
      }
    });

    // ── Event: right-click / long-press ───────────────────────────────────────
    // vis-network's `oncontext` populates params.nodes / params.edges from the
    // CURRENT SELECTION rather than a hit-test at the click point. So an
    // unselected node gets `nodes: []` and our menu never opens. Hit-test
    // ourselves using the pointer vis-network already gave us.
    const handleContextEvent = (params) => {
      const event = params.event;
      if (event && event.preventDefault) event.preventDefault();
      const pointerDom = params.pointer?.DOM;
      if (!pointerDom) return;
      const x = event?.clientX ?? 0;
      const y = event?.clientY ?? 0;

      let nodeId = null;
      let edgeId = null;
      try { nodeId = net.getNodeAt(pointerDom) ?? null; } catch (_) {}
      if (!nodeId) {
        try { edgeId = net.getEdgeAt(pointerDom) ?? null; } catch (_) {}
      }

      if (nodeId) {
        const node = (networkDataRef.current.nodes || []).find(n => n.id === nodeId);
        if (!node) return;
        if (setContextMenu) setContextMenu({ show: true, node, x, y });
        if (setLinkContextMenu) setLinkContextMenu({ show: false });
        return;
      }

      if (edgeId) {
        const edgeData = edgesDatasetRef.current?.get(edgeId);
        if (!edgeData || edgeData._rightClickDisabled) return;
        const primary = edgeData._originalLinks?.[0] || {};
        if (setLinkContextMenu) {
          setLinkContextMenu({ show: true, x, y, link: primary, allLinks: edgeData._originalLinks || [] });
        }
        if (setContextMenu) setContextMenu({ show: false });
      }
    };

    net.on('oncontext', handleContextEvent);
    // hold fires for both mouse and touch; only use for touch (desktop uses oncontext)
    net.on('hold', (params) => {
      const evt = params.event;
      const isTouch = evt?.pointerType === 'touch' || String(evt?.type ?? '').includes('touch');
      if (!isTouch) return;
      handleContextEvent(params);
    });

    // ── Event: drag isolation ─────────────────────────────────────────────────
    // vis-network automatically restarts physics when the user grabs a node.
    // Any node with physics:true in its DataSet entry is then subject to spring
    // forces from the dragged node — which is why neighbors follow along. Freezing
    // every non-dragged node here severs that coupling without affecting the user's
    // ability to drag any other node individually.
    net.on('dragStart', (params) => {
      // ANY drag — whether it's a node drag or a background pan — means the
      // previous click was effectively a "press" gesture, so cancel any pending
      // expansion + prefetch from that click. Critically, this must run BEFORE
      // the early-return below; otherwise a click-then-pan (background drag,
      // nodes:[]) leaves the 220ms expansion timer alive and fires anyway.
      try { clearPendingNodeActionRef.current?.(); } catch (_) {}
      if (!params.nodes || params.nodes.length !== 1) return;
      const draggedId = params.nodes[0];
      // Track the dragged node so afterDrawing can re-render it on top of every
      // other paint pass — nodes AND arrows. vis-network's draw order is
      // edges → nodes → arrows, so a dragged node is otherwise covered by the
      // arrow-pass even if its own node-pass position was on top.
      draggedNodeIdRef.current = draggedId;
      // Freeze physics on every other node so edge springs don't pull connected
      // nodes after the dragged one. Doing this synchronously inside dragStart
      // (with hundreds of nodes) noticeably stalls vis-network's gesture loop,
      // so defer one tick — the user's drag motion proceeds without lag and the
      // freeze applies before any spring force has a chance to take hold.
      const nodesDs = nodesDatasetRef.current;
      if (!nodesDs) return;
      setTimeout(() => {
        try {
          const allIds = nodesDs.getIds();
          const updates = allIds
            .filter(id => id !== draggedId)
            .map(id => ({ id, physics: false }));
          if (updates.length) nodesDs.update(updates);
        } catch (_) {}
      }, 0);
    });

    // ── Event: stabilization done ─────────────────────────────────────────────
    net.on('stabilizationIterationsDone', () => {
      // Stop simulation — nodes are settled; user drag will briefly restart it
      try { net.setOptions({ physics: { enabled: false } }); } catch (_) {}
      syncPositions();
      syncZoom();
      // If a graph replacement queued a fit, reframe now that nodes have actually
      // settled (running fit() before stabilization frames a transient compressed
      // state and makes everything look unusably tiny).
      if (pendingFitRef.current) {
        pendingFitRef.current = false;
        if (pendingFitTimerRef.current) {
          clearTimeout(pendingFitTimerRef.current);
          pendingFitTimerRef.current = null;
        }
        try {
          net.fit({ animation: { duration: 300, easingFunction: 'easeInOutQuad' } });
        } catch (_) {}
      }
      // Release expansion pins
      if (nodesDatasetRef.current) {
        // Re-enable physics on nodes that were only pinned during expansion
        const toUnpin = (networkDataRef.current.nodes || [])
          .filter(n => n.__pinDuringExpansion)
          .map(n => ({ id: n.id, physics: false })); // keep off — already settled
        if (toUnpin.length && nodesDatasetRef.current) {
          try { nodesDatasetRef.current.update(toUnpin); } catch (_) {}
          toUnpin.forEach(({ id }) => {
            const n = (networkDataRef.current.nodes || []).find(nd => nd.id === id);
            if (n) delete n.__pinDuringExpansion;
          });
        }
      }
    });

    // ── Event: zoom ────────────────────────────────────────────────────────────
    net.on('zoom', syncZoom);
    // 'zoom' only fires for user-driven zoom (mouse wheel / pinch). Programmatic
    // fit() / moveTo() with animation emit 'animationFinished' instead — without
    // listening here, __cmg_zoomTransform stays stale (e.g. left at the default
    // {k:1,x:0,y:0}) so history snapshots can't restore the real camera.
    net.on('animationFinished', syncZoom);
    // Single-node drag only moves the grabbed node — no cluster drag. Dropping
    // freezes the node via physics:false but leaves fixed:false so the user can
    // grab and redrag it freely.
    // Re-render every node ON TOP of arrowheads. vis-network's draw order is
    // edges → nodes → arrows, so unrelated edges' arrowheads can otherwise land
    // on top of any node they overlap (whether dragged or not). Painting all
    // nodes once more after the arrow pass keeps nodes consistently above arrows.
    net.on('afterDrawing', (ctx) => {
      const bodyNodes = net.body?.nodes;
      const indices = net.body?.nodeIndices;
      if (!bodyNodes || !Array.isArray(indices)) return;
      for (const id of indices) {
        const node = bodyNodes[id];
        if (node && typeof node.draw === 'function') {
          try { node.draw(ctx); } catch (_) {}
        }
      }
    });

    net.on('dragEnd', (params) => {
      draggedNodeIdRef.current = null;
      // Stamp the moment the drag ended so the click handler can refuse trailing
      // 'click' events that Hammer.js sometimes emits for short-distance drags.
      justDraggedAtRef.current = Date.now();
      syncZoom();
      syncPositions();
      if (!params.nodes || params.nodes.length === 0) return;
      const positions = net.getPositions(params.nodes);
      // Freeze dropped nodes in place without hard-fixing them — physics: false stops
      // the simulation from tugging them, but the user can still grab and redrag.
      // fixed: {x:true,y:true} would block user drag too, which is not what we want.
      const datasetUpdates = params.nodes.map(id => ({
        id,
        x: positions[id]?.x,
        y: positions[id]?.y,
        physics: false,
        fixed: { x: false, y: false },
      }));
      try { nodesDatasetRef.current.update(datasetUpdates); } catch (_) {}
      // Persist on networkData objects so sync cycles don't undo the position
      params.nodes.forEach(id => {
        const node = (networkDataRef.current.nodes || []).find(n => n.id === id);
        if (node && positions[id]) {
          node.x = positions[id].x;
          node.y = positions[id].y;
          node.__pinned = true;
        }
      });
    });

    return () => {
      try { net.destroy(); } catch (_) {}
      networkRef.current = null;
      nodesDatasetRef.current = null;
      edgesDatasetRef.current = null;
      // Reset diff-tracking refs so a StrictMode remount (or any future remount)
      // doesn't bail out of the sync effect via a stale signature match.
      prevSyncSigRef.current = '';
      prevNodesRef.current = new Map();
      prevEdgeStateRef.current = new Map();
      expansionContextRef.current = null;
      expansionBoostRef.current = new Set();
      tempUnpinnedRef.current = [];
      pendingFitRef.current = false;
      draggedNodeIdRef.current = null;
      if (pendingFitTimerRef.current) {
        clearTimeout(pendingFitTimerRef.current);
        pendingFitTimerRef.current = null;
      }
      // smooth-retraction lerp cleanup (2026-05-05)
      if (boostRetractionRaf != null) {
        cancelAnimationFrame(boostRetractionRaf);
        boostRetractionRaf = null;
      }
      window.__cmg_centerGraph = null;
      window.__cmg_resetZoom = null;
      window.__cmg_applyZoom = null;
      window.__cmg_applyPositions = null;
      try { window.__cmg_suppressNextFit = false; } catch (_) {}
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Sync networkData → vis-network DataSets ───────────────────────────────
  useEffect(() => {
    const nodes = nodesDatasetRef.current;
    const edges = edgesDatasetRef.current;
    if (!nodes || !edges) return;

    const currNodes = networkData.nodes || [];
    const currLinks = networkData.links || [];

    const currNodeMap = new Map(currNodes.map(n => [n.id, n]));
    const prevIds = new Set(prevNodesRef.current.keys());
    const currIds = new Set(currNodeMap.keys());

    const selectedId = selectedNode?.id;

    // Signature-based early-return. App.jsx's sanitizeGraphData useEffect re-emits
    // an equivalent networkData object on every change, which would otherwise trigger
    // a redundant edge rebuild + physics restart mid-expansion. Bail when the graph
    // is semantically unchanged AND selection is unchanged.
    // Style fingerprint: include voiceType so post-fact enrichment (e.g. path
    // visualization fetching voice types after the graph mounts) re-runs the
    // sync and pushes new colors into the vis-network DataSet.
    const styleSig = currNodes
      .map(n => `${n.id}:${n.voiceType || ''}`)
      .sort()
      .join(';');
    const nodeSig = [...currIds].sort().join(',');
    const linkSig = currLinks
      .map(l => {
        const s = resolveId(l.source), t = resolveId(l.target);
        return `${s}>${t}:${l.type || l.label || ''}`;
      })
      .sort()
      .join('|');
    const currSig = `${nodeSig}#${linkSig}#${selectedId || ''}#${styleSig}`;
    if (currSig === prevSyncSigRef.current) {
      return;
    }
    prevSyncSigRef.current = currSig;

    // Removed nodes
    const toRemove = [...prevIds].filter(id => !currIds.has(id));
    if (toRemove.length) {
      try { nodes.remove(toRemove); } catch (_) {}
    }

    // Added nodes. Seed initial positions for any node without explicit x/y — with
    // layout.improvedLayout:false, vis-network otherwise stacks every new node at
    // (0,0), and Barnes-Hut repulsion at r=0 produces NaN forces so the physics
    // engine bails and nothing renders.
    const toAdd = currNodes.filter(n => !prevIds.has(n.id));
    if (toAdd.length) {
      const visnodes = toAdd.map(n => buildVisNode(
        n,
        n.id === selectedId,
        isNodeVisible ? (isNodeVisible(n) ? (getNodeOpacity ? getNodeOpacity(n) : 1) : 0.15) : 1
      ));
      visnodes.forEach(vn => {
        if (!Number.isFinite(vn.x)) vn.x = (Math.random() - 0.5) * 200;
        if (!Number.isFinite(vn.y)) vn.y = (Math.random() - 0.5) * 200;
      });
      try { nodes.add(visnodes); } catch (_) {}
      // React StrictMode double-mount can leave the Network bound to the previous
      // DataSet instance. Detect that mismatch and rebind — setData re-reads the
      // current DataSet contents (the 18 nodes we just added) and re-attaches event
      // listeners for future changes.
      const net = networkRef.current;
      if (net && (net.body?.data?.nodes !== nodes || net.body?.data?.edges !== edges)) {
        try { net.setData({ nodes, edges }); } catch (_) {}
      }
      // Re-enable physics briefly so new nodes settle, then auto-stop via minVelocity
      if (networkRef.current) {
        try { networkRef.current.setOptions({ physics: { enabled: true } }); } catch (_) {}
      }
    }

    // Updated nodes (selection, position, etc.)
    const toUpdate = currNodes
      .filter(n => prevIds.has(n.id))
      .map(n => buildVisNode(
        n,
        n.id === selectedId,
        isNodeVisible ? (isNodeVisible(n) ? (getNodeOpacity ? getNodeOpacity(n) : 1) : 0.15) : 1
      ));
    if (toUpdate.length) {
      try { nodes.update(toUpdate); } catch (_) {}
    }

    // Compute visible degree for each node (drives per-edge spring lengths)
    const degreeMap = new Map();
    currLinks.forEach(link => {
      const src = resolveId(link.source);
      const tgt = resolveId(link.target);
      if (src && tgt && src !== tgt) {
        degreeMap.set(src, (degreeMap.get(src) || 0) + 1);
        degreeMap.set(tgt, (degreeMap.get(tgt) || 0) + 1);
      }
    });
    const maxDeg = degreeMap.size ? Math.max(...degreeMap.values()) : 1;

    // Scale global repulsion to match overall graph density
    // deg=1 → -3000, deg=71 → -8000 (sqrt curve keeps mid-range from feeling over-repelled)
    const gravConst = -(3000 + Math.min(Math.sqrt(maxDeg - 1) / Math.sqrt(70), 1) * 5000);
    if (networkRef.current) {
      try { networkRef.current.setOptions({ physics: { barnesHut: { gravitationalConstant: Math.round(gravConst) } } }); } catch (_) {}
    }

    // On expansion: compute context BEFORE building edges so the boost bakes in
    // from the start — including on subsequent rerenders (e.g. App.jsx's sanitize
    // useEffect) that would otherwise overwrite a separately-patched boost.
    if (toAdd.length > 0) {
      const newIds = new Set(toAdd.map(n => n.id));
      const anchorCounts = new Map();
      currLinks.forEach(link => {
        const s = resolveId(link.source), t = resolveId(link.target);
        if (newIds.has(s) && !newIds.has(t)) anchorCounts.set(t, (anchorCounts.get(t) || 0) + 1);
        if (newIds.has(t) && !newIds.has(s)) anchorCounts.set(s, (anchorCounts.get(s) || 0) + 1);
      });

      if (anchorCounts.size > 0) {
        const anchorId = [...anchorCounts.entries()].sort((a, b) => b[1] - a[1])[0][0];
        const anchorNode = (networkDataRef.current.nodes || []).find(n => n.id === anchorId);
        const { min: rMin, max: rMax, spacing: rSpacing } = getExpansionRingConfig(toAdd.length);
        const ringRadius = computeRingRadius(toAdd.length, rMin, rMax, rSpacing);
        // Boost length controls how far the springs WANT new nodes to sit during
        // the settling phase (vs ringRadius where they are initially placed).
        // Larger multiplier = more dramatic spread, then more dramatic retraction.
        // 1.4 + 30 gives a gentle outward pull — enough space for Barnes-Hut to
        // distribute neighbors evenly without producing a noticeable "shrink"
        // phase when the boost is cleared.
        const boostLength = Math.round(ringRadius * 1.4 + 30);

        // Store context so every edge rebuild (this run AND future rerenders) uses it.
        // newNodeIds lets buildVisEdges boost ALL edges touching new nodes — not just
        // anchor edges — so cross-cluster connections (e.g. new node that also connects
        // to an already-visible node) don't pull new nodes back prematurely.
        expansionContextRef.current = { anchorId, boostLength, newNodeIds: newIds };
        boostStartTimeRef.current = Date.now();

        // Free the anchor so physics can move it to empty space
        if (anchorNode && !anchorNode.__pinned) {
          delete anchorNode.__pinDuringExpansion;
          try { nodes.update([{ id: anchorId, physics: true, fixed: { x: false, y: false } }]); } catch (_) {}
        }

        // Find immediate existing neighbors of the anchor for gap-detection and push.
        // We keep them PINNED so they don't drift toward their other connections and
        // collide with each other. We just nudge any that sit inside the ring radius
        // outward so the new nodes have room.
        const neighborIds = new Set();
        currLinks.forEach(link => {
          const s = resolveId(link.source), t = resolveId(link.target);
          if (s === anchorId && !newIds.has(t)) neighborIds.add(t);
          if (t === anchorId && !newIds.has(s)) neighborIds.add(s);
        });
        tempUnpinnedRef.current = []; // nothing to restore; neighbors stay pinned

        const net = networkRef.current;
        const anchorPos = net ? net.getPositions([anchorId])[anchorId] : null;

        if (anchorPos) {
          // Push existing neighbors inside ring radius outward, keeping them pinned.
          // Track pushed positions so the gap algorithm uses the updated angles.
          const pushedPositions = new Map();
          if (neighborIds.size > 0) {
            const pushUpdates = [];
            neighborIds.forEach(id => {
              const nPos = net.getPositions([id])[id];
              if (!nPos) return;
              const dx = nPos.x - anchorPos.x;
              const dy = nPos.y - anchorPos.y;
              const dist = Math.sqrt(dx * dx + dy * dy) || 1;
              if (dist < ringRadius) {
                const scale = (ringRadius + 20) / dist;
                const nx = anchorPos.x + dx * scale;
                const ny = anchorPos.y + dy * scale;
                pushUpdates.push({ id, x: nx, y: ny });
                pushedPositions.set(id, { x: nx, y: ny });
              } else {
                pushedPositions.set(id, nPos);
              }
            });
            if (pushUpdates.length > 0) {
              try { nodes.update(pushUpdates); } catch (_) {}
            }
          }

          if (toAdd.length > 0) {
            // Build angle list from pushed (or current) neighbor positions
            const existingAngles = [];
            neighborIds.forEach(id => {
              const pos = pushedPositions.get(id) || net.getPositions([id])[id];
              if (!pos) return;
              existingAngles.push(Math.atan2(pos.y - anchorPos.y, pos.x - anchorPos.x));
            });

            // Determine placement arc: largest gap when neighbors exist, full circle otherwise
            let gapCenter = 0;
            let usableArc = Math.PI * 2;

            if (existingAngles.length > 0) {
              existingAngles.sort((a, b) => a - b);

              // Find the largest gap between consecutive existing-neighbor angles (including wraparound)
              let maxGap = 0;
              let gapStart = existingAngles[0];
              for (let i = 0; i < existingAngles.length; i++) {
                const curr = existingAngles[i];
                const next = i < existingAngles.length - 1
                  ? existingAngles[i + 1]
                  : existingAngles[0] + Math.PI * 2;
                const gap = next - curr;
                if (gap > maxGap) { maxGap = gap; gapStart = curr; }
              }

              gapCenter = gapStart + maxGap / 2;
              usableArc = Math.min(maxGap * 0.8, Math.PI * 1.5);
            }

            const posUpdates = toAdd.map((newNode, idx) => {
              // Use slice-centers so nodes fill the arc evenly with equal margins at both ends
              const t = (2 * idx + 1) / (2 * toAdd.length);
              const angle = gapCenter - usableArc / 2 + t * usableArc;
              const x = anchorPos.x + ringRadius * Math.cos(angle);
              const y = anchorPos.y + ringRadius * Math.sin(angle);
              const dataNode = (networkDataRef.current.nodes || []).find(n => n.id === newNode.id);
              if (dataNode) { dataNode.x = x; dataNode.y = y; }
              return { id: newNode.id, x, y };
            });
            try { nodes.update(posUpdates); } catch (_) {}
          }
        }
      }
    }

    // Rebuild edges — expansionContextRef is read inside buildVisEdges so the boost
    // survives any number of rerenders until stabilized clears it. Differential update
    // preserves physics state for unchanged edges.
    const visEdges = buildVisEdges(currLinks, degreeMap, expansionContextRef.current);
    prevEdgeStateRef.current = applyEdgeDiff(edges, prevEdgeStateRef.current, visEdges);

    // Dim any new edges to match their endpoints' filter state (2026-05-06).
    // Without this, an expansion under an active filter adds full-opacity
    // edges/labels even when one endpoint is filtered out.
    syncEdgeOpacity();

    // Update prevNodesRef
    prevNodesRef.current = currNodeMap;

    // Viewport reset: fresh canvas getting its first node, OR a total graph replacement
    // (new search from a populated canvas). vis-network's stabilization.fit=true only
    // triggers after a real stabilization pass — a single node barely stabilizes, so
    // the viewport inherits the previous session's zoom/pan. Explicit fit is reliable.
    const isFirstNode = prevIds.size === 0 && currNodes.length > 0;
    // Treat as a replacement if most of the old graph was dropped — covers the
    // "click a search result" case where the clicked node's id carries over but
    // the rest of the old result set is gone, leaving the viewport stuck at the
    // old zoom level (e.g. zoomed out to fit hundreds of nodes).
    const isFullReplace =
      prevIds.size > 0 &&
      toAdd.length > 0 &&
      toRemove.length >= Math.ceil(prevIds.size * 0.5);
    // Also refit when the graph grows from a trivial state (≤2 nodes) into a
    // larger one. Happens after the click→clear→fetch flow: the viz is briefly
    // showing just the clicked node, then the full network arrives.
    const grewFromTrivial = prevIds.size > 0 && prevIds.size <= 2 &&
                            toAdd.length > 0 && currNodes.length > prevIds.size;
    // Allow Back/Forward (snapshot restore) to bypass auto-fit so the saved
    // camera position takes effect instead of being overwritten by fit().
    const suppressFit = (typeof window !== 'undefined' && window.__cmg_suppressNextFit === true);
    if (suppressFit) {
      try { window.__cmg_suppressNextFit = false; } catch (_) {}
    }
    if (!suppressFit && (isFirstNode || isFullReplace || grewFromTrivial)) {
      if (currNodes.length <= 2) {
        // Trivial graphs barely stabilize. fit() also caps zoom at maxZoomLevel,
        // which leaves a single 40px node tiny on a wide canvas — focus() with an
        // explicit scale renders it at a usable size.
        const targetId = currNodes[0]?.id;
        setTimeout(() => {
          try {
            if (targetId && networkRef.current?.focus) {
              networkRef.current.focus(targetId, { scale: 1.5, animation: false });
            } else {
              networkRef.current?.fit({ animation: false });
            }
            // animation:false → no animationFinished event, so sync manually.
            syncZoom();
          } catch (_) {}
        }, 60);
      } else {
        // Multi-node graphs: defer fit to the stabilizationIterationsDone handler
        // so the camera frames the SETTLED layout, not a mid-compression transient.
        pendingFitRef.current = true;
        if (pendingFitTimerRef.current) clearTimeout(pendingFitTimerRef.current);
        // Safety net — if stabilization never fires (e.g. all nodes pinned, nothing
        // for physics to do), fit anyway after a generous wait. The stabilization
        // handler clears this timer to prevent a second racing fit.
        pendingFitTimerRef.current = setTimeout(() => {
          pendingFitTimerRef.current = null;
          if (!pendingFitRef.current) return;
          pendingFitRef.current = false;
          try {
            networkRef.current?.fit({ animation: { duration: 300, easingFunction: 'easeInOutQuad' } });
          } catch (_) {}
        }, 2500);
      }
    }

    // If graph was cleared, stop physics
    if (currNodes.length === 0 && networkRef.current) {
      try { networkRef.current.stopSimulation(); } catch (_) {}
    }
  }, [networkData, selectedNode]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Filter opacity sync ──────────────────────────────────────────────────────
  useEffect(() => {
    const nodes = nodesDatasetRef.current;
    if (!nodes || !isNodeVisible || !getNodeOpacity) return;
    // Only touch nodes already in the DataSet. DataSet.update() on a non-existent
    // id creates a bare entry (no color/label) — on the first search, filter deps
    // fire before the main sync has populated the DataSet, which was producing
    // unlabeled light-blue default nodes.
    const existingIds = new Set(nodes.getIds());
    const currNodes = networkData.nodes || [];
    const updates = currNodes
      .filter(n => existingIds.has(n.id))
      .map(n => ({
        id: n.id,
        opacity: isNodeVisible(n) ? getNodeOpacity(n) : 0.15,
      }));
    if (updates.length) {
      try { nodes.update(updates); } catch (_) {}
    }
    // Cascade dimming onto connected edges + labels.
    syncEdgeOpacity();
  }, [filtersVersion, selectedVoiceTypes, selectedBirthplaces,
    birthYearRange?.[0], birthYearRange?.[1],
    deathYearRange?.[0], deathYearRange?.[1]]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Freeze physics when menus are open ───────────────────────────────────────
  useEffect(() => {
    const net = networkRef.current;
    if (!net) return;
    const anyOpen = contextMenu.show || linkContextMenu.show;
    try {
      if (anyOpen) net.stopSimulation();
      // Don't auto-resume — let next user action or stabilization do it
    } catch (_) {}
  }, [contextMenu.show, linkContextMenu.show]);

  // ── Container height ─────────────────────────────────────────────────────────
  useEffect(() => {
    if (!networkRef.current || !containerRef.current) return;
    try { networkRef.current.redraw(); } catch (_) {}
  }, [visualizationHeight]);

  // ── Link tooltip rendering ────────────────────────────────────────────────────
  const LinkTooltip = () => {
    if (!linkContextMenu.show) return null;
    const { x, y, link = {}, allLinks = [] } = linkContextMenu;
    const isPreviewed = (allLinks.length > 0 ? allLinks : [link])
      .some(l => l.type === 'premiered');

    const primaryRole = (allLinks.length > 0 ? allLinks : [link])
      .find(l => l.type === 'premiered')?.role || null;

    // ── BEGIN edge-tooltip source link unification (2026-05-06) ──────────────
    // Use the same renderRelationshipSourceLink helper that NetworkDetailCards
    // uses so the edge right-click tooltip renders sources clickably whenever
    // the side cards do. Was previously a homegrown sourceText/sourceUrl pair
    // with simpler URL extraction that missed some inline-URL cases.
    // To revert: restore the old sourceText / deriveRelationshipSourceUrl
    // computation (see git history) and the inline rendering further down.
    const sourceArgs = [
      { text: link.teacher_rel_source_text, url: link.teacher_rel_source_url },
      { text: link.opera_source_text, url: link.opera_source_url },
      link.relationshipSourceDisplay,
      link.relationship_source_display,
      link.relationship_source,
      link.teacher_rel_source_text,
      link.teacher_rel_source_url,
      link.opera_source_text,
      link.opera_source_url,
      link.teacher_rel_source,
      link.sourceUrl,
      link.sourceInfo,
      link.source,
    ];
    const sourceContent = renderRelationshipSourceLink(...sourceArgs);
    // ── END edge-tooltip source link unification ─────────────────────────────

    return (
      <div
        style={{
          position: 'fixed',
          top: y,
          left: x,
          background: '#ffffff',
          border: '2px solid #3e96e2',
          borderRadius: 8,
          padding: '10px 14px',
          boxShadow: '0 4px 16px rgba(0,0,0,0.15)',
          zIndex: 2000,
          minWidth: 200,
          maxWidth: 320,
          fontSize: 13,
          color: '#374151',
        }}
        onMouseDown={e => e.stopPropagation()}
        onClick={e => e.stopPropagation()}
      >
        <button
          onClick={() => setLinkContextMenu({ show: false })}
          style={{
            position: 'absolute', top: 6, right: 8,
            background: 'none', border: 'none', cursor: 'pointer',
            fontSize: 16, color: '#6b7280', lineHeight: 1,
          }}
          aria-label="Close"
        >×</button>
        {isPreviewed && (
          <div style={{ fontWeight: 600, marginBottom: 4 }}>Premiered role in</div>
        )}
        {primaryRole && (
          <div style={{ marginBottom: 6, color: '#1e40af' }}>Role: {primaryRole}</div>
        )}
        <div style={{ color: '#6b7280', fontSize: 12, marginBottom: 2 }}>Relationship source:</div>
        <div style={{ wordBreak: 'break-word' }}>
          {sourceContent
            || <span style={{ color: '#9ca3af', fontStyle: 'italic' }}>Not provided</span>}
        </div>
      </div>
    );
  };

  // ── Render ────────────────────────────────────────────────────────────────────
  const isMobile = viewport.isPhone || viewport.isTablet;

  return (
    <div style={{ position: 'relative', width: '100%', height: visualizationHeight }}>
      {/* vis-network canvas mount point — blue container matches original */}
      <div
        ref={containerRef}
        style={{
          position: 'relative',
          width: '100%',
          height: '100%',
          border: isMobile ? '4px solid #FFFFFF' : '6px solid #FFFFFF',
          borderRadius: '8px',
          backgroundColor: '#3e96e2',
          overflow: 'hidden',
          boxSizing: 'border-box',
        }}
        onContextMenu={e => e.preventDefault()}
      />

      {/* Node context menu */}
      {contextMenu.show && contextMenu.node && (
        <ContextMenu
          contextMenu={contextMenu}
          setContextMenu={setContextMenu}
          actualCounts={actualCounts}
          networkData={networkData}
          expandSubmenu={expandSubmenu}
          setExpandSubmenu={setExpandSubmenu}
          submenuTimeoutRef={submenuTimeoutRef}
          showFullInformation={showFullInformation}
          expandAllRelationships={expandAllRelationships}
          expandSpecificRelationship={expandSpecificRelationship}
          dismissOtherNodes={dismissOtherNodes}
          dismissNode={dismissNode}
          getExpandableRelationshipCounts={getExpandableRelationshipCounts}
        />
      )}

      {/* Edge right-click tooltip */}
      <LinkTooltip />

      {/* Profile card */}
      {profileCard.show && (
        <ProfileCard
          show={profileCard.show}
          data={profileCard.data}
          onClose={() => setProfileCard({ show: false, data: null })}
          // Use isPhone (not isMobile = isPhone||isTablet) so the breakpoint
          // matches the .mobile-profile-card CSS at max-width: 767px. Otherwise
          // tablet widths (768–1023) get the mobile inline style with no visible
          // background/border, while the CSS class is also dormant.
          isMobileViewport={viewport.isPhone}
          showPathPanel={showPathPanel}
        />
      )}
    </div>
  );
};

export default NetworkVisualization;
