// Graph layout computation utilities (positioning, bounding box, ring placement).
// Requires d3 for computeCenteredTransform.

import * as d3 from 'd3';

export const computeCenteredTransform = (
  nodes = [],
  containerWidth = 0,
  containerHeight = 0,
  padding = 80
) => {
  const positionedNodes = Array.isArray(nodes)
    ? nodes.filter((node) => Number.isFinite(node?.x) && Number.isFinite(node?.y))
    : [];
  const width = Math.max(1, Number(containerWidth) || 0);
  const height = Math.max(1, Number(containerHeight) || 0);
  if (!positionedNodes.length || width <= 0 || height <= 0) return null;
  const xs = positionedNodes.map((node) => node.x);
  const ys = positionedNodes.map((node) => node.y);
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const paddingValue = Math.max(0, Number(padding) || 0);
  const paddedWidth = Math.max(1, (maxX - minX) + paddingValue * 2);
  const paddedHeight = Math.max(1, (maxY - minY) + paddingValue * 2);
  const scaleX = width / paddedWidth;
  const scaleY = height / paddedHeight;
  const scale = Math.min(Math.max(Math.min(scaleX, scaleY), 0.1), 4);
  const centerX = (minX + maxX) / 2;
  const centerY = (minY + maxY) / 2;
  return d3.zoomIdentity
    .translate(width / 2, height / 2)
    .scale(scale)
    .translate(-centerX, -centerY);
};

// Debug logging utilities for layout issues
export const isLayoutDebug = () => (typeof window !== 'undefined' && window.__CMG_DEBUG_LAYOUT === true);
export const debugLog = (kind, data) => {
  try {
    if (typeof window !== 'undefined') {
      window.__CMG_DEBUG_EVENTS = window.__CMG_DEBUG_EVENTS || [];
      window.__CMG_DEBUG_EVENTS.push({ t: Date.now(), kind, data });
    }
    if (isLayoutDebug()) {
      // eslint-disable-next-line no-console
      console.debug(`[cmg-layout] ${kind}`, data);
    }
  } catch (_) {}
};

// Compute the bounding box and centroid of currently positioned nodes
export const computeGraphBBox = (nodes = []) => {
  const positioned = (Array.isArray(nodes) ? nodes : []).filter(
    (n) => Number.isFinite(n?.x) && Number.isFinite(n?.y)
  );
  if (positioned.length === 0) {
    return {
      minX: 0,
      maxX: 0,
      minY: 0,
      maxY: 0,
      cx: 0,
      cy: 0,
      width: 0,
      height: 0,
      radius: 0
    };
  }
  const xs = positioned.map((n) => n.x);
  const ys = positioned.map((n) => n.y);
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const width = Math.max(0, maxX - minX);
  const height = Math.max(0, maxY - minY);
  const cx = (minX + maxX) / 2;
  const cy = (minY + maxY) / 2;
  const radius = Math.max(width, height) / 2;
  return { minX, maxX, minY, maxY, width, height, cx, cy, radius };
};

// Stable pseudo-random angle from a string key
export const stableAngleFromString = (key = '') => {
  const s = String(key || '0');
  let h = 0;
  for (let i = 0; i < s.length; i += 1) h = (h * 31 + s.charCodeAt(i)) >>> 0;
  const frac = (h % 3600) / 10; // 0..360
  return (frac * Math.PI) / 180;
};

// Choose a spawn center outside the current bbox along the vector from centroid -> anchor
export const computeSpawnOutsideBBox = (nodes = [], anchor = { x: 0, y: 0, key: '' }, margin = 280, bounds) => {
  const { cx, cy, radius } = computeGraphBBox(nodes);
  let dx = (Number(anchor?.x) || 0) - cx;
  let dy = (Number(anchor?.y) || 0) - cy;
  const len = Math.hypot(dx, dy);
  if (!Number.isFinite(dx) || !Number.isFinite(dy) || len < 1e-3) {
    const ang = stableAngleFromString(anchor?.key || `${Date.now()}`);
    dx = Math.cos(ang);
    dy = Math.sin(ang);
  } else {
    dx /= len; dy /= len;
  }
  const dist = (Number.isFinite(radius) ? radius : 0) + Math.max(160, Number(margin) || 0);
  let x = cx + dx * dist;
  let y = cy + dy * dist;
  // Optionally clamp into visible bounds to avoid being marked as out-of-bounds & re-positioned
  if (bounds && Number.isFinite(bounds.width) && Number.isFinite(bounds.height)) {
    const pad = Number.isFinite(bounds.pad) ? bounds.pad : 50;
    const w = Math.max(200, bounds.width);
    const h = Math.max(200, bounds.height);
    x = Math.max(pad, Math.min(w - pad, x));
    y = Math.max(pad, Math.min(h - pad, y));
  }
  return { x, y };
};

// Compute a reasonable ring radius for placing a cluster of `count` nodes
export const computeRingRadius = (count, minR = 110, maxR = 220, spacing = 88) => {
  const n = Math.max(1, Number(count) || 0);
  // Spacing-based radius so circumference roughly fits `n` nodes with diameter+gap ~ spacing
  const spacingBased = (n * spacing) / (2 * Math.PI);
  // Gentle linear growth to avoid too small for modest n
  const seed = 90 + n * 8;
  const proposed = Math.max(spacingBased, seed);
  const capped = (Number.isFinite(maxR) && maxR > 0) ? Math.min(maxR, proposed) : proposed;
  return Math.max(minR, capped);
};

export const getExpansionRingConfig = (count) => {
  const n = Math.max(1, Number(count) || 0);
  const win = (typeof window !== 'undefined') ? window : {};
  const overrideMin = Number.isFinite(win?.__CMG_EXPAND_RING_MIN) ? Number(win.__CMG_EXPAND_RING_MIN) : null;
  const overrideSpacing = Number.isFinite(win?.__CMG_EXPAND_RING_SPACING) ? Number(win.__CMG_EXPAND_RING_SPACING) : null;
  let overrideMax = null;
  if (win && typeof win.__CMG_EXPAND_RING_MAX === 'function') {
    try { overrideMax = win.__CMG_EXPAND_RING_MAX(n); } catch (_) { overrideMax = null; }
  } else if (Number.isFinite(win?.__CMG_EXPAND_RING_MAX)) {
    overrideMax = Number(win.__CMG_EXPAND_RING_MAX);
  }

  const min = Math.max(100, overrideMin ?? 180);
  const spacing = Math.max(60, overrideSpacing ?? 120);
  const defaultMax = 280 + n * 12;
  const computedMax = Number.isFinite(overrideMax) ? overrideMax : defaultMax;
  const max = Math.max(min + 40, computedMax);

  return { min, max, spacing };
};
