// Pure functions for merging and sanitizing graph data.
// No React, no D3 dependencies.

import { normalizePersonNode, normalizeLinks } from './normalization';
import { normalizeNodeId, registerNodeAliases, resolveAliasIdFromMap, stripOperaBookFields } from './nodeUtils';
import { resolveLinkEndpointId, isPlaceholderName } from './nodeFactory';

export const mergeNodeAttributes = (base, incoming) => {
  if (!incoming) return base;
  const result = base || {};
  Object.entries(incoming).forEach(([key, value]) => {
    if (value === undefined || value === null) return;
    if (key === 'id') {
      result.id = normalizeNodeId(value);
      return;
    }
    if (key === 'name') {
      const incomingName = String(value).trim();
      const existingName = String(result.name || '').trim();
      const shouldReplace =
        !existingName ||
        /^unknown\b/i.test(existingName);
      if (incomingName && shouldReplace) {
        result.name = incomingName;
      } else if (!existingName) {
        result.name = incomingName || result.id || '';
      }
      return;
    }
    if (key === 'x' || key === 'y' || key === 'vx' || key === 'vy' || key === 'fx' || key === 'fy' || key === 'homeX' || key === 'homeY') {
      if (!Number.isFinite(result[key])) {
        result[key] = value;
      }
      return;
    }
    if (key === 'recentlyExpandedAt' || key === 'expansionBatchId') {
      result[key] = value;
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
  if (result.type === 'person') {
    const normalized = normalizePersonNode(result);
    return normalized || result;
  }
  stripOperaBookFields(result);
  return result;
};

export const finalizeNodeCandidate = (candidate) => {
  if (!candidate) return null;
  const normalizedId = normalizeNodeId(candidate.id ?? candidate.name);
  if (!normalizedId) return null;
  if (isPlaceholderName(normalizedId)) return null;
  const normalizedName = candidate.name ? String(candidate.name).trim() : normalizedId;
  let nodeObj = { ...candidate, id: normalizedId, name: normalizedName };
  if (!Number.isFinite(nodeObj.x)) nodeObj.x = undefined;
  if (!Number.isFinite(nodeObj.y)) nodeObj.y = undefined;
  if (nodeObj.type === 'person') {
    const normalized = normalizePersonNode(nodeObj);
    if (normalized && normalized !== nodeObj) {
      nodeObj = normalized;
    }
  } else {
    stripOperaBookFields(nodeObj);
  }
  return nodeObj;
};

export const createLinkKey = (sourceIdRaw, targetIdRaw, type) => {
  const sourceId = normalizeNodeId(sourceIdRaw);
  const targetId = normalizeNodeId(targetIdRaw);
  if (!sourceId || !targetId) return '';
  const typeKey = String(type || '').toLowerCase();
  return JSON.stringify([sourceId, targetId, typeKey]);
};

export const buildLinkKeySet = (links = []) => {
  const keys = new Set();
  (Array.isArray(links) ? links : []).forEach((link) => {
    const sourceId = resolveLinkEndpointId(link?.source);
    const targetId = resolveLinkEndpointId(link?.target);
    if (!sourceId || !targetId) return;
    const key = createLinkKey(sourceId, targetId, link?.type);
    if (key) keys.add(key);
  });
  return keys;
};

export const normalizeLinkForMerge = (link) => {
  if (!link) return { key: '', link: null };
  const sourceId = resolveLinkEndpointId(link.source);
  const targetId = resolveLinkEndpointId(link.target);
  if (!sourceId || !targetId) return { key: '', link: null };
  const normalizedLink = { ...link, source: sourceId, target: targetId };
  return {
    key: createLinkKey(sourceId, targetId, link.type),
    link: normalizedLink
  };
};

export const sanitizeGraphData = (graph) => {
  if (!graph) return { nodes: [], links: [] };
  const nodeAccumulator = new Map();
  (graph.nodes || []).forEach(node => {
    const normalizedId = normalizeNodeId(node?.id ?? node?.name);
    if (!normalizedId || isPlaceholderName(normalizedId)) return;
    const normalizedName = String(node?.name ?? '').trim() || normalizedId;
    const base = nodeAccumulator.get(normalizedId) || { id: normalizedId, name: normalizedName };
    nodeAccumulator.set(normalizedId, mergeNodeAttributes(base, { ...node, id: normalizedId, name: normalizedName }));
  });

  const sanitizedNodes = Array.from(nodeAccumulator.values()).map(node => {
    if (!node) return node;
    if (node.type === 'person') {
      const normalized = normalizePersonNode(node);
      return normalized || node;
    }
    stripOperaBookFields(node);
    return node;
  });

  const validIds = new Set(sanitizedNodes.map(n => n.id));
  const sanitizedLinks = [];
  (graph.links || []).forEach(link => {
    const sourceId = normalizeNodeId(
      typeof link?.source === 'string'
        ? link.source
        : link?.source?.id ?? link?.source?.name
    );
    const targetId = normalizeNodeId(
      typeof link?.target === 'string'
        ? link.target
        : link?.target?.id ?? link?.target?.name
    );
    if (!sourceId || !targetId) return;
    if (isPlaceholderName(sourceId) || isPlaceholderName(targetId)) return;
    if (!validIds.has(sourceId) || !validIds.has(targetId)) return;
    sanitizedLinks.push({ ...link, source: sourceId, target: targetId });
  });

  return {
    nodes: sanitizedNodes,
    links: normalizeLinks(sanitizedLinks)
  };
};

export const sanitizeIncrementalGraph = (nodes, links, { anchorId, existingNodeIds } = {}) => {
  const normalizedAnchorId = normalizeNodeId(anchorId);
  const baseNodes = Array.isArray(nodes) ? [...nodes] : [];
  const stubbedNodeIds = new Set();
  const existingIdsSet = existingNodeIds instanceof Set
    ? existingNodeIds
    : Array.isArray(existingNodeIds)
      ? new Set(existingNodeIds.map(id => normalizeNodeId(id)))
      : null;
  const baseNodeIds = new Set(
    baseNodes
      .map(node => normalizeNodeId(node?.id ?? node?.name))
      .filter(Boolean)
  );
  const ensureStubForId = (id) => {
    const normalizedId = normalizeNodeId(id);
    if (!normalizedId) return;
    if (baseNodeIds.has(normalizedId)) return;
    baseNodes.push({ id: normalizedId, name: normalizedId });
    baseNodeIds.add(normalizedId);
    stubbedNodeIds.add(normalizedId);
  };
  if (normalizedAnchorId) {
    ensureStubForId(normalizedAnchorId);
  }
  (Array.isArray(links) ? links : []).forEach(link => {
    const sourceId = normalizeNodeId(resolveLinkEndpointId(link?.source));
    const targetId = normalizeNodeId(resolveLinkEndpointId(link?.target));
    if (existingIdsSet) {
      if (existingIdsSet.has(sourceId)) {
        ensureStubForId(sourceId);
      }
      if (existingIdsSet.has(targetId)) {
        ensureStubForId(targetId);
      }
    }
  });
  const graphForSanitization = { nodes: baseNodes, links };
  const sanitized = sanitizeGraphData(graphForSanitization);
  const filteredNodes = (sanitized.nodes || []).filter(node => {
    const id = normalizeNodeId(node?.id ?? node?.name);
    if (!id) return false;
    if (stubbedNodeIds.has(id)) return false;
    if (isPlaceholderName(id)) return false;
    return true;
  });
  const filteredLinks = (sanitized.links || []).filter(link => {
    const sourceId = normalizeNodeId(resolveLinkEndpointId(link?.source));
    const targetId = normalizeNodeId(resolveLinkEndpointId(link?.target));
    if (!sourceId || !targetId) return false;
    if (isPlaceholderName(sourceId) || isPlaceholderName(targetId)) return false;
    return true;
  });
  return {
    nodes: filteredNodes,
    links: filteredLinks
  };
};

export const mergeNetworkUpdates = (prev, nodesToAdd = [], linksToAdd = [], nodeUpdates) => {
  const prevNodesArray = Array.isArray(prev?.nodes) ? prev.nodes : [];
  const aliasLookup = new Map();
  prevNodesArray.forEach(node => registerNodeAliases(aliasLookup, node));
  const updatesMap = new Map();
  const registerUpdate = (key, payload) => {
    const normalizedKey = normalizeNodeId(key);
    if (!normalizedKey) return;
    const candidate = finalizeNodeCandidate({ id: normalizedKey, ...payload });
    if (!candidate) return;
    const current = updatesMap.get(normalizedKey) || { id: normalizedKey };
    updatesMap.set(normalizedKey, mergeNodeAttributes(current, candidate));
    registerNodeAliases(aliasLookup, { ...candidate, id: normalizedKey });
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
    if (!key) {
      registerNodeAliases(aliasLookup, node);
      return node;
    }
    const patch = updatesMap.get(key);
    if (!patch) {
      registerNodeAliases(aliasLookup, node);
      return node;
    }
    const merged = mergeNodeAttributes(node, patch);
    // If caller explicitly requested a reposition, force-update coordinates and clear fixed positions
    if (patch && patch.__reposition) {
      if (Number.isFinite(patch.x)) merged.x = patch.x;
      if (Number.isFinite(patch.y)) merged.y = patch.y;
      merged.fx = null; merged.fy = null; // unpin so simulation can settle around new spot
      // Also clear userPlaced flag so forces apply normally after programmatic reposition
      if (merged.userPlaced) merged.userPlaced = false;
      merged.vx = 0; merged.vy = 0;
      if (!Number.isFinite(merged.homeX)) merged.homeX = merged.x;
      if (!Number.isFinite(merged.homeY)) merged.homeY = merged.y;
    }
    registerNodeAliases(aliasLookup, merged);
    return merged;
  });
  const updatedNodeIndex = new Map();
  updatedNodes.forEach((node, idx) => {
    const key = normalizeNodeId(node?.id ?? node?.name);
    if (key) updatedNodeIndex.set(key, idx);
  });

  const existingIds = new Set(updatedNodes.map(n => normalizeNodeId(n.id ?? n.name)).filter(Boolean));
  const pendingNewMap = new Map();

  (nodesToAdd || []).forEach(node => {
    if (!node) return;
    const resolvedId = resolveAliasIdFromMap(aliasLookup, node);
    const candidateBase = resolvedId ? { ...node, id: resolvedId } : node;
    const candidate = finalizeNodeCandidate(candidateBase);
    if (!candidate) return;
    registerNodeAliases(aliasLookup, candidate);
    if (existingIds.has(candidate.id)) {
      const idx = updatedNodeIndex.get(candidate.id);
      if (typeof idx === 'number' && idx >= 0) {
        updatedNodes[idx] = mergeNodeAttributes(updatedNodes[idx], candidate);
      }
      const existingPending = pendingNewMap.get(candidate.id);
      if (existingPending) {
        pendingNewMap.set(candidate.id, mergeNodeAttributes(existingPending, candidate));
      }
      return;
    }
    if (pendingNewMap.has(candidate.id)) {
      pendingNewMap.set(candidate.id, mergeNodeAttributes(pendingNewMap.get(candidate.id), candidate));
    } else {
      pendingNewMap.set(candidate.id, candidate);
      existingIds.add(candidate.id);
    }
  });

  const existingLinkKeys = buildLinkKeySet(prev.links);
  const mergedLinks = [...(prev.links || [])];
  const pendingLinkKeys = new Set();
  (linksToAdd || []).forEach(link => {
    const normalized = normalizeLinkForMerge(link);
    if (!normalized.key || !normalized.link) return;
    if (existingLinkKeys.has(normalized.key) || pendingLinkKeys.has(normalized.key)) return;
    pendingLinkKeys.add(normalized.key);
    mergedLinks.push(normalized.link);
  });

  const combinedNodes = [...updatedNodes, ...pendingNewMap.values()].filter(n => {
    const nodeId = normalizeNodeId(n?.id ?? n?.name);
    if (!nodeId) return false;
    if (isPlaceholderName(nodeId)) return false;
    n.id = nodeId;
    if (typeof n.name !== 'string' || !n.name.trim()) {
      n.name = nodeId;
    }
    return true;
  });
  const validIds = new Set(combinedNodes.map(n => n.id));
  const filteredLinks = [];
  (mergedLinks || []).forEach(link => {
    const sourceId = normalizeNodeId(resolveLinkEndpointId(link?.source));
    const targetId = normalizeNodeId(resolveLinkEndpointId(link?.target));
    if (!sourceId || !targetId) return;
    if (!validIds.has(sourceId) || !validIds.has(targetId)) return;
    if (link.source !== sourceId) link.source = sourceId;
    if (link.target !== targetId) link.target = targetId;
    filteredLinks.push(link);
  });

  return sanitizeGraphData({
    nodes: combinedNodes,
    links: filteredLinks
  });
};
