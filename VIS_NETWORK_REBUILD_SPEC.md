# The Aspen Grove — vis-network Visualization Rebuild Specification

**Project:** The Aspen Grove (www.theaspengrove.org)  
**Stack:** React 18, Vite, Railway (backend), Netlify (frontend), Neo4j + MySQL  
**Date:** April 2026  

---

## 1. Overview

Replace the existing `NetworkVisualization.jsx` (D3/SVG, ~3150 lines) with a new implementation using **vis-network** — the same library powering Neo4j Browser. The replacement restores the familiar Neo4j look-and-feel while eliminating the chronic click/drag detection bugs present in the D3 implementation.

### What changes
- `src/components/NetworkVisualization.jsx` — full replacement
- `package.json` — add `vis-network` and `vis-data`

### What does NOT change
- `App.jsx`
- `ContextMenu.jsx`
- `ProfileCard.jsx`
- `PathPanelContent.jsx`
- `NetworkDetailCards.jsx`
- `SearchResults.jsx`
- `hooks/useSaveExport.js`
- All utils, hooks, constants

---

## 2. Package Installation

```bash
npm install vis-network vis-data
```

Import in the component:
```js
import { Network } from 'vis-network';
import { DataSet } from 'vis-data';
```

Optional CSS (navigation buttons, tooltips):
```js
import 'vis-network/styles/vis-network.css';
```

---

## 3. Props Interface

The new component accepts **all the same props as the current one** — extra props are simply ignored. Key props used:

| Prop | Type | Purpose |
|------|------|---------|
| `networkData` | `{ nodes: [], links: [] }` | Source of truth for graph data |
| `setNetworkData` | function | Update graph state |
| `handleNodeSingleActivation` | function(node) | Single-click: expand all |
| `handleNodeDoubleActivation` | function(node) | Double-click: center new search |
| `isNodeVisible` | function(node) → boolean | Filter visibility |
| `getNodeOpacity` | function(node) → 0–1 | Filter opacity |
| `isLinkVisible` | function(link) → boolean | Filter link visibility |
| `selectedNode` | node or null | Currently selected node |
| `contextMenu` | `{ show, node, x, y }` | Context menu state |
| `setContextMenu` | function | Open/close context menu |
| `linkContextMenu` | `{ show, link, x, y }` | Edge tooltip state |
| `setLinkContextMenu` | function | Open/close edge tooltip |
| `profileCard` | object | Profile card state |
| `setProfileCard` | function | Open/close profile card |
| `expandSubmenu` | value | Expand submenu open state |
| `setExpandSubmenu` | function | Control expand submenu |
| `actualCounts` | object | DB relationship counts by node id |
| `visualizationHeight` | number | Canvas height in px |
| `showFilterPanel` | boolean | Filter panel open state |
| `token` | string | Auth token |
| `selectedVoiceTypes` | Set | Active voice type filters |
| `selectedBirthplaces` | Set | Active birthplace filters |
| `birthYearRange` | [min, max] | Birth year filter range |
| `deathYearRange` | [min, max] | Death year filter range |
| `filtersVersion` | number | Filter change counter |
| `simulationRef` | ref | Expose physics control to App.jsx |
| `uiZoomRef` | ref | Expose zoom state to useSaveExport |
| `showFullInformation` | function(node) | Trigger profile card |
| `expandAllRelationships` | function(node) | Expand all rels |
| `expandSpecificRelationship` | function(node, type) | Expand one rel type |
| `clearPendingNodeAction` | function | Cancel pending expansion |
| `dismissOtherNodes` | function(node) | Remove all others |
| `dismissNode` | function(node) | Remove this node |
| `getExpandableRelationshipCounts` | function(node) | Counts for submenu |
| `getNodeStyle` | function(node, selectedNode) | Style helper |
| `pushHistory` | function | Record history step |
| `showHelperMessage` | function(msg, duration) | Status messages |
| `flushPendingHelperMessage` | function | Dismiss helper message |
| `viewport` | object | `{ isPhone, isTablet }` |
| `fetchWithRetry` | function | Retrying fetch wrapper |
| `sanitizeGraphData` | function | Clean graph data |
| `positionNodesWithoutOverlap` | function | Anti-overlap positioning |
| `normalizePersonNode` | function | Normalize person data |
| `extendDateRangesForNodes` | function | Extend filter date ranges |

---

## 4. Data Formats

### Node object (from `networkData.nodes`)
```js
{
  id: string,           // e.g. "person:12345" or normalized name
  name: string,
  type: 'person' | 'opera' | 'book',
  voiceType: string,    // person nodes only
  birthYear: number | null,
  deathYear: number | null,
  birthplace: string | null,
  x: number,            // position (mutable, synced from vis-network)
  y: number,
  // source citation fields (preserved for save/load):
  spelling_source, voice_type_source, dates_source, birthplace_source
}
```

### Link object (from `networkData.links`)
```js
{
  source: string | node,   // id or resolved node object
  target: string | node,
  type: 'family' | 'taught' | 'premiered' | 'wrote' | 'authored' | 'edited',
  label: string,           // 'parent' | 'spouse' | 'grandparent' | 'sibling' | 'taught' | 'premiered role in' | 'wrote' | 'authored' | 'edited'
  role: string | null,     // premiered role name (person-to-opera only)
  relationship_source: string | null,
  relationshipSourceDisplay: string | null,
  // ... additional source fields
}
```

---

## 5. Node Visual Styling

### Colors

| Type | Condition | Fill | Border | Border Width |
|------|-----------|------|--------|-------------|
| opera | any | `#9CA3AF` | `#FFFFFF` | 3 |
| book | any | `#9CA3AF` | `#6a7304` | 3 |
| person | selected | base color | darkened base (50%) | 3 |
| person | unselected | by voice type | none | 0 |

### Voice type → fill color
| Voice Type | Color |
|-----------|-------|
| Soprano | `#ae996b` |
| Mezzo-soprano | `#695531` |
| Contralto | `#443f39` |
| Countertenor | `#4e2d06` |
| Tenor | `#e4a201` |
| Baritone | `#6a7304` |
| Bass-baritone | `#a09602` |
| Bass | `#a09602` |
| Castrato / Soprano castrato / Alto castrato / Haute-contre / Treble, unchanged voice | `#99c0e3` |
| Composer / Conductor / Instrumentalist / Opera director / Teacher, other / Vocal coach / Speech Language Pathologist / Librettist / Critic / Actor / Inventor / Non-singing / Unknown | `#7c8b23` |
| null/undefined voiceType | `#8cc400` |
| unmapped voiceType | `#6B7280` |

### Labels
- **Person:** `"Maria Callas"` (name, word-wrapped)
- **Opera:** `"🎵\nLa Traviata"` (emoji + newline + title, word-wrapped)
- **Book:** `"📚\nTreatise on Singing"` (emoji + newline + title, word-wrapped)

Set `widthConstraint: { maximum: 80 }` and `font.multi: false` with `\n` separators. Text wraps automatically.

### Text color
Use WCAG contrast: white or near-black depending on node background. Compute with relative luminance. Lighter backgrounds get dark text, darker backgrounds get white text.

### Filter opacity
- Visible nodes: `opacity` from `getNodeOpacity(node)` (1.0 = fully visible)
- Filtered-out nodes: `opacity: 0.15` (dimmed, not hidden)
- Never set `hidden: true` — use opacity only

---

## 6. Edge Preprocessing

Before sending links to vis-network, preprocess to handle multiple relationships between the same node pair.

### Algorithm
1. Group links by node pair `[sourceId, targetId]` (sorted, so A→B and B→A are in the same group)
2. Self-loops (source === target) get `selfReference` option
3. Within each group, separate by direction (forward: source < target alphabetically; backward: source > target)
4. Same-direction links: merge into one vis-network edge, label = all labels joined by `\n`
5. Both directions present: two curved edges (CW one direction, CCW the other)
6. Single direction: one straight (dynamic) edge

### Edge options
```js
// Same direction only
{ smooth: { type: 'dynamic' }, arrows: { to: { enabled: true, scaleFactor: 0.8 } } }

// Forward of bidirectional pair
{ smooth: { type: 'curvedCW', roundness: 0.2 }, arrows: { to: { enabled: true, scaleFactor: 0.8 } } }

// Backward of bidirectional pair
{ smooth: { type: 'curvedCCW', roundness: 0.2 }, arrows: { to: { enabled: true, scaleFactor: 0.8 } } }

// Self-loop
{ selfReference: { size: 20, angle: Math.PI / 4, renderBehindTheNode: true },
  arrows: { to: { enabled: true, scaleFactor: 0.8 } } }
```

### Storing original link data
Each vis-network edge stores `_originalLinks: [link, ...]` for use in edge right-click.

### Right-click disabled edges
Links of type `authored`, `edited`, `wrote` → disable right-click on their vis-network edges. Store `_rightClickDisabled: true` in the edge data.

---

## 7. Physics Configuration

```js
physics: {
  enabled: true,
  solver: 'barnesHut',
  barnesHut: {
    gravitationalConstant: -8000,
    centralGravity: 0.3,
    springLength: 200,
    springConstant: 0.04,
    damping: 0.09,
    avoidOverlap: 1,
  },
  stabilization: {
    enabled: true,
    iterations: 200,
    updateInterval: 25,
  }
}
```

After `stabilizationIterationsDone` event: disable physics on user-placed nodes.

---

## 8. Initial Node Placement (Expansion Ring)

When new nodes are added via expansion, they already have `x/y` set by App.jsx's `expandAllRelationships` (ring placement logic). The vis-network component should:

1. On `networkData` change, detect newly added nodes (those not in previous dataset)
2. New nodes with `x/y` set: use those positions as starting points, `physics: true` (let them settle)
3. Anchor node with `__pinDuringExpansion: true`: set `physics: false`, `x/y` from node data
4. After expansion stabilizes (`stabilizationIterationsDone`), clear pin on anchor node

---

## 9. Interaction Specification

### Single click → expand all
```
network.on('click', params => {
  if (params.nodes.length === 1) {
    const node = findNode(params.nodes[0]);
    handleNodeSingleActivation(node);
  }
})
```

### Double click → center new search
```
network.on('doubleClick', params => {
  if (params.nodes.length === 1) {
    const node = findNode(params.nodes[0]);
    handleNodeDoubleActivation(node);
  }
})
```

### Right-click on node → context menu
```
network.on('oncontext', params => {
  params.event.preventDefault();
  if (params.nodes.length > 0) {
    setContextMenu({ show: true, node: findNode(params.nodes[0]),
                     x: params.event.clientX, y: params.event.clientY });
  } else if (params.edges.length > 0) {
    handleEdgeRightClick(params);
  }
})
```

### Mobile long-press (600ms hold) → same as right-click
```
network.on('hold', params => {
  // same logic as oncontext
})
```

### Background click → close menus
```
network.on('click', params => {
  if (params.nodes.length === 0 && params.edges.length === 0) {
    setContextMenu({ show: false });
    setLinkContextMenu({ show: false });
  }
})
```

### Background click-drag → pan (vis-network native)
### Mouse wheel → zoom (vis-network native)
### Node drag → place node (vis-network native + mark userPlaced)
### Pinch to zoom (mobile) → vis-network native

### Cluster drag (1-hop)
On `dragStart`: collect the dragged node + all directly connected nodes → store start positions.  
On `dragging`: compute delta from start, move all cluster nodes except the dragged one via `network.moveNode()`.  
On `dragEnd`: mark all cluster nodes `physics: false`, sync positions to `networkData.nodes`.

### Clicking outside the visualization div
Attach `onMouseDown` on the outer wrapper to prevent any event propagation from outside affecting the canvas. vis-network naturally ignores external DOM events.

---

## 10. Edge Right-Click Tooltip

When a right-clickable edge is right-clicked, render a floating overlay:

```
setLinkContextMenu({
  show: true,
  x: event.clientX,
  y: event.clientY,
  link: edge._originalLinks[0],  // primary link for display
  allLinks: edge._originalLinks,
})
```

### Display rules
- **Person-to-person (family, taught):** Show relationship label + source text/URL or "Not provided"
- **Person-to-opera (premiered):** Show "Premiered role in" header, role name if available, source text/URL or "Not provided"
- **Person-to-book (authored, edited, wrote):** Right-click disabled on these edges

### Styling
White box, 2px solid `#3e96e2` border, 8px border-radius, box-shadow, close button (×) top-right.

---

## 11. Context Menu

Use existing `ContextMenu.jsx`. It receives the same props as before. Position it via `contextMenu.x/y` (screen coordinates). The component handles its own absolute positioning.

**When context menu is open:** pause vis-network physics via `network.stopSimulation()`.  
**When context menu closes:** resume via `network.startSimulation()`.

Expose this via `simulationRef.current`:
```js
simulationRef.current = {
  stop: () => networkRef.current?.stopSimulation(),
  alphaTarget: () => ({ restart: () => networkRef.current?.startSimulation() }),
};
```

---

## 12. Filter Panel

The filter panel slides in from the **left** side, managed by App.jsx (`showFilterPanel` prop). When open:
- On large screens: pushes visualization content right
- On small screens: overlays the canvas (higher z-index)

The filter panel itself is rendered in App.jsx — the visualization component only needs to be aware of `showFilterPanel` to adjust its container width if needed.

When filter props change (`filtersVersion`, `selectedVoiceTypes`, `selectedBirthplaces`, `birthYearRange`, `deathYearRange`), update all node opacities in the vis-network DataSet:
```js
const updates = networkData.nodes.map(n => ({
  id: n.id,
  opacity: isNodeVisible(n) ? getNodeOpacity(n) : 0.15,
}));
nodesDataset.update(updates);
```

---

## 13. Helper Messages

`showHelperMessage(message, duration)` is called by App.jsx to display status text. The visualization component must render a floating message overlay inside its container when a message is active.

Implementation:
- Register the `showHelperMessage` callback with the component's own state setter
- Display as a small pill/badge centered near the top of the canvas
- Auto-clear after `duration` ms

Show during:
- Node expansion start: `"Expanding…"`
- Path finding: `"Finding path…"`
- Load: `"Loading…"`

---

## 14. History (Forward / Back)

History tracks network topology changes (node/edge adds and removes). **Filter changes do NOT add to history.**

Each history entry:
```js
{
  nodes: networkData.nodes.map(n => ({ ...n })),   // full node data including x/y
  links: [...networkData.links],
}
```

**When to push a history entry:**
- New search executed
- Node expanded
- Node dismissed
- Path found
- Saved view loaded

**When NOT to push:**
- Filter changes

Max history depth: 50 entries. Trim oldest when exceeded.

Forward/back calls `setNetworkData(historyEntry)` and repositions nodes via `network.moveNode(id, x, y)` for each node in the snapshot.

The `pushHistory` prop (from App.jsx) is called before each topology change.

---

## 15. Save / Load

### Save
`useSaveExport.js` reads positions directly from `networkData.nodes[i].x` and `networkData.nodes[i].y`. These must be kept in sync with vis-network positions.

**Sync strategy:** After stabilization ends and after each drag end, call:
```js
const positions = networkRef.current.getPositions();
networkData.nodes.forEach(n => {
  if (positions[n.id]) {
    n.x = positions[n.id].x;
    n.y = positions[n.id].y;
  }
});
```

**Zoom state:** `useSaveExport.js` reads `window.__cmg_zoomTransform || uiZoomRef.current`. Update on every zoom change:
```js
network.on('zoom', () => {
  const scale = network.getScale();
  const viewPos = network.getViewPosition();
  const zoomState = { k: scale, x: viewPos.x, y: viewPos.y };
  uiZoomRef.current = zoomState;
  window.__cmg_zoomTransform = zoomState;
});
```

### Load
When a saved view is applied (`applySnapshot` in `useSaveExport.js`), nodes will have `x/y` from the snapshot. The component reads these from `networkData` when syncing to vis-network DataSet. Restore zoom:
```js
// In a useEffect watching for snapshot load signal:
network.moveTo({
  scale: snapshot.ui.zoom.k,
  position: { x: snapshot.ui.zoom.x, y: snapshot.ui.zoom.y },
  animation: false,
});
```

---

## 16. "Operas / Books / People" Tab Buttons

These are handled in App.jsx. When clicked, they call `setNetworkData({ nodes: [], links: [] })`. The visualization component must handle receiving empty `networkData` gracefully:
- Clear vis-network DataSets
- Reset any expansion tracking state
- Stop physics

---

## 17. Node Deduplication

**Absolute rule: never add a node whose `id` already exists in the vis-network DataSet.**

Before adding nodes:
```js
const existingIds = new Set(nodesDataset.getIds());
const newNodes = incomingNodes.filter(n => !existingIds.has(n.id));
```

This is enforced both in vis-network DataSet operations AND in App.jsx's `sanitizeGraphData`. Double-enforced.

---

## 18. Mobile Specifics

| Behavior | Desktop | Mobile |
|----------|---------|--------|
| Right-click context menu | `oncontext` event | `hold` event (600ms) |
| Edge right-click | `oncontext` on edge | `hold` on edge |
| Zoom | Mouse wheel (native) | Pinch (native) |
| Pan | Click-drag background (native) | Touch-drag background (native) |
| Node drag | Click-drag node (native) | Touch-drag node (native) |
| Cluster drag | Same as desktop | Same implementation |
| Profile card position | Bottom-left, 20px margin | Centered horizontally, above bottom panels |

---

## 19. vis-network Full Options Object

```js
const VIS_OPTIONS = {
  physics: {
    enabled: true,
    solver: 'barnesHut',
    barnesHut: {
      gravitationalConstant: -8000,
      centralGravity: 0.3,
      springLength: 200,
      springConstant: 0.04,
      damping: 0.09,
      avoidOverlap: 1,
    },
    stabilization: { enabled: true, iterations: 200, updateInterval: 25 },
  },
  interaction: {
    hover: true,
    multiselect: false,
    selectConnectedEdges: false,
    tooltipDelay: 99999,   // disable built-in tooltips; we handle them
    navigationButtons: false,
    keyboard: false,
    zoomView: true,
    dragView: true,
    dragNodes: true,
  },
  nodes: {
    shape: 'dot',
    size: 30,
    font: { size: 11, align: 'center', multi: false },
    borderWidth: 2,
    chosen: false,
    widthConstraint: { maximum: 80 },
  },
  edges: {
    arrows: { to: { enabled: true, scaleFactor: 0.8 } },
    color: { color: '#94a3b8', highlight: '#3e96e2', hover: '#3e96e2' },
    font: { size: 10, align: 'middle', strokeWidth: 2, strokeColor: '#ffffff' },
    smooth: { type: 'dynamic' },
    chosen: false,
    width: 1.5,
  },
  layout: {
    randomSeed: 42,
    improvedLayout: false,
  },
};
```

---

## 20. Component Structure

```
NetworkVisualization (function component)
├── containerRef          → vis-network mount point
├── networkRef            → vis-network Network instance
├── nodesDatasetRef       → vis-network DataSet (nodes)
├── edgesDatasetRef       → vis-network DataSet (edges)
├── prevNetworkDataRef    → previous networkData for diffing
├── clusterDragRef        → cluster drag state {startPositions, clusterIds}
├── helperMsgRef          → setTimeout handle for helper message
├── [helperText, setHelperText]  → current helper message string
│
├── useEffect #1: Initialize vis-network (runs once on mount)
│   ├── Create DataSets
│   ├── Create Network
│   ├── Register all event handlers
│   ├── Set simulationRef.current (adapter for App.jsx)
│   └── Return cleanup (network.destroy())
│
├── useEffect #2: Sync networkData → DataSets
│   ├── Diff nodes: add/remove/update
│   ├── Rebuild edges (clear + add preprocessed)
│   ├── Honor __pinDuringExpansion
│   └── Deps: [networkData, selectedNode]
│
├── useEffect #3: Filter opacity sync
│   └── Deps: [filtersVersion, selectedVoiceTypes, selectedBirthplaces,
│               birthYearRange[0], birthYearRange[1],
│               deathYearRange[0], deathYearRange[1]]
│
├── useEffect #4: Resize observer (update canvas height)
│
└── return JSX:
    <div style={{ position: 'relative', height: visualizationHeight }}>
      <div ref={containerRef} style={{ width: '100%', height: '100%' }} />
      {helperText && <HelperMessageOverlay />}
      {contextMenu.show && <ContextMenu ... />}
      {linkContextMenu.show && <LinkTooltipOverlay ... />}
      {profileCard.show && <ProfileCard ... />}
    </div>
```

---

## 21. Known Constraints and Trade-offs

1. **Old saved views:** Zoom coordinates stored by D3 (SVG translation) vs vis-network (canvas center) are incompatible. Old saved views will load correctly for node positions but may have slightly different zoom/pan on first load.

2. **D3 removed from this component:** The new `NetworkVisualization.jsx` does not import D3. D3 remains in the project for `useSaveExport.js` and App.jsx's `getNodeStyle`.

3. **Physics feel:** vis-network's Barnes-Hut solver feels slightly different from D3's force simulation. The constants above were tuned to approximate the current feel but may need adjustment after visual testing.

4. **Cluster drag on mobile:** vis-network fires identical drag events for touch and mouse. The cluster drag implementation works on both. However, if a user accidentally initiates a cluster drag on mobile (touch is less precise), they may unintentionally move many nodes.

5. **Edge label rendering:** vis-network renders edge labels on the canvas. Very long relationship labels (>20 chars) may overlap at low zoom. Labels are kept short by design (e.g. "taught", "parent", "premiered role in").
