import * as d3 from 'd3';

const PAN_ACTIVATION_PX = (typeof window !== 'undefined' && 'ontouchstart' in window) ? 20 : 6;
const PINCH_ACTIVATION_PX = 20;
const POINTER_STALE_MS = 4000;

const isDomElement = (el) => (typeof Element !== 'undefined') && el instanceof Element;

const isContextTarget = (el) => {
  if (!isDomElement(el)) return false;
  if (el.closest('.context-menu')) return true;
  if (el.closest('button') || el.closest('input') || el.closest('textarea')) return true;
  if (el.closest('.mobile-toolbar__button')) return true;
  if (el.closest('.mobile-tap-target')) return true;
  return false;
};

const isNodeSurface = (el) => {
  if (!isDomElement(el)) return false;
  if (el.closest('circle')) return true;
  if (el.closest('g.node-label')) return true;
  if (el.closest('rect.link-label-hit')) return true;
  if (el.closest('path.link') || el.closest('path.h-up') || el.closest('path.h-down')) return true;
  return false;
};

const shouldIgnoreTarget = (target) => isContextTarget(target);

const getEligiblePointers = (activePointers) =>
  Array.from(activePointers.values()).filter((meta) => !meta.onNodeSurface);

const updateTransformRefs = (svgElement, zoomTransformRef, uiZoomRef) => {
  if (!svgElement) return;
  const next = d3.zoomTransform(svgElement);
  if (zoomTransformRef) {
    zoomTransformRef.current = next;
  }
  if (uiZoomRef) {
    uiZoomRef.current = next;
  }
  if (typeof window !== 'undefined') {
    try {
      window.__cmg_zoomTransform = next;
    } catch (_) {}
  }
};

const applyPinch = (zoomBehavior, selection, zoomTransformRef, uiZoomRef, scaleFactor, center, svgElement) => {
  if (!zoomBehavior) return;
  if (!Number.isFinite(scaleFactor) || scaleFactor <= 0) return;
  if (Math.abs(scaleFactor - 1) < 0.01) return;
  const rect = svgElement.getBoundingClientRect();
  const focalPoint = [center.x - rect.left, center.y - rect.top];
  zoomBehavior.scaleBy(selection, scaleFactor, focalPoint);
  updateTransformRefs(svgElement, zoomTransformRef, uiZoomRef);
};

const translateDuringPinch = (svgElement, zoomBehavior, selection, zoomTransformRef, uiZoomRef, center, previousCenter) => {
  if (!zoomBehavior) return;
  const dx = center.x - previousCenter.x;
  const dy = center.y - previousCenter.y;
  if (Math.abs(dx) < 0.01 && Math.abs(dy) < 0.01) return;
  const current = zoomTransformRef.current || d3.zoomIdentity;
  zoomBehavior.translateBy(selection, dx / current.k, dy / current.k);
  updateTransformRefs(svgElement, zoomTransformRef, uiZoomRef);
};

export default function initTouchInteractions({
  svgElement,
  svgSelection,
  zoomBehavior,
  zoomTransformRef,
  uiZoomRef,
  closeOpenMenus = () => {}
} = {}) {
  if (!svgElement || !zoomBehavior) {
    return () => {};
  }

  const selection = svgSelection || d3.select(svgElement);
  const activePointers = new Map();
  let panState = null;
  let pinchState = null;

  const resetPanState = () => {
    panState = null;
  };

  const resetPinchState = () => {
    pinchState = null;
  };

  const startPanIfNeeded = (meta, zoomTransformRef) => {
    if (panState || pinchState) return;
    if (!meta || meta.onNodeSurface) return;
    panState = {
      pointerId: meta.pointerId,
      startX: meta.startX,
      startY: meta.startY,
      lastX: meta.lastX,
      lastY: meta.lastY,
      activated: false,
      startTransform: zoomTransformRef.current ? zoomTransformRef.current : d3.zoomIdentity
    };
  };

  const startPinchIfNeeded = () => {
    if (activePointers.size < 2) {
      resetPinchState();
      return;
    }
    const pointers = getEligiblePointers(activePointers);
    if (pointers.length < 2) {
      resetPinchState();
      return;
    }
    const [first, second] = pointers;
    const dist = Math.max(Math.hypot(second.lastX - first.lastX, second.lastY - first.lastY), 1);
    pinchState = {
      pointerIds: [first.pointerId, second.pointerId],
      previousDistance: dist,
      previousCenter: {
        x: (first.lastX + second.lastX) / 2,
        y: (first.lastY + second.lastY) / 2
      },
      initialDistance: dist,
      activated: false
    };
  };

  const cleanupStalePointers = () => {
    const now = Date.now();
    for (const [id, meta] of activePointers.entries()) {
      if (now - meta.timestamp > POINTER_STALE_MS) {
        activePointers.delete(id);
      }
    }
  };

  const processPanMove = (meta, event) => {
    if (!panState || panState.pointerId !== meta.pointerId) return;
    const totalDx = event.clientX - panState.startX;
    const totalDy = event.clientY - panState.startY;
    if (!panState.activated) {
      const distance = Math.hypot(totalDx, totalDy);
      if (distance >= PAN_ACTIVATION_PX) {
        panState.activated = true;
        closeOpenMenus();
      } else {
        panState.lastX = event.clientX;
        panState.lastY = event.clientY;
        return;
      }
    }
    if (event.cancelable) {
      event.preventDefault();
    }
    const startTransform = panState.startTransform || zoomTransformRef.current || d3.zoomIdentity;
    const dxFromStart = event.clientX - panState.startX;
    const dyFromStart = event.clientY - panState.startY;
    panState.lastX = event.clientX;
    panState.lastY = event.clientY;
    const next = startTransform.translate(dxFromStart / startTransform.k, dyFromStart / startTransform.k);
    zoomBehavior.transform(selection, next);
    updateTransformRefs(svgElement, zoomTransformRef, uiZoomRef);
  };

  const processPinchMove = (event) => {
    if (!pinchState) return false;
    const first = activePointers.get(pinchState.pointerIds[0]);
    const second = activePointers.get(pinchState.pointerIds[1]);
    if (!first || !second) {
      resetPinchState();
      return false;
    }

    const distance = Math.max(Math.hypot(second.lastX - first.lastX, second.lastY - first.lastY), 1);
    const center = {
      x: (first.lastX + second.lastX) / 2,
      y: (first.lastY + second.lastY) / 2
    };

    if (!pinchState.activated) {
      const distanceChange = Math.abs(distance - pinchState.initialDistance);
      if (distanceChange >= PINCH_ACTIVATION_PX) {
        pinchState.activated = true;
        resetPanState();
      } else {
        pinchState.previousDistance = distance;
        pinchState.previousCenter = center;
        return false;
      }
    }

    const scaleFactor = distance / (pinchState.previousDistance || distance);
    const centerShift = {
      x: center.x - pinchState.previousCenter.x,
      y: center.y - pinchState.previousCenter.y
    };

    if (event.cancelable) {
      event.preventDefault();
    }

    if (Math.abs(centerShift.x) > 0.01 || Math.abs(centerShift.y) > 0.01) {
      translateDuringPinch(svgElement, zoomBehavior, selection, zoomTransformRef, uiZoomRef, center, pinchState.previousCenter);
    }
    if (Math.abs(scaleFactor - 1) > 0.01) {
      applyPinch(zoomBehavior, selection, zoomTransformRef, uiZoomRef, scaleFactor, center, svgElement);
    }

    pinchState.previousDistance = distance;
    pinchState.previousCenter = center;
    return true;
  };

  const handlePointerDown = (event) => {
    if (event.pointerType !== 'touch') return;
    const target = event.target;
    if (shouldIgnoreTarget(target)) return;

    cleanupStalePointers();

    const onNodeSurface = isNodeSurface(target);
    if (!onNodeSurface && event.cancelable) {
      event.preventDefault();
    }

    const captureTarget = isDomElement(target) ? target : svgElement;
    if (captureTarget && typeof captureTarget.setPointerCapture === 'function') {
      try {
        captureTarget.setPointerCapture(event.pointerId);
      } catch (_) {}
    }

    const meta = {
      pointerId: event.pointerId,
      startX: event.clientX,
      startY: event.clientY,
      lastX: event.clientX,
      lastY: event.clientY,
      target,
      captureTarget,
      onNodeSurface,
      timestamp: Date.now()
    };

    activePointers.set(event.pointerId, meta);

    if (!meta.onNodeSurface) {
      startPanIfNeeded(meta, zoomTransformRef);
    }

    if (activePointers.size >= 2) {
      startPinchIfNeeded();
    }
  };

  const handlePointerMove = (event) => {
    if (event.pointerType !== 'touch') return;
    const meta = activePointers.get(event.pointerId);
    if (!meta) return;
    meta.lastX = event.clientX;
    meta.lastY = event.clientY;

    if (pinchState) {
      const didPinch = processPinchMove(event);
      if (!didPinch && panState && panState.pointerId === event.pointerId) {
        processPanMove(meta, event);
      }
      return;
    }

    if (panState && panState.pointerId === event.pointerId) {
      processPanMove(meta, event);
    }
  };

  const handlePointerEnd = (event) => {
    if (event.pointerType !== 'touch') return;
    const meta = activePointers.get(event.pointerId);
    activePointers.delete(event.pointerId);

    const captureTarget = meta && meta.captureTarget ? meta.captureTarget : svgElement;
    if (captureTarget && typeof captureTarget.releasePointerCapture === 'function') {
      try {
        captureTarget.releasePointerCapture(event.pointerId);
      } catch (_) {}
    }

    if (panState && panState.pointerId === event.pointerId) {
      resetPanState();
    }
    if (pinchState && pinchState.pointerIds.includes(event.pointerId)) {
      resetPinchState();
    }

    if (!pinchState && !panState) {
      const remaining = Array.from(activePointers.values()).find(p => !p.onNodeSurface);
      if (remaining) {
        startPanIfNeeded(remaining);
      }
    }
  };

  svgElement.addEventListener('pointerdown', handlePointerDown, { passive: false });
  svgElement.addEventListener('pointermove', handlePointerMove, { passive: false });
  svgElement.addEventListener('pointerup', handlePointerEnd, { passive: false });
  svgElement.addEventListener('pointercancel', handlePointerEnd, { passive: false });

  return () => {
    activePointers.clear();
    resetPanState();
    resetPinchState();
    svgElement.removeEventListener('pointerdown', handlePointerDown);
    svgElement.removeEventListener('pointermove', handlePointerMove);
    svgElement.removeEventListener('pointerup', handlePointerEnd);
    svgElement.removeEventListener('pointercancel', handlePointerEnd);
  };
}
