import React, { useState, useEffect } from 'react';

const ContextMenu = React.memo(({
  contextMenu,
  setContextMenu,
  actualCounts,
  networkData,
  expandSubmenu,
  setExpandSubmenu,
  submenuTimeoutRef,
  showFullInformation,
  expandAllRelationships,
  expandSpecificRelationship,
  dismissOtherNodes,
  dismissNode,
  getExpandableRelationshipCounts,
}) => {
    const node = contextMenu.node;
    const [hoveredMenuIndex, setHoveredMenuIndex] = useState(null);
    useEffect(() => {
      if (!contextMenu.show) {
        setHoveredMenuIndex(null);
      }
    }, [contextMenu.show]);

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
      const hasApiData = !!nodeActualCount;
      const hasAnyExpandable = hasApiData
        ? Object.values(counts).some(v => (typeof v === 'number' ? v : 0) > 0)
        : true; // allow expand even before counts load
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
          ...(node?.type === 'book' ? [
            ...(counts.authored > 0 ? [{
              label: `Authored (${counts.authored} nodes)`,
              action: () => {
                // For books, authors are inbound; use authoredBy to make intent clear
                expandSpecificRelationship(node, 'authoredBy');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.editedBy > 0 ? [{
              label: `Edited (${counts.editedBy} nodes)`,
              action: () => {
                // Editors inbound to a book
                expandSpecificRelationship(node, 'editedBy');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : [])
          ] : [])
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

    const activeSubmenuItem = (expandSubmenu != null && menuItems[expandSubmenu]?.hasSubmenu)
      ? { index: expandSubmenu, item: menuItems[expandSubmenu] }
      : null;

    const dismissMenu = () => {
      setContextMenu({ show: false, x: 0, y: 0, node: null });
      setExpandSubmenu(null);
      setHoveredMenuIndex(null);
    };

   return (
      <div
        className="context-menu"
        style={{
          // fixed (not absolute) so contextMenu.x/y — which are event.clientX/Y
          // viewport coordinates — render at the actual click point. With absolute,
          // those coords were interpreted relative to the visualization wrapper,
          // which pushed the menu off-screen on most layouts.
          position: 'fixed',
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
                transition: 'background-color 0.1s',
                backgroundColor:
                  (!item.disabled && hoveredMenuIndex === index) ||
                  (item.hasSubmenu && expandSubmenu === index)
                    ? '#f3f4f6'
                    : 'transparent'
              }}
              onMouseEnter={() => {
                setHoveredMenuIndex(index);
                if (item.hasSubmenu) {
                  if (submenuTimeoutRef.current) {
                    clearTimeout(submenuTimeoutRef.current);
                  }
                  setExpandSubmenu(index);
                }
              }}
              onMouseLeave={() => {
                setHoveredMenuIndex((current) => (current === index ? null : current));
                if (item.hasSubmenu) {
                  if (submenuTimeoutRef.current) {
                    clearTimeout(submenuTimeoutRef.current);
                  }
                  submenuTimeoutRef.current = setTimeout(() => {
                    setExpandSubmenu(null);
                  }, 300);
                }
              }}
              onTouchStart={() => {
                setHoveredMenuIndex(index);
                if (item.hasSubmenu) {
                  if (submenuTimeoutRef.current) {
                    clearTimeout(submenuTimeoutRef.current);
                  }
                  setExpandSubmenu(index);
                }
              }}
              onClick={() => {
                if (item.disabled) return;
                if (item.hasSubmenu) {
                  // Toggle submenu on click for touch/keyboard users
                  const willOpen = expandSubmenu !== index;
                  setExpandSubmenu(willOpen ? index : null);
                  setHoveredMenuIndex(willOpen ? index : null);
                  return;
                }
                if (typeof item.action === 'function') {
                  item.action();
                }
              }}
            >
              <span>{item.label}</span>
              {item.hasSubmenu && <span style={{ color: '#9ca3af' }}>▶</span>}
            </div>
          </div>
        ))}
        {activeSubmenuItem?.item?.submenu && (
          <div
            style={{
              position: 'absolute',
              top: '10px',
              left: '10px',
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
              if (submenuTimeoutRef.current) {
                clearTimeout(submenuTimeoutRef.current);
              }
              if (activeSubmenuItem) {
                setExpandSubmenu(activeSubmenuItem.index);
                setHoveredMenuIndex(activeSubmenuItem.index);
              }
            }}
            onMouseLeave={() => {
              if (submenuTimeoutRef.current) {
                clearTimeout(submenuTimeoutRef.current);
              }
              submenuTimeoutRef.current = setTimeout(() => {
                setExpandSubmenu(null);
                setHoveredMenuIndex(null);
              }, 300);
            }}
            onTouchStart={() => {
              if (submenuTimeoutRef.current) {
                clearTimeout(submenuTimeoutRef.current);
              }
              if (activeSubmenuItem) {
                setExpandSubmenu(activeSubmenuItem.index);
                setHoveredMenuIndex(activeSubmenuItem.index);
              }
            }}
            onClick={(e) => e.stopPropagation()}
          >
            {activeSubmenuItem.item.submenu.map((subItem, subIndex) => (
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
                onMouseEnter={(e) => {
                  if (e.currentTarget) e.currentTarget.style.backgroundColor = '#f3f4f6';
                }}
                onMouseLeave={(e) => {
                  if (e.currentTarget) e.currentTarget.style.backgroundColor = 'transparent';
                }}
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
    );
});

export default ContextMenu;
