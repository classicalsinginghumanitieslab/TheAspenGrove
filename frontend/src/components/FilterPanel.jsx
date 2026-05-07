import React, { useState, useRef, useLayoutEffect } from 'react';
import { DEFAULT_BIRTH_RANGE, DEFAULT_DEATH_RANGE } from '../constants/defaults';

const FilterPanel = ({
  getFilterCounts,
  getDateRanges,
  selectedVoiceTypes,
  birthRangeIsUserSet,
  deathRangeIsUserSet,
  selectedBirthplaces,
  filterSectionsOpen,
  setFilterSectionsOpen,
  birthYearRange,
  deathYearRange,
  birthRangeIsUserSetRef,
  deathRangeIsUserSetRef,
  setBirthRangeIsUserSet,
  setDeathRangeIsUserSet,
  updateBirthYearRange,
  updateDeathYearRange,
  setFiltersVersion,
  showFilterPanel,
  setShowFilterPanel,
  isMobileViewport,
  clearAllFilters,
  getVisibleVoiceTypes,
  toggleVoiceTypeFilter,
  getVisibleBirthplaces,
  normalizePlaceName,
  toggleBirthplaceFilter,
}) => {
    const { totalNodes, visibleNodes } = getFilterCounts();
    const { birthRange, deathRange } = getDateRanges();
    const hasVoiceFilters = selectedVoiceTypes.size > 0;
    const hasBirthFilter = birthRangeIsUserSet;
    const hasDeathFilter = deathRangeIsUserSet;
    const hasBirthplaceFilters = selectedBirthplaces.size > 0;
    const hasAnyFilters = hasVoiceFilters || hasBirthFilter || hasDeathFilter || hasBirthplaceFilters;

    // Local input state to prevent re-renders during typing
    const [birthMinInput, setBirthMinInput] = useState('');
    const [birthMaxInput, setBirthMaxInput] = useState('');
    const [deathMinInput, setDeathMinInput] = useState('');
    const [deathMaxInput, setDeathMaxInput] = useState('');
    const contentRef = useRef(null);
    const isVoiceOpen = filterSectionsOpen.voice;
    const isBirthOpen = filterSectionsOpen.birth;
    const isDeathOpen = filterSectionsOpen.death;
    const isBirthplacesOpen = filterSectionsOpen.birthplaces;

    useLayoutEffect(() => {
      // Sync inputs when ranges or panel visibility changes
      if (birthRangeIsUserSet) {
        setBirthMinInput(String(birthYearRange[0]));
        setBirthMaxInput(String(birthYearRange[1]));
      } else {
        setBirthMinInput('');
        setBirthMaxInput('');
      }
      if (deathRangeIsUserSet) {
        setDeathMinInput(String(deathYearRange[0]));
        setDeathMaxInput(String(deathYearRange[1]));
      } else {
        setDeathMinInput('');
        setDeathMaxInput('');
      }
    }, [birthYearRange, deathYearRange, birthRangeIsUserSet, deathRangeIsUserSet, showFilterPanel]);

    const clamp = (value, min, max) => Math.max(min, Math.min(value, max));

    const applyBirthRange = () => {
      const scrollEl = contentRef.current;
      const prevTop = scrollEl ? scrollEl.scrollTop : null;
      const winY = typeof window !== 'undefined' ? window.scrollY : null;
      const ranges = getDateRanges();
      const dataBirthRange = Array.isArray(ranges.birthRange) ? ranges.birthRange : DEFAULT_BIRTH_RANGE;
      const minBound = Math.min(DEFAULT_BIRTH_RANGE[0], dataBirthRange[0] ?? DEFAULT_BIRTH_RANGE[0]);
      const maxBound = Math.max(DEFAULT_BIRTH_RANGE[1], dataBirthRange[1] ?? DEFAULT_BIRTH_RANGE[1]);
      const hasMin = typeof birthMinInput === 'string' && birthMinInput.trim() !== '';
      const hasMax = typeof birthMaxInput === 'string' && birthMaxInput.trim() !== '';

      if (!hasMin && !hasMax) {
        birthRangeIsUserSetRef.current = false;
        setBirthRangeIsUserSet(false);
        updateBirthYearRange([...DEFAULT_BIRTH_RANGE], { userInitiated: false });
        setBirthMinInput('');
        setBirthMaxInput('');
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
        return;
      }

      const parsedMin = parseInt(birthMinInput, 10);
      const parsedMax = parseInt(birthMaxInput, 10);
      let nextMin = hasMin
        ? (Number.isNaN(parsedMin) ? birthYearRange[0] : clamp(parsedMin, minBound, maxBound))
        : birthYearRange[0];
      let nextMax = hasMax
        ? (Number.isNaN(parsedMax) ? birthYearRange[1] : clamp(parsedMax, minBound, maxBound))
        : birthYearRange[1];
      if (nextMax < nextMin) nextMax = nextMin;
      updateBirthYearRange([nextMin, nextMax], { userInitiated: true });
      setBirthMinInput(hasMin ? String(nextMin) : '');
      setBirthMaxInput(hasMax ? String(nextMax) : '');
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
      const ranges = getDateRanges();
      const dataDeathRange = Array.isArray(ranges.deathRange) ? ranges.deathRange : DEFAULT_DEATH_RANGE;
      const minBound = Math.min(DEFAULT_DEATH_RANGE[0], dataDeathRange[0] ?? DEFAULT_DEATH_RANGE[0]);
      const maxBound = Math.max(DEFAULT_DEATH_RANGE[1], dataDeathRange[1] ?? DEFAULT_DEATH_RANGE[1]);
      const hasMin = typeof deathMinInput === 'string' && deathMinInput.trim() !== '';
      const hasMax = typeof deathMaxInput === 'string' && deathMaxInput.trim() !== '';

      if (!hasMin && !hasMax) {
        deathRangeIsUserSetRef.current = false;
        setDeathRangeIsUserSet(false);
        updateDeathYearRange([...DEFAULT_DEATH_RANGE], { userInitiated: false });
        setDeathMinInput('');
        setDeathMaxInput('');
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
        return;
      }

      const parsedMin = parseInt(deathMinInput, 10);
      const parsedMax = parseInt(deathMaxInput, 10);
      let nextMin = hasMin
        ? (Number.isNaN(parsedMin) ? deathYearRange[0] : clamp(parsedMin, minBound, maxBound))
        : deathYearRange[0];
      let nextMax = hasMax
        ? (Number.isNaN(parsedMax) ? deathYearRange[1] : clamp(parsedMax, minBound, maxBound))
        : deathYearRange[1];
      if (nextMax < nextMin) nextMax = nextMin;
      updateDeathYearRange([nextMin, nextMax], { userInitiated: true });
      setDeathMinInput(hasMin ? String(nextMin) : '');
      setDeathMaxInput(hasMax ? String(nextMax) : '');
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
              <h3 style={{ margin: 0, fontSize: isMobileViewport ? '16px' : '18px', fontWeight: '600', color: '#1f2937' }}>
                Filters
              </h3>
              <button
                onClick={() => setShowFilterPanel(false)}
                style={{
                  background: 'none',
                  border: 'none',
                  fontSize: isMobileViewport ? '24px' : '24px',
                  cursor: 'pointer',
                  color: '#6b7280',
                  padding: isMobileViewport ? '2px' : '0',
                  width: isMobileViewport ? '32px' : '32px',
                  height: isMobileViewport ? '32px' : '32px',
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
                    padding: isMobileViewport ? '8px 12px' : '6px 12px',
                    borderRadius: '8px',
                    fontSize: isMobileViewport ? '14px' : '16px',
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
                  padding: isMobileViewport ? '8px 12px' : '6px 12px',
                  borderRadius: '8px',
                  fontSize: isMobileViewport ? '14px' : '16px',
                  cursor: 'pointer'
                }}
              >
                Apply Filters
              </button>
            </div>

            {/* Filter Count Display */}
            {totalNodes > 0 && (
              <div style={{
                marginTop: hasAnyFilters ? (isMobileViewport ? '6px' : '8px') : (isMobileViewport ? '8px' : '12px'),
                padding: isMobileViewport ? '8px 10px' : '8px 12px',
                backgroundColor: hasAnyFilters ? '#f0f9ff' : '#f9fafb',
                border: hasAnyFilters ? '1px solid #0ea5e9' : '1px solid #e5e7eb',
                borderRadius: '6px',
                fontSize: isMobileViewport ? '14px' : '16px',
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
            <div style={{ marginBottom: isMobileViewport ? '16px' : '24px' }}>
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  setFilterSectionsOpen(prev => ({ ...prev, voice: !prev.voice }));
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    e.stopPropagation();
                    setFilterSectionsOpen(prev => ({ ...prev, voice: !prev.voice }));
                  }
                }}
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
                onClick={(e) => {
                  e.stopPropagation();
                  setFilterSectionsOpen(prev => ({ ...prev, birthplaces: !prev.birthplaces }));
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    e.stopPropagation();
                    setFilterSectionsOpen(prev => ({ ...prev, birthplaces: !prev.birthplaces }));
                  }
                }}
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
                onClick={(e) => {
                  e.stopPropagation();
                  setFilterSectionsOpen(prev => ({ ...prev, birth: !prev.birth }));
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    e.stopPropagation();
                    setFilterSectionsOpen(prev => ({ ...prev, birth: !prev.birth }));
                  }
                }}
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
                    placeholder="yyyy"
                    value={birthMinInput}
                    onChange={(e) => setBirthMinInput(e.target.value)}
                    onBlur={applyBirthRange}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        e.preventDefault();
                        e.stopPropagation();
                        applyBirthRange();
                      }
                    }}
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
                    placeholder="yyyy"
                    value={birthMaxInput}
                    onChange={(e) => setBirthMaxInput(e.target.value)}
                    onBlur={applyBirthRange}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        e.preventDefault();
                        e.stopPropagation();
                        applyBirthRange();
                      }
                    }}
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
                onClick={(e) => {
                  e.stopPropagation();
                  setFilterSectionsOpen(prev => ({ ...prev, death: !prev.death }));
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    e.stopPropagation();
                    setFilterSectionsOpen(prev => ({ ...prev, death: !prev.death }));
                  }
                }}
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
                    placeholder="yyyy"
                    value={deathMinInput}
                    onChange={(e) => setDeathMinInput(e.target.value)}
                    onBlur={applyDeathRange}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        e.preventDefault();
                        e.stopPropagation();
                        applyDeathRange();
                      }
                    }}
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
                    placeholder="yyyy"
                    value={deathMaxInput}
                    onChange={(e) => setDeathMaxInput(e.target.value)}
                    onBlur={applyDeathRange}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        e.preventDefault();
                        e.stopPropagation();
                        applyDeathRange();
                      }
                    }}
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

export default FilterPanel;
