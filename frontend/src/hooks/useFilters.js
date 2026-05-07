import { useState, useRef, useEffect } from 'react';
import { DEFAULT_BIRTH_RANGE, DEFAULT_DEATH_RANGE } from '../constants/defaults';
import { normalizePersonNode, parseYearValue } from '../utils/normalization';
import {
  normalizePlaceName,
  computeRangesFromNodes,
  isNodeVisible as isNodeVisibleUtil,
  getNodeOpacity as getNodeOpacityUtil,
  isNodeVisibleWithoutVoiceFilter as isNodeVisibleWithoutVoiceFilterUtil,
  isNodeVisibleWithoutBirthplaceFilter as isNodeVisibleWithoutBirthplaceFilterUtil,
  getVisibleVoiceTypes as getVisibleVoiceTypesUtil,
  getVisibleBirthplaces as getVisibleBirthplacesUtil,
} from '../utils/filterUtils';

const useFilters = ({ networkData = { nodes: [], links: [] } } = {}) => {
  const [filtersVersion, setFiltersVersion] = useState(0);
  const [showFilterPanel, setShowFilterPanel] = useState(false);
  const toggleFilterPanel = (force) => {
    setShowFilterPanel((prev) => (typeof force === 'boolean' ? force : !prev));
  };
  const [selectedVoiceTypes, setSelectedVoiceTypes] = useState(new Set());
  const [selectedBirthplaces, setSelectedBirthplaces] = useState(new Set());
  const [birthYearRange, setBirthYearRange] = useState([...DEFAULT_BIRTH_RANGE]);
  const [deathYearRange, setDeathYearRange] = useState([...DEFAULT_DEATH_RANGE]);
  const [birthRangeIsUserSet, setBirthRangeIsUserSet] = useState(false);
  const [deathRangeIsUserSet, setDeathRangeIsUserSet] = useState(false);
  const birthRangeIsUserSetRef = useRef(birthRangeIsUserSet);
  const deathRangeIsUserSetRef = useRef(deathRangeIsUserSet);
  const [filterSectionsOpen, setFilterSectionsOpen] = useState({ voice: false, birth: false, death: false, birthplaces: false });

  useEffect(() => { birthRangeIsUserSetRef.current = birthRangeIsUserSet; }, [birthRangeIsUserSet]);
  useEffect(() => { deathRangeIsUserSetRef.current = deathRangeIsUserSet; }, [deathRangeIsUserSet]);
  useEffect(() => {
    if (!showFilterPanel) {
      setFilterSectionsOpen({ voice: false, birth: false, death: false, birthplaces: false });
    }
  }, [showFilterPanel]);

  // ---------------------------------------------------------------------------
  // filterState — passed to pure utils so they don't close over stale values
  // ---------------------------------------------------------------------------
  const getFilterState = () => ({
    selectedVoiceTypes,
    selectedBirthplaces,
    birthYearRange,
    deathYearRange,
    birthRangeIsUserSet,
    deathRangeIsUserSet,
  });

  // ---------------------------------------------------------------------------
  // State updaters
  // ---------------------------------------------------------------------------

  const updateSelectedVoiceTypes = (newSelection) => setSelectedVoiceTypes(newSelection);

  const updateBirthYearRange = (newRange, { userInitiated = false } = {}) => {
    setBirthYearRange(prev => Array.isArray(newRange) ? [...newRange] : prev);
    if (userInitiated) {
      birthRangeIsUserSetRef.current = true;
      setBirthRangeIsUserSet(true);
    }
  };

  const updateDeathYearRange = (newRange, { userInitiated = false } = {}) => {
    setDeathYearRange(prev => Array.isArray(newRange) ? [...newRange] : prev);
    if (userInitiated) {
      deathRangeIsUserSetRef.current = true;
      setDeathRangeIsUserSet(true);
    }
  };

  const toggleVoiceTypeFilter = (voiceType) => {
    const newSelection = new Set(selectedVoiceTypes);
    if (newSelection.has(voiceType)) newSelection.delete(voiceType);
    else newSelection.add(voiceType);
    updateSelectedVoiceTypes(newSelection);
  };

  const toggleBirthplaceFilter = (birthplace) => {
    const key = normalizePlaceName(birthplace);
    if (!key) return;
    const newSelection = new Set(selectedBirthplaces);
    if (newSelection.has(key)) newSelection.delete(key);
    else newSelection.add(key);
    setSelectedBirthplaces(newSelection);
  };

  // ---------------------------------------------------------------------------
  // Date range helpers (depend on networkData or current state)
  // ---------------------------------------------------------------------------

  const extendDateRangesForNodes = (nodesList = [], options = {}) => {
    const { resetUserRangeFlags = false } = options;
    if (!Array.isArray(nodesList) || nodesList.length === 0) return;
    let [birthMin, birthMax] = birthYearRange;
    let [deathMin, deathMax] = deathYearRange;
    let birthChanged = false;
    let deathChanged = false;

    nodesList.forEach((node) => {
      if (node && node.type === 'person') {
        const n = normalizePersonNode(node) ?? node;
        const birthValue =
          n.birthYear ??
          n.birth_year ??
          (n.birth ? (n.birth.year ?? n.birth.low ?? n.birth.high ?? n.birth) : null);
        const deathValue =
          n.deathYear ??
          n.death_year ??
          (n.death ? (n.death.year ?? n.death.low ?? n.death.high ?? n.death) : null);

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

    const allowBirthUpdate = resetUserRangeFlags || !birthRangeIsUserSetRef.current;
    const allowDeathUpdate = resetUserRangeFlags || !deathRangeIsUserSetRef.current;

    if (birthChanged && allowBirthUpdate) {
      updateBirthYearRange([birthMin, birthMax], { userInitiated: false });
      if (resetUserRangeFlags && (birthRangeIsUserSetRef.current || birthRangeIsUserSet)) {
        birthRangeIsUserSetRef.current = false;
        setBirthRangeIsUserSet(false);
      }
    }
    if (deathChanged && allowDeathUpdate) {
      updateDeathYearRange([deathMin, deathMax], { userInitiated: false });
      if (resetUserRangeFlags && (deathRangeIsUserSetRef.current || deathRangeIsUserSet)) {
        deathRangeIsUserSetRef.current = false;
        setDeathRangeIsUserSet(false);
      }
    }
  };

  const getDateRanges = () => {
    const personNodes = networkData.nodes
      .filter(node => node.type === 'person')
      .map(node => normalizePersonNode(node) ?? node);

    const birthYears = personNodes
      .map(node => node.birthYear)
      .filter(year => year && !isNaN(year))
      .map(year => parseInt(year));

    const deathYears = personNodes
      .map(node => node.deathYear)
      .filter(year => year && !isNaN(year))
      .map(year => parseInt(year));

    return {
      birthRange: [
        birthYears.length > 0 ? Math.min(...birthYears) : DEFAULT_BIRTH_RANGE[0],
        birthYears.length > 0 ? Math.max(...birthYears) : DEFAULT_BIRTH_RANGE[1],
      ],
      deathRange: [
        deathYears.length > 0 ? Math.min(...deathYears) : DEFAULT_DEATH_RANGE[0],
        deathYears.length > 0 ? Math.max(...deathYears) : DEFAULT_DEATH_RANGE[1],
      ],
    };
  };

  const resetDateRanges = () => {
    const { birthRange, deathRange } = getDateRanges();
    updateBirthYearRange(birthRange, { userInitiated: false });
    updateDeathYearRange(deathRange, { userInitiated: false });
    birthRangeIsUserSetRef.current = false;
    setBirthRangeIsUserSet(false);
    deathRangeIsUserSetRef.current = false;
    setDeathRangeIsUserSet(false);
  };

  // ---------------------------------------------------------------------------
  // Filter reset helpers
  // ---------------------------------------------------------------------------

  const resetFiltersForNodeSet = (nodesList) => {
    updateSelectedVoiceTypes(new Set());
    setSelectedBirthplaces(new Set());
    const { birthRange, deathRange } = computeRangesFromNodes(nodesList ?? networkData.nodes);
    updateBirthYearRange(birthRange, { userInitiated: false });
    updateDeathYearRange(deathRange, { userInitiated: false });
    birthRangeIsUserSetRef.current = false;
    setBirthRangeIsUserSet(false);
    deathRangeIsUserSetRef.current = false;
    setDeathRangeIsUserSet(false);
  };

  const clearFiltersForNewSearch = (nodesList = []) => {
    const nextNodes = (Array.isArray(nodesList) && nodesList.length > 0) ? nodesList : networkData.nodes;
    resetFiltersForNodeSet(nextNodes);
    setShowFilterPanel(false);
  };

  const clearAllFilters = () => resetFiltersForNodeSet();

  // ---------------------------------------------------------------------------
  // Hook wrappers around pure filter utils
  // ---------------------------------------------------------------------------

  const isNodeVisible = (node) => isNodeVisibleUtil(node, getFilterState());
  const getNodeOpacity = (node) => getNodeOpacityUtil(node, getFilterState());
  const isNodeVisibleWithoutVoiceFilter = (node) => isNodeVisibleWithoutVoiceFilterUtil(node, getFilterState());
  const isNodeVisibleWithoutBirthplaceFilter = (node) => isNodeVisibleWithoutBirthplaceFilterUtil(node, getFilterState());
  const getVisibleVoiceTypes = () => getVisibleVoiceTypesUtil(networkData.nodes, getFilterState());
  const getVisibleBirthplaces = () => getVisibleBirthplacesUtil(networkData.nodes, getFilterState());

  const isLinkVisible = (link) => {
    const sourceNode = typeof link.source === 'string'
      ? networkData.nodes.find(n => n.id === link.source) : link.source;
    const targetNode = typeof link.target === 'string'
      ? networkData.nodes.find(n => n.id === link.target) : link.target;
    return sourceNode && targetNode && isNodeVisible(sourceNode) && isNodeVisible(targetNode);
  };

  const getFilterCounts = () => ({
    totalNodes: networkData.nodes.length,
    visibleNodes: networkData.nodes.filter(isNodeVisible).length,
  });

  return {
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
    getNodeOpacity,
    isLinkVisible,
    getFilterCounts,
  };
};

export default useFilters;
