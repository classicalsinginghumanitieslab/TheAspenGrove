import React, { useState, useEffect, useRef } from 'react';
import * as d3 from 'd3';

const ClassicalMusicGenealogy = () => {
  const [token, setToken] = useState('');
  const [currentView, setCurrentView] = useState('search');
  const [searchType, setSearchType] = useState('singers');
  const [originalSearchType, setOriginalSearchType] = useState('singers');
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [originalSearchResults, setOriginalSearchResults] = useState([]);
  const [selectedItem, setSelectedItem] = useState(null);
  const [itemDetails, setItemDetails] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [networkData, setNetworkData] = useState({ nodes: [], links: [] });
  const [shouldRunSimulation, setShouldRunSimulation] = useState(false);
  const [contextMenu, setContextMenu] = useState({ show: false, x: 0, y: 0, node: null });
  const [visualizationHeight, setVisualizationHeight] = useState(600);
  const [isResizing, setIsResizing] = useState(false);
  const [resizeStartY, setResizeStartY] = useState(0);
  const [resizeStartHeight, setResizeStartHeight] = useState(0);
  const [selectedNode, setSelectedNode] = useState(null);
  const [expandSubmenu, setExpandSubmenu] = useState(null);
  const [profileCard, setProfileCard] = useState({ show: false, data: null });
  const [actualCounts, setActualCounts] = useState({});

  const svgRef = useRef(null);
  const simulationRef = useRef(null);
  const submenuTimeoutRef = useRef(null);

  const API_BASE = 'http://localhost:3001';

  // Initialize token on component mount
  useEffect(() => {
    const savedToken = localStorage.getItem('token');
    if (savedToken) {
      setToken(savedToken);
    }
  }, []);

  // Close context menu when clicking elsewhere
  useEffect(() => {
    const handleClick = () => {
      setContextMenu({ show: false, x: 0, y: 0, node: null });
      setExpandSubmenu(null);
      // Clear any pending submenu timeout
      if (submenuTimeoutRef.current) {
        clearTimeout(submenuTimeoutRef.current);
      }
    };
    document.addEventListener('click', handleClick);
    return () => document.removeEventListener('click', handleClick);
  }, []);

  // Fetch actual counts when context menu opens for isolated nodes
  useEffect(() => {
    if (contextMenu.show && contextMenu.node) {
      const isNodeAlone = networkData.nodes.length === 1 || networkData.links.length === 0;
      if (isNodeAlone && !actualCounts[contextMenu.node.id]) {
        fetchActualCounts(contextMenu.node);
      }
    }
  }, [contextMenu.show, contextMenu.node, networkData.nodes.length, networkData.links.length]);

  // Handle resize functionality
  useEffect(() => {
    const handleMouseMove = (e) => {
      if (!isResizing) return;
      
      const deltaY = e.clientY - resizeStartY;
      const newHeight = Math.max(200, Math.min(1000, resizeStartHeight + deltaY));
      setVisualizationHeight(newHeight);
    };

    const handleMouseUp = () => {
      setIsResizing(false);
    };

    if (isResizing) {
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
    }

    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [isResizing, resizeStartY, resizeStartHeight]);

  const handleResizeStart = (e) => {
    setIsResizing(true);
    setResizeStartY(e.clientY);
    setResizeStartHeight(visualizationHeight);
  };

  const login = async (email, password) => {
    try {
      setLoading(true);
      const response = await fetch(`${API_BASE}/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password })
      });
      
      const data = await response.json();
      if (response.ok) {
        setToken(data.token);
        localStorage.setItem('token', data.token);
        setError('');
      } else {
        setError(data.error);
      }
    } catch (err) {
      setError('Login failed - please try again');
    } finally {
      setLoading(false);
    }
  };

  const register = async (email, password) => {
    try {
      setLoading(true);
      const response = await fetch(`${API_BASE}/auth/register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password })
      });
      
      const data = await response.json();
      if (response.ok) {
        setToken(data.token);
        localStorage.setItem('token', data.token);
        setError('');
      } else {
        setError(data.error);
      }
    } catch (err) {
      setError('Registration failed - please try again');
    } finally {
      setLoading(false);
    }
  };

  const performSearch = async () => {
    if (!searchQuery.trim() || searchQuery.length < 2) {
      setError('Please enter at least 2 characters');
      return;
    }

    try {
      setLoading(true);
      setError('');
      
      const endpoint = searchType === 'singers' ? '/search/singers' : 
                     searchType === 'operas' ? '/search/operas' : '/search/books';
      
      const response = await fetch(`${API_BASE}${endpoint}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({ query: searchQuery })
      });

      const data = await response.json();
      if (response.ok) {
        setSearchResults(data[searchType] || []);
        setOriginalSearchResults(data[searchType] || []);
        setOriginalSearchType(searchType);
        setCurrentView('results');
        
        // Generate network data from search results
        generateNetworkFromSearchResults(data[searchType] || [], searchType);
      } else {
        setError(data.error || 'Search failed');
      }
    } catch (err) {
      setError('Search failed - please try again');
    } finally {
      setLoading(false);
    }
  };

  const generateNetworkFromSearchResults = (results, type) => {
    const nodes = [];
    const links = [];
    
    results.forEach((item, index) => {
      if (type === 'singers') {
        nodes.push({
          id: item.name,
          name: item.name,
          type: 'person',
          voiceType: item.properties.voice_type,
          birthYear: item.properties.birth_year,
          deathYear: item.properties.death_year,
          x: Math.random() * 800,
          y: Math.random() * 600
        });
      } else if (type === 'operas') {
        nodes.push({
          id: item.properties.title,
          name: item.properties.title,
          type: 'opera',
          composer: item.properties.composer,
          x: Math.random() * 800,
          y: Math.random() * 600
        });
      } else if (type === 'books') {
        nodes.push({
          id: item.properties.title,
          name: item.properties.title,
          type: 'book',
          author: item.properties.author,
          x: Math.random() * 800,
          y: Math.random() * 600
        });
      }
    });

    setNetworkData({ nodes, links });
    setShouldRunSimulation(true); // Trigger simulation for search results
    setShouldRunSimulation(true); // Trigger simulation for new network
  };

  const generateNetworkFromDetails = (details, centerName, type) => {
    const nodes = [];
    const links = [];
    const addedNodes = new Set(); // Track which people have been added
    
    // Helper function to add a person node only if not already added
    const addPersonNode = (person, defaultX, defaultY) => {
      if (!addedNodes.has(person.full_name)) {
        nodes.push({
          id: person.full_name,
          name: person.full_name,
          type: 'person',
          voiceType: person.voice_type,
          birthYear: person.birth_year,
          deathYear: person.death_year,
          x: defaultX,
          y: defaultY
        });
        addedNodes.add(person.full_name);
      }
    };
    
    // Add center node
    const centerNode = {
      id: centerName,
      name: centerName,
      type: 'person',
      isCenter: true,
      x: 400,
      y: 300
    };
    
    if (type === 'singers' && details.center) {
      centerNode.voiceType = details.center.voice_type;
      centerNode.birthYear = details.center.birth_year;
      centerNode.deathYear = details.center.death_year;
    }
    
    nodes.push(centerNode);
    addedNodes.add(centerName);

    // Add teachers
    if (details.teachers) {
      details.teachers.forEach((teacher, index) => {
        addPersonNode(teacher, 200 + (index * 50), 150);
        
        links.push({
          source: teacher.full_name,
          target: centerName,
          type: 'taught',
          label: 'taught'
        });
      });
    }

    // Add students
    if (details.students) {
      details.students.forEach((student, index) => {
        addPersonNode(student, 200 + (index * 50), 450);
        
        links.push({
          source: centerName,
          target: student.full_name,
          type: 'taught',
          label: 'taught'
        });
      });
    }

    // Add family
    if (details.family) {
      details.family.forEach((relative, index) => {
        addPersonNode(relative, 600 + (index * 50), 200 + (index * 50));
        
        links.push({
          source: centerName,
          target: relative.full_name,
          type: 'family',
          label: relative.relationship_type || 'family'
        });
      });
    }

    // Add works
    if (details.works) {
      // Add operas
      if (details.works.operas) {
        details.works.operas.forEach((opera, index) => {
          const operaId = `opera_${opera.title}`;
          nodes.push({
            id: operaId,
            name: opera.title,
            type: 'opera',
            role: opera.role,
            x: 100 + (index * 80),
            y: 500
          });
          
          links.push({
            source: centerName,
            target: operaId,
            type: 'premiered',
            label: 'premiered role in'
          });
        });
      }

      // Add books
      if (details.works.books) {
        details.works.books.forEach((book, index) => {
          const bookId = `book_${book.title}`;
          nodes.push({
            id: bookId,
            name: book.title,
            type: 'book',
            x: 500 + (index * 80),
            y: 500
          });
          
          links.push({
            source: centerName,
            target: bookId,
            type: 'authored',
            label: 'authored'
          });
        });
      }

      // Add composed operas
      if (details.works.composedOperas) {
        details.works.composedOperas.forEach((opera, index) => {
          const operaId = `composed_opera_${opera.title}`;
          nodes.push({
            id: operaId,
            name: opera.title,
            type: 'opera',
            x: 100 + (index * 80),
            y: 400
          });
          
          links.push({
            source: centerName,
            target: operaId,
            type: 'composed',
            label: 'composed'
          });
        });
      }
    }

    // Add premiered roles for operas
    if (details.premieredRoles) {
      details.premieredRoles.forEach((role, index) => {
        const singerId = role.singer;
        // Use the same deduplication logic
        if (!addedNodes.has(singerId)) {
          nodes.push({
            id: singerId,
            name: singerId,
            type: 'person',
            x: 300 + (index * 60),
            y: 400
          });
          addedNodes.add(singerId);
        }
        
        links.push({
          source: singerId,
          target: centerName,
          type: 'premiered',
          label: 'premiered role in'
        });
      });
    }

    setNetworkData({ nodes, links });
  };

  // Function to show full information profile card
  const showFullInformation = async (node) => {
    if (node.type === 'person') {
      try {
        setLoading(true);
        const response = await fetch(`${API_BASE}/singer/network`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ singerName: node.name, depth: 1 })
        });

        const data = await response.json();
        if (response.ok) {
          setProfileCard({ show: true, data: data.center });
        } else {
          setError(data.error);
        }
      } catch (err) {
        setError('Failed to fetch profile information');
      } finally {
        setLoading(false);
      }
    }
  };

  // Function to expand all relationships for a node
  const expandAllRelationships = async (node) => {
    try {
      setLoading(true);
      let response, data;
      
      if (node.type === 'person') {
        response = await fetch(`${API_BASE}/singer/network`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ singerName: node.name, depth: 2 })
        });
      } else if (node.type === 'opera') {
        response = await fetch(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ operaName: node.name })
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

      if (response) {
        data = await response.json();
        if (response.ok) {
          // Merge new data with existing network
          const existingNodes = new Set(networkData.nodes.map(n => n.id));
          const existingLinks = new Set(networkData.links.map(l => {
            const sourceId = typeof l.source === 'string' ? l.source : l.source?.id;
            const targetId = typeof l.target === 'string' ? l.target : l.target?.id;
            return `${sourceId}-${targetId}-${l.type}`;
          }));
          
          const newNodes = [];
          const newLinks = [];
          
          // Handle different node types and their data structures
          if (node.type === 'person') {
            // Add new nodes from the expanded data for people
            if (data.teachers) {
              data.teachers.forEach(teacher => {
                if (!existingNodes.has(teacher.full_name)) {
                  newNodes.push({
                    id: teacher.full_name,
                    name: teacher.full_name,
                    type: 'person',
                    voiceType: teacher.voice_type,
                    birthYear: teacher.birth_year,
                    deathYear: teacher.death_year,
                    x: Math.random() * 800,
                    y: Math.random() * 600
                  });
                }
                
                const linkKey = `${teacher.full_name}-${node.name}-taught`;
                if (!existingLinks.has(linkKey)) {
                  newLinks.push({
                    source: teacher.full_name,
                    target: node.name,
                    type: 'taught',
                    label: 'taught'
                  });
                }
              });
            }
            
            if (data.students) {
              data.students.forEach(student => {
                if (!existingNodes.has(student.full_name)) {
                  newNodes.push({
                    id: student.full_name,
                    name: student.full_name,
                    type: 'person',
                    voiceType: student.voice_type,
                    birthYear: student.birth_year,
                    deathYear: student.death_year,
                    x: Math.random() * 800,
                    y: Math.random() * 600
                  });
                }
                
                const linkKey = `${node.name}-${student.full_name}-taught`;
                if (!existingLinks.has(linkKey)) {
                  newLinks.push({
                    source: node.name,
                    target: student.full_name,
                    type: 'taught',
                    label: 'taught'
                  });
                }
              });
            }
            
            if (data.family) {
              data.family.forEach(relative => {
                if (!existingNodes.has(relative.full_name)) {
                  newNodes.push({
                    id: relative.full_name,
                    name: relative.full_name,
                    type: 'person',
                    voiceType: relative.voice_type,
                    birthYear: relative.birth_year,
                    deathYear: relative.death_year,
                    x: Math.random() * 800,
                    y: Math.random() * 600
                  });
                }
                
                const linkKey = `${node.name}-${relative.full_name}-family`;
                if (!existingLinks.has(linkKey)) {
                  newLinks.push({
                    source: node.name,
                    target: relative.full_name,
                    type: 'family',
                    label: relative.relationship_type || 'family'
                  });
                }
              });
            }
            
            if (data.works) {
              if (data.works.operas) {
                data.works.operas.forEach(opera => {
                  const operaId = `opera_${opera.title}`;
                  if (!existingNodes.has(operaId)) {
                    newNodes.push({
                      id: operaId,
                      name: opera.title,
                      type: 'opera',
                      role: opera.role,
                      x: Math.random() * 800,
                      y: Math.random() * 600
                    });
                  }
                  
                  const linkKey = `${node.name}-${operaId}-premiered`;
                  if (!existingLinks.has(linkKey)) {
                    newLinks.push({
                      source: node.name,
                      target: operaId,
                      type: 'premiered',
                      label: 'premiered role in'
                    });
                  }
                });
              }
              
              if (data.works.books) {
                data.works.books.forEach(book => {
                  const bookId = `book_${book.title}`;
                  if (!existingNodes.has(bookId)) {
                    newNodes.push({
                      id: bookId,
                      name: book.title,
                      type: 'book',
                      x: Math.random() * 800,
                      y: Math.random() * 600
                    });
                  }
                  
                  const linkKey = `${node.name}-${bookId}-authored`;
                  if (!existingLinks.has(linkKey)) {
                    newLinks.push({
                      source: node.name,
                      target: bookId,
                      type: 'authored',
                      label: 'authored'
                    });
                  }
                });
              }
            }
          } else if (node.type === 'opera') {
            // Handle opera expansion - add performers, composers, etc.
            if (data.premieredRoles) {
              data.premieredRoles.forEach(role => {
                const singerId = role.singer;
                if (!existingNodes.has(singerId)) {
                  newNodes.push({
                    id: singerId,
                    name: singerId,
                    type: 'person',
                    x: Math.random() * 800,
                    y: Math.random() * 600
                  });
                }
                
                const linkKey = `${singerId}-${node.name}-premiered`;
                if (!existingLinks.has(linkKey)) {
                  newLinks.push({
                    source: singerId,
                    target: node.name,
                    type: 'premiered',
                    label: 'premiered role in'
                  });
                }
              });
            }
          } else if (node.type === 'book') {
            // Handle book expansion - add authors, editors, etc.
            if (data.book && data.book.author) {
              const authorId = data.book.author;
              if (!existingNodes.has(authorId)) {
                newNodes.push({
                  id: authorId,
                  name: authorId,
                  type: 'person',
                  x: Math.random() * 800,
                  y: Math.random() * 600
                });
              }
              
              const linkKey = `${authorId}-${node.name}-authored`;
              if (!existingLinks.has(linkKey)) {
                newLinks.push({
                  source: authorId,
                  target: node.name,
                  type: 'authored',
                  label: 'authored'
                });
              }
            }
          }
          
          // Update network data with new nodes and links
          setNetworkData({
            nodes: [...networkData.nodes, ...newNodes],
            links: [...networkData.links, ...newLinks]
          });
          setShouldRunSimulation(true);
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
    try {
      setLoading(true);
      let response, data;
      
      if (node.type === 'person') {
        response = await fetch(`${API_BASE}/singer/network`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ singerName: node.name, depth: 2 })
        });
      } else if (node.type === 'opera') {
        response = await fetch(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ operaName: node.name })
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

      if (response) {
        data = await response.json();
        if (response.ok) {
          // Merge new data with existing network
          const existingNodes = new Set(networkData.nodes.map(n => n.id));
          const existingLinks = new Set(networkData.links.map(l => {
            const sourceId = typeof l.source === 'string' ? l.source : l.source?.id;
            const targetId = typeof l.target === 'string' ? l.target : l.target?.id;
            return `${sourceId}-${targetId}-${l.type}`;
          }));
          
          const newNodes = [];
          const newLinks = [];
          
          // Handle specific relationship types for people
          if (node.type === 'person') {
            if (relationshipType === 'taughtBy' && data.teachers) {
              data.teachers.forEach(teacher => {
                if (!existingNodes.has(teacher.full_name)) {
                  newNodes.push({
                    id: teacher.full_name,
                    name: teacher.full_name,
                    type: 'person',
                    voiceType: teacher.voice_type,
                    birthYear: teacher.birth_year,
                    deathYear: teacher.death_year,
                    x: Math.random() * 800,
                    y: Math.random() * 600
                  });
                }
                
                const linkKey = `${teacher.full_name}-${node.name}-taught`;
                if (!existingLinks.has(linkKey)) {
                  newLinks.push({
                    source: teacher.full_name,
                    target: node.name,
                    type: 'taught',
                    label: 'taught'
                  });
                }
              });
            }
            
            if (relationshipType === 'taught' && data.students) {
              data.students.forEach(student => {
                if (!existingNodes.has(student.full_name)) {
                  newNodes.push({
                    id: student.full_name,
                    name: student.full_name,
                    type: 'person',
                    voiceType: student.voice_type,
                    birthYear: student.birth_year,
                    deathYear: student.death_year,
                    x: Math.random() * 800,
                    y: Math.random() * 600
                  });
                }
                
                const linkKey = `${node.name}-${student.full_name}-taught`;
                if (!existingLinks.has(linkKey)) {
                  newLinks.push({
                    source: node.name,
                    target: student.full_name,
                    type: 'taught',
                    label: 'taught'
                  });
                }
              });
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
                  if (!existingNodes.has(relative.full_name)) {
                    newNodes.push({
                      id: relative.full_name,
                      name: relative.full_name,
                      type: 'person',
                      voiceType: relative.voice_type,
                      birthYear: relative.birth_year,
                      deathYear: relative.death_year,
                      x: Math.random() * 800,
                      y: Math.random() * 600
                    });
                  }
                  
                  const linkKey = `${node.name}-${relative.full_name}-family`;
                  if (!existingLinks.has(linkKey)) {
                    newLinks.push({
                      source: node.name,
                      target: relative.full_name,
                      type: 'family',
                      label: relative.relationship_type || 'family'
                    });
                  }
                }
              });
            }
            
            if (relationshipType === 'authored' && data.works && data.works.books) {
              data.works.books.forEach(book => {
                const bookId = `book_${book.title}`;
                if (!existingNodes.has(bookId)) {
                  newNodes.push({
                    id: bookId,
                    name: book.title,
                    type: 'book',
                    x: Math.random() * 800,
                    y: Math.random() * 600
                  });
                }
                
                const linkKey = `${node.name}-${bookId}-authored`;
                if (!existingLinks.has(linkKey)) {
                  newLinks.push({
                    source: node.name,
                    target: bookId,
                    type: 'authored',
                    label: 'authored'
                  });
                }
              });
            }
            
            if (relationshipType === 'premieredRoleIn' && data.works && data.works.operas) {
              data.works.operas.forEach(opera => {
                const operaId = `opera_${opera.title}`;
                if (!existingNodes.has(operaId)) {
                  newNodes.push({
                    id: operaId,
                    name: opera.title,
                    type: 'opera',
                    role: opera.role,
                    x: Math.random() * 800,
                    y: Math.random() * 600
                  });
                }
                
                const linkKey = `${node.name}-${operaId}-premiered`;
                if (!existingLinks.has(linkKey)) {
                  newLinks.push({
                    source: node.name,
                    target: operaId,
                    type: 'premiered',
                    label: 'premiered role in'
                  });
                }
              });
            }
          } else if (node.type === 'opera') {
            if ((relationshipType === 'wrote' || relationshipType === 'wroteBy') && data.premieredRoles) {
              data.premieredRoles.forEach(role => {
                const singerId = role.singer;
                if (!existingNodes.has(singerId)) {
                  newNodes.push({
                    id: singerId,
                    name: singerId,
                    type: 'person',
                    x: Math.random() * 800,
                    y: Math.random() * 600
                  });
                }
                
                const linkKey = `${singerId}-${node.name}-premiered`;
                if (!existingLinks.has(linkKey)) {
                  newLinks.push({
                    source: singerId,
                    target: node.name,
                    type: 'premiered',
                    label: 'premiered role in'
                  });
                }
              });
            }
          } else if (node.type === 'book') {
            if ((relationshipType === 'authored' || relationshipType === 'authoredBy') && data.book && data.book.author) {
              const authorId = data.book.author;
              if (!existingNodes.has(authorId)) {
                newNodes.push({
                  id: authorId,
                  name: authorId,
                  type: 'person',
                  x: Math.random() * 800,
                  y: Math.random() * 600
                });
              }
              
              const linkKey = `${authorId}-${node.name}-authored`;
              if (!existingLinks.has(linkKey)) {
                newLinks.push({
                  source: authorId,
                  target: node.name,
                  type: 'authored',
                  label: 'authored'
                });
              }
            }
          }
          
          // Update network data with new nodes and links
          setNetworkData({
            nodes: [...networkData.nodes, ...newNodes],
            links: [...networkData.links, ...newLinks]
          });
          setShouldRunSimulation(true);
        } else {
          setError(data.error);
        }
      }
    } catch (err) {
      setError('Failed to expand specific relationship');
    } finally {
      setLoading(false);
    }
  };

  // Function to dismiss other nodes (keep only the selected node, no relationships)
  const dismissOtherNodes = (selectedNode) => {
    const filteredNodes = networkData.nodes.filter(node => node.id === selectedNode.id);
    
    // Remove all relationships - this creates a new visualization starting from this node
    setNetworkData({
      nodes: filteredNodes,
      links: [] // No relationships - clean slate
    });
  };

  // Function to dismiss the selected node
  const dismissNode = (nodeToRemove) => {
    const filteredNodes = networkData.nodes.filter(node => node.id !== nodeToRemove.id);
    const filteredLinks = networkData.links.filter(link => {
      const sourceId = typeof link.source === 'string' ? link.source : link.source?.id;
      const targetId = typeof link.target === 'string' ? link.target : link.target?.id;
      return sourceId !== nodeToRemove.id && targetId !== nodeToRemove.id;
    });
    
    setNetworkData({
      nodes: filteredNodes,
      links: filteredLinks
    });
  };

  const getItemDetails = async (item) => {
    try {
      setLoading(true);
      setSelectedItem(item);
      
      if (searchType === 'singers') {
        const response = await fetch(`${API_BASE}/singer/network`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ singerName: item.name, depth: 2 })
        });

        const data = await response.json();
        if (response.ok) {
          console.log('Singer network data:', data);
          setItemDetails(data);
          setCurrentView('network');
          generateNetworkFromDetails(data, item.name, 'singers');
        } else {
          setError(data.error);
        }
      } else if (searchType === 'operas') {
        const response = await fetch(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ operaName: item.properties.title })
        });

        const data = await response.json();
        if (response.ok) {
          setItemDetails(data);
          setCurrentView('network');
          generateNetworkFromDetails(data, item.properties.title, 'operas');
        } else {
          setError(data.error);
        }
      } else if (searchType === 'books') {
        const response = await fetch(`${API_BASE}/book/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ bookTitle: item.properties.title })
        });

        const data = await response.json();
        if (response.ok) {
          setItemDetails(data);
          setCurrentView('network');
          generateNetworkFromDetails(data, item.properties.title, 'books');
        } else {
          setError(data.error);
        }
      }
    } catch (err) {
      setError('Failed to fetch details');
    } finally {
      setLoading(false);
    }
  };

  const searchForPerson = async (personName) => {
    try {
      setLoading(true);
      setError('');
      
      const response = await fetch(`${API_BASE}/singer/network`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({ singerName: personName, depth: 2 })
      });

      const data = await response.json();
      if (response.ok) {
        console.log('Singer network data:', data);
        setItemDetails(data);
        setSelectedItem({ name: personName });
        setSearchType('singers');
        setCurrentView('network');
        generateNetworkFromDetails(data, personName, 'singers');
        setShouldRunSimulation(true); // Trigger simulation for person search
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
    
    const colorMap = {
      'Soprano': '#228B22',
      'Mezzo-soprano': '#8B4513',
      'Alto': '#654321',
      'Tenor': '#DAA520',
      'Baritone': '#B0C4DE',
      'Bass': '#708090',
      'Bass-baritone': '#2F4F4F',
      'Composer': '#FF6347'
    };
    
    return colorMap[node.voiceType] || '#6B7280';
  };

  // Network visualization using D3
  const NetworkVisualization = () => {
    const containerRef = useRef(null);
    const isSimulationActiveRef = useRef(true);
    const hasSimulationRunRef = useRef(false); // Track if simulation has run for this network

    useEffect(() => {
      if (!networkData.nodes.length || !containerRef.current) return;

      const container = containerRef.current;
      const width = container.clientWidth;
      const height = visualizationHeight;

      // Clear previous visualization
      d3.select(svgRef.current).selectAll("*").remove();

      const svg = d3.select(svgRef.current)
        .attr("width", width)
        .attr("height", height)
        .style("background", "#FEFEFE");

      // Create zoom behavior
      const zoom = d3.zoom()
        .scaleExtent([0.1, 4])
        .on("zoom", (event) => {
          g.attr("transform", event.transform);
        });

      svg.call(zoom);

      // Create main group for zooming/panning
      const g = svg.append("g");

      // Create links (using paths)
      const link = g.append("g")
        .selectAll("path")
        .data(networkData.links)
        .enter()
        .append("path")
        .attr("stroke", "#6B7280")
        .attr("stroke-opacity", .6)
        .attr("stroke-width", 1.5)
        .attr("fill", "none");

      // Create link labels
      const linkLabels = g.append("g")
        .selectAll("text")
        .data(networkData.links)
        .enter()
        .append("text")
        .attr("font-family", "'Inter', 'Helvetica Neue', Arial, sans-serif")
        .attr("font-size", "10px")
        .attr("font-weight", "400")
        .attr("fill", "#666")
        .attr("text-anchor", "middle")
        .attr("dy", "-5px")
        .style("pointer-events", "none")
        .text(d => d.label);

      // Create nodes
      const node = g.append("g")
        .selectAll("circle")
        .data(networkData.nodes)
        .enter()
        .append("circle")
        .attr("r", 40)
        .attr("fill", d => getNodeColor(d))
        .attr("stroke", d => {
          if (selectedNode && selectedNode.id === d.id) {
            return d3.color(getNodeColor(d)).darker(0.5);
          } else if (d.type === 'opera') {
            return "#DC2626";
          } else if (d.type === 'book') {
            return "#1D4ED8";
          } else {
            return "none";
          }
        })
        .attr("stroke-width", d => {
          if (selectedNode && selectedNode.id === d.id) {
            return 3;
          } else if (d.type === 'opera' || d.type === 'book') {
            return 3;
          } else {
            return 0;
          }
        })
        .style("cursor", "pointer")
        .on("click", (event, d) => {
          setSelectedNode(selectedNode && selectedNode.id === d.id ? null : d);
        })
        .on("contextmenu", (event, d) => {
          event.preventDefault();
          event.stopPropagation();
          
          // Calculate position 20px to the right of the node's right edge
          const nodeRadius = 40;
          const menuOffset = 20;
          const containerRect = container.getBoundingClientRect();
          
          // Position relative to the container
          const nodeX = d.x + nodeRadius + menuOffset;
          const nodeY = d.y - nodeRadius; // Align with top of the node
          
          // Calculate absolute position
          const absoluteX = containerRect.left + nodeX;
          const absoluteY = containerRect.top + nodeY;
          
          // Keep menu within container bounds
          const menuWidth = 250; // estimated menu width
          const menuHeight = 300; // estimated menu height
          
          let finalX = absoluteX;
          let finalY = absoluteY;
          
          // Adjust if menu would go outside container
          if (absoluteX + menuWidth > containerRect.right) {
            finalX = containerRect.left + d.x - nodeRadius - menuOffset - menuWidth;
          }
          
          if (absoluteY + menuHeight > containerRect.bottom) {
            finalY = containerRect.bottom - menuHeight;
          }
          
          if (finalY < containerRect.top) {
            finalY = containerRect.top;
          }
          
          setContextMenu({
            show: true,
            x: finalX,
            y: finalY,
            node: d
          });
          setExpandSubmenu(null); // Reset submenu state
          // Clear any pending submenu timeout
          if (submenuTimeoutRef.current) {
            clearTimeout(submenuTimeoutRef.current);
          }
        })
        .call(d3.drag()
          .on("start", dragstarted)
          .on("drag", dragged)
          .on("end", dragended));

      // Create node labels with multi-line text support
      const nodeLabels = g.append("g")
        .selectAll("g")
        .data(networkData.nodes)
        .enter()
        .append("g")
        .attr("class", "node-label");

      // Function to wrap text in multiple lines
      const wrapText = (textElement, text, maxWidth, fontSize) => {
        const words = text.split(/(\s+|-)/);
        const lines = [];
        let currentLine = '';
        
        const charWidth = fontSize * 0.55;
        const maxCharsPerLine = Math.floor(maxWidth / charWidth);
        
        for (let word of words) {
          if (word === '') continue;
          
          const testLine = currentLine + word;
          
          if (testLine.length <= maxCharsPerLine || currentLine === '') {
            currentLine = testLine;
          } else {
            if (currentLine) {
              lines.push(currentLine);
              currentLine = word;
            } else {
              lines.push(word.substring(0, maxCharsPerLine - 1) + '-');
              currentLine = word.substring(maxCharsPerLine - 1);
            }
          }
        }
        
        if (currentLine) {
          lines.push(currentLine);
        }
        
        const finalLines = lines.slice(0, 3);
        if (lines.length > 3) {
          finalLines[2] = finalLines[2].substring(0, finalLines[2].length - 3) + '...';
        }
        
        return finalLines;
      };

      // Add text elements for each node
      nodeLabels.each(function(d) {
        const group = d3.select(this);
        const fontSize = d.isCenter ? 11 : 10;
        const radius = 40;
        const maxWidth = radius * 1.6;
        
        const lines = wrapText(this, d.name, maxWidth, fontSize);
        const lineHeight = fontSize * 1.2;
        
        if (lines.length === 1) {
          group.append("text")
            .attr("font-family", "'Inter', 'Helvetica Neue', Arial, sans-serif")
            .attr("font-size", `${fontSize}px`)
            .attr("font-weight", d.isCenter ? "600" : "500")
            .attr("fill", "#1F2937")
            .attr("text-anchor", "middle")
            .attr("x", 0)
            .attr("y", 0)
            .attr("dy", "0.35em")
            .style("pointer-events", "none")
            .text(lines[0]);
        } else {
          const totalHeight = (lines.length - 1) * lineHeight;
          const startOffset = -(totalHeight / 2);
          
          lines.forEach((line, i) => {
            group.append("text")
              .attr("font-family", "'Inter', 'Helvetica Neue', Arial, sans-serif")
              .attr("font-size", `${fontSize}px`)
              .attr("font-weight", d.isCenter ? "600" : "500")
              .attr("fill", "#1F2937")
              .attr("text-anchor", "middle")
              .attr("x", 0)
              .attr("y", startOffset + (i * lineHeight))
              .attr("dy", "0.35em")
              .style("pointer-events", "none")
              .text(line);
          });
        }
      });

      
      const renderNetwork = () => {
        // Convert link source/target to objects if they're strings
        const processedLinks = networkData.links.map(link => ({
          ...link,
          source: typeof link.source === 'string' ? 
            networkData.nodes.find(n => n.id === link.source) : link.source,
          target: typeof link.target === 'string' ? 
            networkData.nodes.find(n => n.id === link.target) : link.target
        }));

        // Position links
        link.attr("d", d => {
          const source = typeof d.source === 'string' ? 
            networkData.nodes.find(n => n.id === d.source) : d.source;
          const target = typeof d.target === 'string' ? 
            networkData.nodes.find(n => n.id === d.target) : d.target;
            
          if (!source || !target) return '';
          
          const dx = target.x - source.x;
          const dy = target.y - source.y;
          const distance = Math.sqrt(dx * dx + dy * dy);
          const nodeRadius = 40;
          
          const endX = target.x - (dx / distance) * (nodeRadius + 17);
          const endY = target.y - (dy / distance) * (nodeRadius + 17);
          
          return `M${source.x},${source.y}L${endX},${endY}`;
        });

        // Remove any existing arrows and recreate them
        g.selectAll(".arrow-group").remove();
        
        // Create arrows directly for each link
        processedLinks.forEach(linkData => {
          if (!linkData.source || !linkData.target) return;
          
          const dx = linkData.target.x - linkData.source.x;
          const dy = linkData.target.y - linkData.source.y;
          const distance = Math.sqrt(dx * dx + dy * dy);
          
          if (distance > 0) {
            const nodeRadius = 40;
            const arrowX = linkData.target.x - (dx / distance) * (nodeRadius + 10);
            const arrowY = linkData.target.y - (dy / distance) * (nodeRadius + 10);
            
            const angle = Math.atan2(dy, dx);
            const arrowLength = 8;
            
            const x1 = arrowX - Math.cos(angle - Math.PI / 6) * arrowLength;
            const y1 = arrowY - Math.sin(angle - Math.PI / 6) * arrowLength;
            
            const x2 = arrowX - Math.cos(angle + Math.PI / 6) * arrowLength;
            const y2 = arrowY - Math.sin(angle + Math.PI / 6) * arrowLength;
            
            g.append("polygon")
              .attr("class", "arrow-group")
              .attr("points", `${arrowX},${arrowY} ${x1},${y1} ${x2},${y2}`)
              .attr("fill", "#6B7280")
              .attr("opacity", ".8")
              .attr("stroke", "none");
          }
        });

        // Position link labels
        linkLabels
          .attr("x", d => {
            const source = typeof d.source === 'string' ? 
              networkData.nodes.find(n => n.id === d.source) : d.source;
            const target = typeof d.target === 'string' ? 
              networkData.nodes.find(n => n.id === d.target) : d.target;
            return source && target ? (source.x + target.x) / 2 : 0;
          })
          .attr("y", d => {
            const source = typeof d.source === 'string' ? 
              networkData.nodes.find(n => n.id === d.source) : d.source;
            const target = typeof d.target === 'string' ? 
              networkData.nodes.find(n => n.id === d.target) : d.target;
            return source && target ? (source.y + target.y) / 2 : 0;
          })
          .attr("transform", d => {
            const source = typeof d.source === 'string' ? 
              networkData.nodes.find(n => n.id === d.source) : d.source;
            const target = typeof d.target === 'string' ? 
              networkData.nodes.find(n => n.id === d.target) : d.target;
              
            if (!source || !target) return '';
            
            const dx = target.x - source.x;
            const dy = target.y - source.y;
            const angle = Math.atan2(dy, dx) * 180 / Math.PI;
            const x = (source.x + target.x) / 2;
            const y = (source.y + target.y) / 2;
            
            const adjustedAngle = Math.abs(angle) > 90 ? angle + 180 : angle;
            
            return `rotate(${adjustedAngle}, ${x}, ${y})`;
          });

        // Position nodes
        node
          .attr("cx", d => d.x)
          .attr("cy", d => d.y)
          .attr("stroke", d => {
            if (selectedNode && selectedNode.id === d.id) {
              return d3.color(getNodeColor(d)).darker(0.5);
            } else if (d.type === 'opera') {
              return "#DC2626";
            } else if (d.type === 'book') {
              return "#1D4ED8";
            } else {
              return "none";
            }
          })
          .attr("stroke-width", d => {
            if (selectedNode && selectedNode.id === d.id) {
              return 3;
            } else if (d.type === 'opera' || d.type === 'book') {
              return 3;
            } else {
              return 0;
            }
          });

        // Position node labels
        nodeLabels
          .attr("transform", d => `translate(${d.x}, ${d.y})`);
      };

      // Create simulation for initial positioning only - controlled by shouldRunSimulation flag
      if (shouldRunSimulation) {
        // Randomize initial positions to force the simulation to work
        networkData.nodes.forEach(node => {
          node.x = Math.random() * width;
          node.y = Math.random() * height;
          node.fx = null;
          node.fy = null;
          node.vx = 0; // Reset velocity
          node.vy = 0;
        });

        console.log("Starting controlled simulation for new network");

        try {
          const simulation = d3.forceSimulation(networkData.nodes)
            .force("link", d3.forceLink(networkData.links).id(d => d.id).distance(120).strength(0.3))
            .force("charge", d3.forceManyBody().strength(-800))
            .force("center", d3.forceCenter(width / 2, height / 2))
            .force("collision", d3.forceCollide().radius(50))
            .alpha(1)
            .alphaDecay(0.02)
            .alphaMin(0.01);

          simulationRef.current = simulation;
          isSimulationActiveRef.current = true;

          // Set up event handlers
          simulation.on("tick", renderNetwork);
          
          simulation.on("end", () => {
            console.log("Simulation ended - switching to static mode");
            isSimulationActiveRef.current = false;
            setShouldRunSimulation(false); // Clear flag when simulation ends
          });
          
          simulation.restart();
          
        } catch (error) {
          console.error("Error creating simulation:", error);
          isSimulationActiveRef.current = false;
          setShouldRunSimulation(false);
        }
      } else {
        // No simulation - just render static network
        isSimulationActiveRef.current = false;
        renderNetwork();
      }

      // Drag functions
      function dragstarted(event, d) {
        if (isSimulationActiveRef.current && simulationRef.current && !event.active) {
          simulationRef.current.alphaTarget(0.3).restart();
        }
        if (isSimulationActiveRef.current) {
          d.fx = d.x;
          d.fy = d.y;
        }
      }

      function dragged(event, d) {
        if (isSimulationActiveRef.current) {
          d.fx = event.x;
          d.fy = event.y;
        } else {
          // Static mode - update position directly
          d.x = event.x;
          d.y = event.y;
          renderNetwork();
        }
      }

      function dragended(event, d) {
        if (isSimulationActiveRef.current && simulationRef.current) {
          if (!event.active) simulationRef.current.alphaTarget(0);
          // Keep node fixed at dragged position
          d.fx = event.x;
          d.fy = event.y;
        } else {
          // Static mode - node stays where dragged
          d.x = event.x;
          d.y = event.y;
        }
      }

      // Initial render (simulation will take over immediately)
      renderNetwork();

      // Cleanup
      return () => {
        if (simulationRef.current) {
          simulationRef.current.stop();
        }
      };
    }, [networkData, visualizationHeight, selectedNode, shouldRunSimulation]);

          return (
        <div style={{ position: 'relative' }}>
          <div ref={containerRef} style={{ width: '100%', height: `${visualizationHeight}px`, border: '1px solid #ddd', borderRadius: '8px' }}>
            <svg ref={svgRef}></svg>
            <ProfileCard />
          </div>
        
        <div
          onMouseDown={handleResizeStart}
          style={{
            position: 'absolute',
            bottom: '-5px',
            left: '50%',
            transform: 'translateX(-50%)',
            width: '60px',
            height: '10px',
            backgroundColor: '#667eea',
            borderRadius: '5px',
            cursor: 'ns-resize',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            opacity: 0.7,
            transition: 'opacity 0.2s'
          }}
          onMouseEnter={(e) => e.target.style.opacity = '1'}
          onMouseLeave={(e) => e.target.style.opacity = '0.7'}
        >
          <div style={{
            width: '20px',
            height: '2px',
            backgroundColor: 'white',
            borderRadius: '1px',
            position: 'relative'
          }}>
            <div style={{
              position: 'absolute',
              top: '-3px',
              width: '20px',
              height: '2px',
              backgroundColor: 'white',
              borderRadius: '1px'
            }}></div>
            <div style={{
              position: 'absolute',
              top: '3px',
              width: '20px',
              height: '2px',
              backgroundColor: 'white',
              borderRadius: '1px'
            }}></div>
          </div>
        </div>
        
        <div style={{
          position: 'absolute',
          top: '10px',
          right: '10px',
          backgroundColor: 'rgba(0,0,0,0.7)',
          color: 'white',
          padding: '4px 8px',
          borderRadius: '4px',
          fontSize: '12px',
          pointerEvents: 'none'
        }}>
          {visualizationHeight}px
        </div>
      </div>
    );
  };

  // Function to fetch actual relationship counts from database
  const fetchActualCounts = async (node) => {
    if (actualCounts[node.id]) {
      return actualCounts[node.id];
    }

    try {
      let response, data;
      
      if (node.type === 'person') {
        response = await fetch(`${API_BASE}/singer/network`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ singerName: node.name, depth: 1 })
        });
      } else if (node.type === 'opera') {
        response = await fetch(`${API_BASE}/opera/details`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
          },
          body: JSON.stringify({ operaName: node.name })
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

      if (response && response.ok) {
        data = await response.json();
        
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
          if (data.works) {
            if (data.works.operas) counts.premieredRoleIn = data.works.operas.length;
            if (data.works.books) counts.authored = data.works.books.length;
          }
        } else if (node.type === 'opera') {
          if (data.premieredRoles) counts.wrote = data.premieredRoles.length;
        } else if (node.type === 'book') {
          if (data.book && data.book.author) counts.authored = 1;
        }

        // Cache the counts
        setActualCounts(prev => ({ ...prev, [node.id]: counts }));
        return counts;
      }
    } catch (err) {
      console.error('Failed to fetch actual counts:', err);
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

  // Helper function to get relationship counts for a node
  const getRelationshipCounts = (node) => {
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
            // Count performers who premiered in this work
            break;
          case 'wrote':
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

    const ContextMenu = () => {
    if (!contextMenu.show) return null;

    const node = contextMenu.node;
    
    // Check if node is alone (no other nodes or no relationships)
    const isNodeAlone = networkData.nodes.length === 1 || networkData.links.length === 0;
    
    // Use actual counts for isolated nodes, network counts otherwise
    const counts = isNodeAlone ? (actualCounts[node.id] || {
      taughtBy: 0, taught: 0, authored: 0, premieredRoleIn: 0, wrote: 0,
      parent: 0, parentOf: 0, spouse: 0, grandparent: 0, grandparentOf: 0,
      sibling: 0, edited: 0, editedBy: 0
    }) : getRelationshipCounts(node);
    
    const menuItems = [
      {
        label: 'Full information',
        action: () => {
          showFullInformation(node);
          setContextMenu({ show: false, x: 0, y: 0, node: null });
        }
      },
      {
        label: 'Expand',
        hasSubmenu: true,
                submenu: [
          // Only show "All" if node is not alone
          ...(!isNodeAlone ? [{
            label: 'All',
            action: () => {
              expandAllRelationships(node);
              setContextMenu({ show: false, x: 0, y: 0, node: null });
            }
          }] : []),
          ...(node?.type === 'person' ? [
            // Only show relationships that actually exist (count > 0)
            ...(counts.taughtBy > 0 ? [{
              label: `← Taught - (${counts.taughtBy} nodes)`,
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
              label: `← Parent - (${counts.parentOf} nodes)`,
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
              label: `← Spouse - (${counts.spouse} nodes)`,
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
              label: `← Grandparent - (${counts.grandparentOf} nodes)`,
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
            ...(counts.wrote > 0 ? [{
              label: `- Wrote -> (${counts.wrote} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'wrote');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.wrote > 0 ? [{
              label: `← Wrote - (${counts.wrote} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'wroteBy');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : [])
          ] : []),
          ...(node?.type === 'book' ? [
            ...(counts.authored > 0 ? [{
              label: `- Authored -> (${counts.authored} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'authored');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.authored > 0 ? [{
              label: `← Authored - (${counts.authored} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'authoredBy');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.edited > 0 ? [{
              label: `- Edited -> (${counts.edited} nodes)`,
              action: () => {
                expandSpecificRelationship(node, 'edited');
                setContextMenu({ show: false, x: 0, y: 0, node: null });
              }
            }] : []),
            ...(counts.editedBy > 0 ? [{
              label: `← Edited - (${counts.editedBy} nodes)`,
              action: () => {
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

     return (
      <div
        style={{
          position: 'fixed',
          top: contextMenu.y,
          left: contextMenu.x,
          backgroundColor: 'white',
          border: '1px solid #ccc',
          borderRadius: '6px',
          padding: '4px 0',
          boxShadow: '0 4px 16px rgba(0,0,0,0.15)',
          zIndex: 1000,
          minWidth: '220px',
          maxWidth: '300px',
          fontFamily: "'Inter', 'Helvetica Neue', Arial, sans-serif",
          fontSize: '14px'
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div style={{ 
          padding: '8px 12px', 
          fontWeight: '600', 
          borderBottom: '1px solid #e5e7eb',
          color: '#1f2937',
          fontSize: '13px'
        }}>
          {node?.name}
        </div>

        {/* Menu Items */}
        {menuItems.map((item, index) => (
          <div key={index} style={{ position: 'relative' }}>
            <div
              style={{
                padding: '8px 12px',
                cursor: 'pointer',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                color: '#374151',
                transition: 'background-color 0.1s'
              }}
                             onMouseEnter={(e) => {
                 e.target.style.backgroundColor = '#f3f4f6';
                 if (item.hasSubmenu) {
                   // Clear any existing timeout
                   if (submenuTimeoutRef.current) {
                     clearTimeout(submenuTimeoutRef.current);
                   }
                   setExpandSubmenu(index);
                 }
               }}
               onMouseLeave={(e) => {
                 e.target.style.backgroundColor = 'transparent';
                 if (item.hasSubmenu) {
                   // Set timeout to close submenu
                   submenuTimeoutRef.current = setTimeout(() => {
                     setExpandSubmenu(null);
                   }, 300);
                 }
               }}
              onClick={() => {
                if (!item.hasSubmenu) {
                  item.action();
                }
              }}
            >
              <span>{item.label}</span>
              {item.hasSubmenu && <span style={{ color: '#9ca3af' }}>▶</span>}
            </div>

                        {/* Submenu */}
            {item.hasSubmenu && expandSubmenu === index && (
              <div
                style={{
                  position: 'absolute',
                  left: '100%',
                  top: '0',
                  backgroundColor: 'white',
                  border: '2px solid #333',
                  borderRadius: '6px',
                  padding: '8px 0',
                  boxShadow: '0 4px 16px rgba(0,0,0,0.15)',
                  minWidth: '280px',
                  maxWidth: '400px',
                  zIndex: 1001,
                  fontSize: '14px'
                }}
                onMouseEnter={() => {
                  // Clear any existing timeout when entering submenu
                  if (submenuTimeoutRef.current) {
                    clearTimeout(submenuTimeoutRef.current);
                  }
                  setExpandSubmenu(index);
                }}
                onMouseLeave={() => {
                  // Set timeout to close submenu when leaving
                  submenuTimeoutRef.current = setTimeout(() => {
                    setExpandSubmenu(null);
                  }, 300);
                }}
                onClick={(e) => e.stopPropagation()}
              >
                {item.submenu.map((subItem, subIndex) => (
                  <div
                    key={subIndex}
                    style={{
                      padding: '8px 12px',
                      cursor: 'pointer',
                      color: '#374151',
                      fontSize: '14px',
                      fontFamily: "'Inter', 'Helvetica Neue', Arial, sans-serif",
                      whiteSpace: 'nowrap',
                      minHeight: '24px',
                      display: 'flex',
                      alignItems: 'center'
                    }}
                    onMouseEnter={(e) => e.target.style.backgroundColor = '#f3f4f6'}
                    onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
                    onClick={() => {
                      subItem.action();
                    }}
                                      >
                      {subItem.label}
                    </div>
                ))}
              </div>
            )}
          </div>
        ))}
      </div>
    );
  };

  const ProfileCard = () => {
    if (!profileCard.show || !profileCard.data) return null;

    const data = profileCard.data;

    return (
      <div
        style={{
          position: 'absolute',
          bottom: '20px',
          left: '20px',
          width: '300px',
          maxHeight: '400px',
          backgroundColor: 'white',
          borderRadius: '8px',
          boxShadow: '0 8px 32px rgba(0,0,0,0.15)',
          border: '1px solid #e5e7eb',
          zIndex: 1000,
          fontFamily: "'Inter', 'Helvetica Neue', Arial, sans-serif",
          overflow: 'hidden'
        }}
      >
        {/* Header */}
        <div style={{
          padding: '16px',
          backgroundColor: '#f9fafb',
          borderBottom: '1px solid #e5e7eb',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center'
        }}>
          <h3 style={{
            margin: 0,
            fontSize: '16px',
            fontWeight: '600',
            color: '#1f2937'
          }}>
            👤 Singer Profile
          </h3>
          <button
            onClick={() => setProfileCard({ show: false, data: null })}
            style={{
              background: 'none',
              border: 'none',
              fontSize: '18px',
              cursor: 'pointer',
              color: '#6b7280',
              padding: '0',
              width: '24px',
              height: '24px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center'
            }}
          >
            ×
          </button>
        </div>

        {/* Content */}
        <div style={{
          padding: '16px',
          maxHeight: '340px',
          overflowY: 'auto'
        }}>
          <div style={{ marginBottom: '12px' }}>
            <strong style={{ color: '#1f2937' }}>Name:</strong>
            <div style={{ color: '#374151', marginTop: '2px' }}>
              {data.full_name}
            </div>
          </div>

          {data.voice_type && (
            <div style={{ marginBottom: '12px' }}>
              <strong style={{ color: '#1f2937' }}>Voice type:</strong>
              <div style={{ color: '#374151', marginTop: '2px' }}>
                {data.voice_type}
              </div>
            </div>
          )}

          {(data.birth_year || data.death_year) && (
            <div style={{ marginBottom: '12px' }}>
              <strong style={{ color: '#1f2937' }}>Dates:</strong>
              <div style={{ color: '#374151', marginTop: '2px' }}>
                {data.birth_year && data.death_year
                  ? `${data.birth_year} - ${data.death_year}`
                  : data.birth_year
                  ? `${data.birth_year} - `
                  : data.death_year
                  ? ` - ${data.death_year}`
                  : ''}
              </div>
            </div>
          )}

          {data.citizen && (
            <div style={{ marginBottom: '12px' }}>
              <strong style={{ color: '#1f2937' }}>Birthplace:</strong>
              <div style={{ color: '#374151', marginTop: '2px' }}>
                {data.citizen}
              </div>
            </div>
          )}

          {data.underrepresented_group && (
            <div style={{ marginBottom: '12px' }}>
              <strong style={{ color: '#1f2937' }}>Underrepresented group:</strong>
              <div style={{ color: '#374151', marginTop: '2px' }}>
                {data.underrepresented_group}
              </div>
            </div>
          )}

          {/* Sources section */}
          <div style={{ 
            marginTop: '16px', 
            paddingTop: '16px', 
            borderTop: '1px solid #e5e7eb' 
          }}>
            <strong style={{ color: '#1f2937', fontSize: '14px' }}>Sources:</strong>
            <div style={{ marginTop: '8px' }}>
              {data.spelling_source && (
                <div style={{ 
                  fontSize: '12px', 
                  color: '#6b7280', 
                  marginBottom: '4px' 
                }}>
                  <strong>Spelling:</strong> {data.spelling_source}
                </div>
              )}
              {data.voice_type_source && (
                <div style={{ 
                  fontSize: '12px', 
                  color: '#6b7280', 
                  marginBottom: '4px' 
                }}>
                  <strong>Voice type:</strong> {data.voice_type_source}
                </div>
              )}
              {data.dates_source && (
                <div style={{ 
                  fontSize: '12px', 
                  color: '#6b7280', 
                  marginBottom: '4px' 
                }}>
                  <strong>Dates:</strong> {data.dates_source}
                </div>
              )}
              {data.citizenship_source && (
                <div style={{ 
                  fontSize: '12px', 
                  color: '#6b7280', 
                  marginBottom: '4px' 
                }}>
                  <strong>Birthplace:</strong> {data.citizenship_source}
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    );
  };

  const AuthForm = () => {
    const [isLogin, setIsLogin] = useState(true);
    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');

    const handleSubmit = () => {
      if (isLogin) {
        login(email, password);
      } else {
        register(email, password);
      }
    };

    return (
      <div style={{ 
        minHeight: '100vh', 
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '20px'
      }}>
        <div style={{
          maxWidth: '400px',
          width: '100%',
          backgroundColor: 'white',
          borderRadius: '10px',
          boxShadow: '0 10px 25px rgba(0,0,0,0.1)',
          padding: '40px'
        }}>
          <div style={{ textAlign: 'center', marginBottom: '30px' }}>
            <h1 style={{ fontSize: '28px', fontWeight: 'bold', color: '#333', marginBottom: '10px' }}>
              Classical Singer Family Tree
            </h1>
            <p style={{ color: '#666' }}>Explore the teaching lineages of opera singers</p>
          </div>

          <div style={{ marginBottom: '20px' }}>
            <label style={{ display: 'block', marginBottom: '5px', fontWeight: '500' }}>Email</label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              style={{
                width: '100%',
                padding: '10px',
                border: '1px solid #ddd',
                borderRadius: '5px',
                fontSize: '16px',
                boxSizing: 'border-box'
              }}
              placeholder="your@email.com"
            />
          </div>

          <div style={{ marginBottom: '20px' }}>
            <label style={{ display: 'block', marginBottom: '5px', fontWeight: '500' }}>Password</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              style={{
                width: '100%',
                padding: '10px',
                border: '1px solid #ddd',
                borderRadius: '5px',
                fontSize: '16px',
                boxSizing: 'border-box'
              }}
              placeholder="••••••••"
            />
          </div>

          {error && (
            <div style={{
              backgroundColor: '#fee',
              border: '1px solid #fcc',
              color: '#c33',
              padding: '10px',
              borderRadius: '5px',
              marginBottom: '20px'
            }}>
              {error}
            </div>
          )}

          <button
            onClick={handleSubmit}
            disabled={loading}
            style={{
              width: '100%',
              backgroundColor: '#667eea',
              color: 'white',
              padding: '12px',
              border: 'none',
              borderRadius: '5px',
              fontSize: '16px',
              cursor: 'pointer',
              opacity: loading ? 0.7 : 1
            }}
          >
            {loading ? 'Loading...' : (isLogin ? 'Sign In' : 'Create Account')}
          </button>

          <div style={{ marginTop: '20px', textAlign: 'center' }}>
            <button
              onClick={() => setIsLogin(!isLogin)}
              style={{
                background: 'none',
                border: 'none',
                color: '#667eea',
                cursor: 'pointer',
                textDecoration: 'underline'
              }}
            >
              {isLogin ? "Don't have an account? Sign up" : "Already have an account? Sign in"}
            </button>
          </div>
        </div>
      </div>
    );
  };

  if (!token) {
    return <AuthForm />;
  }

  return (
    <div style={{ minHeight: '100vh', backgroundColor: '#f8f9fa' }}>
      <header style={{
        backgroundColor: 'white',
        borderBottom: '1px solid #dee2e6',
        padding: '15px 0'
      }}>
        <div style={{
          maxWidth: '1200px',
          margin: '0 auto',
          padding: '0 20px',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center'
        }}>
          <h1 style={{ fontSize: '20px', fontWeight: 'bold', color: '#333' }}>
            Classical Singer Family Tree
          </h1>
          
          <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
            <button
              onClick={() => setCurrentView('search')}
              style={{
                padding: '8px 16px',
                backgroundColor: currentView === 'search' ? '#e3f2fd' : 'transparent',
                color: currentView === 'search' ? '#1976d2' : '#666',
                border: 'none',
                borderRadius: '5px',
                cursor: 'pointer'
              }}
            >
              Search
            </button>
            
            <button
              onClick={() => {
                setToken('');
                localStorage.removeItem('token');
                setCurrentView('search');
              }}
              style={{
                backgroundColor: '#dc3545',
                color: 'white',
                padding: '8px 16px',
                border: 'none',
                borderRadius: '5px',
                cursor: 'pointer'
              }}
            >
              Logout
            </button>
          </div>
        </div>
      </header>

      <main style={{ maxWidth: '1200px', margin: '0 auto', padding: '30px 20px' }}>
        <div style={{ textAlign: 'center', marginBottom: '40px' }}>
          <h2 style={{ fontSize: '32px', fontWeight: 'bold', marginBottom: '15px' }}>
            Discover Connections Among Classical Singers
          </h2>
          <p style={{ fontSize: '18px', color: '#666' }}>
            Explore singers, operas, and vocal pedagogy books
          </p>
        </div>

        <div style={{
          display: 'flex',
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
                setSearchType(type.key);
                setSearchResults([]);
                setCurrentView('search');
                setItemDetails(null);
                setSelectedItem(null);
                setError('');
                setSearchQuery('');
                setNetworkData({ nodes: [], links: [] });
              }}
              style={{
                padding: '12px 20px',
                backgroundColor: searchType === type.key ? '#667eea' : 'white',
                color: searchType === type.key ? 'white' : '#333',
                border: '2px solid #667eea',
                borderRadius: '8px',
                cursor: 'pointer',
                fontSize: '16px',
                fontWeight: '500'
              }}
            >
              {type.icon} {type.label}
            </button>
          ))}
        </div>

        <div style={{ maxWidth: '600px', margin: '0 auto 30px' }}>
          <div style={{ display: 'flex', gap: '15px' }}>
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onKeyPress={(e) => e.key === 'Enter' && performSearch()}
              placeholder={
                searchType === 'singers' ? 'Search for opera singers and teachers... (e.g., Garcia II)' :
                searchType === 'operas' ? 'Search for operas... (e.g., La Traviata)' :
                'Search for vocal pedagogy books... (e.g., Vocal wisdom, c1931)'
              }
              style={{
                flex: 1,
                padding: '15px',
                border: '1px solid #ddd',
                borderRadius: '8px',
                fontSize: '16px'
              }}
            />
            <button
              onClick={performSearch}
              disabled={loading || !searchQuery.trim()}
              style={{
                backgroundColor: '#667eea',
                color: 'white',
                padding: '15px 25px',
                border: 'none',
                borderRadius: '8px',
                cursor: 'pointer',
                opacity: (loading || !searchQuery.trim()) ? 0.5 : 1
              }}
            >
              {loading ? '🔍' : 'Search'}
            </button>
          </div>
        </div>

        {error && (
          <div style={{
            maxWidth: '600px',
            margin: '0 auto',
            backgroundColor: '#fee',
            border: '1px solid #fcc',
            color: '#c33',
            padding: '15px',
            borderRadius: '8px'
          }}>
            {error}
          </div>
        )}

        {currentView === 'results' && searchResults.length > 0 && (
          <div style={{ marginBottom: '30px' }}>
            <h3>Search Results ({searchResults.length})</h3>
            <div style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))',
              gap: '20px'
            }}>
              {searchResults.map((item, index) => (
                <div
                  key={index}
                  onClick={() => getItemDetails(item)}
                  style={{
                    backgroundColor: 'white',
                    borderRadius: '8px',
                    padding: '20px',
                    boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                    cursor: 'pointer',
                    transition: 'box-shadow 0.2s'
                  }}
                  onMouseOver={(e) => e.currentTarget.style.boxShadow = '0 4px 8px rgba(0,0,0,0.15)'}
                  onMouseOut={(e) => e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)'}
                >
                  <h4>{searchType === 'singers' ? item.name : item.properties.title}</h4>
                  {searchType === 'singers' && item.properties.voice_type && (
                    <p style={{ margin: '4px 0', fontSize: '14px', color: '#666' }}>
                      <strong>Voice type:</strong> {item.properties.voice_type}
                    </p>
                  )}
                  {searchType === 'singers' && (item.properties.birth_year || item.properties.death_year) && (
                    <p style={{ margin: '4px 0', fontSize: '14px', color: '#666' }}>
                      <strong>Dates:</strong> {
                        item.properties.birth_year && item.properties.death_year
                          ? `${item.properties.birth_year} - ${item.properties.death_year}`
                          : item.properties.birth_year
                          ? `${item.properties.birth_year} - `
                          : item.properties.death_year
                          ? ` - ${item.properties.death_year}`
                          : ''
                      }
                    </p>
                  )}
                  {searchType === 'operas' && item.properties.composer && (
                    <p style={{ margin: '4px 0', fontSize: '14px', color: '#666' }}>
                      <strong>Composer:</strong> {item.properties.composer}
                    </p>
                  )}
                  {searchType === 'books' && item.properties.author && (
                    <p style={{ margin: '4px 0', fontSize: '14px', color: '#666' }}>
                      <strong>Author:</strong> {item.properties.author}
                    </p>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}

        {(currentView === 'results' || currentView === 'network') && networkData.nodes.length > 0 && (
          <div style={{ width: '100%', marginBottom: '30px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '15px' }}>
              <h3 style={{ marginBottom: '0', fontSize: '24px', fontWeight: 'bold' }}>
                Network Visualization
              </h3>
              <div style={{ fontSize: '14px', color: '#666' }}>
                Drag nodes to reposition • Scroll to zoom • Drag to pan
              </div>
            </div>
            <NetworkVisualization />
          </div>
        )}

        {currentView === 'results' && searchResults.length === 0 && (
          <div style={{ textAlign: 'center', padding: '40px' }}>
            <p style={{ fontSize: '18px', color: '#666' }}>No search results to display.</p>
            <button
              onClick={() => setCurrentView('search')}
              style={{
                backgroundColor: '#667eea',
                color: 'white',
                padding: '10px 20px',
                border: 'none',
                borderRadius: '5px',
                cursor: 'pointer',
                marginTop: '15px'
              }}
            >
              Back to Search
            </button>
          </div>
        )}

        {currentView === 'network' && itemDetails && (
          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '30px' }}>
              <h2 style={{ fontSize: '24px', fontWeight: 'bold' }}>
                {searchType === 'singers' && itemDetails.center ? itemDetails.center.full_name : 
                 searchType === 'operas' && itemDetails.opera ? itemDetails.opera.opera_name :
                 searchType === 'books' && itemDetails.book ? itemDetails.book.title :
                 selectedItem.name || (selectedItem.properties && selectedItem.properties.title)} - Details
              </h2>
              <button
                onClick={() => {
                  setSearchType(originalSearchType);
                  setSearchResults(originalSearchResults);
                  if (originalSearchResults.length === 0) {
                    setCurrentView('search');
                    setNetworkData({ nodes: [], links: [] });
                  } else {
                    setCurrentView('results');
                    generateNetworkFromSearchResults(originalSearchResults, originalSearchType);
                  }
                }}
                style={{
                  color: '#667eea',
                  background: 'none',
                  border: 'none',
                  cursor: 'pointer',
                  fontSize: '16px'
                }}
              >
                ← Back to {originalSearchResults.length === 0 ? 'Search' : 'Results'}
              </button>
            </div>

            <div style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))',
              gap: '20px'
            }}>
              {searchType === 'singers' && itemDetails.center && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: '20px',
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  height: '300px',
                  overflowY: 'auto'
                }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#7b1fa2', marginBottom: '15px' }}>
                    👤 Singer Profile
                  </h3>
                  <p style={{ margin: '8px 0' }}>
                    <strong>Name:</strong> {itemDetails.center.full_name}
                  </p>
                  {itemDetails.center.voice_type && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Voice type:</strong> {itemDetails.center.voice_type}
                    </p>
                  )}
                  {(itemDetails.center.birth_year || itemDetails.center.death_year) && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Dates:</strong> {
                        itemDetails.center.birth_year && itemDetails.center.death_year
                          ? `${itemDetails.center.birth_year} - ${itemDetails.center.death_year}`
                          : itemDetails.center.birth_year
                          ? `${itemDetails.center.birth_year} - `
                          : itemDetails.center.death_year
                          ? ` - ${itemDetails.center.death_year}`
                          : ''
                      }
                    </p>
                  )}
                  {itemDetails.center.citizen && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Birthplace:</strong> {itemDetails.center.citizen}
                    </p>
                  )}
                  {itemDetails.center.underrepresented_group && (
                    <p style={{ margin: '8px 0' }}>
                      <strong>Underrepresented group:</strong> {itemDetails.center.underrepresented_group}
                    </p>
                  )}
                  
                  {/* Sources section */}
                  <div style={{ marginTop: '15px', paddingTop: '15px', borderTop: '1px solid #e5e7eb' }}>
                    {itemDetails.center.spelling_source && (
                      <p style={{ margin: '4px 0', fontSize: '12px', color: '#888'}}>
                        Spelling source: {itemDetails.center.spelling_source}
                      </p>
                    )}
                    {itemDetails.center.voice_type_source && (
                      <p style={{ margin: '4px 0', fontSize: '12px', color: '#888'}}>
                        Voice type source:{itemDetails.center.voice_type_source}
                      </p>
                    )}
                    {itemDetails.center.dates_source && (
                      <p style={{ margin: '4px 0', fontSize: '12px', color: '#888'}}>
                        Dates source: {itemDetails.center.dates_source}
                      </p>
                    )}
                    {itemDetails.center.citizenship_source && (
                      <p style={{ margin: '4px 0', fontSize: '12px', color: '#888'}}>
                        Birthplace source:{itemDetails.center.citizenship_source}
                      </p>
                    )}
                  </div>
                </div>
              )}

              {searchType === 'singers' && itemDetails.teachers && itemDetails.teachers.length > 0 && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: '20px',
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  height: '300px',
                  overflowY: 'auto'
                }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#2563eb', marginBottom: '15px' }}>
                    👨‍🏫 Teachers ({itemDetails.teachers.length})
                  </h3>
                  {itemDetails.teachers.map((teacher, index) => (
                    <div 
                      key={index} 
                      style={{ 
                        marginBottom: '12px', 
                        paddingBottom: '12px', 
                        borderBottom: index < itemDetails.teachers.length - 1 ? '1px solid #e5e7eb' : 'none',
                        cursor: 'pointer',
                        padding: '8px',
                        borderRadius: '4px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={() => searchForPerson(teacher.full_name)}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{teacher.full_name}</p>
                      {teacher.voice_type && (
                        <p style={{ margin: '4px 0', fontSize: '14px', color: '#666' }}>
                          <strong>Voice type:</strong> {teacher.voice_type}
                        </p>
                      )}
                      {(teacher.birth_year || teacher.death_year) && (
                        <p style={{ margin: '4px 0', fontSize: '14px', color: '#666' }}>
                          <strong>Dates:</strong> {
                            teacher.birth_year && teacher.death_year
                              ? `${teacher.birth_year} - ${teacher.death_year}`
                              : teacher.birth_year
                              ? `${teacher.birth_year} - `
                              : teacher.death_year
                              ? ` - ${teacher.death_year}`
                              : ''
                          }
                        </p>
                      )}
                      {teacher.relationship_source && (
                        <p style={{ margin: '4px 0', fontSize: '12px', color: '#888', fontStyle: 'italic' }}>
                          Source: {teacher.relationship_source}
                        </p>
                      )}
                    </div>
                  ))}
                </div>
              )}

              {searchType === 'singers' && itemDetails.students && itemDetails.students.length > 0 && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: '20px',
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  height: '300px',
                  overflowY: 'auto'
                }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#059669', marginBottom: '15px' }}>
                    👨‍🎓 Students ({itemDetails.students.length})
                  </h3>
                  {itemDetails.students.map((student, index) => (
                    <div 
                      key={index} 
                      style={{ 
                        marginBottom: '12px', 
                        paddingBottom: '12px', 
                        borderBottom: index < itemDetails.students.length - 1 ? '1px solid #e5e7eb' : 'none',
                        cursor: 'pointer',
                        padding: '8px',
                        borderRadius: '4px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={() => searchForPerson(student.full_name)}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{student.full_name}</p>
                      {student.voice_type && (
                        <p style={{ margin: '4px 0', fontSize: '14px', color: '#666' }}>
                          <strong>Voice type:</strong> {student.voice_type}
                        </p>
                      )}
                      {(student.birth_year || student.death_year) && (
                        <p style={{ margin: '4px 0', fontSize: '14px', color: '#666' }}>
                          <strong>Dates:</strong> {
                            student.birth_year && student.death_year
                              ? `${student.birth_year} - ${student.death_year}`
                              : student.birth_year
                              ? `${student.birth_year} - `
                              : student.death_year
                              ? ` - ${student.death_year}`
                              : ''
                          }
                        </p>
                      )}
                      {student.relationship_source && (
                        <p style={{ margin: '4px 0', fontSize: '12px', color: '#888', fontStyle: 'italic' }}>
                          Source: {student.relationship_source}
                        </p>
                      )}
                    </div>
                  ))}
                </div>
              )}

              {searchType === 'singers' && itemDetails.family && itemDetails.family.length > 0 && (
                <div style={{
                  backgroundColor: 'white',
                  borderRadius: '8px',
                  padding: '20px',
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                  height: '300px',
                  overflowY: 'auto'
                }}>
                  <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#dc2626', marginBottom: '15px' }}>
                    👨‍👩‍👧‍👦 Family ({itemDetails.family.length})
                  </h3>
                  {itemDetails.family.map((relative, index) => (
                    <div 
                      key={index} 
                      style={{ 
                        marginBottom: '12px', 
                        paddingBottom: '12px', 
                        borderBottom: index < itemDetails.family.length - 1 ? '1px solid #e5e7eb' : 'none',
                        cursor: 'pointer',
                        padding: '8px',
                        borderRadius: '4px',
                        transition: 'background-color 0.2s'
                      }}
                      onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                      onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                      onClick={() => searchForPerson(relative.full_name)}
                    >
                      <p style={{ margin: '4px 0', fontWeight: '500' }}>{relative.full_name}</p>
                      {relative.relationship_type && (
                        <p style={{ margin: '4px 0', fontSize: '14px', color: '#666' }}>
                          <strong>Relationship:</strong> {relative.relationship_type}
                        </p>
                      )}
                      {relative.voice_type && (
                        <p style={{ margin: '4px 0', fontSize: '14px', color: '#666' }}>
                          <strong>Voice type:</strong> {relative.voice_type}
                        </p>
                      )}
                      {(relative.birth_year || relative.death_year) && (
                        <p style={{ margin: '4px 0', fontSize: '14px', color: '#666' }}>
                          <strong>Dates:</strong> {
                            relative.birth_year && relative.death_year
                              ? `${relative.birth_year} - ${relative.death_year}`
                              : relative.birth_year
                              ? `${relative.birth_year} - `
                              : relative.death_year
                              ? ` - ${relative.death_year}`
                              : ''
                          }
                        </p>
                      )}
                    </div>
                  ))}
                </div>
              )}

              {searchType === 'singers' && itemDetails.works && (
                <>
                  {itemDetails.works.operas && itemDetails.works.operas.length > 0 && (
                    <div style={{
                      backgroundColor: 'white',
                      borderRadius: '8px',
                      padding: '20px',
                      boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                      height: '300px',
                      overflowY: 'auto'
                    }}>
                      <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#7c3aed', marginBottom: '15px' }}>
                        🎭 Operas premiered ({itemDetails.works.operas.length})
                      </h3>
                      {itemDetails.works.operas.map((opera, index) => (
                        <div 
                          key={index} 
                          style={{ 
                            marginBottom: '12px', 
                            paddingBottom: '12px', 
                            borderBottom: index < itemDetails.works.operas.length - 1 ? '1px solid #e5e7eb' : 'none',
                            cursor: 'pointer',
                            padding: '8px',
                            borderRadius: '4px',
                            transition: 'background-color 0.2s'
                          }}
                          onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                          onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                          onClick={async () => {
                            try {
                              setLoading(true);
                              const response = await fetch(`${API_BASE}/opera/details`, {
                                method: 'POST',
                                headers: {
                                  'Content-Type': 'application/json',
                                  'Authorization': `Bearer ${token}`
                                },
                                body: JSON.stringify({ operaName: opera.title })
                              });

                              const data = await response.json();
                              if (response.ok) {
                                setItemDetails(data);
                                setSelectedItem({ properties: { title: opera.title } });
                                setSearchType('operas');
                                setCurrentView('network');
                                generateNetworkFromDetails(data, opera.title, 'operas');
                                setShouldRunSimulation(true); // Trigger simulation for clicked opera
                              } else {
                                setError(data.error);
                              }
                            } catch (err) {
                              setError('Failed to fetch opera details');
                            } finally {
                              setLoading(false);
                            }
                          }}
                        >
                          <p style={{ margin: '4px 0', fontWeight: '500' }}>{opera.title}</p>
                          {opera.role && (
                            <p style={{ margin: '4px 0', fontSize: '14px', color: '#666' }}>
                              <strong>Role:</strong> {opera.role}
                            </p>
                          )}
                          {opera.source && (
                            <p style={{ margin: '4px 0', fontSize: '12px', color: '#888', fontStyle: 'italic' }}>
                              Source: {opera.source}
                            </p>
                          )}
                        </div>
                      ))}
                    </div>
                  )}

                  {itemDetails.works.books && itemDetails.works.books.length > 0 && (
                    <div style={{
                      backgroundColor: 'white',
                      borderRadius: '8px',
                      padding: '20px',
                      boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                      height: '300px',
                      overflowY: 'auto'
                    }}>
                      <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#ea580c', marginBottom: '15px' }}>
                        📚 Books ({itemDetails.works.books.length})
                      </h3>
                      {itemDetails.works.books.map((book, index) => (
                        <div 
                          key={index} 
                          style={{ 
                            marginBottom: '12px', 
                            paddingBottom: '12px', 
                            borderBottom: index < itemDetails.works.books.length - 1 ? '1px solid #e5e7eb' : 'none',
                            cursor: 'pointer',
                            padding: '8px',
                            borderRadius: '4px',
                            transition: 'background-color 0.2s'
                          }}
                          onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                          onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                          onClick={async () => {
                            try {
                              setLoading(true);
                              const response = await fetch(`${API_BASE}/book/details`, {
                                method: 'POST',
                                headers: {
                                  'Content-Type': 'application/json',
                                  'Authorization': `Bearer ${token}`
                                },
                                body: JSON.stringify({ bookTitle: book.title })
                              });

                              const data = await response.json();
                              if (response.ok) {
                                setItemDetails(data);
                                setSelectedItem({ properties: { title: book.title } });
                                setSearchType('books');
                                setCurrentView('network');
                                generateNetworkFromDetails(data, book.title, 'books');
                                setShouldRunSimulation(true); // Trigger simulation for clicked book
                              } else {
                                setError(data.error);
                              }
                            } catch (err) {
                              setError('Failed to fetch book details');
                            } finally {
                              setLoading(false);
                            }
                          }}
                        >
                          <p style={{ margin: '4px 0', fontWeight: '500' }}>{book.title}</p>
                        </div>
                      ))}
                    </div>
                  )}

                  {itemDetails.works.composedOperas && itemDetails.works.composedOperas.length > 0 && (
                    <div style={{
                      backgroundColor: 'white',
                      borderRadius: '8px',
                      padding: '20px',
                      boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                      height: '300px',
                      overflowY: 'auto'
                    }}>
                      <h3 style={{ fontSize: '18px', fontWeight: '600', color: '#0891b2', marginBottom: '15px' }}>
                        🎼 Composed Operas ({itemDetails.works.composedOperas.length})
                      </h3>
                      {itemDetails.works.composedOperas.map((opera, index) => (
                        <div 
                          key={index} 
                          style={{ 
                            marginBottom: '12px', 
                            paddingBottom: '12px', 
                            borderBottom: index < itemDetails.works.composedOperas.length - 1 ? '1px solid #e5e7eb' : 'none',
                            cursor: 'pointer',
                            padding: '8px',
                            borderRadius: '4px',
                            transition: 'background-color 0.2s'
                          }}
                          onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                          onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                          onClick={async () => {
                            try {
                              setLoading(true);
                              const response = await fetch(`${API_BASE}/opera/details`, {
                                method: 'POST',
                                headers: {
                                  'Content-Type': 'application/json',
                                  'Authorization': `Bearer ${token}`
                                },
                                body: JSON.stringify({ operaName: opera.title })
                              });

                              const data = await response.json();
                              if (response.ok) {
                                setItemDetails(data);
                                setSelectedItem({ properties: { title: opera.title } });
                                setSearchType('operas');
                                setCurrentView('network');
                                generateNetworkFromDetails(data, opera.title, 'operas');
                                setShouldRunSimulation(true); // Trigger simulation for clicked composed opera
                              } else {
                                setError(data.error);
                              }
                            } catch (err) {
                              setError('Failed to fetch opera details');
                            } finally {
                              setLoading(false);
                            }
                          }}
                        >
                          <p style={{ margin: '4px 0', fontWeight: '500' }}>{opera.title}</p>
                        </div>
                      ))}
                    </div>
                  )}
                </>
              )}
            </div>
          </div>
        )}
      </main>
      
      <ContextMenu />
    </div>
  );
};

export default ClassicalMusicGenealogy;