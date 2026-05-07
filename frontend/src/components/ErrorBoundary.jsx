import React from 'react';

class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, info) {
    console.error('[ErrorBoundary]', error, info.componentStack);
  }

  render() {
    if (!this.state.hasError) return this.props.children;

    return (
      <div style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        minHeight: '100vh',
        background: '#0f172a',
        color: '#e2e8f0',
        fontFamily: 'system-ui, sans-serif',
        padding: '2rem',
        textAlign: 'center',
      }}>
        <div style={{ fontSize: '2rem', marginBottom: '1rem' }}>🌿</div>
        <h1 style={{ fontSize: '1.25rem', fontWeight: 600, marginBottom: '0.5rem' }}>
          Something went wrong
        </h1>
        <p style={{ color: '#94a3b8', marginBottom: '1.5rem', maxWidth: '380px', lineHeight: 1.6 }}>
          An unexpected error occurred. Please refresh the page to continue.
        </p>
        <button
          onClick={() => window.location.reload()}
          style={{
            background: '#1e40af',
            color: '#fff',
            border: 'none',
            borderRadius: '6px',
            padding: '0.6rem 1.4rem',
            fontSize: '0.95rem',
            cursor: 'pointer',
          }}
        >
          Refresh page
        </button>
      </div>
    );
  }
}

export default ErrorBoundary;
