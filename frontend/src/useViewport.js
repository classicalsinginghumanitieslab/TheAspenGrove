import { useEffect, useState } from 'react';

const isBrowser = typeof window !== 'undefined';

const createViewportState = () => {
  if (!isBrowser) {
    return {
      width: 0,
      height: 0,
      isTablet: false,
      isPhone: false
    };
  }

  const width = window.innerWidth;
  const height = window.innerHeight;

  return {
    width,
    height,
    isTablet: width <= 1023,
    isPhone: width <= 767
  };
};

const addMqListener = (mq, handler) => {
  if (!mq) return () => {};
  if (typeof mq.addEventListener === 'function') {
    mq.addEventListener('change', handler);
    return () => mq.removeEventListener('change', handler);
  }
  if (typeof mq.addListener === 'function') {
    mq.addListener(handler);
    return () => mq.removeListener(handler);
  }
  return () => {};
};

const useViewport = () => {
  const [viewport, setViewport] = useState(createViewportState);

  useEffect(() => {
    if (!isBrowser) return undefined;

    const handleResize = () => {
      setViewport(createViewportState());
    };

    const tabletQuery = window.matchMedia('(max-width: 1023px)');
    const phoneQuery = window.matchMedia('(max-width: 767px)');

    const cleanupFns = [
      () => window.removeEventListener('resize', handleResize),
      addMqListener(tabletQuery, handleResize),
      addMqListener(phoneQuery, handleResize)
    ];

    window.addEventListener('resize', handleResize);

    return () => {
      cleanupFns.forEach((cleanup) => {
        try {
          cleanup();
        } catch (err) {
          // no-op
        }
      });
    };
  }, []);

  return viewport;
};

export default useViewport;
