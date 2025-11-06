import React from 'react';

export default function HelpCenter({ onBack }) {
  return (
    <div style={{
      marginTop: '80px',
      display: 'flex',
      justifyContent: 'center'
    }}>
      <div style={{
        width: 'min(960px, 92vw)',
        backgroundColor: 'rgba(255,255,255,0.9)',
        borderRadius: '18px',
        border: '2px solid #3e96e2',
        boxShadow: '0 24px 48px rgba(0,0,0,0.25)',
        padding: '36px',
        color: '#1f2937',
        fontSize: '18px',
        lineHeight: 1.6,
        display: 'flex',
        flexDirection: 'column',
        gap: '28px'
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 16 }}>
          <h2 style={{ margin: 0, fontSize: '34px', color: '#0f172a' }}>Help Center</h2>
          <button
            onClick={() => onBack && onBack()}
            style={{
              padding: '8px 16px',
              backgroundColor: '#ffffff',
              color: '#374151',
              border: '2px solid #3e96e2',
              borderRadius: '8px',
              cursor: 'pointer',
              fontSize: '16px'
            }}
          >
            ← Back to Search
          </button>
        </div>

        <section>
          <h3 style={{ margin: '0 0 12px 0', fontSize: '22px', color: '#0f172a' }}>About</h3>
          <details style={{ backgroundColor: '#f6faff', border: '2px solid #cbdaf7', borderRadius: '12px', padding: '16px 20px' }}>
            <summary style={{ cursor: 'pointer', fontWeight: 600, color: '#1d4ed8', fontSize: '18px' }}>Project overview</summary>
            <p style={{ marginTop: '12px', color: '#374151' }}>
              When my family and I moved to the mountain west, I was amazed to learn that aspen trees in a grove are all one organism. They are rhizomatic - that is, they all grow from the same system of roots. We can trace the beginning of trained, classical singing to the Florentine Camerata, and in this manner of thinking, all classical singers share the same system of roots, too. This site is my attempt to show our vast interconnectedness.
            </p>
            <p style={{ marginTop: '12px', color: '#374151' }}>
              From 2022 to 2025, I worked to create a ‘family tree’ of successful opera singers and those who taught them. I was motivated to do this work because I frequently marveled at highly skilled classical singers and wondered who their teachers were. Like <a href="https://www.songhelix.com" target="_blank" rel="noreferrer">SongHelix</a> (released in 2019), I wanted to make another useful tool that allowed for deep and broad insights. I hoped to create a tool that would allow singers, fans of classical singing, and scholars a simple way to discover teacher-singer lineage. I have pulled data from a variety of online and print sources. Each of those can be investigated on examination of any piece of data on the site. Note that when Wikipedia is cited, it can refer to any language version of the student or teacher's Wikipedia site. Frequently another language's version will have different information from the English version.
            </p>
            <p style={{ marginTop: '12px', color: '#374151' }}>
              I have used various methods for gathering data at scale including querying Wikidata, webscraping, APIs, and using python scripts. While Artificial Intelligence has helped me gather information (and to code the entire website(!)), no information has been created through the use of AI.
            </p>
            <p style={{ marginTop: '12px', color: '#374151' }}>
              For any questions regarding the tool's creation, the data collection methods, to license the background systems for a similar site of your own, for presentation inquiries, or to send any comments, please contact me <a href="mailto:classicalsinginghumanitieslab@gmail.com">here</a>.
            </p>
            <p style={{ marginTop: '12px', color: '#374151',  textAlign: 'right'}}>
              - Seth Keeton, founder <br/>
              Classical Singing Humanities Lab
            </p>
          </details>
        </section>

        <details style={{ backgroundColor: '#f6faff', border: '2px solid #cbdaf7', borderRadius: '12px', padding: '16px 20px' }}>
          <summary style={{ cursor: 'pointer', fontWeight: 600, color: '#1d4ed8', fontSize: '18px' }}>How to use</summary>
          <p style={{ marginTop: '12px', color: '#374151' }}>
            Explore the search, open details, and view relationships in the graph. Use filters to narrow results.
          </p>
        </details>

        <details style={{ backgroundColor: '#f6faff', border: '2px solid #cbdaf7', borderRadius: '12px', padding: '16px 20px' }}>
          <summary style={{ cursor: 'pointer', fontWeight: 600, color: '#1d4ed8', fontSize: '18px' }}>Accuracy & limits</summary>
          <p style={{ marginTop: '12px', color: '#374151' }}>
            Sources are cited alongside data. Not all relationships are fully verified; see citations for details.
          </p>
        </details>

        <section>
          <h3 style={{ margin: '0 0 18px 0', fontSize: '22px', color: '#0f172a' }}>Videos</h3>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '16px' }}>
            {[
              { title: 'Overview', src: '/1_Basic_tour.mp4' },
              { title: 'Right Click Features', src: '/2_Right_click.mp4' },
              { title: 'Path and Filter', src: '/3_Path_and_filters.mp4' },
              { title: 'Save and Export', src: '/4_Save_and_export.mp4' }
            ].map((vid, idx) => (
              <div key={idx} style={{ display: 'flex', flexDirection: 'column' }}>
                <h4 style={{ margin: '0 0 8px 0', textAlign: 'center', fontSize: '18px', color: '#0f172a' }}>{vid.title}</h4>
                <div style={{
                  backgroundColor: '#000',
                  border: '2px solid #3e96e2',
                  borderRadius: '12px',
                  overflow: 'hidden'
                }}>
                  <video controls playsInline preload="metadata" style={{ width: '100%', display: 'block', aspectRatio: '16 / 9' }}>
                    <source src={vid.src} type="video/mp4" />
                    Your browser does not support the video tag.
                  </video>
                </div>
              </div>
            ))}
          </div>
        </section>

        <section>
          <h3 style={{ margin: '0 0 12px 0', fontSize: '22px', color: '#0f172a' }}>Acknowledgements</h3>
          <p style={{ margin: 0, color: '#374151' }}>
            This project would not have been possible without the help of a large community that surrounds me. I am grateful for my friends and colleagues, and to the many researchers whose work I have collected and distilled here.
          </p>
        </section>
      </div>
    </div>
  );
}

