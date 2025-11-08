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
              From 2022 to 2025, I worked to create a ‘family tree’ of successful opera singers and those who taught them. I was motivated to do this work because I frequently marveled at highly skilled classical singers and wondered who their teachers were. Like <a href="https://www.songhelix.com" target="_blank">SongHelix</a> (released in 2019), I wanted to make another useful tool that allowed for deep and broad insights. I hoped to create a tool that would allow singers, fans of classical singing, and scholars a simple way to discover teacher-singer lineage. I have pulled data from a variety of online and print sources. Each of those can be investigated on examination of any piece of data on the site. Note that when Wikipedia is cited, it can refer to any language version of the student or teacher's Wikipedia site. Frequently another language's version will have different information from the English version.
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
          <summary style={{ cursor: 'pointer', fontWeight: 600, color: '#1d4ed8', fontSize: '18px' }}>Color key</summary>
          <ul
            style={{
              listStyle: 'none',
              margin: '12px 0 0 0',
              padding: 0,
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
              gap: '10px'
            }}
          >
            {[
              { label: 'Soprano', color: '#ae996b' },
              { label: 'Mezzo-soprano', color: '#695531' },
              { label: 'Contralto', color: '#443f39' },
              { label: 'Countertenor', color: '#4e2d06' },
              { label: 'Tenor', color: '#e4a201' },
              { label: 'Baritone', color: '#6a7304' },
              { label: 'Bass-baritone', color: '#a09602' },
              { label: 'Bass', color: '#a09602' },
              { label: 'Castrato', color: '#99c0e3' },
              { label: 'Soprano castrato', color: '#99c0e3' },
              { label: 'Alto castrato', color: '#99c0e3' },
              { label: 'Haute-contre', color: '#99c0e3' },
              { label: 'Treble, unchanged voice', color: '#99c0e3' },
              { label: 'Composer', color: '#7c8b23' },
              { label: 'Conductor', color: '#7c8b23' },
              { label: 'Instrumentalist', color: '#7c8b23' },
              { label: 'Opera director', color: '#7c8b23' },
              { label: 'Teacher, other', color: '#7c8b23' },
              { label: 'Vocal coach', color: '#7c8b23' },
              { label: 'Speech Language Pathologist', color: '#7c8b23' },
              { label: 'Librettist', color: '#7c8b23' },
              { label: 'Critic', color: '#7c8b23' },
              { label: 'Actor', color: '#7c8b23' },
              { label: 'Inventor', color: '#7c8b23' },
              { label: 'Non-singing', color: '#7c8b23' },
              { label: 'Unknown', color: '#7c8b23' }
            ].map((entry) => (
              <li key={entry.label} style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                <span
                  style={{
                    width: 16,
                    height: 16,
                    borderRadius: 4,
                    border: '2px solid #3e96e2',
                    backgroundColor: entry.color,
                    flex: '0 0 auto'
                  }}
                />
                <span style={{ color: '#1f2937' }}>{entry.label}</span>
              </li>
            ))}
          </ul>
        </details>

        <details style={{ backgroundColor: '#f6faff', border: '2px solid #cbdaf7', borderRadius: '12px', padding: '16px 20px' }}>
          <summary style={{ cursor: 'pointer', fontWeight: 600, color: '#1d4ed8', fontSize: '18px' }}>Disclaimer</summary>
          <p style={{ marginTop: '12px', color: '#374151' }}>
            Welcome to The Aspen Grove of Opera Singers, and thank you for your interest in this project.
          </p>
          <p style={{ marginTop: '12px', color: '#374151' }}>
            I have spent the last three summers and this current sabbatical collecting information for this database. I have endeavored to include "successful" opera singers and their teachers. Success can, of course, be defined many ways. For the purposes of this tool, I have chosen to include singers who have sung roles at A- and B-level houses and their equivalents, singers who are managed, and singers who have been documented in reference books and websites specializing in classical singing and teaching history. Though the information is vast (15,000 singers, 6,100 relationships), it is far from exhaustive. I can make no claims about the quality of the teaching or of the quality of the relationship between teacher and student. Further, there is no guarantee that any teacher's methods carry forward to their students or the next generation, or to those that follow.
          </p>
          <p style={{ marginTop: '12px', color: '#374151' }}>
            If you would like some or all of your personal information to be removed from the dataset for any reason, please <a href="mailto:classicalsinginghumanitieslab@gmail.com">let me know</a>. I will happily remove anyone's information. If you have a correction from a credible source, I will happily incorporate that too. If you have information to add that meets the criteria described above, please fill out <a href="https://forms.gle/TZmuaPpMUu9ob4jT8" target="_blank" rel="noopener noreferrer">this form</a>, and I will incorporate it as quickly as I can.
          </p>
          <p style={{ marginTop: '12px', color: '#374151', fontWeight: 600 }}>
            By reviewing this site you acknowledge the extent of the current contents and the limitations expressed in this disclaimer.
          </p>
        </details>

        <section>
          <h3 style={{ margin: '0 0 18px 0', fontSize: '22px', color: '#0f172a' }}>Videos</h3>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '16px' }}>
            {[
              { title: 'Overview', src: 'https://www.youtube.com/embed/7YsPYQHkVD8' },
              { title: 'Right Click Features', src: 'https://www.youtube.com/embed/7s90T9xYRdI' },
              { title: 'Path and Filter', src: 'https://www.youtube.com/embed/yCaz35oLSbg' },
              { title: 'Save and Export', src: 'https://www.youtube.com/embed/TN6TJmkXrsY' }
            ].map((vid, idx) => (
              <div key={idx} style={{ display: 'flex', flexDirection: 'column' }}>
                <h4 style={{ margin: '0 0 8px 0', textAlign: 'center', fontSize: '18px', color: '#0f172a' }}>{vid.title}</h4>
                <div style={{
                  backgroundColor: '#000',
                  border: '2px solid #3e96e2',
                  borderRadius: '12px',
                  overflow: 'hidden',
                  aspectRatio: '16 / 9'
                }}>
                  <iframe
                    title={vid.title}
                    src={`${vid.src}?rel=0`}
                    allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                    allowFullScreen
                    style={{ width: '100%', height: '100%', border: 'none', display: 'block' }}
                  />
                </div>
              </div>
            ))}
          </div>
        </section>

        <section>
          <h3 style={{ margin: '0 0 12px 0', fontSize: '22px', color: '#0f172a' }}>Acknowledgements</h3>
          <p style={{ margin: 0, color: '#374151' }}>
            This project would not have been possible without the help of a large community that surrounds me. I am grateful for my friends and colleagues at the Universtiy of Utah who entertain and support my wild ideas. I am grateful for this faculty position which allows me to choose my research agenda and work on throughout the summers. I am grateful to the Universtiy of Utah and the College of Fine Arts in granting me a sabbatical so that I have time to create this site and to recharge after the first 10 years of my work here. Thanks, too, to my friends at the Marriott Library. Thank you to those countless researchers whose work I have collected and distilled here. There would be no Aspen Grove without you. Thanks to Angie and Miles who have heard about my daily successes and failures for so long. Thank you to the supportive group of singing teachers whose work is of such an inspiringly high caliber. Finally a thanks to my first teacher, Mr. Wolfe. The reverence with which he spoke of his colossal teachers created the spark that became this project 35 years later.
          </p>
        </section>
      </div>
    </div>
  );
}
