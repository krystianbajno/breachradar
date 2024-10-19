'use client'
import {
  InstantSearch,
  SearchBox,
  Stats,
  Configure,
  useInstantSearch,
  useSearchBox
} from 'react-instantsearch'
import Client from '@searchkit/instantsearch-client'
import { useState } from 'react'

const searchClient = Client({
  url: '/api/search'
})

const downloadContent = (filename, content) => {
  const blob = new Blob([content], { type: 'text/plain' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
};

const HitView = ({ hit, detailsOpen }) => {
  const handleDownload = () => {
    const filename = `${hit.title.replace(/\s+/g, '_')}_content.txt`;
    downloadContent(filename, hit.filteredContent);
  };

  return (
    <div className="hit__details">
      <details open={detailsOpen}>
        <summary className="hit-details-panel">
          <div>
            <h3>{hit.title} - Part {hit.chunk_number}</h3>
            <p className={'hash'}>{hit.hash}</p>
          </div>
          <button className="download-btn" onClick={handleDownload}>Download</button>
        </summary>
        <pre>{hit.filteredContent}</pre>
      </details>
    </div>
  );
};

const CustomHits = ({ detailsOpen }) => {
  const { query } = useSearchBox();
  const { results } = useInstantSearch();

  if (results.nbHits === 0) {
    return <p>No results found.</p>;
  }

  const processedHits = results.hits
    .map(hit => {
      const filteredContent = hit.content
        .split("\r\n")
        .filter(line => line.includes(query))
        .join("\n");
      return { ...hit, filteredContent };
    })
    .filter(hit => hit.filteredContent.trim());

  if (processedHits.length === 0) {
    return <p>No relevant content found.</p>;
  }

  const downloadAllHits = () => {
    const allContent = processedHits
      .map(hit => `Title: ${hit.title} - Part ${hit.chunk_number}\n\n${hit.filteredContent}`)
      .join('\n\n-----\n\n');
    downloadContent('all_hits_content.txt', allContent);
  };

  return (
    <div>
      <div className="main-hits-panel">
        <button className="download-btn" onClick={downloadAllHits}>Download All</button>
      </div>
      {processedHits.map(hit => (
        <HitView key={hit.objectID} hit={hit} detailsOpen={detailsOpen} />
      ))}
    </div>
  );
};

const SearchStatus = () => {
  const { status } = useInstantSearch();
  return status === 'loading' ? <p>Loading...</p> : null;
};

export default function Web() {
  const [detailsOpen, setDetailsOpen] = useState(false);

  const toggleDetails = () => setDetailsOpen(prev => !prev);

  return (
    <div className="container">
      <a className={"no-a"} href="/"><h1>Breach<span className={"radar"}>Radar</span></h1></a>
      <div className={"version"}>v0.0.1 Sigma Edition</div>

      <InstantSearch indexName="scrapes_chunks" searchClient={searchClient} routing>
        <div className="search-panel">
          <div className="search-panel__filters">
            <div className="searchbox">
              <SearchBox placeholder="Search in content, hash" searchAsYouType={false} />
              <SearchStatus />
              <span
                className="radar-emoji"
                onClick={toggleDetails}
                style={{ cursor: 'pointer' }}
              >
                📡
              </span>
            </div>
            <div className="stats">
              <Stats />
            </div>
          </div>

          <div className="search-panel__results">
            <Configure hitsPerPage={9999} />
            <CustomHits detailsOpen={detailsOpen} />
          </div>
        </div>
      </InstantSearch>
    </div>
  );
}
