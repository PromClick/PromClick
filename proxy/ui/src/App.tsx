import { BrowserRouter, Routes, Route, NavLink, Navigate, Link } from 'react-router-dom'
import GraphPage from './pages/GraphPage'
import TSDBStatusPage from './pages/TSDBStatusPage'
import StatusPage from './pages/StatusPage'

function NavBar() {
  return (
    <nav className="navbar">
      <Link to="/graph" className="navbar-brand">
        <span className="logo-icon">&#9654;</span>
        <span className="brand-text">PromQL-CH</span>
      </Link>
      <div className="navbar-links">
        <NavLink to="/graph" className={({ isActive }) => isActive ? 'active' : ''}>
          Query
        </NavLink>
        <NavLink to="/tsdb-status" className={({ isActive }) => isActive ? 'active' : ''}>
          TSDB Status
        </NavLink>
        <div className="nav-dropdown">
          <NavLink
            to="/status?tab=runtime"
            className={({ isActive }) => `nav-dropdown-trigger ${isActive ? 'active' : ''}`}
          >
            Status &#9662;
          </NavLink>
          <div className="nav-dropdown-menu">
            <Link to="/status?tab=runtime">Runtime</Link>
            <Link to="/status?tab=config">Config</Link>
            <Link to="/status?tab=flags">Flags</Link>
          </div>
        </div>
      </div>
    </nav>
  )
}

export default function App() {
  return (
    <BrowserRouter>
      <NavBar />
      <main className="main-content">
        <Routes>
          <Route path="/" element={<Navigate to="/graph" replace />} />
          <Route path="/graph" element={<GraphPage />} />
          <Route path="/tsdb-status" element={<TSDBStatusPage />} />
          <Route path="/status" element={<StatusPage />} />
        </Routes>
      </main>
    </BrowserRouter>
  )
}
