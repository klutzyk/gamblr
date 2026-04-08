import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import '../material-kit-master/assets/css/material-kit.css'
import './index.css'
import App from './App.tsx'
import AdminPage from './AdminPage.tsx'
import PerformancePage from './PerformancePage.tsx'

const cleanPath = window.location.pathname.replace(/\/+$/, "") || "/";
const isAdminRoute = cleanPath === "/admin";
const isPerformanceRoute = cleanPath === "/performance";

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    {isAdminRoute ? <AdminPage /> : isPerformanceRoute ? <PerformancePage /> : <App />}
  </StrictMode>,
)
