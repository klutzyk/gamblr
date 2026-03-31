import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import '../material-kit-master/assets/css/material-kit.css'
import './index.css'
import App from './App.tsx'
import AdminPage from './AdminPage.tsx'

const cleanPath = window.location.pathname.replace(/\/+$/, "") || "/";
const isAdminRoute = cleanPath === "/admin";

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    {isAdminRoute ? <AdminPage /> : <App />}
  </StrictMode>,
)
