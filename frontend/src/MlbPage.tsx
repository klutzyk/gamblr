import { useState } from "react";
import "./App.css";
import logo from "./assets/logo2.png";

type UserRegion = "au" | "us" | "uk";

export default function MlbPage() {
  const [userRegion, setUserRegion] = useState<UserRegion>("au");

  return (
    <div className="app-shell min-vh-100">
      <header className="hero-header position-relative overflow-hidden">
        <div className="hero-glow"></div>
        <div className="container position-relative">
          <nav className="navbar navbar-expand-lg navbar-dark py-4 px-0">
            <div className="brand-hero brand-hero-left">
              <img src={logo} alt="Gamblr logo" className="brand-logo brand-logo-xl" />
              <div className="brand-text-wrap">
                <h2 className="mb-0 text-white brand-title">GAMBLR</h2>
              </div>
            </div>
            <div className="ms-auto d-flex flex-column align-items-end gap-2">
              <div className="d-flex align-items-center gap-3">
                <a className="nav-link nav-link-top text-white opacity-9 px-0 py-1" href="/nba">
                  NBA
                </a>
                <span className="nav-separator">|</span>
                <a className="nav-link nav-link-top text-white opacity-9 px-0 py-1" href="/mlb">
                  MLB
                </a>
                <span className="nav-separator">|</span>
                <a className="nav-link nav-link-top text-white opacity-9 px-0 py-1" href="/performance">
                  How We&apos;re Doing
                </a>
                <span className="nav-separator">|</span>
                <a className="nav-link nav-link-top text-white opacity-9 px-0 py-1" href="#about">
                  About
                </a>
              </div>
              <div className="d-flex align-items-center gap-2">
                <label className="text-xs text-white opacity-8 mb-0" htmlFor="mlb-region-select">
                  Region
                </label>
                <select
                  id="mlb-region-select"
                  className="form-select form-select-sm region-select"
                  value={userRegion}
                  onChange={(e) => setUserRegion(e.target.value as UserRegion)}
                >
                  <option value="au">Australia</option>
                  <option value="us">USA</option>
                  <option value="uk">England</option>
                </select>
              </div>
            </div>
          </nav>

          <div className="row">
            <div className="col-lg-8 col-xl-7">
              <div className="hero-copy">
                <p className="hero-slogan mb-3">MLB</p>
                <h1 className="display-4 text-white mb-3">Baseball predictions are being built next.</h1>
                <p className="lead text-white opacity-8 mb-4">
                  The MLB section is being set up on the same platform with its own models,
                  data pipeline, and player prop markets.
                </p>
                <p className="text-sm text-white opacity-8 mb-0">
                  NBA stays live on the current site while MLB infrastructure is added separately.
                </p>
              </div>
            </div>
          </div>
        </div>
      </header>

      <main className="dashboard-section pb-5">
        <div className="container">
          <div className="section-card">
            <div className="row g-4 align-items-stretch">
              <div className="col-lg-7">
                <div className="section-header mb-4">
                  <p className="text-uppercase text-xs text-secondary fw-bold mb-2">What Comes Next</p>
                  <h3 className="mb-2">MLB will have its own markets and models.</h3>
                  <p className="text-secondary mb-0">
                    The MLB rollout will start with separate ingestion, schedule handling, and prop-specific
                    models for markets like pitcher strikeouts, batter hits, total bases, and home runs.
                  </p>
                </div>
                <div className="row g-3">
                  <div className="col-md-6">
                    <div className="card shadow-none border h-100">
                      <div className="card-body">
                        <p className="text-uppercase text-xs text-secondary fw-bold mb-2">Initial markets</p>
                        <ul className="text-sm text-secondary mb-0 ps-3">
                          <li>Pitcher strikeouts</li>
                          <li>Batter hits</li>
                          <li>Total bases</li>
                          <li>Home runs</li>
                        </ul>
                      </div>
                    </div>
                  </div>
                  <div className="col-md-6">
                    <div className="card shadow-none border h-100">
                      <div className="card-body">
                        <p className="text-uppercase text-xs text-secondary fw-bold mb-2">Platform setup</p>
                        <ul className="text-sm text-secondary mb-0 ps-3">
                          <li>Separate MLB backend modules</li>
                          <li>Separate MLB model pipeline</li>
                          <li>Same site shell and navigation</li>
                          <li>Separate MLB review and performance later</li>
                        </ul>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
              <div className="col-lg-5">
                <div className="card glass-card text-white h-100">
                  <div className="card-body d-flex flex-column justify-content-between">
                    <div>
                      <p className="text-uppercase text-xs text-white-50 fw-bold mb-2">Current status</p>
                      <h4 className="text-white mb-3">MLB page is live as a placeholder.</h4>
                      <p className="text-white-50 mb-0">
                        The site routing is now ready for separate sport pages. MLB data, models, and predictions
                        still need to be added.
                      </p>
                    </div>
                    <div className="mt-4">
                      <a className="btn btn-outline-white btn-sm mb-0" href="/nba">
                        Go to NBA
                      </a>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div id="about" className="mt-5">
              <div className="section-header mb-3">
                <p className="text-uppercase text-xs text-secondary fw-bold mb-2">About</p>
                <h3 className="mb-2">One platform, separate sports.</h3>
                <p className="text-secondary mb-0">
                  NBA remains fully live while MLB is added as a separate sport surface on the same site and backend.
                </p>
              </div>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
