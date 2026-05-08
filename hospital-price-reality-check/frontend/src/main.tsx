import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, HashRouter } from "react-router-dom";
import App from "./App";
import "./index.css";

const basename = (import.meta.env.BASE_URL || "/").replace(/\/$/, "") || "/";
const useHashRouter = import.meta.env.VITE_HASH_ROUTER === "true";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    {useHashRouter ? (
      <HashRouter>
        <App />
      </HashRouter>
    ) : (
      <BrowserRouter basename={basename}>
        <App />
      </BrowserRouter>
    )}
  </React.StrictMode>
);
