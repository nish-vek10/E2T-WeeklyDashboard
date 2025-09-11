(function () {
  try {
    const qp = new URLSearchParams(window.location.search);
    if (qp.get('viewport') === 'mobile') {
      document.documentElement.classList.add('force-mobile');
    }
  } catch (e) {}
})();

import React from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import './responsive.css';
import App from "./App";

const container = document.getElementById("root");
const root = createRoot(container);
root.render(<App />);
