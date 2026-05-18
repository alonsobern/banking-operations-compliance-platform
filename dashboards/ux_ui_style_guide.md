# UX/UI Style Guide: Banking Operations & Compliance Dashboard

This style guide establishes the visual specifications, layout principles, and UX standards for the **Banking Operations & Compliance Monitoring Platform** Power BI dashboard. Following these guidelines ensures a modern fintech appearance that is executive-ready, highly readable, and cohesive.

---

## 🎨 1. Color System (Fintech Palette)

To reflect a premium modern fintech app (e.g., Revolut or Wise), we utilize a clean, dark-mode color scheme. Dark themes reduce visual fatigue, increase focus, and make vibrant accent alerts stand out instantly.

### Core Canvas Colors
*   **Main Background**: Deep Slate Black (`#0b0f19`) — Used for the main canvas background.
*   **Card Containers**: Dark Charcoal (`#1e293b`) — Used for individual card containers and visual panels.
*   **Card Borders**: Muted Slate (`#334155`) — 1px solid border to give subtle container definition.

### Semantic Accent Colors
*   🟢 **Positive/Success**: Emerald Mint (`#10b981`) — Used for completed transactions, success rates, and upward growth trends.
*   🔴 **Critical/Alert**: Crimson Coral (`#f43f5e`) — Reserved exclusively for critical risk levels, compliance flags, and transaction failures.
*   🟡 **Warning/Action**: Amber Gold (`#f59e0b`) — Used for medium-risk categories, pending reviews, or near-SLA warnings.
*   🔵 **Platform Neutral**: Indigo Blue (`#6366f1`) — Used for baseline transactions, time-series volume curves, and non-critical metrics.

### Text Hierarchy Colors
*   **Primary Text**: High-Contrast White (`#ffffff`) — Used for KPI numbers, page titles, and active status text.
*   **Secondary Text**: Cool Gray (`#94a3b8`) — Used for axis labels, helper text, and category names.
*   **Muted Text**: Deep Slate Gray (`#64748b`) — Used for grid lines, unit legends, and timestamp footers.

---

## ✍️ 2. Typography & Type Scale

Consistent typography makes dashboards look cohesive and clean. **Inter** is the primary recommended font family. If unavailable, fall back to **Segoe UI** or **Segoe UI Semibold**.

| Element | Font Weight | Size | Color | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **Page Title** | Bold (700) | `24 pt` | `#ffffff` | Main dashboard page identification (Top Left). |
| **Section Header** | Semi-Bold (600) | `14 pt` | `#ffffff` | Individual card titles and category headers. |
| **KPI Value** | Bold (700) | `32 pt` | `#ffffff` | Main metric number (centered inside KPI cards). |
| **KPI Label** | Regular (400) | `9 pt` | `#94a3b8` | Small description text directly below/above the KPI value. |
| **Chart Axis Labels** | Regular (400) | `9 pt` | `#94a3b8` | X and Y axis text. |
| **Table Header** | Semi-Bold (600) | `10 pt` | `#ffffff` | Grid column headers. |
| **Table Cells** | Regular (400) | `10 pt` | `#cbd5e1` | Main data row values. |

---

## 📏 3. Spacing, Alignment, & Layout Grid

Strict alignment and consistent spacing are what distinguish amateur dashboards from professional executive-level reporting platforms.

*   **Outer Canvas Margin**: Maintain a uniform **16px** or **24px** margin around the entire page canvas. No visuals should touch the outer edges of the screen.
*   **Inter-Element Gap (Gutter)**: Enforce a strict **12px** or **16px** spacing between all card components. Use the Power BI "Align" tool to ensure cards are perfectly aligned horizontally and vertically.
*   **Card Container Rounding**: Set all container card corner radiuses to **8px** or **12px**. Avoid sharp 90-degree corners.
*   **Inner Card Padding**: Maintain **16px** of whitespace *inside* each card container before data or labels begin, ensuring visual elements do not feel cramped or run into the container border.

---

## 👁️ 4. Visual Hierarchy Best Practices

### The "F-Shape" Scanning Pattern
Place the most critical information in the top-left and top rows, as users naturally scan screens from top-to-bottom and left-to-right.

1.  **Top Left**: Brand logo and Page Title.
2.  **Top Row**: Row of 4 to 5 core KPI cards showing current high-level status.
3.  **Center-Left (Largest Visual)**: Key historical trend or distribution chart.
4.  **Center-Right**: Regional or category breakdown.
5.  **Bottom / Right Sidebar**: Watchlists, details tables, or interactive filter panes.

### White Space as a Visual Tool
Do not attempt to fill every pixel of the canvas. Whitespace guides the user's eyes to what is important and prevents cognitive overload. If a page feels too crowded, move secondary metrics to a drill-through page.

### Custom Tooltips for Clean Presentation
Use Power BI's custom tooltip pages to display secondary details (e.g., historical breakdown or failure codes) when a user hovers over a chart element. This keeps the primary view exceptionally clean and uncluttered.

---
*Status: UX/UI Design Standard Approved | Ready for Dashboard Theme File (.json) Creation*
