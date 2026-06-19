# Design

## Theme

Daytime office users review traffic records on a desktop monitor under normal ambient light, so the interface uses a light, calm operational theme with high contrast and restrained color.

## Color

Use OKLCH tokens. The palette is restrained: tinted neutrals for structure, one green-blue primary accent for actions and selection, and semantic colors for risk/status.

- Background: `oklch(0.975 0.006 210)`
- Surface: `oklch(0.995 0.004 210)`
- Surface raised: `oklch(0.988 0.006 210)`
- Border: `oklch(0.89 0.012 210)`
- Text: `oklch(0.24 0.025 235)`
- Muted text: `oklch(0.48 0.025 235)`
- Primary: `oklch(0.55 0.12 185)`
- Primary soft: `oklch(0.94 0.045 185)`
- Danger: `oklch(0.55 0.18 25)`
- Warning: `oklch(0.66 0.14 75)`
- Info: `oklch(0.58 0.13 245)`

## Typography

Use system UI fonts with Chinese fallbacks: `-apple-system, BlinkMacSystemFont, "Segoe UI", "Microsoft YaHei UI", "PingFang SC", system-ui, sans-serif`.

Keep product scale compact:

- Page title: 24px, 700
- Section title: 16px, 700
- Body: 14px, 400
- Labels: 13px, 650
- Table text: 13px, 400

## Layout

Use an app shell with a top bar, left workflow rail on desktop, and single-column flow on mobile. Main content max width can expand for tables. Forms use clear bands and two-column grids where space permits. Cards are reserved for repeated summaries and contained tools, not every page section.

## Components

Buttons use one shape vocabulary, 8px radius, and stable heights. Inputs and selects share borders, focus rings, and disabled states. Tables use sticky headers, compact cells, horizontal overflow, and textual risk badges. Checklists need search, select-all affordances, and visible checked states.

## Motion

Use short 150ms to 220ms transitions for hover, focus, reveal, and loading states. Avoid page-load choreography and layout animation.
