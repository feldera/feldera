# Feldera Theme

Minimal package that exports the Feldera modern theme CSS for reuse across multiple projects.

## Installation

In a workspace with Bun:

```bash
bun add feldera-theme@workspace:*
```

## Usage

Import the theme CSS in your application:

```javascript
import 'feldera-theme/feldera-modern.css'
```

Or in your HTML:

```html
<link rel="stylesheet" href="node_modules/feldera-theme/feldera-modern.css">
```

## Theme Application

The theme uses the data attribute `data-theme="feldera-modern-theme"`. Apply it to your root element:

```html
<html data-theme="feldera-modern-theme">
  <!-- Your app content -->
</html>
```

Or programmatically:

```javascript
document.documentElement.setAttribute('data-theme', 'feldera-modern-theme')
```

## CSS Variables

This theme provides comprehensive CSS custom properties including:

- Color palettes (primary, secondary, tertiary, success, warning, error, surface)
- Typography settings (fonts, sizes, weights, spacing)
- Layout properties (spacing, border radius, border widths)
- Dark mode variants

All colors use the modern OKLCH color space for perceptual uniformity.
