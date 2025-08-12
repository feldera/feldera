# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the Feldera documentation website.

## Key Development Commands

### Package Management

This project uses **Yarn** as the package manager.

```bash
# Install dependencies
yarn

# Start development server
yarn start

# Build static site
yarn build

# Serve built site locally
yarn serve

# Clear Docusaurus cache
yarn clear

# Type check
yarn typecheck
```

## Architecture Overview

### Technology Stack

- **Static Site Generator**: Docusaurus 3.8+ for modern documentation sites
- **Content**: MDX (Markdown + React) for interactive documentation
- **API Docs**: OpenAPI integration with docusaurus-preset-openapi
- **Analytics**: PostHog integration for user analytics
- **Styling**: Custom CSS with Docusaurus theming
- **Deployment**: Automatic deployment to docs.feldera.com

### Project Structure

#### Core Directories

- `docs/` - Main documentation content in MDX format
- `src/` - React components and custom styling
- `static/` - Static assets (images, PDFs, Python docs)
- `openapi/` - OpenAPI specification for REST API docs
- `build/` - Generated static site output

#### Content Organization

- `docs/get-started/` - Installation and quickstart guides
- `docs/sql/` - SQL reference and examples
- `docs/connectors/` - Data source and sink connectors
- `docs/tutorials/` - Step-by-step tutorials
- `docs/use_cases/` - Real-world use case examples
- `docs/interface/` - Web console and CLI documentation

## Important Implementation Details

### Documentation Features

- **Interactive Examples**: Sandbox integration for live SQL demos
- **API Documentation**: Auto-generated from OpenAPI specification
- **Python SDK Docs**: Generated Python documentation integration
- **Multi-format Content**: Support for diagrams, videos, and interactive components

### Custom Components

- **Sandbox Button**: Direct integration with Feldera sandbox for trying examples
- **Code Blocks**: Enhanced code blocks with copy functionality
- **Custom Styling**: Branded theme with Feldera colors and typography

### Build Process

The build process includes:
1. Copy OpenAPI specification from parent directory
2. Generate static site with Docusaurus
3. Include Python documentation from `/static/python/`
4. Process MDX components and custom React components

### Deployment

- **Automatic Deployment**: Configured to deploy automatically on releases
- **Domain**: Deploys to docs.feldera.com
- **CDN**: Static hosting with global CDN distribution

## Development Workflow

### For Content Changes

1. Edit MDX files in `docs/` directory
2. Start development server with `yarn start`
3. Preview changes in browser (auto-reload enabled)
4. Test build with `yarn build`
5. Commit changes for automatic deployment

### For Component Changes

1. Modify React components in `src/` directory
2. Update styling in `src/css/custom.css`
3. Test components in development mode
4. Ensure responsive design works across devices

### Testing Strategy

- **Local Development**: Live reload for content iteration
- **Build Validation**: Ensure clean builds without errors
- **Link Checking**: Docusaurus validates internal links
- **Cross-browser**: Test across different browsers and devices

### Content Management

#### Writing Guidelines
- Use MDX for interactive content combining Markdown and React
- Include code examples with syntax highlighting
- Add sandbox buttons for executable SQL examples
- Maintain consistent structure and navigation

#### Asset Management
- Store images and diagrams in appropriate `docs/` subdirectories
- Use descriptive filenames for assets
- Optimize images for web delivery
- Include alt text for accessibility

### Configuration Files

- `docusaurus.config.ts` - Main Docusaurus configuration
- `sidebars.ts` - Navigation sidebar configuration
- `package.json` - Dependencies and build scripts
- `tsconfig.json` - TypeScript configuration

### Dependencies

#### Core Dependencies
- `@docusaurus/core` - Static site generator core
- `@docusaurus/preset-classic` - Standard documentation features
- `docusaurus-preset-openapi` - API documentation integration
- `@mdx-js/react` - MDX component support

#### Analytics and Plugins
- `posthog-docusaurus` - User analytics integration
- `docusaurus-plugin-hubspot` - Marketing integration
- `react-lite-youtube-embed` - Embedded video support

### Best Practices

#### Content Creation
- **Clear Structure**: Organize content logically with proper headings
- **Interactive Examples**: Include runnable code examples where possible
- **Visual Aids**: Use diagrams and screenshots to illustrate concepts
- **Cross-references**: Link related content appropriately

#### Performance
- **Optimized Images**: Compress images and use appropriate formats
- **Lazy Loading**: Leverage Docusaurus lazy loading for better performance
- **Bundle Size**: Monitor JavaScript bundle size for fast loading

#### SEO and Accessibility
- **Meta Tags**: Proper meta descriptions and titles
- **Alt Text**: Include alt text for all images
- **Semantic HTML**: Use proper heading hierarchy
- **Mobile Friendly**: Ensure responsive design

This documentation site serves as the primary resource for Feldera users, providing comprehensive guides, tutorials, and API reference materials.