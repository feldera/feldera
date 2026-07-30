// Ambient types for `cytoscape-svg`, which ships no declarations. The extension is a
// registration function passed to `cytoscape.use`, matching cytoscape's `Ext` shape.
declare module 'cytoscape-svg' {
    const register: import('cytoscape').Ext
    export default register
}
