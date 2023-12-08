// declare module '*.svg' {
//   const ReactComponent: React.FC<React.SVGProps<SVGSVGElement>>;
//   const content: string;

//   export { ReactComponent };
//   export default content;
// }

/**
 * @see https://duncanleung.com/next-js-typescript-svg-any-module-declaration/
 */
export type SVGImport = React.FC<React.SVGProps<SVGSVGElement>>
