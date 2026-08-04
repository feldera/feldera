import React, {type ComponentProps, type ReactNode} from "react";
import MDXComponents from "@theme-original/MDXComponents";
import LiteYouTubeEmbed from "react-lite-youtube-embed";
import "react-lite-youtube-embed/dist/LiteYouTubeEmbed.css";

// A `<table>` with its own overflow-x can size its internal row/column
// layout independently of its specified width, leaving narrow tables with
// dead space inside a full-width border. Wrapping in a dedicated scroll
// container keeps normal table layout (columns stretch to fill) while still
// scrolling horizontally when content genuinely can't fit.
function Table(props: ComponentProps<"table">): ReactNode {
  return (
    <div className="table-scroll-container">
      <table {...props} />
    </div>
  );
}

export default {
  // Re-use the default mapping
  ...MDXComponents,
  LiteYouTubeEmbed,
  table: Table,
};
